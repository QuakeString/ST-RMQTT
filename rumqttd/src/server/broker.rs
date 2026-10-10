use crate::link::alerts::{self};
use crate::link::console::ConsoleLink;
use crate::link::network::{self, Network, N};
use crate::link::remote::{self, handle_auth_with_client_id, mqtt_connect, RemoteLink};
use crate::link::{bridge, timer};
use crate::local::LinkBuilder;
use crate::protocol::v4::V4;
use crate::protocol::v5::V5;
use crate::protocol::unified::UnifiedProtocol;
use crate::protocol::{Packet, Protocol};
#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use crate::server::tls::{self, TLSAcceptor};
use crate::{meters, scoped_client_id, ConnToken, ConnectionSettings, DisconnectHandler, Meter};
use flume::{RecvError, SendError, Sender};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{error, field, info, warn, Instrument, Span};
use uuid::Uuid;

#[cfg(feature = "websocket")]
use async_tungstenite::tokio::accept_hdr_async;
#[cfg(feature = "websocket")]
use async_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
#[cfg(feature = "websocket")]
use async_tungstenite::tungstenite::http::HeaderValue;
#[cfg(feature = "websocket")]
use ws_stream_tungstenite::WsStream;

use metrics::gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::time::Duration;
use std::{io, thread};

use crate::link::console;
use crate::link::local::{self, LinkRx, LinkTx};
use crate::router::{Event, Notification, Router};
use crate::{Config, ConnectionId, ServerSettings};

use tokio::net::{TcpListener, TcpStream};
use tokio::time::error::Elapsed;
use tokio::{task, time};

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
    #[error("Certs error = {0}")]
    Certs(#[from] tls::Error),
    #[error("Accept error = {0}")]
    Accept(String),
    #[error("Remote error = {0}")]
    Remote(#[from] remote::Error),
    #[error("Invalid configuration")]
    Config(String),
}

pub struct Broker {
    config: Arc<Config>,
    router_tx: Sender<(ConnectionId, Event)>,
}

impl Broker {
    pub fn new(config: Config) -> Broker {
        let config = Arc::new(config);
        let router_config = config.router.clone();
        let router: Router = Router::new(config.id, router_config);

        // Setup cluster if cluster settings are configured.
        match config.cluster.clone() {
            Some(_cluster_config) => {
                // let node_id = cluster_config.node_id;
                // let listen = cluster_config.listen;
                // let seniors = cluster_config.seniors;
                // let mut cluster = RemoteCluster::new(node_id, &listen, seniors);
                // Broker::setup_remote_cluster(&mut router, node_id, &mut cluster);

                // Start router first and then cluster in the background
                let router_tx = router.spawn();
                // cluster.spawn();
                Broker { config, router_tx }
            }
            None => {
                let router_tx = router.spawn();
                Broker { config, router_tx }
            }
        }
    }

    // pub fn new_local_cluster(
    //     config: Config,
    //     node_id: NodeId,
    //     seniors: Vec<(NodeId, Sender<ReplicationData>)>,
    // ) -> (Broker, Sender<ReplicationData>) {
    //     let config = Arc::new(config);
    //     let router_config = config.router.clone();

    //     let mut router = Router::new(config.id, router_config);
    //     let (mut cluster, tx) = LocalCluster::new(node_id, seniors);
    //     Broker::setup_local_cluster(&mut router, node_id, &mut cluster);

    //     // Start router first and then cluster in the background
    //     let router_tx = router.spawn();
    //     cluster.spawn();

    //     (Broker { config, router_tx }, tx)
    // }

    // fn setup_remote_cluster(router: &mut Router, node_id: NodeId, cluster: &mut RemoteCluster) {
    //     // Retrieve pre-configured list of router <-> replica link from router
    //     // and add the link to cluster
    //     let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
    //     for node_id in nodes {
    //         let link = router.get_replica_handle(node_id);
    //         cluster.add_replica_router_handle(node_id, link);
    //     }
    // }

    // fn setup_local_cluster(router: &mut Router, node_id: NodeId, cluster: &mut LocalCluster) {
    //     // Retrieve pre-configured list of router <-> replica link from router
    //     // and add the link to cluster
    //     let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
    //     for node_id in nodes {
    //         let link = router.get_replica_handle(node_id);
    //         cluster.add_replica_router_handle(node_id, link);
    //     }
    // }

    // Link to get meters
    pub fn meters(&self) -> Result<meters::MetersLink, meters::LinkError> {
        let link = meters::MetersLink::new(self.router_tx.clone())?;
        Ok(link)
    }

    // Link to get alerts
    pub fn alerts(&self) -> Result<alerts::AlertsLink, alerts::LinkError> {
        let link = alerts::AlertsLink::new(self.router_tx.clone())?;
        Ok(link)
    }

    pub fn link(&self, client_id: &str) -> Result<(LinkTx, LinkRx), local::LinkError> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex. max connection limit).
        let (link_tx, link_rx, _ack) = LinkBuilder::new(client_id, self.router_tx.clone())
            .internal(true)
            .build()?;
        Ok((link_tx, link_rx))
    }

    /// Create a link with dynamic_filters enabled, allowing it to receive
    /// PUBLISH messages from remote clients by auto-creating subscription filters.
    pub fn link_with_dynamic_filters(
        &self,
        client_id: &str,
    ) -> Result<(LinkTx, LinkRx, Notification), local::LinkError> {
        LinkBuilder::new(client_id, self.router_tx.clone())
            .dynamic_filters(true)
            .internal(true)
            .build()
    }

    #[tracing::instrument(skip(self))]
    pub fn start(&mut self) -> Result<(), Error> {
        if self.config.v4.is_none()
            && self.config.v5.is_none()
            && self.config.unified.is_none()
            && (cfg!(not(feature = "websocket")) || self.config.ws.is_none())
        {
            return Err(Error::Config(
                "Atleast one server config must be specified, \
                consider adding either of [v4.x]/[v5.x]/[unified.x] or [ws.x] (if enabled) in config file."
                    .to_string(),
            ));
        }

        // we don't know which servers (v4/v5/ws) user will spawn
        // so we collect handles for all of the spawned servers
        let mut server_thread_handles = Vec::new();

        if let Some(metrics_config) = self.config.metrics.clone() {
            let timer_thread = thread::Builder::new().name("timer".to_owned());
            let router_tx = self.router_tx.clone();
            timer_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async move {
                    timer::start(metrics_config, router_tx).await;
                });
            })?;
        }

        // Spawn bridge in a separate thread.
        if let Some(bridge_config) = self.config.bridge.clone() {
            let bridge_thread = thread::Builder::new().name(bridge_config.name.clone());
            let router_tx = self.router_tx.clone();
            bridge_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async move {
                    if let Err(e) = bridge::start(bridge_config, router_tx, V4).await {
                        error!(error=?e, "Bridge Link error");
                    };
                });
            })?;
        }

        // Spawn servers in a separate thread.
        if let Some(v4_config) = &self.config.v4 {
            for (_, config) in v4_config.clone() {
                let server_thread = thread::Builder::new().name(config.name.clone());
                let mut server = Server::new(config, self.router_tx.clone(), V4);
                let handle = server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        if let Err(e) = server.start(LinkType::Remote).await {
                            error!(error=?e, "Server error - V4");
                        }
                    });
                })?;
                server_thread_handles.push(handle)
            }
        }

        if let Some(v5_config) = &self.config.v5 {
            for (_, config) in v5_config.clone() {
                let server_thread = thread::Builder::new().name(config.name.clone());
                let mut server = Server::new(config, self.router_tx.clone(), V5);
                let handle = server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        if let Err(e) = server.start(LinkType::Remote).await {
                            error!(error=?e, "Server error - V5");
                        }
                    });
                })?;
                server_thread_handles.push(handle)
            }
        }

        // Spawn unified servers (auto-detect protocol version) in a separate thread.
        if let Some(unified_config) = &self.config.unified {
            for (_, config) in unified_config.clone() {
                let server_thread = thread::Builder::new().name(config.name.clone());
                let mut server = Server::new(config, self.router_tx.clone(), UnifiedProtocol::new());
                let handle = server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        if let Err(e) = server.start(LinkType::Remote).await {
                            error!(error=?e, "Server error - Unified");
                        }
                    });
                })?;
                server_thread_handles.push(handle)
            }
        }

        #[cfg(not(feature = "websocket"))]
        if self.config.ws.is_some() {
            warn!("websocket feature is disabled, [ws] config will be ignored.");
        }

        #[cfg(feature = "websocket")]
        if let Some(ws_config) = &self.config.ws {
            for (_, config) in ws_config.clone() {
                let server_thread = thread::Builder::new().name(config.name.clone());
                //TODO: Add support for V5 procotol with websockets. Registered in config or on ServerSettings
                let mut server = Server::new(config, self.router_tx.clone(), V4);
                let handle = server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        if let Err(e) = server.start(LinkType::Websocket).await {
                            error!(error=?e, "Server error - WS");
                        }
                    });
                })?;
                server_thread_handles.push(handle)
            }
        }

        if let Some(prometheus_setting) = &self.config.prometheus {
            let timeout = prometheus_setting.interval;
            // If port is specified use it instead of listen.
            // NOTE: This means listen is ignored when `port` is specified.
            // `port` will be removed in future release in favour of `listen`
            let addr = {
                #[allow(deprecated)]
                match prometheus_setting.port {
                    Some(port) => SocketAddr::new("127.0.0.1".parse().unwrap(), port),
                    None => prometheus_setting.listen.unwrap_or(SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        9042,
                    )),
                }
            };
            let metrics_thread = thread::Builder::new().name("Metrics".to_owned());
            let meter_link = self.meters().unwrap();
            metrics_thread.spawn(move || {
                let builder = PrometheusBuilder::new().with_http_listener(addr);
                builder.install().unwrap();

                let total_publishes = gauge!("metrics.router.total_publishes");
                let total_connections = gauge!("metrics.router.total_connections");
                let failed_publishes = gauge!("metrics.router.failed_publishes");
                loop {
                    if let Ok(metrics) = meter_link.recv() {
                        for m in metrics {
                            match m {
                                Meter::Router(_, ref r) => {
                                    total_connections.set(r.total_connections as f64);
                                    total_publishes.set(r.total_publishes as f64);
                                    failed_publishes.set(r.failed_publishes as f64);
                                }
                                _ => continue,
                            }
                        }
                    }

                    std::thread::sleep(Duration::from_secs(timeout));
                }
            })?;
        }

        if let Some(console) = self.config.console.clone() {
            let console_link = ConsoleLink::new(console, self.router_tx.clone());

            let console_link = Arc::new(console_link);
            let console_thread = thread::Builder::new().name("Console".to_string());
            console_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();
                runtime.block_on(console::start(console_link));
            })?;
        }

        // in ideal case, where server doesn't crash, join() will never resolve
        // we still try to join threads so that we don't return from function
        // unless everything crashes.
        server_thread_handles.into_iter().for_each(|handle| {
            // join() might panic in case the thread panics
            // we just ignore it
            let _ = handle.join();
        });

        Ok(())
    }
}

#[derive(Copy, Clone)]
pub enum LinkType {
    #[cfg(feature = "websocket")]
    Websocket,
    Remote,
}

#[derive(PartialEq)]
enum AwaitingWill {
    Cancel,
    Fire,
}

pub struct Server<P> {
    config: ServerSettings,
    router_tx: Sender<(ConnectionId, Event)>,
    protocol: P,
    awaiting_will_handler: Arc<Mutex<HashMap<String, Sender<AwaitingWill>>>>,
}

impl<P: Protocol + Clone + Send + 'static> Server<P> {
    pub fn new(
        config: ServerSettings,
        router_tx: Sender<(ConnectionId, Event)>,
        protocol: P,
    ) -> Server<P> {
        Server {
            config,
            router_tx,
            protocol,
            awaiting_will_handler: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    // Depending on TLS or not create a new Network
    async fn tls_accept(
        &self,
        stream: TcpStream,
    ) -> Result<(Box<dyn N>, Option<String>, Option<String>), Error> {
        #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
        match &self.config.tls {
            Some(c) => {
                let (tenant_id, cn, network) = TLSAcceptor::new(c)?.accept(stream).await?;
                Ok((network, tenant_id, cn))
            }
            None => Ok((Box::new(stream), None, None)),
        }
        #[cfg(not(any(feature = "use-rustls", feature = "use-native-tls")))]
        Ok((Box::new(stream), None, None))
    }

    pub async fn start(&mut self, link_type: LinkType) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.listen).await?;
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count: usize = 0;

        let config = Arc::new(self.config.connections.clone());
        info!(
            config = self.config.name,
            listen_addr = self.config.listen.to_string(),
            "Listening for remote connections",
        );
        loop {
            // Await new network connection.
            let (stream, addr) = match listener.accept().await {
                Ok((s, r)) => (s, r),
                Err(e) => {
                    error!(error=?e, "Unable to accept socket.");
                    continue;
                }
            };

            let (network, tenant_id, cn) = match self.tls_accept(stream).await {
                Ok(o) => o,
                Err(e) => {
                    error!(error=?e, "Tls accept error");
                    continue;
                }
            };

            info!(
                name=?self.config.name, ?addr, count, tenant=?tenant_id, "accept"
            );

            let config = config.clone();
            let router_tx = self.router_tx.clone();
            count += 1;

            let protocol = self.protocol.clone();
            match link_type {
                #[cfg(feature = "websocket")]
                LinkType::Websocket => {
                    let stream = match accept_hdr_async(network, WSCallback).await {
                        Ok(s) => Box::new(WsStream::new(s)),
                        Err(e) => {
                            error!(error=?e, "Websocket failed handshake");
                            continue;
                        }
                    };
                    task::spawn(
                        remote(
                            config,
                            tenant_id.clone(),
                            cn.clone(),
                            router_tx,
                            stream,
                            protocol,
                            self.awaiting_will_handler.clone(),
                        )
                        .instrument(tracing::info_span!(
                            "websocket_link",
                            client_id = field::Empty,
                            connection_id = field::Empty
                        )),
                    )
                }
                LinkType::Remote => task::spawn(
                    remote(
                        config,
                        tenant_id.clone(),
                        cn.clone(),
                        router_tx,
                        network,
                        protocol,
                        self.awaiting_will_handler.clone(),
                    )
                    .instrument(tracing::error_span!(
                        "remote_link",
                        ?tenant_id,
                        client_id = field::Empty,
                        connection_id = field::Empty,
                    )),
                ),
            };

            time::sleep(delay).await;
        }
    }
}

/// Configures the Websocket connection to indicate the correct protocol
/// by adding the "sec-websocket-protocol" with value of "mqtt" to the response header
#[cfg(feature = "websocket")]
struct WSCallback;
#[cfg(feature = "websocket")]
impl Callback for WSCallback {
    fn on_request(
        self,
        _request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        response
            .headers_mut()
            .insert("sec-websocket-protocol", HeaderValue::from_static("mqtt"));
        Ok(response)
    }
}

/// Source of [`crate::ConnToken`]s: one per remote connection that reaches auth.
static NEXT_CONN_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Fires the embedder's [`DisconnectHandler`] exactly once, when dropped.
///
/// Created as soon as auth accepts a connection, so every way `remote()` can
/// end after that point — the client closing, a network error, a router kick,
/// a refused registration, or the task itself being cancelled — reports the
/// disconnect, and no path can report it twice.
struct DisconnectGuard {
    handler: Option<DisconnectHandler>,
    router_id: String,
    conn_token: ConnToken,
}

impl DisconnectGuard {
    fn new(handler: Option<DisconnectHandler>, router_id: String, conn_token: ConnToken) -> Self {
        Self {
            handler,
            router_id,
            conn_token,
        }
    }
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        let Some(handler) = self.handler.take() else {
            return;
        };
        let router_id = std::mem::take(&mut self.router_id);
        let conn_token = self.conn_token;
        // A panicking hook must not take the connection task down with it
        // (or abort the process if we are already unwinding).
        let fired = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handler(router_id, conn_token)
        }));
        if fired.is_err() {
            error!(conn_token, "Disconnect handler panicked");
        }
    }
}

/// A new network connection should wait for a mqtt connect packet. This should be handled
/// asynchronously to avoid blocking other new connections while this connection is
/// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
/// denial of service attacks (rogue clients which only establish network connections without
/// sending a mqtt connection packet to make the server reach its concurrent connection limit).
async fn remote<P: Protocol>(
    config: Arc<ConnectionSettings>,
    tenant_id: Option<String>,
    cn: Option<String>,
    router_tx: Sender<(ConnectionId, Event)>,
    stream: Box<dyn N>,
    protocol: P,
    will_handlers: Arc<Mutex<HashMap<String, Sender<AwaitingWill>>>>,
) {
    let mut network = Network::new(
        stream,
        config.max_payload_size,
        config.max_inflight_count,
        protocol,
    );

    let dynamic_filters = config.dynamic_filters;

    let connect_packet = match mqtt_connect(config.clone(), &mut network).await {
        Ok(p) => p,
        Err(e) => {
            error!(error=?e, "Error while handling MQTT connect packet");
            return;
        }
    };

    let (mut client_id, clean_session) = match &connect_packet {
        Packet::Connect(ref connect, _, _, _, _) => {
            (connect.client_id.clone(), connect.clean_session)
        }
        _ => unreachable!(),
    };

    let mut assigned_client_id = None;
    if client_id.is_empty() {
        let uuid = Uuid::new_v4().simple();
        client_id = format!("rumqtt-{uuid}");
        assigned_client_id = Some(client_id.clone());
    }

    if let Some(tenant_id) = &tenant_id {
        // client_id is set to "tenant_id.client_id"
        // this is to make sure we are consistent,
        // as Connection uses this format of client_id
        client_id = format!("{tenant_id}.{client_id}");
    }

    // Validate, then authenticate, the finalized client_id (after generation and tenant prefix)
    // This ensures the auth callback receives the correct client_id for session storage
    let conn_token = NEXT_CONN_TOKEN.fetch_add(1, Ordering::Relaxed);
    let disconnect_handler = config.disconnect_handler.clone();
    let namespace =
        match handle_auth_with_client_id(config, &connect_packet, &client_id, cn, conn_token).await
        {
            Ok(namespace) => namespace,
            Err(e) => {
                error!(error=?e, "Connect refused for client_id={}", client_id);
                return;
            }
        };

    // The id this connection is known by in the router. When the auth handler
    // scoped the connection, every piece of state keyed by client id — the
    // duplicate-id takeover, the persistent session, the will — is confined to
    // that namespace, so a different principal reusing the same raw id can
    // neither displace this connection nor inherit its state. The raw id is
    // what auth saw and what CONNACK's assigned identifier reports.
    let client_id = match namespace {
        Some(namespace) => scoped_client_id(&namespace, &client_id),
        None => client_id,
    };
    Span::current().record("client_id", &client_id);

    // From here on the embedder has accepted this connection; tell it when the
    // connection ends, on every path out of this function.
    let disconnect_guard = DisconnectGuard::new(disconnect_handler, client_id.clone(), conn_token);

    if let Some(sender) = will_handlers.lock().unwrap().remove(&client_id) {
        let awaiting_will = if clean_session {
            AwaitingWill::Fire
        } else {
            AwaitingWill::Cancel
        };
        sender.try_send(awaiting_will).unwrap();
    }

    let (will_tx, will_rx) = flume::bounded::<AwaitingWill>(1);
    will_handlers
        .lock()
        .unwrap()
        .insert(client_id.clone(), will_tx);

    // Start the link
    let mut link = match RemoteLink::new(
        router_tx.clone(),
        tenant_id.clone(),
        network,
        connect_packet,
        dynamic_filters,
        assigned_client_id,
        client_id.clone(),
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            error!(error=?e, "Remote link error");
            return;
        }
    };

    let connection_id = link.connection_id;
    let will_delay_interval = link.will_delay_interval;
    let mut send_disconnect = true;

    match link.start().await {
        // Connection got closed. This shouldn't usually happen.
        Ok(_) => error!("connection-stop"),
        // No need to send a disconnect message when disconnection
        // originated internally in the router.
        Err(remote::Error::Link(e)) => {
            error!(error=?e, "router-drop");
            send_disconnect = false;
        }
        // Connection was closed by peer
        Err(remote::Error::Network(network::Error::Io(err)) | remote::Error::Io(err))
            if err.kind() == io::ErrorKind::ConnectionAborted =>
        {
            info!(error=?err, "disconnected");
        }
        // Any other error
        Err(e) => {
            error!(error=?e, "disconnected");
        }
    };

    if send_disconnect {
        let disconnect = Event::Disconnect;
        let message = (connection_id, disconnect);
        router_tx.send(message).ok();
    }

    // this is important to stop the connection
    drop(link);

    // The connection is gone; report it now rather than after the will delay.
    drop(disconnect_guard);

    let publish_will = match tokio::time::timeout(
        Duration::from_secs(will_delay_interval as u64),
        will_rx.recv_async(),
    )
    .await
    {
        Ok(w) => w.is_ok_and(|k| k == AwaitingWill::Fire),
        Err(_) => {
            // no need to keep the sender after timeout
            will_handlers.lock().unwrap().remove(&client_id);
            // as will delay interval has passed, publish the will message
            true
        }
    };

    if publish_will {
        let message = Event::PublishWill((client_id, tenant_id));
        // is this connection_id really correct at this point?
        // as we have disconnected already, some other connection
        // might be using this connection ID!
        // It won't matter in this case as we don't use it
        // but might affect logs?
        router_tx.send((connection_id, message)).ok();
    }
}

/// The disconnect hook (`ConnectionSettings::set_disconnect_handler`) fires
/// exactly once on every way an accepted connection can end, and never for a
/// connection that auth refused.
#[cfg(test)]
mod disconnect_hook_tests {
    use super::{remote, AwaitingWill, DisconnectGuard};
    use crate::local::LinkBuilder;
    use crate::protocol::v4::V4;
    use crate::protocol::{Connect, Login, Packet, Protocol};
    use crate::router::Event;
    use crate::{AuthOutcome, ConnectionId, ConnectionSettings, Router, RouterConfig, Strategy};
    use bytes::BytesMut;
    use flume::{Receiver, Sender};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    type Fired = Receiver<(String, u64)>;

    fn router() -> Sender<(ConnectionId, Event)> {
        let config = RouterConfig {
            max_segment_size: 1024 * 1024,
            max_connections: 64,
            max_segment_count: 10,
            max_outgoing_packet_count: 1024,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Strategy::RoundRobin,
        };
        Router::new(0, config).spawn()
    }

    /// Settings whose auth accepts with `namespace` (or rejects when `accept`
    /// is false) and records each token it hands out; plus the hook's events.
    fn settings(
        namespace: Option<&str>,
        accept: bool,
    ) -> (Arc<ConnectionSettings>, Receiver<u64>, Fired) {
        let (tok_tx, tok_rx) = flume::unbounded();
        let (fired_tx, fired_rx) = flume::unbounded();
        let namespace = namespace.map(str::to_owned);
        let mut cfg = ConnectionSettings {
            connection_timeout_ms: 5000,
            max_payload_size: 1024 * 1024,
            max_inflight_count: 100,
            auth: None,
            external_auth: None,
            dynamic_filters: false,
            disconnect_handler: None,
        };
        cfg.set_auth_handler(
            move |_: String, _: String, _: String, _: Option<String>, tok: u64| {
                tok_tx.send(tok).unwrap();
                let namespace = namespace.clone();
                async move {
                    if accept {
                        AuthOutcome::Accept { namespace }
                    } else {
                        AuthOutcome::Reject
                    }
                }
            },
        );
        cfg.set_disconnect_handler(move |router_id, tok| {
            fired_tx.send((router_id, tok)).unwrap();
        });
        (Arc::new(cfg), tok_rx, fired_rx)
    }

    /// Run `remote()` on one end of an in-memory stream and send CONNECT on
    /// the other. Returns the client end and the connection task.
    async fn connect(
        cfg: &Arc<ConnectionSettings>,
        router_tx: &Sender<(ConnectionId, Event)>,
        will_handlers: &Arc<Mutex<HashMap<String, Sender<AwaitingWill>>>>,
        client_id: &str,
    ) -> (DuplexStream, tokio::task::JoinHandle<()>) {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let task = tokio::spawn(remote(
            cfg.clone(),
            None,
            None,
            router_tx.clone(),
            Box::new(server),
            V4,
            will_handlers.clone(),
        ));
        let mut buf = BytesMut::new();
        V4.write(
            Packet::Connect(
                Connect {
                    keep_alive: 60,
                    client_id: client_id.to_owned(),
                    clean_session: true,
                },
                None,
                None,
                None,
                Some(Login {
                    username: "token".to_owned(),
                    password: String::new(),
                }),
            ),
            &mut buf,
        )
        .unwrap();
        client.write_all(&buf).await.unwrap();
        (client, task)
    }

    /// Read the 4-byte v3.1.1 CONNACK and assert it accepted the connection.
    async fn expect_connack(client: &mut DuplexStream) {
        let mut ack = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut ack))
            .await
            .expect("no CONNACK")
            .unwrap();
        assert_eq!(ack, [0x20, 0x02, 0x00, 0x00]);
    }

    async fn next_fired(fired: &Fired) -> (String, u64) {
        tokio::time::timeout(Duration::from_secs(3), fired.recv_async())
            .await
            .expect("disconnect hook did not fire")
            .unwrap()
    }

    async fn assert_no_more(fired: &Fired) {
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(fired.try_recv().is_err(), "disconnect hook fired again");
    }

    async fn join(task: tokio::task::JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("remote() did not return")
            .unwrap();
    }

    #[tokio::test]
    async fn fires_once_when_the_client_closes() {
        let router_tx = router();
        let wills = Arc::new(Mutex::new(HashMap::new()));
        let (cfg, toks, fired) = settings(Some("devA"), true);

        let (mut client, task) = connect(&cfg, &router_tx, &wills, "X").await;
        expect_connack(&mut client).await;
        let tok = toks.try_recv().unwrap();
        assert!(fired.try_recv().is_err(), "fired while still connected");

        drop(client);
        assert_eq!(next_fired(&fired).await, ("devA.X".to_owned(), tok));
        join(task).await;
        assert_no_more(&fired).await;
    }

    #[tokio::test]
    async fn fires_for_the_connection_the_router_kicks() {
        // Same device, same id: the newer connection takes over (MQTT rule),
        // and the hook reports the OLDER one by its own token.
        let router_tx = router();
        let wills = Arc::new(Mutex::new(HashMap::new()));
        let (cfg, toks, fired) = settings(Some("devA"), true);

        let (mut c1, t1) = connect(&cfg, &router_tx, &wills, "X").await;
        expect_connack(&mut c1).await;
        let tok1 = toks.try_recv().unwrap();

        let (mut c2, t2) = connect(&cfg, &router_tx, &wills, "X").await;
        expect_connack(&mut c2).await;
        let tok2 = toks.try_recv().unwrap();
        assert_ne!(tok1, tok2);

        assert_eq!(next_fired(&fired).await, ("devA.X".to_owned(), tok1));
        join(t1).await;
        assert!(fired.try_recv().is_err(), "the live connection was reported");

        drop(c2);
        assert_eq!(next_fired(&fired).await, ("devA.X".to_owned(), tok2));
        join(t2).await;
        assert_no_more(&fired).await;
        drop(c1);
    }

    #[tokio::test]
    async fn fires_once_when_the_router_refuses_registration() {
        // The internal-link guard refuses a network client using the platform
        // link's id; RemoteLink::new fails after auth already accepted.
        let router_tx = router();
        let (_ptx, _prx, _ack) = LinkBuilder::new("sentient-processor", router_tx.clone())
            .internal(true)
            .build()
            .unwrap();
        let wills = Arc::new(Mutex::new(HashMap::new()));
        let (cfg, toks, fired) = settings(None, true);

        let (client, task) = connect(&cfg, &router_tx, &wills, "sentient-processor").await;
        join(task).await;
        let tok = toks.try_recv().unwrap();
        assert_eq!(next_fired(&fired).await, ("sentient-processor".to_owned(), tok));
        assert_no_more(&fired).await;
        drop(client);
    }

    #[tokio::test]
    async fn never_fires_when_auth_rejects() {
        let router_tx = router();
        let wills = Arc::new(Mutex::new(HashMap::new()));
        let (cfg, toks, fired) = settings(Some("devA"), false);

        let (client, task) = connect(&cfg, &router_tx, &wills, "X").await;
        join(task).await;
        assert!(toks.try_recv().is_ok(), "auth must have run");
        assert_no_more(&fired).await;
        drop(client);
    }

    #[tokio::test]
    async fn never_fires_for_an_id_refused_before_auth() {
        let router_tx = router();
        let wills = Arc::new(Mutex::new(HashMap::new()));
        let (cfg, toks, fired) = settings(Some("devA"), true);

        let (client, task) = connect(&cfg, &router_tx, &wills, "a/b").await;
        join(task).await;
        assert!(toks.try_recv().is_err(), "auth must not run");
        assert_no_more(&fired).await;
        drop(client);
    }

    #[test]
    fn guard_fires_once_and_contains_a_panicking_hook() {
        let count = Arc::new(Mutex::new(Vec::new()));
        let c = count.clone();
        let handler: crate::DisconnectHandler = Arc::new(move |id, tok| {
            c.lock().unwrap().push((id, tok));
        });
        drop(DisconnectGuard::new(Some(handler), "ns.x".to_owned(), 9));
        assert_eq!(*count.lock().unwrap(), vec![("ns.x".to_owned(), 9)]);

        // No handler installed: dropping is a no-op.
        drop(DisconnectGuard::new(None, "ns.x".to_owned(), 1));

        let panicking: crate::DisconnectHandler = Arc::new(|_, _| panic!("hook bug"));
        drop(DisconnectGuard::new(Some(panicking), "ns.x".to_owned(), 2));
    }
}
