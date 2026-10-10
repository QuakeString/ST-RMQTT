use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::local::LinkBuilder;
use crate::protocol::{ConnAck, Connect, ConnectReturnCode, Login, Packet, Protocol};
use crate::router::{Event, Notification};
use crate::{AuthOutcome, ConnToken, ConnectionId, ConnectionSettings};

use flume::{RecvError, SendError, Sender, TrySendError};
use std::cmp::min;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::time::error::Elapsed;
use tokio::{select, time};
use tracing::{trace, Span};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Zero keep alive")]
    ZeroKeepAlive,
    #[error("Not connect packet")]
    NotConnectPacket(Packet),
    #[error("Network {0}")]
    Network(#[from] network::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Got new session, disconnecting old one")]
    SessionEnd,
    #[error("Persistent session requires valid client id")]
    InvalidClientId,
    #[error("Client id contains a forbidden character: {0}")]
    ForbiddenClientId(String),
    #[error("Auth handler returned an invalid namespace: {0:?}")]
    InvalidNamespace(String),
    #[error("Unexpected router message")]
    NotConnectionAck,
    #[error("ConnAck error {0}")]
    ConnectionAck(String),
    #[error("Authentication error")]
    InvalidAuth,
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
}

/// Orchestrates between Router and Network.
pub struct RemoteLink<P> {
    connect: Connect,
    pub(crate) connection_id: ConnectionId,
    network: Network<P>,
    link_tx: LinkTx,
    link_rx: LinkRx,
    notifications: VecDeque<Notification>,
    pub(crate) will_delay_interval: u32,
}

impl<P: Protocol> RemoteLink<P> {
    pub async fn new(
        router_tx: Sender<(ConnectionId, Event)>,
        tenant_id: Option<String>,
        mut network: Network<P>,
        connect_packet: Packet,
        dynamic_filters: bool,
        assigned_client_id: Option<String>,
        router_client_id: String,
    ) -> Result<RemoteLink<P>, Error> {
        let Packet::Connect(connect, props, lastwill, lastwill_props, _) = connect_packet else {
            return Err(Error::NotConnectPacket(connect_packet));
        };

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = assigned_client_id.as_ref().unwrap_or(&connect.client_id);
        let clean_session = connect.clean_session;

        let topic_alias_max = props.as_ref().and_then(|p| p.topic_alias_max);
        let session_expiry = props
            .as_ref()
            .and_then(|p| p.session_expiry_interval)
            .unwrap_or(0);

        let delay_interval = lastwill_props
            .as_ref()
            .and_then(|f| f.delay_interval)
            .unwrap_or(0);

        // The Server delays publishing the Client’s Will Message until
        // the Will Delay Interval has passed or the Session ends, whichever happens first
        let will_delay_interval = min(session_expiry, delay_interval);

        let (link_tx, link_rx, notification) = LinkBuilder::new(client_id, router_tx)
            .tenant_id(tenant_id)
            .router_client_id(Some(router_client_id))
            .clean_session(clean_session)
            .last_will(lastwill)
            .last_will_properties(lastwill_props)
            .dynamic_filters(dynamic_filters)
            .assigned_client_id(assigned_client_id.is_some())
            .topic_alias_max(topic_alias_max.unwrap_or(0))
            .build()?;

        let id = link_rx.id();
        Span::current().record("connection_id", id);

        if let Some(mut packet) = notification.into() {
            if let Packet::ConnAck(_ack, props) = &mut packet {
                let mut new_props = props.clone().unwrap_or_default();
                new_props.assigned_client_identifier = assigned_client_id;
                *props = Some(new_props);
                network.write(packet).await?;
            }
        }

        Ok(RemoteLink {
            connect,
            connection_id: id,
            network,
            link_tx,
            link_rx,
            notifications: VecDeque::with_capacity(100),
            will_delay_interval,
        })
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.read() => {
                    let packet = o?;
                    let len = {
                        let mut buffer = self.link_tx.buffer();
                        buffer.push_back(packet);
                        self.network.readv(&mut buffer)?;
                        buffer.len()
                    };

                    trace!("Packets read from network, count = {}", len);
                    self.link_tx.notify().await?;
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                o = self.link_rx.exchange(&mut self.notifications) => {
                    o?;
                    let mut packets = VecDeque::new();
                    let mut unscheduled = false;

                    for notif in self.notifications.drain(..) {
                        if let Some(packet) = notif.into() {
                            packets.push_back(packet);
                        } else {
                            unscheduled = true;
                        }

                    }
                    self.network.writev(packets).await?;
                    if unscheduled {
                        self.link_rx.wake().await?;
                    }
                }
            }
        }
    }
}

/// Read MQTT connect packet from network and verify it.
/// Note: Authentication is NOT done here - call handle_auth_with_client_id after client_id is finalized.
pub async fn mqtt_connect<P>(
    config: Arc<ConnectionSettings>,
    network: &mut Network<P>,
) -> Result<Packet, Error>
where
    P: Protocol,
{
    // Wait for MQTT connect packet and error out if it's not received in time to prevent
    // DOS attacks by filling total connections that the server can handle with idle open
    // connections which results in server rejecting new connections
    let connection_timeout_ms = config.connection_timeout_ms.into();
    let packet = time::timeout(Duration::from_millis(connection_timeout_ms), async {
        let packet = network.read().await?;
        Ok::<_, network::Error>(packet)
    })
    .await??;

    let (connect, _props, _login) = match packet {
        Packet::Connect(ref connect, ref props, _, _, ref login) => (connect, props, login),
        packet => return Err(Error::NotConnectPacket(packet)),
    };

    Span::current().record("client_id", &connect.client_id);

    // NOTE: Authentication is now done in remote() after client_id is finalized
    // This ensures the auth callback receives the correct (possibly generated) client_id

    // When keep_alive feature is disabled client can live forever, which is not good in
    // distributed broker context so currenlty we don't allow it.
    if connect.keep_alive == 0 {
        return Err(Error::ZeroKeepAlive);
    }

    // WORKAROUND: ThingsBoard Gateway connects with empty client_id and clean_session=false
    // This is technically invalid per MQTT spec, but real brokers allow it and auto-generate a client ID
    // Commenting out this validation to allow ThingsBoard Gateway to connect
    // let empty_client_id = connect.client_id.is_empty();
    // let clean_session = connect.clean_session;

    // if empty_client_id && !clean_session {
    //     let ack = ConnAck {
    //         session_present: false,
    //         code: ConnectReturnCode::ClientIdentifierNotValid,
    //     };

    //     let packet = Packet::ConnAck(ack, None);
    //     network.write(packet).await?;

    //     return Err(Error::InvalidClientId);
    // }

    // Ok((connect, props, lastwill, lastwill_props))
    Ok(packet)
}

/// Authenticate the connection with the finalized client_id.
/// This should be called after client_id is generated (if needed).
///
/// On success returns the namespace the external auth handler scoped the
/// connection to (see [`AuthOutcome::Accept`]), or `None` to register the
/// connection under `final_client_id` unchanged.
pub async fn handle_auth_with_client_id(
    config: Arc<ConnectionSettings>,
    connect_packet: &Packet,
    final_client_id: &str,
    cn: Option<String>,
    conn_token: ConnToken,
) -> Result<Option<String>, Error> {
    // The router refuses these ids when the connection registers, but that is
    // after the auth handler has run and the embedder has recorded a session
    // for the id. Refuse them first, so an id the router will never accept
    // never reaches the auth handler.
    if crate::router::validate_clientid(final_client_id).is_err() {
        return Err(Error::ForbiddenClientId(final_client_id.to_owned()));
    }
    let login = match connect_packet {
        Packet::Connect(_, _, _, _, ref login) => login.as_ref(),
        _ => return Ok(None),
    };
    handle_auth(config, login, final_client_id, cn, conn_token).await
}

async fn handle_auth(
    config: Arc<ConnectionSettings>,
    login: Option<&Login>,
    client_id: &str,
    cn: Option<String>,
    conn_token: ConnToken,
) -> Result<Option<String>, Error> {
    if config.auth.is_none() && config.external_auth.is_none() {
        return Ok(None);
    }

    // if authentication is configured and connect packet doesn't have login details
    // return an error
    let Some(login) = login else {
        return Err(Error::InvalidAuth);
    };

    let username = &login.username;
    let password = &login.password;

    if let Some(auth) = &config.external_auth {
        let outcome = auth(
            client_id.to_owned(),
            username.to_owned(),
            password.to_owned(),
            cn.clone(),
            conn_token,
        )
        .await;
        return match outcome {
            AuthOutcome::Reject => Err(Error::InvalidAuth),
            AuthOutcome::Accept { namespace: None } => Ok(None),
            AuthOutcome::Accept {
                namespace: Some(namespace),
            } => {
                if crate::is_valid_namespace(&namespace) {
                    Ok(Some(namespace))
                } else {
                    Err(Error::InvalidNamespace(namespace))
                }
            }
        };
    }

    if let Some(pairs) = &config.auth {
        if let Some(stored_password) = pairs.get(username) {
            if stored_password.as_bytes().ct_eq(password.as_bytes()).into() {
                return Ok(None);
            }
        }

        return Err(Error::InvalidAuth);
    }

    Err(Error::InvalidAuth)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{protocol::Login, ConnectionSettings};

    use super::handle_auth;

    fn config() -> ConnectionSettings {
        ConnectionSettings {
            connection_timeout_ms: 0,
            max_payload_size: 0,
            max_inflight_count: 0,
            auth: None,
            external_auth: None,
            dynamic_filters: false,
            disconnect_handler: None,
        }
    }

    fn login() -> Login {
        Login {
            username: "u".to_owned(),
            password: "p".to_owned(),
        }
    }

    #[tokio::test]
    async fn no_login_no_auth() {
        let cfg = Arc::new(config());
        let r = handle_auth(cfg, None, "", None, 1).await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn some_login_no_auth() {
        let cfg = Arc::new(config());
        let login = login();
        let r = handle_auth(cfg, Some(&login), "", None, 1).await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn login_matches_static_auth() {
        let login = login();
        let mut map = HashMap::<String, String>::new();
        map.insert(login.username.clone(), login.password.clone());

        let mut cfg = config();
        cfg.auth = Some(map);

        let r = handle_auth(Arc::new(cfg), Some(&login), "", None, 1).await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn login_fails_static_no_external() {
        let login = login();
        let mut map = HashMap::<String, String>::new();
        map.insert("wrong".to_owned(), "wrong".to_owned());

        let mut cfg = config();
        cfg.auth = Some(map);

        let r = handle_auth(Arc::new(cfg), Some(&login), "", None, 1).await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn login_fails_static_matches_external() {
        let login = login();

        let mut map = HashMap::<String, String>::new();
        map.insert("wrong".to_owned(), "wrong".to_owned());

        let dynamic = |_: String, _: String, _: String, _: Option<String>, _: u64| async { true };

        let mut cfg = config();
        cfg.auth = Some(map);
        cfg.set_auth_handler(dynamic);

        let r = handle_auth(Arc::new(cfg), Some(&login), "", None, 1).await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn login_fails_static_fails_external() {
        let login = login();

        let mut map = HashMap::<String, String>::new();
        map.insert("wrong".to_owned(), "wrong".to_owned());

        let dynamic = |_: String, _: String, _: String, _: Option<String>, _: u64| async { false };

        let mut cfg = config();
        cfg.auth = Some(map);
        cfg.set_auth_handler(dynamic);

        let r = handle_auth(Arc::new(cfg), Some(&login), "", None, 1).await;
        assert!(r.is_err());
    }

    fn connect_packet(client_id: &str) -> crate::protocol::Packet {
        crate::protocol::Packet::Connect(
            crate::protocol::Connect {
                keep_alive: 10,
                client_id: client_id.to_owned(),
                clean_session: true,
            },
            None,
            None,
            None,
            Some(login()),
        )
    }

    #[tokio::test]
    async fn forbidden_client_id_is_refused_before_auth_handler() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        let counting = |_: String, _: String, _: String, _: Option<String>, _: u64| async {
            CALLS.fetch_add(1, Ordering::SeqCst);
            true
        };
        let mut cfg = config();
        cfg.set_auth_handler(counting);
        let cfg = Arc::new(cfg);

        for bad in ["a/b", "a+b", "a#b", "$sys", "O.org/x.dev"] {
            let r = super::handle_auth_with_client_id(cfg.clone(), &connect_packet(bad), bad, None, 1)
                .await;
            assert!(
                matches!(r, Err(super::Error::ForbiddenClientId(ref id)) if id == bad),
                "{bad}: {r:?}"
            );
        }
        assert_eq!(CALLS.load(Ordering::SeqCst), 0, "auth handler must not run");

        // A valid id (including the generated and O.-prefixed forms) still reaches it.
        for ok in ["ESP32Client", "rumqtt-0123abcd", "O.Acme.dev-1"] {
            let r =
                super::handle_auth_with_client_id(cfg.clone(), &connect_packet(ok), ok, None, 1).await;
            assert!(r.is_ok(), "{ok}: {r:?}");
        }
        assert_eq!(CALLS.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn forbidden_client_id_is_refused_without_auth_configured() {
        let cfg = Arc::new(config());
        let r = super::handle_auth_with_client_id(cfg, &connect_packet("a/b"), "a/b", None, 1).await;
        assert!(matches!(r, Err(super::Error::ForbiddenClientId(_))));
    }

    #[tokio::test]
    async fn external_auth_clousre_or_fnptr_type_check_or_fail_compile() {
        let closure = |_: String, _: String, _: String, _: Option<String>, _: u64| async { false };
        async fn fnptr(_: String, _: String, _: String, _: Option<String>, _: u64) -> bool {
            true
        }
        async fn scoped(
            _: String,
            _: String,
            _: String,
            _: Option<String>,
            _: u64,
        ) -> crate::AuthOutcome {
            crate::AuthOutcome::Accept { namespace: None }
        }

        let mut cfg = config();
        cfg.set_auth_handler(closure);
        cfg.set_auth_handler(fnptr);
        cfg.set_auth_handler(scoped);
    }

    #[tokio::test]
    async fn external_auth_namespace_is_returned() {
        let login = login();
        let mut cfg = config();
        cfg.set_auth_handler(|cid: String, _: String, _: String, _: Option<String>, tok: u64| async move {
            crate::AuthOutcome::Accept {
                namespace: Some(format!("ns{tok}-{}", cid.len())),
            }
        });
        let r = handle_auth(Arc::new(cfg), Some(&login), "abc", None, 42).await;
        assert_eq!(r.unwrap(), Some("ns42-3".to_owned()));
    }

    #[tokio::test]
    async fn external_auth_receives_raw_client_id_and_token() {
        use std::sync::Mutex;
        static SEEN: Mutex<Option<(String, u64)>> = Mutex::new(None);
        let mut cfg = config();
        cfg.set_auth_handler(|cid: String, _: String, _: String, _: Option<String>, tok: u64| async move {
            *SEEN.lock().unwrap() = Some((cid, tok));
            crate::AuthOutcome::Accept {
                namespace: Some("dev".to_owned()),
            }
        });
        let r = super::handle_auth_with_client_id(
            Arc::new(cfg),
            &connect_packet("O.Acme.esp"),
            "O.Acme.esp",
            Some("cn".to_owned()),
            7,
        )
        .await;
        assert_eq!(r.unwrap(), Some("dev".to_owned()));
        assert_eq!(
            SEEN.lock().unwrap().clone(),
            Some(("O.Acme.esp".to_owned(), 7))
        );
    }

    #[tokio::test]
    async fn external_auth_bool_true_means_no_namespace() {
        let login = login();
        let mut cfg = config();
        cfg.set_auth_handler(|_: String, _: String, _: String, _: Option<String>, _: u64| async { true });
        let r = handle_auth(Arc::new(cfg), Some(&login), "abc", None, 1).await;
        assert_eq!(r.unwrap(), None);
    }

    #[tokio::test]
    async fn external_auth_reject_is_invalid_auth() {
        let login = login();
        let mut cfg = config();
        cfg.set_auth_handler(|_: String, _: String, _: String, _: Option<String>, _: u64| async {
            crate::AuthOutcome::Reject
        });
        let r = handle_auth(Arc::new(cfg), Some(&login), "abc", None, 1).await;
        assert!(matches!(r, Err(super::Error::InvalidAuth)));
    }

    #[tokio::test]
    async fn invalid_namespace_refuses_the_connection() {
        for bad in ["", "a.b", "a/b", "a+b", "a#b", "$a"] {
            let login = login();
            let mut cfg = config();
            let ns = bad.to_owned();
            cfg.set_auth_handler(move |_: String, _: String, _: String, _: Option<String>, _: u64| {
                let ns = ns.clone();
                async move { crate::AuthOutcome::Accept { namespace: Some(ns) } }
            });
            let r = handle_auth(Arc::new(cfg), Some(&login), "abc", None, 1).await;
            assert!(
                matches!(r, Err(super::Error::InvalidNamespace(ref n)) if n == bad),
                "{bad:?}: {r:?}"
            );
        }
    }

    #[test]
    fn scoped_client_id_joins_namespace_and_raw_id() {
        assert_eq!(crate::scoped_client_id("dev1", "ESP32Client"), "dev1.ESP32Client");
        // The O.-prefixed X.509 form keeps its prefix after the namespace.
        assert_eq!(crate::scoped_client_id("dev1", "O.Acme.x"), "dev1.O.Acme.x");
        // A scoped id is always a valid router client id.
        assert!(crate::router::validate_clientid(&crate::scoped_client_id("dev1", "x")).is_ok());
        assert!(crate::is_valid_namespace("0b1e7c1c-3f7e-4a3b-9a7e-1d2c3b4a5f6e"));
        assert!(crate::is_valid_namespace("prov17"));
    }
}
