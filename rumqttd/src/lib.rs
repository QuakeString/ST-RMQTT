use std::fmt;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, path::Path};

use serde::{Deserialize, Serialize};
use tracing_subscriber::{
    filter::EnvFilter,
    fmt::{
        format::{Format, Pretty},
        Layer,
    },
    layer::Layered,
    reload::Handle,
    Registry,
};

pub use link::alerts;
pub use link::local;
pub use link::meters;
pub use router::{Alert, Forward, IncomingMeter, Meter, Notification, OutgoingMeter, Router};
use segments::Storage;
pub use server::{Broker, LinkType, Server};

pub use self::router::shared_subs::Strategy;

mod link;
pub mod protocol;
mod router;
mod segments;
mod server;

pub type ConnectionId = usize;
pub type RouterId = usize;
pub type NodeId = usize;
pub type Topic = String;
pub type Filter = String;
pub type TopicId = usize;
pub type Offset = (u64, u64);
pub type Cursor = (u64, u64);

pub type ClientId = String;
pub type AuthUser = String;
pub type AuthPass = String;
/// Per-connection token handed to the external auth handler.
///
/// Unique for the life of the process (a global counter), so an embedder can
/// tell two connections that share a client id apart.
pub type ConnToken = u64;

/// What the external auth handler decided about a CONNECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthOutcome {
    /// Refuse the connection.
    Reject,
    /// Accept the connection.
    ///
    /// With `namespace: Some(ns)` the connection registers with the router as
    /// [`scoped_client_id`]`(ns, client_id)` instead of the client id it sent.
    /// Every piece of router state keyed by client id — the duplicate-id
    /// takeover, persistent sessions, wills, addressed writes — is then
    /// confined to that namespace, so two principals that pick the same client
    /// id can never displace or inherit each other. The namespace must be
    /// non-empty and contain none of `+ $ # / .`; an invalid one refuses the
    /// connection.
    ///
    /// With `namespace: None` the connection registers under its client id, as
    /// it did before namespaces existed.
    Accept { namespace: Option<String> },
}

impl From<bool> for AuthOutcome {
    /// `true` accepts without a namespace, `false` rejects.
    fn from(accepted: bool) -> Self {
        if accepted {
            AuthOutcome::Accept { namespace: None }
        } else {
            AuthOutcome::Reject
        }
    }
}

/// The id a connection accepted with `namespace` registers under in the router.
///
/// `client_id` is exactly the id the auth handler was given (after empty-id
/// generation and any tenant prefix), so an embedder can compute the same key
/// the router uses, and address writes to it, without another callback.
pub fn scoped_client_id(namespace: &str, client_id: &str) -> String {
    format!("{namespace}.{client_id}")
}

/// Whether `namespace` may be used in [`AuthOutcome::Accept`].
///
/// Refuses the topic metacharacters the router refuses in a client id, and
/// `.` so a namespace can never be confused with a namespace plus a prefix.
pub fn is_valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty() && !namespace.chars().any(|c| "+$#/.".contains(c))
}

pub type AuthHandler = Arc<
    dyn Fn(
            ClientId,
            AuthUser,
            AuthPass,
            Option<String>,
            ConnToken,
        ) -> Pin<Box<dyn std::future::Future<Output = AuthOutcome> + Send>>
        + Send
        + Sync,
>;

/// Called once when a remote connection the auth handler accepted ends.
///
/// Receives the connection's router id (the [`scoped_client_id`] when the auth
/// handler returned a namespace, else the client id it was given) and its
/// [`ConnToken`]. It runs synchronously on the connection's task, so it must
/// not block or panic: hand the event to a channel (e.g. an unbounded send)
/// and do the work elsewhere.
pub type DisconnectHandler = Arc<dyn Fn(ClientId, ConnToken) + Send + Sync>;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    pub id: usize,
    pub router: RouterConfig,
    pub v4: Option<HashMap<String, ServerSettings>>,
    pub v5: Option<HashMap<String, ServerSettings>>,
    /// Unified server settings for auto-detecting protocol version (v3.1, v3.1.1, or v5.0)
    /// on a single port. This allows all MQTT clients to connect to one port regardless
    /// of their protocol version, similar to ThingsBoard's netty-based implementation.
    pub unified: Option<HashMap<String, ServerSettings>>,
    pub ws: Option<HashMap<String, ServerSettings>>,
    pub cluster: Option<ClusterSettings>,
    pub console: Option<ConsoleSettings>,
    pub bridge: Option<BridgeConfig>,
    pub prometheus: Option<PrometheusSetting>,
    pub metrics: Option<HashMap<MetricType, MetricSettings>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrometheusSetting {
    #[deprecated(note = "Use listen instead")]
    port: Option<u16>,
    listen: Option<SocketAddr>,
    // How frequently to update metrics
    interval: u64,
}

// TODO: Change names without _ until config-rs issue is resolved
// https://github.com/mehcode/config-rs/issues/369
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TlsConfig {
    Rustls {
        capath: Option<String>,
        certpath: String,
        keypath: String,
    },
    NativeTls {
        pkcs12path: String,
        pkcs12pass: String,
    },
}

impl TlsConfig {
    // Returns true only if all of the file paths inside `TlsConfig` actually exists on file system.
    // NOTE: This doesn't verify if certificate files are in required format or not.
    pub fn validate_paths(&self) -> bool {
        match self {
            TlsConfig::Rustls {
                capath,
                certpath,
                keypath,
            } => {
                let ca = capath.is_none() || capath.as_ref().is_some_and(|v| Path::new(v).exists());

                ca && [certpath, keypath].iter().all(|v| Path::new(v).exists())
            }
            TlsConfig::NativeTls { pkcs12path, .. } => Path::new(pkcs12path).exists(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub name: String,
    pub listen: SocketAddr,
    pub tls: Option<TlsConfig>,
    pub next_connection_delay_ms: u64,
    pub connections: ConnectionSettings,
}

impl ServerSettings {
    pub fn set_auth_handler<F, O>(&mut self, auth_fn: F)
    where
        F: Fn(ClientId, AuthUser, AuthPass, Option<String>, ConnToken) -> O
            + Send
            + Sync
            + 'static,
        O: IntoFuture + 'static,
        O::Output: Into<AuthOutcome>,
        O::IntoFuture: Send,
    {
        self.connections.set_auth_handler(auth_fn)
    }

    /// See [`ConnectionSettings::set_disconnect_handler`].
    pub fn set_disconnect_handler<F>(&mut self, handler: F)
    where
        F: Fn(ClientId, ConnToken) + Send + Sync + 'static,
    {
        self.connections.set_disconnect_handler(handler)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BridgeConfig {
    pub name: String,
    pub addr: String,
    pub qos: u8,
    pub sub_path: Filter,
    pub reconnection_delay: u64,
    pub ping_delay: u64,
    pub connections: ConnectionSettings,
    #[serde(default)]
    pub transport: Transport,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub max_payload_size: usize,
    pub max_inflight_count: usize,
    pub auth: Option<HashMap<String, String>>,
    #[serde(skip)]
    pub external_auth: Option<AuthHandler>,
    #[serde(default)]
    pub dynamic_filters: bool,
    /// Fired once when a connection accepted by auth ends; see
    /// [`ConnectionSettings::set_disconnect_handler`].
    #[serde(skip)]
    pub disconnect_handler: Option<DisconnectHandler>,
}

impl ConnectionSettings {
    /// Install the external auth handler.
    ///
    /// It receives the finalized client id, the username, the password, the
    /// client certificate CN (TLS only) and the connection's [`ConnToken`],
    /// and returns anything convertible into an [`AuthOutcome`] — a `bool`
    /// for plain accept/reject, or an `AuthOutcome` to scope the connection.
    pub fn set_auth_handler<F, O>(&mut self, auth_fn: F)
    where
        F: Fn(ClientId, AuthUser, AuthPass, Option<String>, ConnToken) -> O
            + Send
            + Sync
            + 'static,
        O: IntoFuture + 'static,
        O::Output: Into<AuthOutcome>,
        O::IntoFuture: Send,
    {
        self.external_auth = Some(Arc::new(
            move |client_id, username, password, cert_cn, conn_token| {
                let auth = auth_fn(client_id, username, password, cert_cn, conn_token).into_future();
                Box::pin(async move { auth.await.into() })
            },
        ));
    }

    /// Install a hook that fires exactly once for every connection the auth
    /// step accepted, when that connection ends — whatever the cause: the
    /// client closing, a network error, the router dropping it (duplicate-id
    /// takeover), or the router refusing to register it (connection limit,
    /// internal-link guard). Connections refused before or by auth never
    /// fire it.
    ///
    /// The hook receives the router id and the [`ConnToken`] the auth handler
    /// was given, so an embedder can tell whether the connection that ended is
    /// still the one it recorded for that id (a fast reconnect may already
    /// have replaced it). It runs synchronously on the connection's task and
    /// must not block; see [`DisconnectHandler`].
    pub fn set_disconnect_handler<F>(&mut self, handler: F)
    where
        F: Fn(ClientId, ConnToken) + Send + Sync + 'static,
    {
        self.disconnect_handler = Some(Arc::new(handler));
    }
}

impl fmt::Debug for ConnectionSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionSettings")
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .field("max_payload_size", &self.max_payload_size)
            .field("max_inflight_count", &self.max_inflight_count)
            .field("auth", &self.auth)
            .field("external_auth", &self.external_auth.is_some())
            .field("dynamic_filters", &self.dynamic_filters)
            .field("disconnect_handler", &self.disconnect_handler.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// Id with which this node connects to other nodes of the mesh
    pub node_id: NodeId,
    /// Address on which this broker is listening for mesh connections
    pub listen: String,
    /// Address of clusters that this node has to initiate connection
    pub seniors: Vec<(ConnectionId, String)>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub max_connections: usize,
    pub max_outgoing_packet_count: u64,
    pub max_segment_size: usize,
    pub max_segment_count: usize,
    pub custom_segment: Option<HashMap<String, SegmentConfig>>,
    pub initialized_filters: Option<Vec<Filter>>,
    // defaults to Round Robin
    #[serde(default)]
    pub shared_subscriptions_strategy: Strategy,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SegmentConfig {
    pub max_segment_size: usize,
    pub max_segment_count: usize,
}

type ReloadHandle = Handle<EnvFilter, Layered<Layer<Registry, Pretty, Format<Pretty>>, Registry>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConsoleSettings {
    pub listen: String,
    #[serde(skip)]
    filter_handle: Option<ReloadHandle>,
}

impl ConsoleSettings {
    pub fn set_filter_reload_handle(&mut self, handle: ReloadHandle) {
        self.filter_handle.replace(handle);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub enum Transport {
    #[serde(rename = "tcp")]
    #[default]
    Tcp,
    #[serde(rename = "tls")]
    Tls {
        ca: PathBuf,
        client_auth: Option<ClientAuth>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientAuth {
    certs: PathBuf,
    key: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    Meters,
    Alerts,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetricSettings {
    push_interval: u64,
}
