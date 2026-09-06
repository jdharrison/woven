//! HTTP control plane and default development server composition.

#![deny(unsafe_code)]

pub mod admission;

use std::{net::SocketAddr, sync::Arc};

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use serde::Serialize;
use tokio::sync::mpsc;
use tower_http::trace::TraceLayer;
use woven_core::{
    AccessGrant, AuthenticatedPrincipal, AuthorizationGrants, ChannelDefinition, ChannelId,
    ChannelScope, CoordinateFrame, CoreConfig, DevAuthenticator, EntityId, NamespaceId,
    PersistenceClass, PrincipalId, RoutingPolicy, SessionId, SessionKey, SpaceDescriptor,
    SpaceEpoch, SpaceId, SpaceKey, TransportIndependentWorker, WovenCore,
};
use woven_inference_coordinator::{AiIdentityConfig, CoordinatorConfig};
use woven_inference_test_provider::DeterministicProvider;
use woven_inference_tools::{ToolRegistry, ToolRegistryError, demo as inference_demo};
use woven_transport::{UnroutedControl, WorkerHandle, spawn_worker};
use woven_transport_quic::webtransport::{
    WebTransportConfig, serve_endpoint as serve_webtransport_endpoint,
    server_endpoint as webtransport_server_endpoint,
};
use woven_transport_quic::{
    PrivateKeyDer, QuicConfig, serve_endpoint as serve_quic_endpoint,
    server_config as quic_server_config, server_endpoint,
};

/// AI dev principal/token/channel used by the bundled deterministic inference demo.
const AI_DEV_TOKEN: &str = "ai-companion-dev-token";
const AI_PRINCIPAL_ID: u64 = 2;
const AI_STATUS_CHANNEL_ID: u64 = 3;
/// Bounded capacity for client-sent inference control messages awaiting the coordinator.
const INFERENCE_INBOUND_CAPACITY: usize = 64;
/// Bounded concurrent in-flight inference requests.
const INFERENCE_QUEUE_CAPACITY: usize = 16;

/// Server runtime configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_address: SocketAddr,
    pub quic_bind_address: SocketAddr,
    pub webtransport_bind_address: SocketAddr,
    pub webtransport_path: String,
    /// Enables the optional adjacent inference plane (`WOVEN_INFERENCE_ENABLED`).
    /// Disabled by default; the relay is fully unaffected either way (ADR 0009).
    pub inference_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
            quic_bind_address: SocketAddr::from(([127, 0, 0, 1], 8081)),
            webtransport_bind_address: SocketAddr::from(([127, 0, 0, 1], 8082)),
            webtransport_path: "/webtransport".to_owned(),
            inference_enabled: false,
        }
    }
}

#[derive(Clone)]
struct AppState {
    quic_enabled: bool,
    webtransport_enabled: bool,
    webtransport_endpoint: Option<String>,
    inference_enabled: bool,
    worker: WorkerHandle,
    max_connections: usize,
    max_sessions: usize,
}

/// Build the HTTP/health/capabilities router with the given transport capabilities reported.
///
/// `webtransport_endpoint` is the relative `port/path` of the WebTransport endpoint as
/// advertised in `/v1/capabilities`; clients already connected to this host resolve it
/// against the host and scheme they used to reach the control plane. `None` when
/// WebTransport is disabled.
#[allow(clippy::too_many_arguments)]
fn router_with_transports(
    quic_enabled: bool,
    webtransport_enabled: bool,
    webtransport_endpoint: Option<String>,
    inference_enabled: bool,
    worker: WorkerHandle,
) -> Router {
    let default_core_config = CoreConfig::default();
    let state = Arc::new(AppState {
        quic_enabled,
        webtransport_enabled,
        webtransport_endpoint,
        inference_enabled,
        worker,
        max_connections: default_core_config.max_connections,
        max_sessions: default_core_config.max_sessions,
    });
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/capabilities", get(capabilities))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Serve the development composition on `config.bind_address`.
pub async fn serve(config: ServerConfig) -> Result<(), ServerError> {
    let worker = spawn_worker(TransportIndependentWorker::new(development_core()?));
    let inference_sink = if config.inference_enabled {
        let (inference_tx, _entity) = spawn_inference_coordinator(worker.clone()).await?;
        Some(inference_tx)
    } else {
        None
    };
    let quic = development_quic_endpoint(config.quic_bind_address)?;
    let quic_address = quic.local_addr()?;
    let mut quic_config = QuicConfig::new(worker.clone());
    quic_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_quic_endpoint(quic, quic_config));
    let webtransport = development_webtransport_endpoint(config.webtransport_bind_address)?;
    let webtransport_address = webtransport.local_addr()?;
    let webtransport_port = webtransport_address.port();
    let webtransport_path = config.webtransport_path.clone();
    let mut webtransport_config = WebTransportConfig::new(worker.clone());
    webtransport_config.path = Arc::from(config.webtransport_path);
    webtransport_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_webtransport_endpoint(
        webtransport,
        webtransport_config,
    ));
    let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
    let http_address = listener.local_addr()?;
    print_development_startup(
        http_address,
        quic_address,
        webtransport_address,
        config.inference_enabled,
    );
    axum::serve(
        listener,
        router_with_transports(
            true,
            true,
            Some(format!("{webtransport_port}{webtransport_path}")),
            inference_sink.is_some(),
            worker,
        ),
    )
    .await?;
    Ok(())
}

fn print_development_startup(
    http_address: SocketAddr,
    quic_address: SocketAddr,
    webtransport_address: SocketAddr,
    inference_enabled: bool,
) {
    let inference = if inference_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let binding = if http_address.ip().is_loopback()
        && quic_address.ip().is_loopback()
        && webtransport_address.ip().is_loopback()
    {
        "loopback only (not reachable from your LAN)"
    } else {
        "non-loopback development bind; do not expose this configuration"
    };
    println!(
        r"
 __        __  _____ __      __ _____ _   _
 \ \      / / | ___ |\ \    / /| ____| \ | |
  \ \ /\ / /  |     | \ \  / / |  _| |  \| |
   \ V  V /   | ___ |  \ \/ /  | |___| |\  |
    \_/\_/    |_____|   \__/   |_____|_| \_|

 ┌─ WOVEN NODE · LOCAL DEVELOPMENT ─────────────────────────────────────────────────
 │ STATUS ready · INFERENCE {inference} · BINDING {binding}
 ├─ CONNECTION ──────────────────────────────────────────────────────────────────────
 │ QUIC quic://{quic_address} · HEALTH http://{http_address}/healthz
 ├─ LOCAL ACCESS ────────────────────────────────────────────────────────────────────
 │ TOKEN dev-token (development only) · STOP Ctrl-C
 └────────────────────────────────────────────────────────────────────────────────────
"
    );
}

/// URLs for a development server started on ephemeral ports.
#[derive(Clone, Debug)]
pub struct DevServeUrls {
    /// HTTP control-plane base, e.g. `http://127.0.0.1:PORT`.
    pub http: String,
    /// Native QUIC connection URL, e.g. `quic://127.0.0.1:PORT`.
    pub quic: String,
    /// WebTransport connection URL, e.g. `wtransport://127.0.0.1:PORT/webtransport`.
    pub webtransport: String,
    /// The AI identity's `EntityId` when inference is enabled, otherwise `None`.
    pub ai_entity: Option<EntityId>,
}

/// Start the full development composition (HTTP + QUIC + WebTransport) on ephemeral ports and
/// return the connection URLs. Intended for integration tests and local tooling.
///
/// When `inference_enabled` is true, the deterministic AI demo is started and its `EntityId`
/// is returned via [`DevServeUrls::ai_entity`].
pub async fn serve_dev_ephemeral(inference_enabled: bool) -> Result<DevServeUrls, ServerError> {
    let worker = spawn_worker(TransportIndependentWorker::new(development_core()?));
    let (inference_sink, ai_entity) = if inference_enabled {
        let (tx, entity) = spawn_inference_coordinator(worker.clone()).await?;
        (Some(tx), Some(entity))
    } else {
        (None, None)
    };

    let quic = development_quic_endpoint(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let quic_address = quic.local_addr()?;
    let mut quic_config = QuicConfig::new(worker.clone());
    quic_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_quic_endpoint(quic, quic_config));

    let webtransport = development_webtransport_endpoint(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let webtransport_address = webtransport.local_addr()?;
    let mut webtransport_config = WebTransportConfig::new(worker.clone());
    webtransport_config.path = Arc::from("/webtransport");
    webtransport_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_webtransport_endpoint(
        webtransport,
        webtransport_config,
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let http_address = listener.local_addr()?;
    let webtransport_port = webtransport_address.port();
    let http_handle = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            router_with_transports(
                true,
                true,
                Some(format!("{webtransport_port}/webtransport")),
                inference_sink.is_some(),
                worker,
            ),
        )
        .await;
    });
    std::mem::drop(http_handle);

    Ok(DevServeUrls {
        http: format!("http://{http_address}"),
        quic: format!("quic://{quic_address}"),
        webtransport: format!("wtransport://127.0.0.1:{webtransport_port}/webtransport"),
        ai_entity,
    })
}

/// Registers the demo tool set, spawns the AI identity's core connection, and starts the
/// coordinator's background tasks. The deterministic fake provider is the only provider
/// wired up in this milestone (a real HTTP provider is explicitly deferred).
async fn spawn_inference_coordinator(
    worker: WorkerHandle,
) -> Result<(mpsc::Sender<UnroutedControl>, EntityId), ServerError> {
    let mut tools = ToolRegistry::new();
    tools
        .register(Arc::new(inference_demo::DiagnosticTool))
        .map_err(|error| inference_registry_error(&error))?;
    tools
        .register(Arc::new(inference_demo::StatusUpdateTool::new(
            ChannelId::new(AI_STATUS_CHANNEL_ID),
        )))
        .map_err(|error| inference_registry_error(&error))?;

    let (inference_tx, inference_rx) = mpsc::channel(INFERENCE_INBOUND_CAPACITY);
    let coordinator_config = CoordinatorConfig {
        worker,
        identity: ai_identity_config(),
        provider: Arc::new(DeterministicProvider),
        tools: Arc::new(tools),
        queue_capacity: INFERENCE_QUEUE_CAPACITY,
    };
    let (_connection, entity) =
        woven_inference_coordinator::spawn(coordinator_config, inference_rx)
            .await
            .map_err(|error| ServerError::Inference(error.to_string()))?;
    Ok((inference_tx, entity))
}

fn inference_registry_error(error: &ToolRegistryError) -> ServerError {
    ServerError::Inference(format!("{error:?}"))
}

fn ai_identity_config() -> AiIdentityConfig {
    AiIdentityConfig {
        token: AI_DEV_TOKEN.to_owned(),
        namespace: NamespaceId::new(1),
        session: SessionId::new(1),
        space: SpaceId::new(1),
        space_epoch: SpaceEpoch::new(1),
        status_channel: ChannelId::new(AI_STATUS_CHANNEL_ID),
    }
}

/// Errors returned while starting the server.
#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
    Core(woven_core::CoreError),
    QuicConfiguration(String),
    Inference(String),
}
impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<woven_core::CoreError> for ServerError {
    fn from(error: woven_core::CoreError) -> Self {
        Self::Core(error)
    }
}
impl std::fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "server I/O error: {error}"),
            Self::Core(error) => write!(formatter, "core setup error: {error:?}"),
            Self::QuicConfiguration(error) => {
                write!(formatter, "QUIC configuration error: {error}")
            }
            Self::Inference(error) => write!(formatter, "inference setup error: {error}"),
        }
    }
}
impl std::error::Error for ServerError {}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum LayerHealth {
    Active,
    Degraded,
    Disabled,
}

#[derive(Serialize)]
struct HealthLayers {
    core: LayerHealth,
    transport_worker: LayerHealth,
    http_control_plane: LayerHealth,
    quic: LayerHealth,
    webtransport: LayerHealth,
    inference: LayerHealth,
}

#[derive(Serialize)]
struct HealthResponse {
    status: LayerHealth,
    layers: HealthLayers,
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: overall_health(state.quic_enabled),
        layers: HealthLayers {
            core: LayerHealth::Active,
            transport_worker: LayerHealth::Active,
            http_control_plane: LayerHealth::Active,
            quic: layer_health(state.quic_enabled),
            webtransport: layer_health(state.webtransport_enabled),
            inference: layer_health(state.inference_enabled),
        },
    })
}

const fn layer_health(enabled: bool) -> LayerHealth {
    if enabled {
        LayerHealth::Active
    } else {
        LayerHealth::Disabled
    }
}

const fn overall_health(quic_enabled: bool) -> LayerHealth {
    if quic_enabled {
        LayerHealth::Active
    } else {
        LayerHealth::Degraded
    }
}

async fn ready(State(state): State<Arc<AppState>>) -> StatusCode {
    if state.quic_enabled {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
/// Prometheus text-exposition metrics for capacity/cost observability. Cheap: live
/// connection/session counts are read directly from core state and the rest are relaxed
/// atomic counters maintained on the hot path regardless of build profile — unlike the
/// debug-only activity log, this is meant to run in production.
async fn metrics(State(state): State<Arc<AppState>>) -> String {
    let live = state.worker.live_counts().await.unwrap_or_default();
    let counters = state.worker.metrics().snapshot();
    let mut body = String::new();

    write_gauge(
        &mut body,
        "woven_connections_active",
        "Currently connected clients.",
        live.connections_active as u64,
    );
    write_gauge(
        &mut body,
        "woven_connections_max",
        "Configured maximum concurrent connections.",
        state.max_connections as u64,
    );
    write_gauge(
        &mut body,
        "woven_sessions_active",
        "Currently provisioned sessions.",
        live.sessions_active as u64,
    );
    write_gauge(
        &mut body,
        "woven_sessions_max",
        "Configured maximum concurrent sessions.",
        state.max_sessions as u64,
    );
    write_counter(
        &mut body,
        "woven_connections_total",
        "Connections accepted since process start.",
        counters.connections_total,
    );
    write_counter(
        &mut body,
        "woven_authenticate_rejected_total",
        "Authentication attempts rejected since process start.",
        counters.authenticate_rejected_total,
    );
    write_counter(
        &mut body,
        "woven_join_rejected_total",
        "Session join attempts rejected since process start.",
        counters.join_rejected_total,
    );
    write_counter(
        &mut body,
        "woven_publishes_total",
        "Publish commands processed since process start.",
        counters.publishes_total,
    );
    write_counter(
        &mut body,
        "woven_transform_publishes_total",
        "Of woven_publishes_total, those carrying an entity transform.",
        counters.transform_publishes_total,
    );
    write_counter(
        &mut body,
        "woven_publish_bytes_received_total",
        "Publish payload bytes received since process start.",
        counters.publish_bytes_received_total,
    );
    write_counter(
        &mut body,
        "woven_publish_bytes_delivered_total",
        "Publish payload bytes fanned out to recipients since process start.",
        counters.publish_bytes_delivered_total,
    );
    write_counter(
        &mut body,
        "woven_events_delivered_total",
        "Recipient delivery attempts since process start.",
        counters.events_delivered_total,
    );
    write_counter(
        &mut body,
        "woven_queue_dropped_total",
        "Outbound messages dropped for capacity since process start.",
        counters.queue_dropped_total,
    );
    write_counter(
        &mut body,
        "woven_queue_evicted_total",
        "Outbound messages evicted or coalesced since process start.",
        counters.queue_evicted_total,
    );

    body
}

fn write_gauge(body: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(body, "# HELP {name} {help}");
    let _ = writeln!(body, "# TYPE {name} gauge");
    let _ = writeln!(body, "{name} {value}");
}

fn write_counter(body: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(body, "# HELP {name} {help}");
    let _ = writeln!(body, "# TYPE {name} counter");
    let _ = writeln!(body, "{name} {value}");
}

#[derive(Serialize)]
struct CapabilitiesResponse {
    protocol_version: u16,
    transports: Vec<&'static str>,
    features: Vec<&'static str>,
    max_frame_bytes: u32,
    max_payload_bytes: u32,
    /// Relative `port/path` of the WebTransport endpoint, resolved against the host
    /// the client already used to reach this control plane. Present only when
    /// WebTransport is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    webtransport: Option<String>,
}
async fn capabilities(State(state): State<Arc<AppState>>) -> Json<CapabilitiesResponse> {
    let mut transports = Vec::new();
    if state.quic_enabled {
        transports.push("quic");
    }
    if state.webtransport_enabled {
        transports.push("webtransport");
    }
    let mut features = Vec::new();
    if state.inference_enabled {
        features.push("inference");
    }
    Json(CapabilitiesResponse {
        protocol_version: woven_protocol::PROTOCOL_VERSION,
        transports,
        features,
        max_frame_bytes: 1_048_576,
        max_payload_bytes: 262_144,
        webtransport: state.webtransport_endpoint.clone(),
    })
}

fn development_webtransport_endpoint(
    bind_address: SocketAddr,
) -> Result<woven_transport_quic::webtransport::ServerEndpoint, ServerError> {
    let identity = wtransport::Identity::self_signed(["localhost", "127.0.0.1"])
        .map_err(|error| ServerError::QuicConfiguration(error.to_string()))?;
    webtransport_server_endpoint(bind_address, identity).map_err(ServerError::Io)
}

fn development_quic_endpoint(bind_address: SocketAddr) -> Result<quinn::Endpoint, ServerError> {
    let certificate =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .map_err(|error| ServerError::QuicConfiguration(error.to_string()))?;
    let config = quic_server_config(
        vec![certificate.cert.der().clone()],
        PrivateKeyDer::Pkcs8(certificate.key_pair.serialize_der().into()),
    )
    .map_err(|error| ServerError::QuicConfiguration(error.to_string()))?;
    server_endpoint(bind_address, config).map_err(ServerError::Io)
}

fn development_core() -> Result<WovenCore<DevAuthenticator>, woven_core::CoreError> {
    let namespace = NamespaceId::new(1);
    let session = SessionKey {
        namespace,
        session: SessionId::new(1),
    };
    let mut grants = AuthorizationGrants::new();
    grants.grant_namespace(namespace, AccessGrant::ReadWrite);
    grants.grant_session(session, AccessGrant::ReadWrite);
    for space_id in [1, 2] {
        grants.grant_space(
            SpaceKey {
                session,
                space: SpaceId::new(space_id),
            },
            AccessGrant::ReadWrite,
        );
    }
    for channel_id in [1, 2] {
        grants.grant_channel(
            ChannelScope::new(session, ChannelId::new(channel_id)),
            AccessGrant::ReadWrite,
        );
    }
    let mut ai_grants = AuthorizationGrants::new();
    ai_grants.grant_namespace(namespace, AccessGrant::ReadWrite);
    ai_grants.grant_session(session, AccessGrant::ReadWrite);
    ai_grants.grant_space(
        SpaceKey {
            session,
            space: SpaceId::new(1),
        },
        AccessGrant::ReadWrite,
    );
    ai_grants.grant_channel(
        ChannelScope::new(session, ChannelId::new(AI_STATUS_CHANNEL_ID)),
        AccessGrant::ReadWrite,
    );

    let mut authenticator = DevAuthenticator::new();
    let _ = authenticator.insert(
        "dev-token",
        AuthenticatedPrincipal::new(PrincipalId::new(1), grants),
    );
    let _ = authenticator.insert(
        AI_DEV_TOKEN,
        AuthenticatedPrincipal::new(PrincipalId::new(AI_PRINCIPAL_ID), ai_grants),
    );
    let mut core = WovenCore::new(authenticator, CoreConfig::default())?;
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(1),
        woven_core::DeliveryClass::ReliableOrdered,
        PersistenceClass::Ephemeral,
        64 * 1024,
    ))?;
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(2),
        woven_core::DeliveryClass::LatestValue,
        PersistenceClass::Stateful { ttl: None },
        64 * 1024,
    ))?;
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(AI_STATUS_CHANNEL_ID),
        woven_core::DeliveryClass::LatestValue,
        PersistenceClass::Stateful { ttl: None },
        64 * 1024,
    ))?;
    core.provision_session(session)?;
    for space_id in [1, 2] {
        core.install_space(
            session,
            SpaceDescriptor {
                id: SpaceId::new(space_id),
                local_frame: CoordinateFrame::Logical,
                parent: None,
                epoch: SpaceEpoch::new(1),
                routing: RoutingPolicy::BroadcastAll,
            },
        )?;
    }
    Ok(core)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    use woven_core::TransportIndependentWorker;
    use woven_transport::spawn_worker;

    use super::{development_core, router_with_transports};

    fn test_worker() -> woven_transport::WorkerHandle {
        spawn_worker(TransportIndependentWorker::new(
            development_core().expect("development core"),
        ))
    }

    #[tokio::test]
    async fn health_reports_active_and_disabled_layers() {
        let app = router_with_transports(
            true,
            true,
            Some("8082/webtransport".to_owned()),
            false,
            test_worker(),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("health request is valid"),
            )
            .await
            .expect("health response is available");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("health response body is readable");
        let health: serde_json::Value =
            serde_json::from_slice(&body).expect("health response is valid JSON");
        assert_eq!(health["status"], "active");
        assert_eq!(health["layers"]["core"], "active");
        assert_eq!(health["layers"]["transport_worker"], "active");
        assert_eq!(health["layers"]["http_control_plane"], "active");
        assert_eq!(health["layers"]["quic"], "active");
        assert_eq!(health["layers"]["webtransport"], "active");
        assert_eq!(health["layers"]["inference"], "disabled");
    }

    #[tokio::test]
    async fn readiness_is_unavailable_without_quic() {
        let app = router_with_transports(false, false, None, false, test_worker());
        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("health request is valid"),
            )
            .await
            .expect("health response is available");
        let body = to_bytes(health.into_body(), usize::MAX)
            .await
            .expect("health response body is readable");
        let health: serde_json::Value =
            serde_json::from_slice(&body).expect("health response is valid JSON");
        assert_eq!(health["status"], "degraded");
        assert_eq!(health["layers"]["quic"], "disabled");

        let readiness = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("readiness request is valid"),
            )
            .await
            .expect("readiness response is available");
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
