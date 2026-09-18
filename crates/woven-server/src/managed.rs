//! Opt-in managed QUIC/WebTransport and an independently authenticated loopback admin listener.
#[cfg(test)]
#[path = "managed_tests.rs"]
mod tests;
use crate::{
    ServerError,
    remote::{read_bounded, validate_token},
    router_with_transports,
};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use ring::rand::SecureRandom;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde::Deserialize;
use serde_json::json;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use woven_core::{
    ChannelDefinition, ChannelId, CoreConfig, Credentials, DeliveryClass, DevAuthenticator,
    ManagedError, ManagedOutcome, ManagedRequest, NamespaceId, PersistenceClass, SessionId,
    SessionKey, TransportIndependentWorker, WovenCore,
};
use woven_protocol::AuthenticationScheme;
use woven_transport::{WorkerHandle, spawn_worker};
use woven_transport_quic::webtransport::{
    OriginPolicy, ServerEndpoint as WebTransportEndpoint, WebTransportConfig,
    serve_shared_endpoint as serve_webtransport_endpoint,
};
use woven_transport_quic::{QuicConfig, serve_endpoint, server_config, server_endpoint};

fn invalid() -> ServerError {
    ServerError::QuicConfiguration("invalid managed configuration; check opt-in, listeners, WebTransport path/origins, TLS and private admin credential file".into())
}

const MAX_WEBTRANSPORT_PATH_BYTES: usize = 256;
const MAX_WEBTRANSPORT_ORIGIN_ENV_BYTES: usize = 2_048;
const MAX_WEBTRANSPORT_ORIGINS: usize = 64;

/// Optional managed WebTransport listener configuration.
#[derive(Clone, Debug)]
pub struct ManagedWebTransportConfig {
    /// Explicit UDP bind address for HTTP/3 WebTransport. Port zero is supported for tests.
    pub bind_address: SocketAddr,
    /// Exact request path accepted by the WebTransport adapter.
    pub path: String,
    /// Nonempty, bounded exact browser `Origin` allowlist. Missing origins are rejected.
    pub allowed_origins: Vec<String>,
}

impl ManagedWebTransportConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.path.is_empty()
            || self.path.len() > MAX_WEBTRANSPORT_PATH_BYTES
            || !self.path.starts_with('/')
            || self.path.contains(['?', '#'])
            || self.allowed_origins.is_empty()
            || self.allowed_origins.len() > MAX_WEBTRANSPORT_ORIGINS
        {
            return Err(invalid());
        }
        let parsed_path = self
            .path
            .parse::<axum::http::Uri>()
            .map_err(|_| invalid())?;
        if parsed_path.scheme().is_some()
            || parsed_path.authority().is_some()
            || parsed_path.path() != self.path
        {
            return Err(invalid());
        }
        let total_origin_bytes = self
            .allowed_origins
            .iter()
            .fold(0_usize, |total, origin| total.saturating_add(origin.len()))
            .saturating_add(self.allowed_origins.len().saturating_sub(1));
        if total_origin_bytes > MAX_WEBTRANSPORT_ORIGIN_ENV_BYTES {
            return Err(invalid());
        }
        let mut unique = std::collections::BTreeSet::new();
        for origin in &self.allowed_origins {
            if origin.is_empty() || origin.trim() != origin {
                return Err(invalid());
            }
            let parsed = url::Url::parse(origin).map_err(|_| invalid())?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.origin().ascii_serialization() != *origin
                || !unique.insert(origin)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ManagedServerConfig {
    pub quic_bind_address: SocketAddr,
    pub management_bind_address: SocketAddr,
    pub admin_bind_address: SocketAddr,
    pub certificate_file: PathBuf,
    pub private_key_file: PathBuf,
    pub admin_token_file: PathBuf,
    /// Optional real WebTransport listener using the same worker and PEM TLS identity.
    pub webtransport: Option<ManagedWebTransportConfig>,
}

impl ManagedServerConfig {
    pub fn from_env() -> Result<Option<Self>, ServerError> {
        Self::from_lookup(|name| {
            std::env::var_os(name)
                .map(|value| value.into_string().map_err(|_| invalid()))
                .transpose()
        })
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Result<Option<String>, ServerError>,
    ) -> Result<Option<Self>, ServerError> {
        let flag = lookup("WOVEN_MANAGED_QUIC")?;
        let admin_bind = lookup("WOVEN_ADMIN_BIND")?;
        let admin_file = lookup("WOVEN_ADMIN_TOKEN_FILE")?;
        let webtransport_flag = lookup("WOVEN_MANAGED_WEBTRANSPORT")?;
        let webtransport_bind = lookup("WOVEN_WEBTRANSPORT_BIND")?;
        let webtransport_path = lookup("WOVEN_WEBTRANSPORT_PATH")?;
        let webtransport_origins = lookup("WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS")?;
        if flag.is_none() {
            if admin_bind.is_some()
                || admin_file.is_some()
                || webtransport_flag.is_some()
                || webtransport_bind.is_some()
                || webtransport_path.is_some()
                || webtransport_origins.is_some()
            {
                return Err(invalid());
            }
            return Ok(None);
        }
        if flag.as_deref() != Some("1")
            || lookup("WOVEN_REMOTE_QUIC")?.is_some()
            || lookup("WOVEN_AUTH_TOKEN_FILE")?.is_some()
        {
            return Err(invalid());
        }
        let webtransport = match webtransport_flag.as_deref() {
            None => {
                if webtransport_bind.is_some()
                    || webtransport_path.is_some()
                    || webtransport_origins.is_some()
                {
                    return Err(invalid());
                }
                None
            }
            Some("1") => {
                let origins = webtransport_origins.ok_or_else(invalid)?;
                if origins.len() > MAX_WEBTRANSPORT_ORIGIN_ENV_BYTES {
                    return Err(invalid());
                }
                Some(ManagedWebTransportConfig {
                    bind_address: webtransport_bind
                        .ok_or_else(invalid)?
                        .parse()
                        .map_err(|_| invalid())?,
                    path: webtransport_path.ok_or_else(invalid)?,
                    allowed_origins: origins
                        .split(',')
                        .take(MAX_WEBTRANSPORT_ORIGINS + 1)
                        .map(str::to_owned)
                        .collect(),
                })
            }
            Some(_) => return Err(invalid()),
        };
        let config = Self {
            quic_bind_address: lookup("WOVEN_QUIC_BIND")?
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?,
            management_bind_address: lookup("WOVEN_MANAGEMENT_BIND")?
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?,
            admin_bind_address: admin_bind
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?,
            certificate_file: lookup("WOVEN_TLS_CERT_FILE")?.ok_or_else(invalid)?.into(),
            private_key_file: lookup("WOVEN_TLS_KEY_FILE")?.ok_or_else(invalid)?.into(),
            admin_token_file: admin_file.ok_or_else(invalid)?.into(),
            webtransport,
        };
        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<(), ServerError> {
        if !self.management_bind_address.ip().is_loopback()
            || !self.admin_bind_address.ip().is_loopback()
            || (self.admin_bind_address.port() != 0
                && self.admin_bind_address == self.management_bind_address)
            || self.webtransport.as_ref().is_some_and(|webtransport| {
                webtransport.bind_address.port() != 0
                    && webtransport.bind_address == self.quic_bind_address
            })
            || [
                &self.certificate_file,
                &self.private_key_file,
                &self.admin_token_file,
            ]
            .iter()
            .any(|path| path.as_os_str().is_empty())
        {
            return Err(invalid());
        }
        if let Some(webtransport) = &self.webtransport {
            webtransport.validate()?;
        }
        Ok(())
    }
}

/// Retain the handle while serving. Drop initiates transport shutdown and aborts all listeners.
pub struct ManagedServer {
    pub quic_address: SocketAddr,
    /// Actual local WebTransport UDP address when enabled.
    pub webtransport_address: Option<SocketAddr>,
    /// Actual local HTTPS WebTransport URL when enabled.
    pub webtransport_url: Option<String>,
    pub management_address: SocketAddr,
    pub admin_address: SocketAddr,
    pub node_incarnation: String,
    /// Trusted local integration boundary; never expose this as an unauthenticated HTTP API.
    pub worker: WorkerHandle,
    endpoint: quinn::Endpoint,
    webtransport_endpoint: Option<Arc<WebTransportEndpoint>>,
    quic_task: tokio::task::JoinHandle<()>,
    webtransport_task: Option<tokio::task::JoinHandle<()>>,
    management_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    admin_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for ManagedServer {
    fn drop(&mut self) {
        self.endpoint.close(0_u32.into(), b"server shutdown");
        self.quic_task.abort();
        if let Some(task) = &self.webtransport_task {
            task.abort();
        }
        if let Some(endpoint) = &self.webtransport_endpoint {
            endpoint.close(wtransport::VarInt::from_u32(0), b"server shutdown");
        }
        self.management_task.abort();
        self.admin_task.abort();
    }
}

fn managed_webtransport_server_config(
    bind_address: SocketAddr,
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<wtransport::ServerConfig, ServerError> {
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|_| invalid())?;
    tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
    Ok(wtransport::ServerConfig::builder()
        .with_bind_address(bind_address)
        .with_custom_tls(tls)
        .build())
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut hex = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 15)]));
    }
    hex
}

struct ManagedTls {
    quic: quinn::ServerConfig,
    webtransport: Option<wtransport::ServerConfig>,
    webtransport_certificate_sha256: Option<String>,
}

fn load_managed_tls(config: &ManagedServerConfig) -> Result<ManagedTls, ServerError> {
    let cert_bytes = read_bounded(&config.certificate_file, 1024 * 1024, false)?;
    let key_bytes = read_bounded(&config.private_key_file, 1024 * 1024, true)?;
    let certificates = CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid())?;
    let key = PrivateKeyDer::from_pem_slice(&key_bytes).map_err(|_| invalid())?;
    let webtransport_certificate_sha256 = config
        .webtransport
        .as_ref()
        .map(|_| {
            certificates
                .first()
                .map(|certificate| sha256_hex(certificate.as_ref()))
                .ok_or_else(invalid)
        })
        .transpose()?;
    let quic = server_config(certificates.clone(), key.clone_key()).map_err(|_| invalid())?;
    let webtransport = config
        .webtransport
        .as_ref()
        .map(|webtransport| {
            managed_webtransport_server_config(webtransport.bind_address, certificates, key)
        })
        .transpose()?;
    Ok(ManagedTls {
        quic,
        webtransport,
        webtransport_certificate_sha256,
    })
}

fn new_node_incarnation() -> Result<String, ServerError> {
    let mut random = [0_u8; 24];
    ring::rand::SystemRandom::new()
        .fill(&mut random)
        .map_err(|_| invalid())?;
    let mut node_incarnation = String::with_capacity(48);
    for byte in random {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        node_incarnation.push(char::from(HEX[usize::from(byte >> 4)]));
        node_incarnation.push(char::from(HEX[usize::from(byte & 15)]));
    }
    Ok(node_incarnation)
}

fn spawn_managed_webtransport(
    endpoint: Option<&Arc<WebTransportEndpoint>>,
    managed_config: Option<&ManagedWebTransportConfig>,
    worker: &WorkerHandle,
) -> Result<Option<tokio::task::JoinHandle<()>>, ServerError> {
    let Some((endpoint, managed_config)) = endpoint.zip(managed_config) else {
        return Ok(None);
    };
    let mut transport_config = WebTransportConfig::new(worker.clone());
    transport_config.path = Arc::from(managed_config.path.as_str());
    transport_config.expected_authentication_scheme = AuthenticationScheme::Bearer;
    transport_config.origin_policy = OriginPolicy::allowlisted(
        managed_config
            .allowed_origins
            .iter()
            .map(|origin| Arc::<str>::from(origin.as_str()))
            .collect(),
        false,
    )
    .map_err(|_| invalid())?;
    Ok(Some(tokio::spawn(serve_webtransport_endpoint(
        endpoint.clone(),
        transport_config,
    ))))
}

pub async fn start_managed(config: ManagedServerConfig) -> Result<ManagedServer, ServerError> {
    config.validate()?;
    let tls = load_managed_tls(&config)?;
    let admin_bytes = read_bounded(&config.admin_token_file, 4098, true)?;
    let admin = validate_token(&admin_bytes)?;
    let node_incarnation = new_node_incarnation()?;
    let mut core =
        WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).map_err(|_| invalid())?;
    core.enable_managed(&Credentials::new(admin))
        .map_err(|_| invalid())?;
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(1),
        DeliveryClass::ReliableOrdered,
        PersistenceClass::Ephemeral,
        65536,
    ))
    .map_err(|_| invalid())?;

    // No listener or worker is started until all configuration, TLS and secrets validate.
    let endpoint = server_endpoint(config.quic_bind_address, tls.quic)?;
    let webtransport_endpoint = tls
        .webtransport
        .map(WebTransportEndpoint::server)
        .transpose()?
        .map(Arc::new);
    let management_listener = tokio::net::TcpListener::bind(config.management_bind_address).await?;
    let admin_listener = tokio::net::TcpListener::bind(config.admin_bind_address).await?;
    let quic_address = endpoint.local_addr()?;
    let webtransport_address = webtransport_endpoint
        .as_ref()
        .map(|endpoint| endpoint.local_addr())
        .transpose()?;
    let webtransport_url = webtransport_address
        .zip(config.webtransport.as_ref())
        .map(|(address, webtransport)| format!("https://{address}{}", webtransport.path));
    let webtransport_capability = webtransport_address
        .zip(config.webtransport.as_ref())
        .map(|(address, webtransport)| format!("{}{}", address.port(), webtransport.path));
    let management_address = management_listener.local_addr()?;
    let admin_address = admin_listener.local_addr()?;
    let webtransport_enabled = webtransport_endpoint.is_some();
    let worker = spawn_worker(TransportIndependentWorker::new(core));
    let state = Arc::new(AdminState::new(
        worker.clone(),
        node_incarnation.clone(),
        admin,
        tls.webtransport_certificate_sha256,
    ));
    let admin_router = Router::new().fallback(admin_request).with_state(state);
    let management_router = router_with_transports(
        true,
        webtransport_enabled,
        webtransport_capability,
        false,
        worker.clone(),
    );
    let mut quic_config = QuicConfig::new(worker.clone());
    quic_config.expected_authentication_scheme = AuthenticationScheme::Bearer;
    let quic_task = tokio::spawn(serve_endpoint(endpoint.clone(), quic_config));
    let webtransport_task = spawn_managed_webtransport(
        webtransport_endpoint.as_ref(),
        config.webtransport.as_ref(),
        &worker,
    )?;
    let management_task =
        tokio::spawn(async move { axum::serve(management_listener, management_router).await });
    let admin_task = tokio::spawn(async move { axum::serve(admin_listener, admin_router).await });
    Ok(ManagedServer {
        quic_address,
        webtransport_address,
        webtransport_url,
        management_address,
        admin_address,
        node_incarnation,
        worker,
        endpoint,
        webtransport_endpoint,
        quic_task,
        webtransport_task,
        management_task,
        admin_task,
    })
}

async fn optional_task_finished(task: &mut Option<tokio::task::JoinHandle<()>>) {
    match task {
        Some(task) => {
            let _ = task.await;
        }
        None => std::future::pending().await,
    }
}

pub async fn serve_managed(config: ManagedServerConfig) -> Result<(), ServerError> {
    let mut server = start_managed(config).await?;
    if let Some(webtransport_url) = &server.webtransport_url {
        println!(
            "Woven managed QUIC ready at {}; WebTransport ready at {}; read-only management {}; authenticated loopback admin {}",
            server.quic_address, webtransport_url, server.management_address, server.admin_address
        );
    } else {
        println!(
            "Woven managed QUIC ready at {}; WebTransport disabled; read-only management {}; authenticated loopback admin {}",
            server.quic_address, server.management_address, server.admin_address
        );
    }
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = &mut server.quic_task => return Err(invalid()),
        () = optional_task_finished(&mut server.webtransport_task) => return Err(invalid()),
        _ = &mut server.management_task => return Err(invalid()),
        _ = &mut server.admin_task => return Err(invalid()),
    }
    Ok(())
}

struct AdminState {
    worker: WorkerHandle,
    incarnation: String,
    webtransport_certificate_sha256: Option<String>,
    verifier: ring::digest::Digest,
    concurrency: tokio::sync::Semaphore,
    started: Instant,
    rate: AtomicU64,
}
impl AdminState {
    fn new(
        worker: WorkerHandle,
        incarnation: String,
        admin: &str,
        webtransport_certificate_sha256: Option<String>,
    ) -> Self {
        Self {
            worker,
            incarnation,
            webtransport_certificate_sha256,
            verifier: ring::digest::digest(&ring::digest::SHA256, admin.as_bytes()),
            concurrency: tokio::sync::Semaphore::new(32),
            started: Instant::now(),
            rate: AtomicU64::new(0),
        }
    }
    fn rate_permitted(&self) -> bool {
        let window = self.started.elapsed().as_secs().min(u64::MAX >> 6) << 6;
        self.rate
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                let count = if old & !63 == window { old & 63 } else { 0 };
                (count < 32).then_some(window | (count + 1))
            })
            .is_ok()
    }
    #[allow(
        deprecated,
        reason = "ring provides a vetted constant-time digest comparison"
    )]
    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(value) = single_header(headers, "authorization") else {
            return false;
        };
        let Some(token) = value.strip_prefix("Bearer ") else {
            return false;
        };
        if !(32..=4096).contains(&token.len()) {
            return false;
        }
        let digest = ring::digest::digest(&ring::digest::SHA256, token.as_bytes());
        ring::constant_time::verify_slices_are_equal(self.verifier.as_ref(), digest.as_ref())
            .is_ok()
    }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

fn error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(json!({"error": {"code": code}}))).into_response()
}
fn managed_error(value: ManagedError) -> Response {
    let (status, code) = match value {
        ManagedError::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
        ManagedError::ScopeNotFound => (StatusCode::NOT_FOUND, "scope_not_found"),
        ManagedError::RevisionConflict => (StatusCode::CONFLICT, "revision_conflict"),
        ManagedError::ScopeRetired => (StatusCode::CONFLICT, "scope_retired"),
        ManagedError::TokenConflict => (StatusCode::CONFLICT, "token_conflict"),
        ManagedError::CapacityExhausted => (StatusCode::SERVICE_UNAVAILABLE, "capacity_exhausted"),
        ManagedError::WorkerUnavailable => (StatusCode::SERVICE_UNAVAILABLE, "worker_unavailable"),
    };
    error(status, code)
}

async fn admin_request(State(state): State<Arc<AdminState>>, request: Request) -> Response {
    let mut response = if let Ok(_permit) = state.concurrency.try_acquire() {
        if !state.rate_permitted() {
            error(StatusCode::TOO_MANY_REQUESTS, "rate_limited")
        } else if !state.authorized(request.headers()) {
            error(StatusCode::UNAUTHORIZED, "unauthorized")
        } else {
            tokio::time::timeout(Duration::from_secs(5), dispatch(&state, request))
                .await
                .unwrap_or_else(|_| managed_error(ManagedError::WorkerUnavailable))
        }
    } else {
        error(StatusCode::TOO_MANY_REQUESTS, "rate_limited")
    };
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

fn canonical_id(value: &str) -> Option<u64> {
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PutBody {
    revision: String,
    #[serde(rename = "allocatedCCU")]
    allocated_ccu: u32,
    client_token: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PatchBody {
    revision: String,
    #[serde(rename = "allocatedCCU")]
    allocated_ccu: u32,
}

fn invalid_body_headers(request: &Request) -> Option<Response> {
    let bad = || Some(error(StatusCode::BAD_REQUEST, "invalid_request"));
    let bodyless = request.method() == Method::GET || request.method() == Method::DELETE;
    if request.uri().query().is_some() {
        return bad();
    }
    if let Some(length) = request.headers().get(header::CONTENT_LENGTH) {
        let Some(length) = length
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        else {
            return bad();
        };
        if length > 8192 {
            return Some(error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large"));
        }
        if length > 0 && bodyless {
            return bad();
        }
    }
    if bodyless && request.headers().contains_key(header::TRANSFER_ENCODING) {
        return bad();
    }
    None
}

async fn dispatch(state: &AdminState, request: Request) -> Response {
    let bad = || error(StatusCode::BAD_REQUEST, "invalid_request");
    if let Some(response) = invalid_body_headers(&request) {
        return response;
    }
    if request.uri().path() == "/v1/node" && request.method() == Method::GET {
        let webtransport = state.webtransport_certificate_sha256.as_ref().map_or_else(
            || json!({"enabled": false}),
            |certificate_sha256| json!({"enabled": true, "certificateSha256": certificate_sha256}),
        );
        return Json(json!({"nodeIncarnation": state.incarnation, "transports": {"quic": true, "webTransport": webtransport}, "limits": {"maxSessions": 1024, "maxScopeHistory": 4096, "maxConnections": 4096, "maxAllocatedCCU": 4096, "maxQueueDepth": 1024}, "spaces": [{"spaceId": "1", "epoch": "1", "channelIds": ["1"]}, {"spaceId": "2", "epoch": "1", "channelIds": ["1"]}], "channels": [{"channelId": "1", "delivery": "ReliableOrdered", "persistence": "Ephemeral", "maxPayloadBytes": 65536}]})).into_response();
    }
    let parts = request.uri().path().split('/').collect::<Vec<_>>();
    if parts.len() != 6
        || !parts[0].is_empty()
        || parts[1] != "v1"
        || parts[2] != "namespaces"
        || parts[4] != "sessions"
    {
        return error(StatusCode::NOT_FOUND, "scope_not_found");
    }
    let (Some(namespace), Some(session)) = (canonical_id(parts[3]), canonical_id(parts[5])) else {
        return bad();
    };
    let session = SessionKey::new(NamespaceId::new(namespace), SessionId::new(session));
    let method = request.method().clone();
    let incarnation = single_header(request.headers(), "woven-node-incarnation");
    if (method != Method::GET || request.headers().contains_key("woven-node-incarnation"))
        && incarnation != Some(state.incarnation.as_str())
    {
        return error(StatusCode::CONFLICT, "incarnation_conflict");
    }
    let command = match method {
        Method::GET => ManagedRequest::Get { session },
        Method::DELETE => {
            let revision = single_header(request.headers(), "if-match")
                .and_then(|value| value.strip_prefix('"')?.strip_suffix('"'))
                .and_then(canonical_id);
            let Some(revision) = revision else {
                return bad();
            };
            ManagedRequest::Delete { session, revision }
        }
        Method::PUT | Method::PATCH => {
            if single_header(request.headers(), "content-type") != Some("application/json") {
                return bad();
            }
            let Ok(body) = to_bytes(request.into_body(), 8192).await else {
                return error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large");
            };
            if method == Method::PUT {
                let Ok(body) = serde_json::from_slice::<PutBody>(&body) else {
                    return bad();
                };
                let Some(revision) = canonical_id(&body.revision) else {
                    return bad();
                };
                ManagedRequest::Put {
                    session,
                    revision,
                    allocated_ccu: body.allocated_ccu,
                    credentials: Credentials::new(body.client_token),
                }
            } else {
                let Ok(body) = serde_json::from_slice::<PatchBody>(&body) else {
                    return bad();
                };
                let Some(revision) = canonical_id(&body.revision) else {
                    return bad();
                };
                ManagedRequest::Patch {
                    session,
                    revision,
                    allocated_ccu: body.allocated_ccu,
                }
            }
        }
        _ => return bad(),
    };
    match state.worker.manage(command).await {
        Ok(ManagedOutcome::Snapshot { created, snapshot }) => {
            let mut value = serde_json::to_value(snapshot).unwrap_or_default();
            value["nodeIncarnation"] = json!(state.incarnation);
            (
                if created {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                Json(value),
            )
                .into_response()
        }
        Ok(ManagedOutcome::Deleted { .. }) => StatusCode::NO_CONTENT.into_response(),
        Err(value) => managed_error(value),
    }
}
