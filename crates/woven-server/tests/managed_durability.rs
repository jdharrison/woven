//! Synthetic credentials, private throwaway directories and bounded loopback traffic only.
#![cfg(target_os = "linux")]
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
    process::{Child, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use woven_client::{Client, ClientConfig, ClientTlsConfig};
use woven_core::{
    ManagedBounds3D, ManagedCheckpoint, ManagedCheckpointMutation, ManagedCheckpointScope,
    ManagedCheckpointSpace, ManagedError, ManagedPoint3D, ManagedRequest, NamespaceId, SessionId,
    SessionKey,
};
use woven_protocol::AuthenticationScheme;
use woven_server::{ManagedServer, ManagedServerConfig, import_managed_state, start_managed};

#[path = "managed_durability/baseline.rs"]
mod baseline;

const ADMIN: &str = "synthetic-durability-admin-0123456789";
const LINEAGE: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    pem: String,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "woven-durable-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join("state"))
            .unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let pem = cert.cert.pem();
        std::fs::write(root.join("cert.pem"), &pem).unwrap();
        std::fs::write(root.join("key.pem"), cert.key_pair.serialize_pem()).unwrap();
        std::fs::write(root.join("admin"), ADMIN).unwrap();
        for name in ["key.pem", "admin"] {
            std::fs::set_permissions(root.join(name), std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        Self { root, pem }
    }
    fn config(&self) -> ManagedServerConfig {
        ManagedServerConfig {
            quic_bind_address: "127.0.0.1:0".parse().unwrap(),
            management_bind_address: "127.0.0.1:0".parse().unwrap(),
            admin_bind_address: "127.0.0.1:0".parse().unwrap(),
            certificate_file: self.root.join("cert.pem"),
            private_key_file: self.root.join("key.pem"),
            admin_token_file: self.root.join("admin"),
            webtransport: None,
            state_directory: Some(self.root.join("state")),
        }
    }
    async fn import(&self) {
        let receipt = import_managed_state(self.config(), import_body().into_bytes())
            .await
            .unwrap();
        assert_eq!(receipt.node_incarnation, LINEAGE);
    }
    async fn start(&self) -> ManagedServer {
        for _ in 0..100 {
            if let Ok(server) = start_managed(self.config()).await {
                return server;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("durable server did not start");
    }
    async fn client(&self, address: SocketAddr, token: &str) -> Client {
        Client::connect_with_tls_and_auth(
            ClientConfig {
                url: format!("quic://{address}"),
                token: token.to_owned(),
                ..ClientConfig::default()
            },
            ClientTlsConfig::from_ca_pem(self.pem.as_bytes()).unwrap(),
            AuthenticationScheme::Bearer,
        )
        .await
        .unwrap()
    }
    fn command(&self, config: &ManagedServerConfig) -> std::process::Command {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_woven-server"));
        command
            .env_clear()
            .env("WOVEN_MANAGED_QUIC", "1")
            .env("WOVEN_QUIC_BIND", config.quic_bind_address.to_string())
            .env(
                "WOVEN_MANAGEMENT_BIND",
                config.management_bind_address.to_string(),
            )
            .env("WOVEN_ADMIN_BIND", config.admin_bind_address.to_string())
            .env("WOVEN_TLS_CERT_FILE", &config.certificate_file)
            .env("WOVEN_TLS_KEY_FILE", &config.private_key_file)
            .env("WOVEN_ADMIN_TOKEN_FILE", &config.admin_token_file)
            .env("WOVEN_MANAGED_STATE_DIR", self.root.join("state"));
        command
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn digest(token: &str) -> String {
    use std::fmt::Write;
    let mut hex = String::new();
    for byte in ring::digest::digest(&ring::digest::SHA256, token.as_bytes()).as_ref() {
        write!(&mut hex, "{byte:02x}").unwrap();
    }
    hex
}
fn checkpoint() -> ManagedCheckpoint {
    let scopes = [(4, 'a', false), (6, 'b', false), (8, 'c', true)]
        .into_iter()
        .map(|(id, token, retired)| ManagedCheckpointScope {
            namespace_id: id,
            session_id: id,
            revision: 5,
            last_mutation: ManagedCheckpointMutation::Space { space_id: 3 },
            allocated_ccu: 4,
            tick_rate_hz: Some(60),
            verifier_sha256: digest(&token.to_string().repeat(64)),
            retired,
            spatial_spaces: vec![ManagedCheckpointSpace {
                space_id: 3,
                epoch: 1,
                meters_per_unit: 0.5,
                cell_size: f64::from_bits(10.0_f64.to_bits() + 1),
                interest_radius: 20.0,
                exact_distance: true,
                bounds: ManagedBounds3D {
                    min: ManagedPoint3D {
                        x: -100.0,
                        y: -100.0,
                        z: -100.0,
                    },
                    max: ManagedPoint3D {
                        x: 100.0,
                        y: 100.0,
                        z: 100.0,
                    },
                },
            }],
        })
        .collect();
    ManagedCheckpoint {
        version: 1,
        migration_baseline: None,
        scopes,
    }
}
fn import_body() -> String {
    json!({"version":1,"nodeIncarnation":LINEAGE,"checkpoint":checkpoint()}).to_string()
}
fn scope(id: u64) -> SessionKey {
    SessionKey::new(NamespaceId::new(id), SessionId::new(id))
}
fn path(id: u64) -> String {
    format!("/v1/namespaces/{id}/sessions/{id}")
}
async fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    auth: bool,
    body: &str,
) -> std::io::Result<(u16, Value)> {
    let (status, _, body) = http_with_headers(address, method, path, auth, "", body).await?;
    Ok((status, serde_json::from_str(&body).unwrap_or(Value::Null)))
}
async fn http_with_headers(
    address: SocketAddr,
    method: &str,
    path: &str,
    auth: bool,
    extra_headers: &str,
    body: &str,
) -> std::io::Result<(u16, String, String)> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        let headers = if auth { format!("Authorization: Bearer {ADMIN}\r\nWoven-Node-Incarnation: {LINEAGE}\r\nContent-Type: application/json\r\n") } else { String::new() };
        stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n{headers}{extra_headers}\r\n{body}", body.len()).as_bytes()).await?;
        let mut bytes = Vec::new(); stream.take(65536).read_to_end(&mut bytes).await?;
        let response = String::from_utf8(bytes).map_err(std::io::Error::other)?;
        let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| std::io::Error::other("no response"))?;
        let status = head.split_whitespace().nth(1).and_then(|s| s.parse().ok()).ok_or_else(|| std::io::Error::other("no status"))?;
        Ok((status, head.to_owned(), body.to_owned()))
    }).await.map_err(std::io::Error::other)?
}
async fn node(address: SocketAddr) -> Value {
    for _ in 0..100 {
        if let Ok((200, body)) = http(address, "GET", "/v1/node", true, "").await {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("node did not become available");
}
fn fixed_config(fixture: &Fixture) -> ManagedServerConfig {
    let admin = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let management = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let quic = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut config = fixture.config();
    config.admin_bind_address = admin.local_addr().unwrap();
    config.management_bind_address = management.local_addr().unwrap();
    config.quic_bind_address = quic.local_addr().unwrap();
    config
}
fn assert_unbound(config: &ManagedServerConfig) {
    std::net::TcpListener::bind(config.admin_bind_address).unwrap();
    std::net::TcpListener::bind(config.management_bind_address).unwrap();
    std::net::UdpSocket::bind(config.quic_bind_address).unwrap();
}
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one bounded migration and restart lifecycle verifies all persisted scope invariants"
)]
async fn graceful_restart_preserves_lineage_verifiers_exact_spatial_revisions_and_tombstones() {
    let fixture = Fixture::new();
    fixture.import().await;
    let server = fixture.start().await;
    let first_boot = server.boot_id.clone();
    let first_node = node(server.admin_address).await;
    assert_eq!(first_node["nodeIncarnation"], LINEAGE);
    assert_eq!(first_node["bootId"], first_boot);
    assert_eq!(
        http(server.management_address, "GET", "/readyz", false, "")
            .await
            .unwrap()
            .0,
        200
    );
    let client = fixture.client(server.quic_address, &"a".repeat(64)).await;
    drop(client);
    let snapshot = http(server.admin_address, "GET", &path(4), true, "")
        .await
        .unwrap();
    assert_eq!(snapshot.1["revision"], "5");
    assert_eq!(snapshot.1["tickRateHz"], 60);
    assert_eq!(snapshot.1["spaces"][2]["spaceId"], "3");
    assert!(!snapshot.1.to_string().contains("verifier"));
    drop(server);
    let server = fixture.start().await;
    assert_eq!(server.node_incarnation, LINEAGE);
    assert_ne!(server.boot_id, first_boot);
    assert_eq!(
        http(server.admin_address, "GET", &path(4), true, "")
            .await
            .unwrap()
            .1,
        snapshot.1
    );
    let client = fixture.client(server.quic_address, &"b".repeat(64)).await;
    drop(client);
    let saved: Value =
        serde_json::from_slice(&std::fs::read(fixture.root.join("state/checkpoint.json")).unwrap())
            .unwrap();
    let restored: ManagedCheckpoint =
        serde_json::from_value(saved["payload"]["checkpoint"].clone()).unwrap();
    assert_eq!(restored, checkpoint());
    assert_eq!(
        restored.scopes[0].spatial_spaces[0].cell_size.to_bits(),
        10.0_f64.to_bits() + 1
    );
    assert_eq!(
        server
            .worker
            .manage(ManagedRequest::Put {
                session: scope(8),
                revision: 6,
                allocated_ccu: 1,
                tick_rate_hz: None,
                credentials: woven_core::Credentials::new("d".repeat(64))
            })
            .await,
        Err(ManagedError::ScopeRetired)
    );
    assert_eq!(
        server
            .worker
            .manage(ManagedRequest::Put {
                session: scope(9),
                revision: 1,
                allocated_ccu: 1,
                tick_rate_hz: None,
                credentials: woven_core::Credentials::new("c".repeat(64))
            })
            .await,
        Err(ManagedError::TokenConflict)
    );
    server
        .worker
        .manage(ManagedRequest::Patch {
            session: scope(4),
            revision: 6,
            allocated_ccu: 3,
            tick_rate_hz: None,
        })
        .await
        .unwrap();
    server
        .worker
        .manage(ManagedRequest::Delete {
            session: scope(6),
            revision: 5,
        })
        .await
        .unwrap();
    drop(server);
    let server = fixture.start().await;
    assert_eq!(
        http(server.admin_address, "GET", &path(4), true, "")
            .await
            .unwrap()
            .1["revision"],
        "6"
    );
    assert_eq!(
        server
            .worker
            .manage(ManagedRequest::Put {
                session: scope(6),
                revision: 7,
                allocated_ccu: 1,
                tick_rate_hz: None,
                credentials: woven_core::Credentials::new("d".repeat(64))
            })
            .await,
        Err(ManagedError::ScopeRetired)
    );
}

fn response_header<'a>(headers: &'a str, name: &str) -> &'a str {
    let mut values = headers.lines().filter_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    });
    let value = values.next().expect("required response header");
    assert!(values.next().is_none(), "one response header value");
    value
}
async fn metrics_sample(server: &ManagedServer) -> String {
    let (status, headers, body) =
        http_with_headers(server.management_address, "GET", "/metrics", false, "", "")
            .await
            .unwrap();
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&headers, "Woven-Node-Incarnation"),
        server.node_incarnation
    );
    assert_eq!(response_header(&headers, "Woven-Boot-Id"), server.boot_id);
    assert!(!body.contains(&server.boot_id));
    body
}
async fn log_page(server: &ManagedServer, after: u64, boot: Option<&str>) -> (u16, Value) {
    let extra = boot.map_or_else(String::new, |boot| format!("Woven-Boot-Id: {boot}\r\n"));
    let (status, headers, body) = http_with_headers(
        server.admin_address,
        "GET",
        &format!("/v1/logs?after={after}&limit=32"),
        true,
        &extra,
        "",
    )
    .await
    .unwrap();
    assert_eq!(response_header(&headers, "Cache-Control"), "no-store");
    (status, serde_json::from_str(&body).unwrap())
}

#[tokio::test]
async fn durable_restart_changes_observability_boot_but_not_lineage_and_never_rewinds_same_boot() {
    let fixture = Fixture::new();
    fixture.import().await;
    let mut previous_boot = None;
    for _ in 0..2 {
        let server = fixture.start().await;
        let identity = node(server.admin_address).await;
        assert_eq!(identity["nodeIncarnation"], LINEAGE);
        assert_eq!(identity["bootId"], server.boot_id);
        assert_eq!(server.boot_id.len(), 48);
        assert!(
            server
                .boot_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        let initial = metrics_sample(&server).await;
        assert!(
            initial
                .lines()
                .any(|line| line == "woven_connections_total 0")
        );
        let (status, empty) = log_page(&server, 0, Some(&server.boot_id)).await;
        assert_eq!(status, 200);
        assert_eq!(empty["bootId"], server.boot_id);
        assert_eq!(empty["entries"], json!([]));
        assert_eq!(empty["nextSequence"], "0");
        if let Some(old_boot) = previous_boot.as_deref() {
            assert_ne!(server.boot_id, old_boot);
            let (status, conflict) = log_page(&server, 2, Some(old_boot)).await;
            assert_eq!(status, 409);
            assert_eq!(conflict, json!({"error":{"code":"boot_conflict"}}));
        }
        let mut client = fixture.client(server.quic_address, &"a".repeat(64)).await;
        assert_eq!(
            client
                .request_admission(4, 4, 1, "boot-observability".into())
                .await
                .unwrap()
                .status,
            woven_protocol::AdmissionStatus::Admitted
        );
        let (status, first) = log_page(&server, 0, Some(&server.boot_id)).await;
        assert_eq!(status, 200);
        assert_eq!(first["nodeIncarnation"], LINEAGE);
        assert_eq!(first["bootId"], server.boot_id);
        assert_eq!(first["entries"][0]["sequence"], "1");
        assert_eq!(first["entries"][0]["event"], "client.connected");
        assert_eq!(first["nextSequence"], "1");
        let connection = first["entries"][0]["connectionId"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        server
            .worker
            .capture_client_log(
                woven_core::ConnectionId::new(connection),
                scope(4),
                woven_protocol::ClientLog {
                    level: woven_protocol::LogLevel::Info,
                    message: "synthetic boot-local event".into(),
                },
            )
            .await
            .unwrap();
        let (status, second) = log_page(&server, 1, Some(&server.boot_id)).await;
        assert_eq!(status, 200);
        assert_eq!(second["bootId"], server.boot_id);
        assert_eq!(second["entries"][0]["sequence"], "2");
        assert_eq!(second["nextSequence"], "2");
        assert_eq!(log_page(&server, 1, None).await, (200, second));
        for after in [2, 100] {
            let (status, page) = log_page(&server, after, Some(&server.boot_id)).await;
            assert_eq!(status, 200);
            assert_eq!(page["bootId"], server.boot_id);
            assert_eq!(page["entries"], json!([]));
            assert_eq!(page["nextSequence"], after.to_string());
        }
        let updated = metrics_sample(&server).await;
        assert!(
            updated
                .lines()
                .any(|line| line == "woven_connections_total 1")
        );
        assert_eq!(metrics_sample(&server).await, updated);
        client
            .close_gracefully(Duration::from_secs(2))
            .await
            .unwrap();
        previous_boot = Some(server.boot_id.clone());
        drop(server);
    }
}

#[tokio::test]
async fn acknowledged_mutation_survives_sigkill_and_boot_id_is_fresh() {
    let fixture = Fixture::new();
    fixture.import().await;
    let config = fixed_config(&fixture);
    let mut process = Process(
        fixture
            .command(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let before = node(config.admin_bind_address).await;
    assert_eq!(
        http(
            config.admin_bind_address,
            "PATCH",
            &path(4),
            true,
            &json!({"revision":"6","allocatedCCU":3}).to_string()
        )
        .await
        .unwrap()
        .0,
        200
    );
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    drop(process);
    let _process = Process(
        fixture
            .command(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let after = node(config.admin_bind_address).await;
    assert_eq!(after["nodeIncarnation"], before["nodeIncarnation"]);
    assert_ne!(after["bootId"], before["bootId"]);
    let snapshot = http(config.admin_bind_address, "GET", &path(4), true, "")
        .await
        .unwrap()
        .1;
    assert_eq!(snapshot["revision"], "6");
    assert_eq!(snapshot["allocatedCCU"], 3);
    let client = fixture
        .client(config.quic_bind_address, &"a".repeat(64))
        .await;
    drop(client);
}

#[tokio::test]
async fn missing_corrupt_unsafe_or_locked_store_binds_no_listeners() {
    let fixture = Fixture::new();
    let config = fixed_config(&fixture);
    assert!(start_managed(config.clone()).await.is_err());
    assert_unbound(&config);
    fixture.import().await;
    let server = fixture.start().await;
    assert!(start_managed(config.clone()).await.is_err());
    assert_unbound(&config);
    assert!(
        import_managed_state(config.clone(), import_body().into_bytes())
            .await
            .is_err()
    );
    drop(server);
    tokio::time::sleep(Duration::from_millis(30)).await;
    let file = fixture.root.join("state/checkpoint.json");
    let original = std::fs::read(&file).unwrap();
    for bytes in [
        b"{".to_vec(),
        b"{}".to_vec(),
        original
            .iter()
            .copied()
            .map(|b| if b == b'5' { b'7' } else { b })
            .collect(),
    ] {
        std::fs::write(&file, bytes).unwrap();
        assert!(start_managed(config.clone()).await.is_err());
        assert_unbound(&config);
    }
    std::fs::write(&file, &original).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(start_managed(config.clone()).await.is_err());
    assert_unbound(&config);
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::rename(&file, fixture.root.join("saved")).unwrap();
    std::os::unix::fs::symlink(fixture.root.join("saved"), &file).unwrap();
    assert!(start_managed(config.clone()).await.is_err());
    assert_unbound(&config);
}

#[tokio::test]
async fn disk_failure_permanently_closes_worker_and_withdraws_all_listeners_and_readiness() {
    let fixture = Fixture::new();
    fixture.import().await;
    let mut config = fixture.config();
    config.webtransport = Some(woven_server::ManagedWebTransportConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        path: "/webtransport".into(),
        origin_policy: woven_server::ManagedWebTransportOriginPolicy::Https,
    });
    let server = start_managed(config).await.unwrap();
    let quic_client = fixture.client(server.quic_address, &"a".repeat(64)).await;
    let (wt_endpoint, wt_connection) = webtransport(&fixture, &server).await;
    std::fs::create_dir(fixture.root.join("state/checkpoint.pending")).unwrap();
    assert_eq!(
        server
            .worker
            .manage(ManagedRequest::Patch {
                session: scope(4),
                revision: 6,
                allocated_ccu: 3,
                tick_rate_hz: None
            })
            .await,
        Err(ManagedError::WorkerUnavailable)
    );
    tokio::time::timeout(Duration::from_secs(2), server.worker.closed())
        .await
        .unwrap();
    assert!(server.worker.is_closed());
    tokio::time::timeout(Duration::from_secs(2), wt_connection.closed())
        .await
        .unwrap();
    drop(wt_endpoint);
    drop(quic_client);
    for _ in 0..100 {
        if server.worker.is_closed()
            && tokio::net::TcpStream::connect(server.management_address)
                .await
                .is_err()
            && tokio::net::TcpStream::connect(server.admin_address)
                .await
                .is_err()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        tokio::net::TcpStream::connect(server.management_address)
            .await
            .is_err()
    );
    assert!(
        tokio::net::TcpStream::connect(server.admin_address)
            .await
            .is_err()
    );
    assert!(
        http(server.management_address, "GET", "/readyz", false, "")
            .await
            .is_err()
    );
    assert!(
        server
            .worker
            .manage(ManagedRequest::Get { session: scope(4) })
            .await
            .is_err()
    );
    let saved: Value =
        serde_json::from_slice(&std::fs::read(fixture.root.join("state/checkpoint.json")).unwrap())
            .unwrap();
    assert_eq!(saved["payload"]["checkpoint"]["scopes"][0]["revision"], 5);
}

async fn webtransport(
    fixture: &Fixture,
    server: &ManagedServer,
) -> (
    wtransport::Endpoint<wtransport::endpoint::endpoint_side::Client>,
    wtransport::Connection,
) {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(fixture.pem.as_bytes()).unwrap())
        .unwrap();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
    let endpoint = wtransport::Endpoint::client(
        wtransport::ClientConfig::builder()
            .with_bind_default()
            .with_custom_tls(tls)
            .build(),
    )
    .unwrap();
    let connection = tokio::time::timeout(
        Duration::from_secs(3),
        endpoint.connect(server.webtransport_url.as_ref().unwrap()),
    )
    .await
    .unwrap()
    .unwrap();
    (endpoint, connection)
}

#[tokio::test]
async fn offline_cli_import_is_explicit_bounded_one_shot_and_never_binds() {
    let fixture = Fixture::new();
    let config = fixed_config(&fixture);
    let mut command = tokio::process::Command::from(fixture.command(&config));
    command
        .arg("--managed-state-import")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(import_body().as_bytes())
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt, json!({"nodeIncarnation":LINEAGE}));
    assert!(output.stderr.is_empty());
    assert_unbound(&config);
    assert!(
        import_managed_state(config.clone(), import_body().into_bytes())
            .await
            .is_err()
    );
    let fixture2 = Fixture::new();
    let mut invalid: Value = serde_json::from_str(&import_body()).unwrap();
    invalid["checkpoint"]["scopes"][0]["verifierSha256"] = json!(digest(ADMIN));
    assert!(
        import_managed_state(fixture2.config(), invalid.to_string().into_bytes())
            .await
            .is_err()
    );
    assert!(!fixture2.root.join("state/checkpoint.json").exists());
    invalid["extra"] = json!(true);
    assert!(
        import_managed_state(fixture2.config(), invalid.to_string().into_bytes())
            .await
            .is_err()
    );
}
