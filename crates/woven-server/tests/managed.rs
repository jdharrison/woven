//! Bounded loopback HTTP and real certificate-verified QUIC; no external targets.
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use woven_client::{Client, ClientConfig, ClientTlsConfig};
use woven_core::{
    Command, CommandResult, ConnectionId, IdempotencyKey, JoinDecision, NamespaceId,
    QueueOperation, QueueStatus, SessionId, SessionKey,
};
use woven_protocol::AuthenticationScheme;
use woven_server::{ManagedServer, ManagedServerConfig, start_managed};

const ADMIN: &str = "test-independent-admin-credential-0123456789";
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Fixture {
    path: PathBuf,
    pem: String,
}
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "woven-managed-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let pem = cert.cert.pem();
        std::fs::write(path.join("cert.pem"), &pem).unwrap();
        std::fs::write(path.join("key.pem"), cert.key_pair.serialize_pem()).unwrap();
        std::fs::write(path.join("admin"), ADMIN).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for name in ["key.pem", "admin"] {
                std::fs::set_permissions(path.join(name), std::fs::Permissions::from_mode(0o600))
                    .unwrap();
            }
        }
        Self { path, pem }
    }
    fn config(&self) -> ManagedServerConfig {
        ManagedServerConfig {
            quic_bind_address: "127.0.0.1:0".parse().unwrap(),
            management_bind_address: "127.0.0.1:0".parse().unwrap(),
            admin_bind_address: "127.0.0.1:0".parse().unwrap(),
            certificate_file: self.path.join("cert.pem"),
            private_key_file: self.path.join("key.pem"),
            admin_token_file: self.path.join("admin"),
            webtransport: None,
        }
    }
    async fn client(
        &self,
        server: &ManagedServer,
        token: &str,
    ) -> Result<Client, woven_client::ClientError> {
        Client::connect_with_tls_and_auth(
            ClientConfig {
                url: format!("quic://{}", server.quic_address),
                token: token.into(),
                ..ClientConfig::default()
            },
            ClientTlsConfig::from_ca_pem(self.pem.as_bytes()).unwrap(),
            AuthenticationScheme::Bearer,
        )
        .await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

async fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &str,
    body: &str,
) -> (u16, Value) {
    tokio::time::timeout(Duration::from_secs(7), async {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n{body}", body.len());
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        stream.take(65536).read_to_end(&mut bytes).await.unwrap();
        let response = String::from_utf8(bytes).unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let code = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        if address.port() != 0 && code != 404 { assert!(head.to_ascii_lowercase().contains("cache-control: no-store")); }
        (code, if body.is_empty() { Value::Null } else { serde_json::from_str(body).unwrap_or(Value::Null) })
    }).await.expect("bounded HTTP request")
}
fn headers(server: &ManagedServer) -> String {
    format!(
        "Authorization: Bearer {ADMIN}\r\nWoven-Node-Incarnation: {}\r\nContent-Type: application/json\r\n",
        server.node_incarnation
    )
}
fn put(token: &str) -> String {
    json!({"revision": "1", "allocatedCCU": 1, "clientToken": token}).to_string()
}
fn scope() -> SessionKey {
    SessionKey::new(NamespaceId::new(1), SessionId::new(1))
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end lifecycle across HTTP and QUIC"
)]
async fn host_contract_and_quic_scope_revocation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = Fixture::new();
        let server = start_managed(fixture.config()).await.unwrap();
        assert_eq!(server.worker.live_counts().await.unwrap().sessions_active, 0);
        assert!(server.webtransport_address.is_none());
        assert!(server.webtransport_url.is_none());
        let path = "/v1/namespaces/1/sessions/1";
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        assert_eq!(http(server.admin_address, "GET", "/v1/node", "", "").await.0, 401);
        assert_eq!(http(server.admin_address, "GET", "/v1/node", &format!("Authorization: Bearer {a}\r\n"), "").await.0, 401);
        assert_eq!(http(server.admin_address, "GET", "/v1/node", &format!("Authorization: Bearer {ADMIN}\r\nAuthorization: Bearer {ADMIN}\r\n"), "").await.0, 401);
        let (status, node) = http(server.admin_address, "GET", "/v1/node", &headers(&server), "").await;
        assert_eq!(status, 200);
        assert_eq!(node["nodeIncarnation"], server.node_incarnation);
        assert_eq!(
            node["transports"],
            json!({"quic": true, "webTransport": {"enabled": false}})
        );
        assert!(node["transports"]["webTransport"]
            .get("certificateSha256")
            .is_none());
        assert_eq!(node["spaces"], json!([
            {"spaceId": "1", "epoch": "1", "channelIds": ["1"]},
            {"spaceId": "2", "epoch": "1", "channelIds": ["1"]}
        ]));
        assert_eq!(node["channels"], json!([
            {"channelId": "1", "delivery": "ReliableOrdered", "persistence": "Ephemeral", "maxPayloadBytes": 65536}
        ]));
        assert_eq!(http(server.admin_address, "GET", path, &headers(&server), "").await.0, 404);
        assert_eq!(http(server.management_address, "PUT", path, &headers(&server), &put(&a)).await.0, 404);
        assert_eq!(http(server.admin_address, "PUT", path, &format!("Authorization: Bearer {ADMIN}\r\nContent-Type: application/json\r\nWoven-Node-Incarnation: stale\r\n"), &put(&a)).await.0, 409);
        let (status, value) = http(server.admin_address, "PUT", path, &headers(&server), &put(&a)).await;
        assert_eq!(status, 201);
        assert_eq!(value, json!({"nodeIncarnation": server.node_incarnation, "namespaceId":"1", "sessionId":"1", "revision":"1", "allocatedCCU":1,"admission":{"effectiveAllocatedCCU":1,"pendingTarget":null,"activeCCU":0,"offeredSlots":0,"queueDepth":0,"availableSlots":1}}));
        assert!(!value.to_string().contains(&a));
        assert_eq!(http(server.admin_address, "PUT", path, &headers(&server), &put(&a)).await.0, 200);
        assert_eq!(http(server.admin_address, "PUT", "/v1/namespaces/2/sessions/2", &headers(&server), &put(&b)).await.0, 201);
        // Sequential connections on a fresh node have IDs 1..4. All are real verified QUIC sockets.
        let first = fixture.client(&server, &a).await.unwrap();
        let mut waiting = fixture.client(&server, &a).await.unwrap();
        let mut idle = fixture.client(&server, &a).await.unwrap();
        let mut other = fixture.client(&server, &b).await.unwrap();
        let admission = |id| Command::RequestSessionAdmission { connection: ConnectionId::new(id), session: scope(), idempotency_key: IdempotencyKey::new("same-key").unwrap() };
        assert!(matches!(server.worker.execute(admission(1)).await.unwrap(), CommandResult::Admission(JoinDecision::Admitted(_))));
        let CommandResult::Admission(JoinDecision::Queued(ticket)) = server.worker.execute(admission(2)).await.unwrap() else { panic!("queued"); };
        assert_eq!(server.worker.execute(Command::SessionQueue { connection: ConnectionId::new(2), session: scope(), ticket: ticket.id, operation: QueueOperation::Heartbeat }).await.unwrap(), CommandResult::Queue(QueueStatus::Waiting { position: 1 }));
        first.close_gracefully(Duration::from_secs(2)).await.unwrap();
        let mut status = QueueStatus::Waiting { position: 1 };
        for _ in 0..20 {
            let CommandResult::Queue(value) = server.worker.execute(Command::SessionQueue { connection: ConnectionId::new(2), session: scope(), ticket: ticket.id, operation: QueueOperation::Status }).await.unwrap() else { panic!("status"); };
            status = value;
            if status == QueueStatus::Offered { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(status, QueueStatus::Offered);
        assert_eq!(server.worker.execute(Command::SessionQueue { connection: ConnectionId::new(2), session: scope(), ticket: ticket.id, operation: QueueOperation::Claim }).await.unwrap(), CommandResult::Queue(QueueStatus::Admitted));
        let mut queued = fixture.client(&server, &a).await.unwrap();
        assert!(matches!(server.worker.execute(admission(5)).await.unwrap(), CommandResult::Admission(JoinDecision::Queued(_))));
        let capacity_body = json!({"revision":"2", "allocatedCCU":0}).to_string();
        let (status, value) = http(server.admin_address, "PATCH", path, &headers(&server), &capacity_body).await;
        assert_eq!(status, 200);
        assert_eq!(value["admission"]["activeCCU"], 1);
        assert_eq!(value["admission"]["pendingTarget"], 0);
        assert_eq!(http(server.admin_address, "PATCH", path, &headers(&server), &capacity_body).await.0, 200);
        assert_eq!(http(server.admin_address, "DELETE", path, &format!("{}If-Match: \"1\"\r\n", headers(&server)), "").await.0, 409);
        assert_eq!(http(server.admin_address, "DELETE", path, &format!("{}If-Match: \"2\"\r\n", headers(&server)), "").await.0, 204);
        for client in [&mut waiting, &mut idle, &mut queued] {
            assert!(tokio::time::timeout(Duration::from_secs(2), client.recv()).await.unwrap().is_err());
        }
        assert_eq!(http(server.admin_address, "DELETE", path, &format!("{}If-Match: \"2\"\r\n", headers(&server)), "").await.0, 204);
        assert_eq!(http(server.admin_address, "PUT", path, &headers(&server), &put(&a)).await.0, 409);
        for token in [a.as_str(), ADMIN, "", "wrong", "dev-token"] { assert!(fixture.client(&server, token).await.is_err()); }
        // Deleting product A neither revokes product B nor closes its authenticated socket.
        assert!(other.recv_timeout(Duration::from_millis(50)).await.unwrap().is_none());
        other.join_session(1, 1).await.unwrap();
        assert!(other.recv_timeout(Duration::from_secs(1)).await.unwrap().is_some());
        assert!(fixture.client(&server, &b).await.is_ok());
    }).await.expect("bounded managed lifecycle");
}

#[tokio::test]
async fn new_incarnation_rejects_old_management_binding() {
    let fixture = Fixture::new();
    let first = start_managed(fixture.config()).await.unwrap();
    let old_headers = headers(&first);
    let old_incarnation = first.node_incarnation.clone();
    drop(first);
    let second = start_managed(fixture.config()).await.unwrap();
    assert_ne!(second.node_incarnation, old_incarnation);
    assert_eq!(
        http(
            second.admin_address,
            "PUT",
            "/v1/namespaces/1/sessions/1",
            &old_headers,
            &put(&"a".repeat(64))
        )
        .await
        .0,
        409
    );
    assert_eq!(
        second.worker.live_counts().await.unwrap().sessions_active,
        0
    );
}

#[tokio::test]
async fn stalled_admin_body_times_out_without_provisioning() {
    let fixture = Fixture::new();
    let server = start_managed(fixture.config()).await.unwrap();
    let mut stream = tokio::net::TcpStream::connect(server.admin_address)
        .await
        .unwrap();
    let request = format!(
        "PUT /v1/namespaces/1/sessions/1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 100\r\n{}\r\n{{",
        headers(&server)
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut bytes = vec![0; 4096];
    let count = tokio::time::timeout(Duration::from_secs(7), stream.read(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let response = std::str::from_utf8(&bytes[..count]).unwrap();
    assert!(response.starts_with("HTTP/1.1 503"));
    assert!(response.contains("worker_unavailable"));
    assert_eq!(
        server.worker.live_counts().await.unwrap().sessions_active,
        0
    );
}

#[tokio::test]
async fn admin_inputs_are_bounded_and_invalid_configuration_binds_nothing() {
    let fixture = Fixture::new();
    let mut config = fixture.config();
    config.admin_bind_address = "0.0.0.0:0".parse().unwrap();
    assert!(start_managed(config).await.is_err());
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = fixture.config();
    config.admin_bind_address = occupied.local_addr().unwrap();
    assert!(start_managed(config).await.is_err());
    let server = start_managed(fixture.config()).await.unwrap();
    let path = "/v1/namespaces/1/sessions/1";
    for body in [
        json!({"revision":"01","allocatedCCU":1,"clientToken":"a".repeat(64)}),
        json!({"revision":"1","allocatedCCU":4097,"clientToken":"a".repeat(64)}),
        json!({"revision":"1","allocatedCCU":1,"clientToken":"a".repeat(64),"extra":true}),
    ] {
        assert_eq!(
            http(
                server.admin_address,
                "PUT",
                path,
                &headers(&server),
                &body.to_string()
            )
            .await
            .0,
            400
        );
    }
    assert_eq!(
        http(
            server.admin_address,
            "PUT",
            path,
            &headers(&server),
            &"x".repeat(8193)
        )
        .await
        .0,
        413
    );
    assert_eq!(
        http(server.admin_address, "PUT", path, "", &"x".repeat(8193))
            .await
            .0,
        401
    );
    assert_eq!(
        http(
            server.admin_address,
            "PUT",
            "/v1/namespaces/0/sessions/1",
            &headers(&server),
            &put(&"a".repeat(64))
        )
        .await
        .0,
        400
    );
    assert_eq!(
        server.worker.live_counts().await.unwrap().sessions_active,
        0
    );
    drop(server);
    let config = fixture.config();
    std::fs::write(&config.admin_token_file, "weak").unwrap();
    assert!(start_managed(config.clone()).await.is_err());
    std::fs::write(&config.admin_token_file, ADMIN).unwrap();
    std::fs::write(&config.private_key_file, "bad pem").unwrap();
    assert!(start_managed(config).await.is_err());
}
