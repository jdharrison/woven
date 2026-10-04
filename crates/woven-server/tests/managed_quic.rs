//! Real WVN1 admission over verified TLS on ephemeral loopback sockets only.
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use woven_client::{Client, ClientConfig, ClientError, ClientTlsConfig, ManagedAdmissionOutcome};
use woven_protocol::{
    AdmissionStatus, AuthenticationScheme, ControlPayload, MessagePayload, ProtocolErrorCode,
    QueueState,
};
use woven_server::{ManagedServer, ManagedServerConfig, start_managed};

use rustls::pki_types::pem::PemObject;
use woven_protocol::{
    Authenticate, Codec, DeliveryClass, Envelope, Hello, MessageKind, QueueUpdate, RequestAdmission,
};

// A small verified wire peer observes handshake principal IDs and error envelopes
// that the high-level client intentionally consumes internally.
struct WirePeer {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}
impl WirePeer {
    async fn connect(fixture: &Fixture, server: &ManagedServer) -> Self {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from_pem_slice(fixture.pem.as_bytes()).unwrap())
            .unwrap();
        let crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let config = quinn::ClientConfig::new(std::sync::Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
        ));
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config);
        let connection = endpoint
            .connect(server.quic_address, "127.0.0.1")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut peer = Self {
            endpoint,
            connection,
            send,
            recv,
        };
        peer.send(Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Hello(Hello {
                min_protocol_version: 1,
                max_protocol_version: 1,
                client_name: "managed-wire-test".into(),
                client_version: "1".into(),
                capability_bits: woven_protocol::CAPABILITY_CLIENT_LOG,
                max_frame_size: 65536,
                max_payload_size: 65536,
            }),
        ))
        .await;
        assert!(matches!(
            peer.recv().await.message,
            MessagePayload::Control(ControlPayload::Capabilities(_))
        ));
        peer
    }
    async fn send(&mut self, envelope: Envelope) {
        self.send
            .write_all(&Codec::default().encode(&envelope).unwrap())
            .await
            .unwrap();
    }
    async fn recv(&mut self) -> Envelope {
        let mut prefix = [0; 4];
        self.recv.read_exact(&mut prefix).await.unwrap();
        let codec = Codec::default();
        let length = codec.expected_frame_len(&prefix).unwrap().unwrap();
        let mut bytes = vec![0; length];
        bytes[..4].copy_from_slice(&prefix);
        self.recv.read_exact(&mut bytes[4..]).await.unwrap();
        codec.decode(&bytes).unwrap()
    }
    async fn authenticate_with_scheme(
        &mut self,
        token: &str,
        scheme: AuthenticationScheme,
    ) -> Envelope {
        self.send(Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Authenticate(Authenticate {
                scheme,
                credentials: token.as_bytes().to_vec(),
            }),
        ))
        .await;
        self.recv().await
    }
    async fn authenticate(&mut self, token: &str) -> u64 {
        let MessagePayload::Control(ControlPayload::Authenticated(value)) = self
            .authenticate_with_scheme(token, AuthenticationScheme::Bearer)
            .await
            .message
        else {
            panic!("expected Authenticated")
        };
        assert_ne!(value.principal_id, 0);
        value.principal_id
    }
}
impl Drop for WirePeer {
    fn drop(&mut self) {
        self.connection
            .close(quinn::VarInt::from_u32(0), b"test done");
        self.endpoint
            .close(quinn::VarInt::from_u32(0), b"test done");
    }
}
fn scoped(control: ControlPayload, correlation: u64) -> Envelope {
    let mut envelope = Envelope::control(DeliveryClass::ReliableOrdered, control);
    envelope.namespace_id = 1;
    envelope.session_id = 1;
    envelope.correlation_id = Some(correlation);
    envelope
}

#[tokio::test]
async fn distinct_principals_correlated_rate_errors_and_result_controls_rejected() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(); let server = fixture.server().await;
        let token = "e".repeat(64); provision(&server, 1, 1, &token).await;
        let mut first = WirePeer::connect(&fixture, &server).await;
        let mut second = WirePeer::connect(&fixture, &server).await;
        assert_ne!(first.authenticate(&token).await, second.authenticate(&token).await);
        for correlation in 1..=5 {
            first.send(scoped(ControlPayload::RequestAdmission(RequestAdmission { idempotency_key: "one".into() }), correlation)).await;
            let reply = first.recv().await;
            assert_eq!((reply.namespace_id, reply.session_id, reply.correlation_id), (1, 1, Some(correlation)));
            if correlation < 5 {
                assert!(matches!(reply.message, MessagePayload::Control(ControlPayload::AdmissionResult(value)) if value.status == AdmissionStatus::Admitted && value.ticket_id.is_none()));
            } else {
                assert!(matches!(reply.message, MessagePayload::Control(ControlPayload::ProtocolError(value)) if value.code == ProtocolErrorCode::RateLimited && value.related_message_kind == MessageKind::RequestAdmission));
            }
        }
        second.send(scoped(ControlPayload::QueueUpdate(QueueUpdate { ticket_id: 1, state: QueueState::Admitted,
            position: 0, poll_after_ms: 0, ticket_remaining_ms: 0, offer_remaining_ms: 0 }), 42)).await;
        let rejected = second.recv().await;
        assert_eq!(rejected.correlation_id, Some(42));
        assert!(matches!(rejected.message, MessagePayload::Control(ControlPayload::ProtocolError(value)) if value.code == ProtocolErrorCode::UnsupportedMessage));
        let mut unauthenticated = WirePeer::connect(&fixture, &server).await;
        unauthenticated.send(scoped(ControlPayload::RequestAdmission(RequestAdmission { idempotency_key: "no-token".into() }), 1)).await;
        assert!(matches!(unauthenticated.recv().await.message, MessagePayload::Control(ControlPayload::ProtocolError(value)) if value.code == ProtocolErrorCode::AuthenticationRequired));
    }).await.expect("bounded wire error checks");
}

#[tokio::test]
async fn managed_quic_rejects_development_authentication_scheme() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "f".repeat(64);
        provision(&server, 1, 1, &token).await;
        let mut peer = WirePeer::connect(&fixture, &server).await;
        let response = peer
            .authenticate_with_scheme(&token, AuthenticationScheme::Development)
            .await;
        assert!(matches!(
            response.message,
            MessagePayload::Control(ControlPayload::ProtocolError(error))
                if error.code == ProtocolErrorCode::Unauthorized
        ));
    })
    .await
    .expect("bounded managed QUIC scheme rejection");
}

#[tokio::test]
async fn socket_log_scope_and_rate_rejections_leave_the_authenticated_connection_healthy() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "c".repeat(64);
        provision(&server, 1, 1, &token).await;
        let mut peer = WirePeer::connect(&fixture, &server).await;
        peer.authenticate(&token).await;
        let control = ControlPayload::ClientLog(woven_protocol::ClientLog {
            level: woven_protocol::LogLevel::Info,
            message: "scoped wire diagnostic".into(),
        });
        peer.send(scoped(control.clone(), 1)).await;
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::InvalidScope && error.related_message_kind == MessageKind::ClientLog));
        peer.send(scoped(ControlPayload::RequestAdmission(RequestAdmission { idempotency_key: "wire-logs".into() }), 2)).await;
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::AdmissionResult(result)) if result.status == AdmissionStatus::Admitted));
        let mut foreign = scoped(control.clone(), 3);
        foreign.namespace_id = 2;
        peer.send(foreign).await;
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::InvalidScope));
        for correlation in 4..=14 {
            peer.send(scoped(control.clone(), correlation)).await;
        }
        let error = peer.recv().await;
        assert_eq!(error.correlation_id, Some(14));
        assert!(matches!(error.message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::RateLimited && error.related_message_kind == MessageKind::ClientLog));
        peer.send(scoped(ControlPayload::RequestAdmission(RequestAdmission { idempotency_key: "wire-logs".into() }), 15)).await;
        let reply = peer.recv().await;
        assert_eq!(reply.correlation_id, Some(15));
        assert!(matches!(reply.message, MessagePayload::Control(ControlPayload::AdmissionResult(result)) if result.status == AdmissionStatus::Admitted));
        let (status, feed) = http(server.admin_address, "GET", "/v1/logs?after=0&limit=32", &headers(&server), "").await;
        assert_eq!(status, 200);
        let entries = feed["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 11);
        assert_eq!(entries.iter().filter(|entry| entry["event"] == "client.connected").count(), 1);
        assert!(entries.iter().all(|entry| entry["namespaceId"] == "1"));
    }).await.expect("bounded socket log rejection check");
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one bounded socket lifecycle verifies admission and disconnect deduplication"
)]
async fn native_client_logs_capture_admission_claim_leave_revoke_and_transport_loss() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "a".repeat(64);
        provision(&server, 1, 1, &token).await;
        let mut first = fixture.client(&server, &token).await.unwrap();
        let mut waiting = fixture.client(&server, &token).await.unwrap();
        assert_eq!(
            first
                .request_admission(1, 1, 1, "logs-first".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        assert_eq!(
            first
                .request_admission(1, 1, 2, "logs-first".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        let ticket = waiting
            .request_admission(1, 1, 1, "logs-waiting".into())
            .await
            .unwrap();
        assert_eq!(ticket.status, AdmissionStatus::Queued);
        assert!(waiting.log("not admitted").await.is_err());
        first.log("info from native API").await.unwrap();
        first
            .logger()
            .warn("warning from native API")
            .await
            .unwrap();
        first.logger().error("error from native API").await.unwrap();
        // A successful send carries no persistence ACK and produces no peer traffic.
        assert!(
            waiting
                .recv_timeout(Duration::from_millis(100))
                .await
                .unwrap()
                .is_none()
        );
        let (status, feed) = http(
            server.admin_address,
            "GET",
            "/v1/logs?after=0&limit=32",
            &headers(&server),
            "",
        )
        .await;
        assert_eq!(status, 200);
        let entries = feed["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0]["event"], "client.connected");
        assert_eq!(entries[1]["level"], "info");
        assert_eq!(entries[2]["level"], "warn");
        assert_eq!(entries[3]["level"], "error");
        assert_eq!(feed["droppedThrough"], "0");
        assert!(
            entries
                .iter()
                .all(|entry| entry["namespaceId"] == "1" && entry["sessionId"] == "1")
        );
        let first_id = entries[0]["connectionId"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        first
            .leave_session("client-provided reason is not captured")
            .await
            .unwrap();
        pace().await;
        let ticket = ticket.ticket_id.unwrap();
        assert_eq!(
            waiting.queue_status(1, 1, 2, ticket).await.unwrap().state,
            QueueState::Offered
        );
        assert_eq!(
            waiting.queue_claim(1, 1, 3, ticket).await.unwrap().state,
            QueueState::Admitted
        );
        assert_eq!(
            waiting.queue_claim(1, 1, 4, ticket).await.unwrap().state,
            QueueState::Admitted
        );
        waiting.logger().warn("queue claim logger").await.unwrap();
        assert!(
            waiting
                .recv_timeout(Duration::from_millis(100))
                .await
                .unwrap()
                .is_none()
        );
        let (status, _) = http(
            server.admin_address,
            "DELETE",
            &path(1, 1),
            &format!("{}If-Match: \"1\"\r\n", headers(&server)),
            "",
        )
        .await;
        assert_eq!(status, 204);
        server
            .worker
            .discard_and_disconnect(woven_core::ConnectionId::new(first_id))
            .await;
        first.close().unwrap();
        waiting.close().unwrap();

        // A separate authenticated tenant proves that feed metadata is server-attached.
        let other_token = "b".repeat(64);
        provision(&server, 2, 7, &other_token).await;
        let mut other = fixture.client(&server, &other_token).await.unwrap();
        assert_eq!(
            other
                .request_admission(2, 7, 1, "other-logs".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        other.log("other tenant").await.unwrap();
        // Sending is not a capture ACK; verify collection before deliberately closing the socket.
        let mut collected = false;
        for _ in 0..10 {
            let (status, page) = http(
                server.admin_address,
                "GET",
                "/v1/logs?after=0&limit=32",
                &headers(&server),
                "",
            )
            .await;
            assert_eq!(status, 200);
            if page["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["message"] == "other tenant")
            {
                collected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            collected,
            "native client log reaches the local capture feed"
        );
        other
            .close_gracefully(Duration::from_secs(2))
            .await
            .unwrap();
        pace().await;
        let (status, feed) = http(
            server.admin_address,
            "GET",
            "/v1/logs?after=0&limit=32",
            &headers(&server),
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(feed["nodeIncarnation"], server.node_incarnation);
        let entries = feed["entries"].as_array().unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["event"] == "client.connected")
                .count(),
            3
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["event"] == "client.disconnected")
                .count(),
            3
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["event"] == "client.log")
                .count(),
            5
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["message"] == "managed session revoked")
                .count(),
            1
        );
        let tenant: Vec<_> = entries
            .iter()
            .filter(|entry| entry["namespaceId"] == "2")
            .collect();
        assert_eq!(tenant.len(), 3);
        assert!(tenant.iter().all(|entry| entry["sessionId"] == "7"));
        let text = feed.to_string();
        assert!(!text.contains(&token) && !text.contains(&other_token) && !text.contains(ADMIN));
        assert!(!text.contains("client-provided reason"));
        let after = feed["nextSequence"].as_str().unwrap();
        let (status, empty) = http(
            server.admin_address,
            "GET",
            &format!("/v1/logs?after={after}&limit=32"),
            &headers(&server),
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(empty["entries"], json!([]));
        assert_eq!(empty["nextSequence"], after);
    })
    .await
    .expect("bounded native client log lifecycle test");
}

const ADMIN: &str = "managed-wire-test-admin-not-a-client-credential";
const LIMIT: Duration = Duration::from_secs(30);
static NEXT: AtomicU64 = AtomicU64::new(1);

struct Fixture {
    path: PathBuf,
    pem: String,
}
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "woven-managed-wire-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let pem = certificate.cert.pem();
        std::fs::write(path.join("cert.pem"), &pem).unwrap();
        std::fs::write(path.join("key.pem"), certificate.key_pair.serialize_pem()).unwrap();
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
    async fn server(&self) -> ManagedServer {
        start_managed(ManagedServerConfig {
            quic_bind_address: "127.0.0.1:0".parse().unwrap(),
            management_bind_address: "127.0.0.1:0".parse().unwrap(),
            admin_bind_address: "127.0.0.1:0".parse().unwrap(),
            certificate_file: self.path.join("cert.pem"),
            private_key_file: self.path.join("key.pem"),
            admin_token_file: self.path.join("admin"),
            webtransport: None,
        })
        .await
        .unwrap()
    }
    async fn client(&self, server: &ManagedServer, token: &str) -> Result<Client, ClientError> {
        Client::connect_with_tls_and_auth(
            ClientConfig {
                url: format!("quic://{}", server.quic_address),
                token: token.to_owned(),
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
        stream.take(65_536).read_to_end(&mut bytes).await.unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        (status, if body.is_empty() { Value::Null } else { serde_json::from_str(body).unwrap() })
    }).await.expect("bounded admin HTTP")
}
fn headers(server: &ManagedServer) -> String {
    format!(
        "Authorization: Bearer {ADMIN}\r\nContent-Type: application/json\r\nWoven-Node-Incarnation: {}\r\n",
        server.node_incarnation
    )
}
fn path(namespace: u64, session: u64) -> String {
    format!("/v1/namespaces/{namespace}/sessions/{session}")
}
async fn provision(server: &ManagedServer, namespace: u64, session: u64, token: &str) {
    let (status, _) = http(
        server.admin_address,
        "PUT",
        &path(namespace, session),
        &headers(server),
        &json!({"revision":"1", "allocatedCCU":1, "clientToken":token}).to_string(),
    )
    .await;
    assert_eq!(status, 201);
}
async fn snapshot(server: &ManagedServer, namespace: u64, session: u64) -> Value {
    let (status, value) = http(
        server.admin_address,
        "GET",
        &path(namespace, session),
        &headers(server),
        "",
    )
    .await;
    assert_eq!(status, 200);
    value["admission"].clone()
}
async fn pace() {
    tokio::time::sleep(Duration::from_millis(1_100)).await;
}
fn unauthorized(error: &ClientError) {
    assert!(
        matches!(error, ClientError::ServerError(value) if value.code == ProtocolErrorCode::Unauthorized),
        "expected sanitized authorization rejection"
    );
}
async fn subscribed(client: &mut Client, namespace: u64, session: u64) -> u64 {
    client
        .subscribe_space(namespace, session, 1, 1, 1)
        .await
        .unwrap();
    assert!(matches!(
        client.recv().await.unwrap().message,
        MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))
    ));
    let entered = client.recv().await.unwrap();
    assert!(matches!(
        entered.message,
        MessagePayload::Control(ControlPayload::EntityEntered(_))
    ));
    entered.entity_id.unwrap()
}

#[tokio::test]
async fn managed_channel_four_datagrams_are_ephemeral_and_channel_three_stays_denied() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "f".repeat(64);
        provision(&server, 1, 1, &token).await;
        let mut peer = WirePeer::connect(&fixture, &server).await;
        let principal = peer.authenticate(&token).await;
        peer.send(scoped(ControlPayload::RequestAdmission(RequestAdmission { idempotency_key: "unreliable".into() }), 1)).await;
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::AdmissionResult(result)) if result.status == AdmissionStatus::Admitted));
        let mut envelope = scoped(ControlPayload::SubscribeSpace(woven_protocol::SubscribeSpace), 2);
        envelope.space_id = 1;
        envelope.space_epoch = 1;
        envelope.channel_id = Some(1);
        peer.send(envelope.clone()).await;
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))));
        let entered = peer.recv().await;
        assert!(matches!(entered.message, MessagePayload::Control(ControlPayload::EntityEntered(_))));
        envelope.entity_id = entered.entity_id;
        envelope.channel_id = Some(4);
        envelope.delivery_class = DeliveryClass::UnreliableSequenced;
        envelope.sender_sequence = 1;
        envelope.correlation_id = None;
        envelope.message = MessagePayload::EntityState(woven_protocol::OpaquePayload { type_id: 1, bytes: vec![7; 25] });
        let codec = Codec::default();
        peer.connection.send_datagram(codec.encode(&envelope).unwrap().into()).unwrap();
        let datagram = tokio::time::timeout(Duration::from_secs(3), peer.connection.read_datagram()).await.unwrap().unwrap();
        assert_eq!(codec.decode(&datagram).unwrap(), envelope);
        let session = woven_core::SessionKey::new(woven_core::NamespaceId::new(1), woven_core::SessionId::new(1));
        let woven_core::CommandResult::Snapshot(snapshot) = server.worker.execute(woven_core::Command::Snapshot { connection: woven_core::ConnectionId::new(principal), session }).await.unwrap() else { panic!("expected snapshot") };
        assert!(snapshot.state.is_empty());
        assert_eq!(snapshot.state_bytes, 0);
        envelope.channel_id = Some(3);
        peer.connection.send_datagram(codec.encode(&envelope).unwrap().into()).unwrap();
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::Unauthorized));
    }).await.expect("bounded managed datagram check");
}

#[tokio::test]
async fn managed_quic_publish_ceiling_is_shared_by_stream_and_datagram_channels() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "f".repeat(64);
        let body = json!({"revision":"1", "allocatedCCU":1, "clientToken":token, "tickRateHz":1}).to_string();
        assert_eq!(http(server.admin_address, "PUT", &path(1, 1), &headers(&server), &body).await.0, 201);
        let mut peer = WirePeer::connect(&fixture, &server).await;
        peer.authenticate(&token).await;
        peer.send(scoped(ControlPayload::RequestAdmission(RequestAdmission { idempotency_key: "publish-rate".into() }), 1)).await;
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::AdmissionResult(result)) if result.status == AdmissionStatus::Admitted));
        let mut envelope = scoped(ControlPayload::SubscribeSpace(woven_protocol::SubscribeSpace), 2);
        envelope.space_id = 1;
        envelope.space_epoch = 1;
        envelope.channel_id = Some(1);
        peer.send(envelope.clone()).await;
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))));
        let entered = peer.recv().await;
        envelope.entity_id = entered.entity_id;
        envelope.correlation_id = None;
        envelope.sender_sequence = 1;
        envelope.message = MessagePayload::ReliableEvent(woven_protocol::OpaquePayload { type_id: 1, bytes: vec![7] });
        peer.send(envelope.clone()).await;
        assert_eq!(peer.recv().await, envelope);
        envelope.message = MessagePayload::EntityState(woven_protocol::OpaquePayload { type_id: 1, bytes: vec![7] });
        envelope.channel_id = Some(4);
        envelope.delivery_class = DeliveryClass::UnreliableSequenced;
        peer.connection.send_datagram(Codec::default().encode(&envelope).unwrap().into()).unwrap();
        assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::RateLimited && error.related_message_kind == MessageKind::EntityState));
    }).await.expect("bounded managed QUIC publish ceiling");
}

#[tokio::test]
async fn managed_quic_leave_readmit_and_resubscribe_cannot_reset_publish_budget() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "f".repeat(64);
        let body = json!({"revision":"1", "allocatedCCU":1, "clientToken":token, "tickRateHz":1}).to_string();
        assert_eq!(http(server.admin_address, "PUT", &path(1, 1), &headers(&server), &body).await.0, 201);
        let mut peer = WirePeer::connect(&fixture, &server).await;
        peer.authenticate(&token).await;
        let started = std::time::Instant::now();
        let mut first_entity = None;
        for attempt in 0..2 {
            if attempt == 1 {
                peer.send(scoped(ControlPayload::LeaveSession(woven_protocol::LeaveSession { reason: "rate regression".into() }), 10)).await;
            }
            peer.send(scoped(ControlPayload::RequestAdmission(RequestAdmission { idempotency_key: format!("rate-rejoin-{attempt}") }), attempt + 1)).await;
            assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::AdmissionResult(result)) if result.status == AdmissionStatus::Admitted));
            let mut envelope = scoped(ControlPayload::SubscribeSpace(woven_protocol::SubscribeSpace), attempt + 3);
            envelope.space_id = 1;
            envelope.space_epoch = 1;
            envelope.channel_id = Some(1);
            peer.send(envelope.clone()).await;
            assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))));
            let entered = peer.recv().await;
            assert!(matches!(entered.message, MessagePayload::Control(ControlPayload::EntityEntered(_))));
            envelope.entity_id = entered.entity_id;
            envelope.correlation_id = None;
            envelope.sender_sequence = 1;
            envelope.message = MessagePayload::ReliableEvent(woven_protocol::OpaquePayload { type_id: 1, bytes: vec![7] });
            peer.send(envelope.clone()).await;
            if attempt == 0 {
                first_entity = entered.entity_id;
                assert_eq!(peer.recv().await, envelope);
            } else {
                assert_ne!(first_entity, entered.entity_id);
                assert!(matches!(peer.recv().await.message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::RateLimited && error.related_message_kind == MessageKind::ReliableEvent));
            }
        }
        assert!(started.elapsed() < Duration::from_secs(1), "regression must exercise one live publish window");
    }).await.expect("bounded managed QUIC leave/readmission rate regression");
}

#[tokio::test]
async fn lite_managed_runtime_exposes_only_ephemeral_channels_one_and_four() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let (status, node) = http(
            server.admin_address,
            "GET",
            "/v1/node",
            &headers(&server),
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            node["transports"],
            json!({"quic": true, "webTransport": {"enabled": false}})
        );
        assert!(node["transports"]["webTransport"]
            .get("certificateSha256")
            .is_none());
        assert_eq!(
            node["spaces"],
            json!([
                {"spaceId": "1", "epoch": "1", "channelIds": ["1", "4"], "system": true},
                {"spaceId": "2", "epoch": "1", "channelIds": ["1", "4"], "system": true}
            ])
        );
        assert_eq!(
            node["channels"],
            json!([
                {"channelId": "1", "delivery": "ReliableOrdered", "persistence": "Ephemeral", "maxPayloadBytes": 65536},
                {"channelId": "4", "delivery": "UnreliableSequenced", "persistence": "Ephemeral", "maxPayloadBytes": 65536}
            ])
        );

        let token = "f".repeat(64);
        provision(&server, 1, 1, &token).await;
        let mut client = fixture.client(&server, &token).await.unwrap();
        assert_eq!(
            client
                .request_admission(1, 1, 1, "lite-channel".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        let entity = subscribed(&mut client, 1, 1).await;
        client
            .publish_state(1, 1, 1, 1, 2, entity, 1, 1, b"stateful".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            client.recv().await.unwrap().message,
            MessagePayload::Control(ControlPayload::ProtocolError(error))
                if error.code == ProtocolErrorCode::Unauthorized
                    && error.related_message_kind == MessageKind::EntityState
        ));
    })
    .await
    .expect("bounded Lite channel policy check");
}

#[tokio::test]
async fn ccu_one_queue_heartbeat_claim_and_duplicate_requests_use_native_api() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "a".repeat(64);
        provision(&server, 1, 1, &token).await;
        let mut first = fixture.client(&server, &token).await.unwrap();
        let mut second = fixture.client(&server, &token).await.unwrap();
        assert_eq!(
            first
                .request_admission(1, 1, 1, "same-key".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        assert_eq!(
            first
                .request_admission(1, 1, 2, "same-key".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        let queued = second
            .request_admission(1, 1, 1, "same-key".into())
            .await
            .unwrap();
        assert_eq!(queued.status, AdmissionStatus::Queued);
        assert_eq!(queued.poll_after_ms, 1_000);
        assert_eq!(queued.ticket_remaining_ms, 0);
        let ticket = queued.ticket_id.unwrap();
        assert_eq!(
            second
                .request_admission(1, 1, 2, "same-key".into())
                .await
                .unwrap()
                .ticket_id,
            Some(ticket)
        );
        let waiting = second.queue_heartbeat(1, 1, 3, ticket).await.unwrap();
        assert_eq!(waiting.state, QueueState::Waiting);
        assert_eq!(waiting.position, 1);
        assert_eq!(snapshot(&server, 1, 1).await["activeCCU"], 1);
        // Admitted is already joined: subscription succeeds without legacy JoinSession.
        let _ = subscribed(&mut first, 1, 1).await;
        first
            .close_gracefully(Duration::from_secs(2))
            .await
            .unwrap();
        pace().await;
        let offered = second.queue_status(1, 1, 4, ticket).await.unwrap();
        assert_eq!(offered.state, QueueState::Offered);
        assert_eq!(offered.offer_remaining_ms, 0);
        assert_eq!(
            second.queue_claim(1, 1, 5, ticket).await.unwrap().state,
            QueueState::Admitted
        );
        assert_eq!(
            second.queue_claim(1, 1, 6, ticket).await.unwrap().state,
            QueueState::Admitted
        );
        assert_eq!(snapshot(&server, 1, 1).await["activeCCU"], 1);
        let _ = subscribed(&mut second, 1, 1).await;
        second
            .close_gracefully(Duration::from_secs(2))
            .await
            .unwrap();
        pace().await;
        assert_eq!(snapshot(&server, 1, 1).await["activeCCU"], 0);
    })
    .await
    .expect("bounded queue handoff");
}

#[tokio::test]
async fn tickets_are_connection_owned_and_cancel_is_idempotent() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "b".repeat(64);
        provision(&server, 1, 1, &token).await;
        let mut owner = fixture.client(&server, &token).await.unwrap();
        owner
            .request_admission(1, 1, 1, "owner".into())
            .await
            .unwrap();
        let mut waiting = fixture.client(&server, &token).await.unwrap();
        let ticket = waiting
            .request_admission(1, 1, 1, "waiter".into())
            .await
            .unwrap()
            .ticket_id
            .unwrap();
        let mut foreign = fixture.client(&server, &token).await.unwrap();
        assert_eq!(
            foreign.queue_status(1, 1, 1, ticket).await.unwrap().state,
            QueueState::Missing
        );
        assert_eq!(
            foreign
                .queue_heartbeat(1, 1, 2, ticket)
                .await
                .unwrap()
                .state,
            QueueState::Missing
        );
        assert_eq!(
            foreign.queue_claim(1, 1, 3, ticket).await.unwrap().state,
            QueueState::Missing
        );
        assert_eq!(
            foreign.queue_cancel(1, 1, 4, ticket).await.unwrap().state,
            QueueState::Missing
        );
        pace().await;
        assert_eq!(
            foreign.queue_status(1, 1, 5, u64::MAX).await.unwrap().state,
            QueueState::Missing
        );
        assert_eq!(
            waiting.queue_status(1, 1, 2, ticket).await.unwrap().state,
            QueueState::Waiting
        );
        assert_eq!(
            waiting.queue_cancel(1, 1, 3, ticket).await.unwrap().state,
            QueueState::Cancelled
        );
        assert_eq!(
            waiting.queue_cancel(1, 1, 4, ticket).await.unwrap().state,
            QueueState::Cancelled
        );
        assert_eq!(snapshot(&server, 1, 1).await["queueDepth"], 0);
        assert_eq!(snapshot(&server, 1, 1).await["activeCCU"], 1);
    })
    .await
    .expect("bounded ticket ownership");
}

#[tokio::test]
async fn two_products_credentials_scope_and_ordinary_join_fail_closed() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(); let server = fixture.server().await;
        let a = "a".repeat(64); let b = "b".repeat(64);
        provision(&server, 1, 1, &a).await; provision(&server, 2, 2, &b).await;
        for token in [ADMIN, "wrong", "dev-token", "", &"c".repeat(64)] {
            assert!(fixture.client(&server, token).await.is_err());
        }
        for token in ["", "wrong", &a] {
            assert_eq!(http(server.admin_address, "GET", "/v1/node", &format!("Authorization: Bearer {token}\r\n"), "").await.0, 401);
        }
        for (token, namespace, session) in [(&a, 2, 2), (&b, 1, 1), (&a, 1, 2), (&a, 99, 99)] {
            let mut client = fixture.client(&server, token).await.unwrap();
            unauthorized(&client.request_admission(namespace, session, 1, "cross".into()).await.unwrap_err());
        }
        for operation in 0..4 {
            let mut client = fixture.client(&server, &b).await.unwrap();
            let result = match operation {
                0 => client.queue_status(1, 1, 1, 1).await,
                1 => client.queue_heartbeat(1, 1, 1, 1).await,
                2 => client.queue_claim(1, 1, 1, 1).await,
                _ => client.queue_cancel(1, 1, 1, 1).await,
            };
            unauthorized(&result.unwrap_err());
        }
        let mut bypass = fixture.client(&server, &a).await.unwrap();
        bypass.join_session(1, 1).await.unwrap();
        assert!(matches!(bypass.recv().await.unwrap().message, MessagePayload::Control(ControlPayload::ProtocolError(ref e)) if e.code == ProtocolErrorCode::Unauthorized));
        assert_eq!(snapshot(&server, 1, 1).await["activeCCU"], 0);
        assert_eq!(http(server.admin_address, "GET", &path(99, 99), &headers(&server), "").await.0, 404);
        let mut other = fixture.client(&server, &b).await.unwrap();
        assert_eq!(other.request_admission(2, 2, 1, "own".into()).await.unwrap().status, AdmissionStatus::Admitted);
        let _ = subscribed(&mut other, 2, 2).await;
    }).await.expect("bounded scope isolation");
}

#[tokio::test]
async fn delete_closes_admitted_waiting_idle_and_revokes_only_its_token() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        provision(&server, 1, 1, &a).await;
        provision(&server, 2, 2, &b).await;
        let mut admitted = fixture.client(&server, &a).await.unwrap();
        admitted
            .request_admission(1, 1, 1, "first".into())
            .await
            .unwrap();
        let _ = subscribed(&mut admitted, 1, 1).await;
        let mut waiting = fixture.client(&server, &a).await.unwrap();
        assert_eq!(
            waiting
                .request_admission(1, 1, 1, "second".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Queued
        );
        let mut idle = fixture.client(&server, &a).await.unwrap();
        let mut other = fixture.client(&server, &b).await.unwrap();
        let delete_headers = format!("{}If-Match: \"1\"\r\n", headers(&server));
        assert_eq!(
            http(
                server.admin_address,
                "DELETE",
                &path(1, 1),
                &delete_headers,
                ""
            )
            .await
            .0,
            204
        );
        for client in [&mut admitted, &mut waiting, &mut idle] {
            assert!(
                tokio::time::timeout(Duration::from_secs(2), client.recv())
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        assert!(fixture.client(&server, &a).await.is_err());
        assert_eq!(
            http(
                server.admin_address,
                "GET",
                &path(1, 1),
                &headers(&server),
                ""
            )
            .await
            .0,
            404
        );
        assert_eq!(
            other
                .request_admission(2, 2, 1, "unaffected".into())
                .await
                .unwrap()
                .status,
            AdmissionStatus::Admitted
        );
        let _ = subscribed(&mut other, 2, 2).await;
        assert!(fixture.client(&server, &b).await.is_ok());
    })
    .await
    .expect("bounded deletion");
}

#[tokio::test]
async fn bounded_native_helper_claims_and_cancellation_releases_waiter() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(); let server = fixture.server().await;
        let token = "d".repeat(64); provision(&server, 1, 1, &token).await;
        let mut first = fixture.client(&server, &token).await.unwrap();
        first.request_admission(1, 1, 1, "first".into()).await.unwrap();
        let waiting = fixture.client(&server, &token).await.unwrap();
        let cancelled = waiting.admit_with_cancellation(1, 1, "cancel".into(), Duration::from_secs(5), async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }).await;
        assert!(cancelled.is_err());
        pace().await;
        assert_eq!(snapshot(&server, 1, 1).await["queueDepth"], 0);
        let waiting = fixture.client(&server, &token).await.unwrap();
        let close = async { tokio::time::sleep(Duration::from_millis(500)).await; first.close_gracefully(Duration::from_secs(2)).await.unwrap(); };
        let (result, ()) = tokio::join!(waiting.admit_with_cancellation(1, 1, "claim".into(), Duration::from_secs(10), std::future::pending()), close);
        let (mut client, outcome) = result.unwrap();
        assert!(matches!(outcome, ManagedAdmissionOutcome::Queue(value) if value.state == QueueState::Admitted));
        let _ = subscribed(&mut client, 1, 1).await;
    }).await.expect("bounded native helper");
}
