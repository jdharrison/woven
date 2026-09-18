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
                capability_bits: 0,
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
    async fn authenticate(&mut self, token: &str) -> u64 {
        self.send(Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Authenticate(Authenticate {
                scheme: AuthenticationScheme::Bearer,
                credentials: token.as_bytes().to_vec(),
            }),
        ))
        .await;
        let MessagePayload::Control(ControlPayload::Authenticated(value)) =
            self.recv().await.message
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
async fn lite_managed_runtime_exposes_only_ephemeral_channel_one() {
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
            node["spaces"],
            json!([
                {"spaceId": "1", "epoch": "1", "channelIds": ["1"]},
                {"spaceId": "2", "epoch": "1", "channelIds": ["1"]}
            ])
        );
        assert_eq!(
            node["channels"],
            json!([
                {"channelId": "1", "delivery": "ReliableOrdered", "persistence": "Ephemeral", "maxPayloadBytes": 65536}
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
