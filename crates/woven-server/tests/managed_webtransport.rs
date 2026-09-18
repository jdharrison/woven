//! Real managed WVN1 admission over TLS-verified WebTransport on loopback only.

use rustls::pki_types::{CertificateDer, pem::PemObject};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use woven_protocol::{
    AdmissionStatus, Authenticate, AuthenticationScheme, Codec, ControlPayload, DeliveryClass,
    Envelope, Hello, MessagePayload, PROTOCOL_VERSION, ProtocolErrorCode, QueueClaim, QueueState,
    QueueStatusRequest, RequestAdmission, SubscribeSpace,
};
use woven_server::{ManagedServer, ManagedServerConfig, ManagedWebTransportConfig, start_managed};
use wtransport::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, VarInt, endpoint::ConnectOptions,
};

const ADMIN: &str = "managed-webtransport-admin-credential-0123456789";
const ALLOWED_ORIGIN: &str = "https://console.example.test";
const LIMIT: Duration = Duration::from_secs(30);
static NEXT: AtomicU64 = AtomicU64::new(1);

struct Fixture {
    path: PathBuf,
    pem: String,
}

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "woven-managed-webtransport-{}-{}",
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
            webtransport: Some(ManagedWebTransportConfig {
                bind_address: "127.0.0.1:0".parse().unwrap(),
                path: "/managed-webtransport".to_owned(),
                allowed_origins: vec![ALLOWED_ORIGIN.to_owned()],
            }),
        })
        .await
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct WirePeer {
    connection: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl WirePeer {
    async fn connect(
        fixture: &Fixture,
        server: &ManagedServer,
        origin: Option<&str>,
    ) -> Result<Self, String> {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(fixture.pem.as_bytes()).unwrap())
            .unwrap();
        let mut tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
        let client_config = ClientConfig::builder()
            .with_bind_default()
            .with_custom_tls(tls)
            .build();
        let endpoint = Endpoint::client(client_config).map_err(|error| error.to_string())?;
        let url = server.webtransport_url.as_ref().unwrap();
        let options = match origin {
            Some(origin) => ConnectOptions::builder(url)
                .add_header("origin", origin)
                .build(),
            None => ConnectOptions::builder(url).build(),
        };
        let connection = tokio::time::timeout(Duration::from_secs(3), endpoint.connect(options))
            .await
            .map_err(|_| "WebTransport connection timed out".to_owned())?
            .map_err(|error| error.to_string())?;
        let (send, recv) = tokio::time::timeout(Duration::from_secs(3), connection.open_bi())
            .await
            .map_err(|_| "WebTransport stream open timed out".to_owned())?
            .map_err(|error| error.to_string())?
            .await
            .map_err(|error| error.to_string())?;
        let mut peer = Self {
            connection,
            send,
            recv,
        };
        peer.send(Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Hello(Hello {
                min_protocol_version: PROTOCOL_VERSION,
                max_protocol_version: PROTOCOL_VERSION,
                client_name: "managed-webtransport-test".to_owned(),
                client_version: "1".to_owned(),
                capability_bits: 0,
                max_frame_size: 65_536,
                max_payload_size: 65_536,
            }),
        ))
        .await?;
        assert!(matches!(
            peer.recv().await?.message,
            MessagePayload::Control(ControlPayload::Capabilities(_))
        ));
        Ok(peer)
    }

    async fn send(&mut self, envelope: Envelope) -> Result<(), String> {
        let frame = Codec::default()
            .encode(&envelope)
            .map_err(|error| error.to_string())?;
        self.send
            .write_all(&frame)
            .await
            .map_err(|error| error.to_string())
    }

    async fn recv(&mut self) -> Result<Envelope, String> {
        tokio::time::timeout(Duration::from_secs(3), read_envelope(&mut self.recv))
            .await
            .map_err(|_| "WVN1 receive timed out".to_owned())?
    }

    async fn authenticate_with_scheme(
        &mut self,
        token: &str,
        scheme: AuthenticationScheme,
    ) -> Result<Envelope, String> {
        self.send(Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Authenticate(Authenticate {
                scheme,
                credentials: token.as_bytes().to_vec(),
            }),
        ))
        .await?;
        self.recv().await
    }

    async fn authenticate(&mut self, token: &str) -> Result<Envelope, String> {
        self.authenticate_with_scheme(token, AuthenticationScheme::Bearer)
            .await
    }

    fn close(&self) {
        self.connection.close(VarInt::from_u32(0), b"test done");
    }
}

async fn read_envelope(stream: &mut RecvStream) -> Result<Envelope, String> {
    let codec = Codec::default();
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|error| error.to_string())?;
    let length = codec
        .expected_frame_len(&prefix)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "incomplete size prefix".to_owned())?;
    let mut frame = vec![0_u8; length];
    frame[..4].copy_from_slice(&prefix);
    stream
        .read_exact(&mut frame[4..])
        .await
        .map_err(|error| error.to_string())?;
    codec.decode(&frame).map_err(|error| error.to_string())
}

fn scoped(control: ControlPayload, namespace: u64, session: u64, correlation: u64) -> Envelope {
    let mut envelope = Envelope::control(DeliveryClass::ReliableOrdered, control);
    envelope.namespace_id = namespace;
    envelope.session_id = session;
    envelope.correlation_id = Some(correlation);
    envelope
}

fn subscription(namespace: u64, session: u64) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: DeliveryClass::ReliableOrdered,
        namespace_id: namespace,
        session_id: session,
        space_id: 1,
        channel_id: Some(1),
        entity_id: None,
        space_epoch: 1,
        server_tick: 0,
        sender_sequence: 0,
        correlation_id: None,
        message: MessagePayload::Control(ControlPayload::SubscribeSpace(SubscribeSpace)),
    }
}

fn certificate_sha256(pem: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let certificate = CertificateDer::from_pem_slice(pem.as_bytes()).unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, certificate.as_ref());
    let mut hex = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 15)]));
    }
    hex
}

async fn authenticate(peer: &mut WirePeer, token: &str) {
    assert!(matches!(
        peer.authenticate(token).await.unwrap().message,
        MessagePayload::Control(ControlPayload::Authenticated(_))
    ));
}

async fn request_admission(peer: &mut WirePeer, key: &str, correlation: u64) -> Envelope {
    peer.send(scoped(
        ControlPayload::RequestAdmission(RequestAdmission {
            idempotency_key: key.to_owned(),
        }),
        1,
        1,
        correlation,
    ))
    .await
    .unwrap();
    peer.recv().await.unwrap()
}

async fn assert_subscribed_without_join(peer: &mut WirePeer) {
    peer.send(subscription(1, 1)).await.unwrap();
    assert!(matches!(
        peer.recv().await.unwrap().message,
        MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))
    ));
    assert!(matches!(
        peer.recv().await.unwrap().message,
        MessagePayload::Control(ControlPayload::EntityEntered(_))
    ));
}

async fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &str,
    body: &str,
) -> (u16, Value) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        stream.take(65_536).read_to_end(&mut bytes).await.unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        let value = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(body).unwrap()
        };
        (status, value)
    })
    .await
    .expect("bounded HTTP request")
}

fn admin_headers(server: &ManagedServer) -> String {
    format!(
        "Authorization: Bearer {ADMIN}\r\nContent-Type: application/json\r\nWoven-Node-Incarnation: {}\r\n",
        server.node_incarnation
    )
}

async fn provision(server: &ManagedServer, allocated_ccu: u32, token: &str) {
    let (status, _) = http(
        server.admin_address,
        "PUT",
        "/v1/namespaces/1/sessions/1",
        &admin_headers(server),
        &json!({
            "revision": "1",
            "allocatedCCU": allocated_ccu,
            "clientToken": token
        })
        .to_string(),
    )
    .await;
    assert_eq!(status, 201);
}

#[tokio::test]
async fn capabilities_bearer_direct_admission_and_atomic_subscription() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let address = server.webtransport_address.unwrap();
        let expected_url = format!("https://{address}/managed-webtransport");
        assert_eq!(
            server.webtransport_url.as_deref(),
            Some(expected_url.as_str())
        );
        let (status, capabilities) =
            http(server.management_address, "GET", "/v1/capabilities", "", "").await;
        assert_eq!(status, 200);
        assert_eq!(capabilities["transports"], json!(["quic", "webtransport"]));
        assert_eq!(
            capabilities["webtransport"],
            format!("{}/managed-webtransport", address.port())
        );
        let (status, node) = http(
            server.admin_address,
            "GET",
            "/v1/node",
            &admin_headers(&server),
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            node["transports"],
            json!({
                "quic": true,
                "webTransport": {
                    "enabled": true,
                    "certificateSha256": certificate_sha256(&fixture.pem)
                }
            })
        );
        assert!(node.get("webTransportAddress").is_none());
        assert!(node.get("webTransportUrl").is_none());

        let token = "a".repeat(64);
        provision(&server, 1, &token).await;
        let mut peer = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        authenticate(&mut peer, &token).await;
        let admission = request_admission(&mut peer, "direct", 1).await;
        assert!(matches!(
            admission.message,
            MessagePayload::Control(ControlPayload::AdmissionResult(result))
                if result.status == AdmissionStatus::Admitted && result.ticket_id.is_none()
        ));
        assert_subscribed_without_join(&mut peer).await;
    })
    .await
    .expect("bounded direct WebTransport admission");
}

#[tokio::test]
async fn managed_webtransport_rejects_development_authentication_scheme() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "f".repeat(64);
        provision(&server, 1, &token).await;
        let mut peer = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        let response = peer
            .authenticate_with_scheme(&token, AuthenticationScheme::Development)
            .await
            .unwrap();
        assert!(matches!(
            response.message,
            MessagePayload::Control(ControlPayload::ProtocolError(error))
                if error.code == ProtocolErrorCode::Unauthorized
        ));
    })
    .await
    .expect("bounded managed WebTransport scheme rejection");
}

#[tokio::test]
async fn queued_connection_claims_and_subscribes_after_disconnect() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let token = "b".repeat(64);
        provision(&server, 1, &token).await;
        let mut first = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        let mut waiting = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        authenticate(&mut first, &token).await;
        authenticate(&mut waiting, &token).await;
        assert!(matches!(
            request_admission(&mut first, "first", 1).await.message,
            MessagePayload::Control(ControlPayload::AdmissionResult(result))
                if result.status == AdmissionStatus::Admitted
        ));
        let queued = request_admission(&mut waiting, "waiting", 1).await;
        let MessagePayload::Control(ControlPayload::AdmissionResult(result)) = queued.message
        else {
            panic!("expected queued admission result")
        };
        assert_eq!(result.status, AdmissionStatus::Queued);
        let ticket = result.ticket_id.unwrap();
        first.close();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        waiting
            .send(scoped(
                ControlPayload::QueueStatusRequest(QueueStatusRequest { ticket_id: ticket }),
                1,
                1,
                2,
            ))
            .await
            .unwrap();
        assert!(matches!(
            waiting.recv().await.unwrap().message,
            MessagePayload::Control(ControlPayload::QueueUpdate(update))
                if update.state == QueueState::Offered && update.ticket_id == ticket
        ));
        waiting
            .send(scoped(
                ControlPayload::QueueClaim(QueueClaim { ticket_id: ticket }),
                1,
                1,
                3,
            ))
            .await
            .unwrap();
        assert!(matches!(
            waiting.recv().await.unwrap().message,
            MessagePayload::Control(ControlPayload::QueueUpdate(update))
                if update.state == QueueState::Admitted && update.ticket_id == ticket
        ));
        assert_subscribed_without_join(&mut waiting).await;
    })
    .await
    .expect("bounded WebTransport queue handoff");
}

#[tokio::test]
async fn dropping_managed_server_closes_webtransport_sessions() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        let mut peer = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        drop(server);
        assert!(peer.recv().await.is_err());
    })
    .await
    .expect("bounded managed WebTransport shutdown");
}

#[tokio::test]
async fn wrong_origin_token_scope_and_deleted_scope_fail_closed() {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new();
        let server = fixture.server().await;
        assert!(
            WirePeer::connect(&fixture, &server, Some("https://evil.example.test"))
                .await
                .is_err()
        );
        assert!(WirePeer::connect(&fixture, &server, None).await.is_err());

        let token = "c".repeat(64);
        provision(&server, 2, &token).await;
        let mut wrong_token = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        assert!(matches!(
            wrong_token.authenticate("wrong-token").await.unwrap().message,
            MessagePayload::Control(ControlPayload::ProtocolError(error))
                if error.code == ProtocolErrorCode::Unauthorized
        ));

        let mut wrong_scope = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        authenticate(&mut wrong_scope, &token).await;
        wrong_scope
            .send(scoped(
                ControlPayload::RequestAdmission(RequestAdmission {
                    idempotency_key: "wrong-scope".to_owned(),
                }),
                1,
                2,
                1,
            ))
            .await
            .unwrap();
        assert!(matches!(
            wrong_scope.recv().await.unwrap().message,
            MessagePayload::Control(ControlPayload::ProtocolError(error))
                if error.code == ProtocolErrorCode::Unauthorized
        ));

        let mut admitted = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        authenticate(&mut admitted, &token).await;
        assert!(matches!(
            request_admission(&mut admitted, "teardown", 1).await.message,
            MessagePayload::Control(ControlPayload::AdmissionResult(result))
                if result.status == AdmissionStatus::Admitted
        ));
        let delete_headers = format!("{}If-Match: \"1\"\r\n", admin_headers(&server));
        assert_eq!(
            http(
                server.admin_address,
                "DELETE",
                "/v1/namespaces/1/sessions/1",
                &delete_headers,
                "",
            )
            .await
            .0,
            204
        );
        assert!(admitted.recv().await.is_err());
        let mut revoked = WirePeer::connect(&fixture, &server, Some(ALLOWED_ORIGIN))
            .await
            .unwrap();
        assert!(matches!(
            revoked.authenticate(&token).await.unwrap().message,
            MessagePayload::Control(ControlPayload::ProtocolError(error))
                if error.code == ProtocolErrorCode::Unauthorized
        ));
    })
    .await
    .expect("bounded WebTransport rejection and teardown checks");
}
