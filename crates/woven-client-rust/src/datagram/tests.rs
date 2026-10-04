use std::{future::pending, net::SocketAddr, sync::Arc, time::Duration};

use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use woven_protocol::{
    Authenticated, CAPABILITY_CLIENT_LOG, CAPABILITY_POSITIONED_ENTITY_STATE, Capabilities,
    ControlPayload, JoinSession, MessageKind, Pong, RoutingPosition3D,
};

use super::*;
use crate::{ClientConfig, UrlScheme, stream::StreamReadState};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_TIMEOUT: Duration = Duration::from_millis(5);

// The existing wtransport dependency supplies test identities; no dev dependency
// or external Woven server is needed for these transport-boundary fixtures.
enum Listener {
    Quic(quinn::Endpoint),
    WebTransport(wtransport::Endpoint<wtransport::endpoint::endpoint_side::Server>),
}

impl Listener {
    fn bind(scheme: UrlScheme, datagrams_enabled: bool) -> Self {
        let identity = wtransport::Identity::self_signed(["localhost", "127.0.0.1"]).unwrap();
        let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        match scheme {
            UrlScheme::Quic => {
                let certificates = identity
                    .certificate_chain()
                    .as_slice()
                    .iter()
                    .map(|certificate| CertificateDer::from(certificate.der().to_vec()))
                    .collect();
                let key = PrivatePkcs8KeyDer::from(identity.private_key().secret_der().to_vec());
                let mut config =
                    quinn::ServerConfig::with_single_cert(certificates, key.into()).unwrap();
                let mut transport = quinn::TransportConfig::default();
                if !datagrams_enabled {
                    transport.datagram_receive_buffer_size(None);
                }
                config.transport_config(Arc::new(transport));
                Self::Quic(quinn::Endpoint::server(config, address).unwrap())
            }
            UrlScheme::WebTransport => {
                assert!(datagrams_enabled);
                let config = wtransport::ServerConfig::builder()
                    .with_bind_address(address)
                    .with_identity(identity)
                    .build();
                Self::WebTransport(wtransport::Endpoint::server(config).unwrap())
            }
        }
    }

    fn url(&self) -> String {
        match self {
            Self::Quic(endpoint) => format!("quic://{}", endpoint.local_addr().unwrap()),
            Self::WebTransport(endpoint) => {
                format!(
                    "wtransport://{}/webtransport",
                    endpoint.local_addr().unwrap()
                )
            }
        }
    }

    async fn accept(self) -> Peer {
        match self {
            Self::Quic(endpoint) => {
                let connection = endpoint.accept().await.unwrap().await.unwrap();
                let (send, recv) = connection.accept_bi().await.unwrap();
                Peer::Quic {
                    _endpoint: endpoint,
                    connection,
                    send,
                    recv,
                }
            }
            Self::WebTransport(endpoint) => {
                let request = endpoint.accept().await.await.unwrap();
                let connection = request.accept().await.unwrap();
                let (send, recv) = connection.accept_bi().await.unwrap();
                Peer::WebTransport {
                    _endpoint: endpoint,
                    connection,
                    send,
                    recv,
                }
            }
        }
    }
}

enum Peer {
    Quic {
        _endpoint: quinn::Endpoint,
        connection: quinn::Connection,
        send: quinn::SendStream,
        recv: quinn::RecvStream,
    },
    WebTransport {
        _endpoint: wtransport::Endpoint<wtransport::endpoint::endpoint_side::Server>,
        connection: wtransport::Connection,
        send: wtransport::SendStream,
        recv: wtransport::RecvStream,
    },
}

impl Peer {
    async fn recv_control(&mut self) -> Envelope {
        let mut read_state = StreamReadState::default();
        let codec = Codec::default();
        tokio::time::timeout(TEST_TIMEOUT, async {
            match self {
                Self::Quic { recv, .. } => read_state.recv(recv, &codec).await,
                Self::WebTransport { recv, .. } => read_state.recv(recv, &codec).await,
            }
        })
        .await
        .unwrap()
        .unwrap()
    }

    async fn write_control_bytes(&mut self, bytes: &[u8]) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            match self {
                Self::Quic { send, .. } => send.write_all(bytes).await.unwrap(),
                Self::WebTransport { send, .. } => send.write_all(bytes).await.unwrap(),
            }
        })
        .await
        .unwrap();
    }

    async fn send_control(&mut self, envelope: &Envelope) {
        self.write_control_bytes(&Codec::default().encode(envelope).unwrap())
            .await;
    }

    fn send_datagram(&self, bytes: Vec<u8>) {
        match self {
            Self::Quic { connection, .. } => connection.send_datagram(bytes.into()).unwrap(),
            Self::WebTransport { connection, .. } => connection.send_datagram(bytes).unwrap(),
        }
    }

    async fn recv_datagram(&self) -> Vec<u8> {
        tokio::time::timeout(TEST_TIMEOUT, async {
            match self {
                Self::Quic { connection, .. } => connection.read_datagram().await.unwrap().to_vec(),
                Self::WebTransport { connection, .. } => connection
                    .receive_datagram()
                    .await
                    .unwrap()
                    .payload()
                    .to_vec(),
            }
        })
        .await
        .unwrap()
    }
}

async fn fixture(
    scheme: UrlScheme,
    config: ClientConfig,
    server_payload_limit: u32,
    datagrams_enabled: bool,
) -> (Client, Peer) {
    fixture_with_capabilities(scheme, config, server_payload_limit, datagrams_enabled, 0).await
}

async fn fixture_with_capabilities(
    scheme: UrlScheme,
    config: ClientConfig,
    server_payload_limit: u32,
    datagrams_enabled: bool,
    server_capability_bits: u64,
) -> (Client, Peer) {
    let listener = Listener::bind(scheme, datagrams_enabled);
    let config = ClientConfig {
        url: listener.url(),
        token: "fixture-token".to_owned(),
        ..config
    };
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (client, peer) = tokio::join!(Client::connect(config), async {
            let mut peer = listener.accept().await;
            assert_eq!(peer.recv_control().await.message_kind(), MessageKind::Hello);
            peer.send_control(&Envelope::control(
                DeliveryClass::ReliableOrdered,
                ControlPayload::Capabilities(Capabilities {
                    selected_protocol_version: PROTOCOL_VERSION,
                    server_name: "fixture".to_owned(),
                    server_version: "1".to_owned(),
                    capability_bits: server_capability_bits,
                    max_frame_size: 1_048_576,
                    max_payload_size: server_payload_limit,
                }),
            ))
            .await;
            assert_eq!(
                peer.recv_control().await.message_kind(),
                MessageKind::Authenticate
            );
            peer.send_control(&Envelope::control(
                DeliveryClass::ReliableOrdered,
                ControlPayload::Authenticated(Authenticated {
                    principal_id: 1,
                    assigned_entity_id: None,
                }),
            ))
            .await;
            peer
        });
        (client.unwrap(), peer)
    })
    .await
    .unwrap()
}

fn state(payload: Vec<u8>) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: DeliveryClass::UnreliableSequenced,
        namespace_id: 1,
        session_id: 2,
        space_id: 3,
        channel_id: Some(4),
        entity_id: Some(6),
        space_epoch: 5,
        server_tick: 0,
        sender_sequence: 7,
        correlation_id: None,
        routing_position: None,
        message: MessagePayload::EntityState(OpaquePayload {
            type_id: 8,
            bytes: payload,
        }),
    }
}

fn publish(client: &Client, payload: Vec<u8>) -> Result<(), ClientError> {
    client.publish_unreliable_state(1, 2, 3, 5, 4, 6, 7, 8, payload)
}

fn datagram_budget(client: &Client) -> usize {
    match &client.transport {
        Transport::Quic { connection, .. } => connection.max_datagram_size().unwrap(),
        Transport::WebTransport { connection, .. } => connection.max_datagram_size().unwrap(),
    }
}

#[tokio::test]
async fn logger_remembers_join_and_sends_once_without_persistence_ack() {
    use woven_protocol::LogLevel;
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        let (mut client, mut peer) = fixture_with_capabilities(
            scheme,
            ClientConfig::default(),
            65_536,
            true,
            CAPABILITY_CLIENT_LOG,
        )
        .await;
        assert!(client.logger().info("before join").await.is_err());
        assert!(client.join_session(0, 22).await.is_err());
        assert!(client.log("still not joined").await.is_err());
        client.join_session(11, 22).await.unwrap();
        assert_eq!(
            peer.recv_control().await.message_kind(),
            MessageKind::JoinSession
        );
        for message in ["", &"é".repeat(513), &format!("{}x", "🧶".repeat(256))] {
            assert!(client.log(message).await.is_err());
        }
        client.logger().info("info").await.unwrap();
        assert_client_log(peer.recv_control().await, LogLevel::Info, "info");
        client.logger().warn("é".repeat(512)).await.unwrap();
        assert_client_log(peer.recv_control().await, LogLevel::Warn, &"é".repeat(512));
        client.logger().error("🧶".repeat(256)).await.unwrap();
        assert_client_log(
            peer.recv_control().await,
            LogLevel::Error,
            &"🧶".repeat(256),
        );
        client.log("alias").await.unwrap();
        assert_client_log(peer.recv_control().await, LogLevel::Info, "alias");
        client.leave_session("done").await.unwrap();
        assert_eq!(
            peer.recv_control().await.message_kind(),
            MessageKind::LeaveSession
        );
        assert!(client.log("after leave").await.is_err());
        client.close().unwrap();
    }
}

#[tokio::test]
async fn logger_without_negotiated_capability_sends_no_bytes_and_keeps_session_usable() {
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        for capability_bits in [0, CAPABILITY_POSITIONED_ENTITY_STATE] {
            let (mut client, mut peer) = fixture_with_capabilities(
                scheme,
                ClientConfig::default(),
                65_536,
                true,
                capability_bits,
            )
            .await;
            client.join_session(11, 22).await.unwrap();
            assert_eq!(
                peer.recv_control().await.message_kind(),
                MessageKind::JoinSession
            );
            for result in [
                client.logger().info("unsupported").await,
                client.logger().warn("unsupported").await,
                client.logger().error("unsupported").await,
                client.log("unsupported").await,
            ] {
                assert!(matches!(
                    result,
                    Err(ClientError::UnsupportedCapability("ClientLog"))
                ));
            }
            assert_eq!(client.session_scope, Some((11, 22)));
            // Probe one byte, not a complete frame, so partial writes cannot pass unnoticed.
            assert!(
                tokio::time::timeout(POLL_TIMEOUT, async {
                    let mut byte = [0_u8; 1];
                    match &mut peer {
                        Peer::Quic { recv, .. } => {
                            recv.read_exact(&mut byte).await.unwrap();
                        }
                        Peer::WebTransport { recv, .. } => {
                            recv.read_exact(&mut byte).await.unwrap();
                        }
                    }
                })
                .await
                .is_err(),
                "unsupported logs must not write any bytes or close the connection"
            );
            client
                .publish_event(11, 22, 3, 1, 1, 6, 1, 8, vec![42])
                .await
                .unwrap();
            let event = peer.recv_control().await;
            assert_eq!(event.message_kind(), MessageKind::ReliableEvent);
            assert_eq!(event.payload_bytes(), &[42]);
            peer.send_control(&event).await;
            assert_eq!(client.recv().await.unwrap(), event);
            client.leave_session("done").await.unwrap();
            let leave = peer.recv_control().await;
            assert_eq!(leave.message_kind(), MessageKind::LeaveSession);
            assert_eq!((leave.namespace_id, leave.session_id), (11, 22));
            client.close().unwrap();
        }
    }
}

fn assert_client_log(envelope: Envelope, level: woven_protocol::LogLevel, message: &str) {
    assert_eq!(envelope.namespace_id, 11);
    assert_eq!(envelope.session_id, 22);
    assert_eq!(envelope.space_id, 0);
    assert_eq!(envelope.channel_id, None);
    assert_eq!(envelope.entity_id, None);
    assert_eq!(envelope.space_epoch, 0);
    assert_eq!(envelope.delivery_class, DeliveryClass::ReliableOrdered);
    let actual = envelope.message;
    assert_eq!(
        actual,
        MessagePayload::Control(ControlPayload::ClientLog(woven_protocol::ClientLog {
            level,
            message: message.to_owned(),
        }))
    );
}

#[tokio::test]
async fn logger_honors_negotiated_limits_and_observed_join_rejection() {
    use woven_protocol::{ProtocolError, ProtocolErrorCode};
    let (mut client, mut peer) = fixture_with_capabilities(
        UrlScheme::Quic,
        ClientConfig::default(),
        64,
        true,
        CAPABILITY_CLIENT_LOG,
    )
    .await;
    client.join_session(11, 22).await.unwrap();
    peer.recv_control().await;
    assert!(client.log("é".repeat(33)).await.is_err());
    client.log("valid").await.unwrap();
    assert_client_log(
        peer.recv_control().await,
        woven_protocol::LogLevel::Info,
        "valid",
    );
    let mut rejection = Envelope::control(
        DeliveryClass::ReliableOrdered,
        ControlPayload::ProtocolError(ProtocolError {
            code: ProtocolErrorCode::Unauthorized,
            related_message_kind: MessageKind::JoinSession,
            message: "rejected".to_owned(),
        }),
    );
    rejection.namespace_id = 11;
    rejection.session_id = 22;
    peer.send_control(&rejection).await;
    client.recv().await.unwrap();
    assert!(client.log("rejected session").await.is_err());
    client.close().unwrap();
}

#[tokio::test]
async fn logger_remembers_verified_admission_and_queue_claim() {
    use woven_protocol::{
        AdmissionRejectionCode, AdmissionResult, AdmissionStatus, LogLevel, QueueState, QueueUpdate,
    };
    for status in [
        AdmissionStatus::Admitted,
        AdmissionStatus::Queued,
        AdmissionStatus::Paused,
        AdmissionStatus::Rejected,
    ] {
        let (mut client, mut peer) = fixture_with_capabilities(
            UrlScheme::Quic,
            ClientConfig::default(),
            65_536,
            true,
            CAPABILITY_CLIENT_LOG,
        )
        .await;
        let (result, ()) = tokio::join!(
            client.request_admission(11, 22, 1, "key".to_owned()),
            async {
                let request = peer.recv_control().await;
                let mut reply = Envelope::control(
                    DeliveryClass::ReliableOrdered,
                    ControlPayload::AdmissionResult(AdmissionResult {
                        status,
                        rejection_code: if status == AdmissionStatus::Rejected {
                            AdmissionRejectionCode::QueueFull
                        } else {
                            AdmissionRejectionCode::None
                        },
                        ticket_id: (status == AdmissionStatus::Queued).then_some(7),
                        poll_after_ms: 0,
                        ticket_remaining_ms: 0,
                    }),
                );
                reply.namespace_id = request.namespace_id;
                reply.session_id = request.session_id;
                reply.correlation_id = request.correlation_id;
                peer.send_control(&reply).await;
            }
        );
        assert_eq!(result.unwrap().status, status);
        if status == AdmissionStatus::Admitted {
            client.log("admitted").await.unwrap();
            assert_client_log(peer.recv_control().await, LogLevel::Info, "admitted");
        } else {
            assert!(client.log("not admitted").await.is_err());
        }
        if status == AdmissionStatus::Queued {
            let (result, ()) = tokio::join!(client.queue_claim(11, 22, 2, 7), async {
                let request = peer.recv_control().await;
                let mut reply = Envelope::control(
                    DeliveryClass::ReliableOrdered,
                    ControlPayload::QueueUpdate(QueueUpdate {
                        ticket_id: 7,
                        state: QueueState::Admitted,
                        position: 0,
                        poll_after_ms: 0,
                        ticket_remaining_ms: 0,
                        offer_remaining_ms: 0,
                    }),
                );
                reply.namespace_id = request.namespace_id;
                reply.session_id = request.session_id;
                reply.correlation_id = request.correlation_id;
                peer.send_control(&reply).await;
            });
            assert_eq!(result.unwrap().state, QueueState::Admitted);
            client.logger().warn("claimed").await.unwrap();
            assert_client_log(peer.recv_control().await, LogLevel::Warn, "claimed");
        }
        client.close().unwrap();
    }
}

#[tokio::test]
async fn logger_remembers_scope_returned_by_the_admission_runner() {
    use woven_protocol::{
        AdmissionRejectionCode, AdmissionResult, AdmissionStatus, LogLevel, QueueState, QueueUpdate,
    };
    for queued in [false, true] {
        let (client, mut peer) = fixture_with_capabilities(
            UrlScheme::Quic,
            ClientConfig::default(),
            65_536,
            true,
            CAPABILITY_CLIENT_LOG,
        )
        .await;
        let (result, ()) = tokio::join!(
            client.admit_with_cancellation(11, 22, "runner".to_owned(), TEST_TIMEOUT, pending()),
            async {
                for step in 0..if queued { 3 } else { 1 } {
                    let request = peer.recv_control().await;
                    let control = if step == 0 {
                        ControlPayload::AdmissionResult(AdmissionResult {
                            status: if queued {
                                AdmissionStatus::Queued
                            } else {
                                AdmissionStatus::Admitted
                            },
                            rejection_code: AdmissionRejectionCode::None,
                            ticket_id: queued.then_some(7),
                            poll_after_ms: 0,
                            ticket_remaining_ms: 0,
                        })
                    } else {
                        ControlPayload::QueueUpdate(QueueUpdate {
                            ticket_id: 7,
                            state: if step == 1 {
                                QueueState::Offered
                            } else {
                                QueueState::Admitted
                            },
                            position: 0,
                            poll_after_ms: 0,
                            ticket_remaining_ms: 0,
                            offer_remaining_ms: 0,
                        })
                    };
                    let mut reply = Envelope::control(DeliveryClass::ReliableOrdered, control);
                    reply.namespace_id = request.namespace_id;
                    reply.session_id = request.session_id;
                    reply.correlation_id = request.correlation_id;
                    peer.send_control(&reply).await;
                }
            }
        );
        let (mut client, _) = result.unwrap();
        client.logger().error("runner admitted").await.unwrap();
        assert_client_log(
            peer.recv_control().await,
            LogLevel::Error,
            "runner admitted",
        );
        client.close().unwrap();
    }
}

#[test]
fn mtu_check_uses_the_whole_frame_and_accepts_the_boundary() {
    assert!(check_datagram_size(1_200, Some(1_200)).is_ok());
    let error = check_datagram_size(1_201, Some(1_200)).unwrap_err();
    assert!(error.to_string().contains("1201"));
    assert!(error.to_string().contains("1200"));
    assert!(matches!(
        check_datagram_size(1, None),
        Err(ClientError::Transport(_))
    ));
}

#[tokio::test]
async fn positioned_state_requires_negotiation_and_encodes_on_both_lanes() {
    let position = RoutingPosition3D {
        x: -1.0,
        y: 2.0,
        z: 3.5,
    };
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        let (mut unsupported, _peer) = fixture(scheme, ClientConfig::default(), 65_536, true).await;
        assert!(!unsupported.supports_positioned_state());
        assert!(matches!(
            unsupported
                .publish_positioned_state(1, 2, 3, 1, 1, 6, 1, 8, position, vec![1])
                .await,
            Err(ClientError::UnsupportedCapability(_))
        ));
        assert!(matches!(
            unsupported.publish_unreliable_positioned_state(
                1,
                2,
                3,
                1,
                4,
                6,
                1,
                8,
                position,
                vec![1]
            ),
            Err(ClientError::UnsupportedCapability(_))
        ));
        unsupported.close().unwrap();

        let (mut client, mut peer) = fixture_with_capabilities(
            scheme,
            ClientConfig::default(),
            65_536,
            true,
            CAPABILITY_POSITIONED_ENTITY_STATE,
        )
        .await;
        assert!(client.supports_positioned_state());
        client
            .publish_positioned_state(1, 2, 3, 1, 1, 6, 1, 8, position, vec![1, 2])
            .await
            .unwrap();
        let reliable = peer.recv_control().await;
        assert_eq!(reliable.delivery_class, DeliveryClass::LatestValue);
        assert_eq!(reliable.routing_position, Some(position));

        client
            .publish_unreliable_positioned_state(1, 2, 3, 1, 4, 6, 2, 8, position, vec![3, 4])
            .unwrap();
        let unreliable = Codec::default()
            .decode(&peer.recv_datagram().await)
            .unwrap();
        assert_eq!(
            unreliable.delivery_class,
            DeliveryClass::UnreliableSequenced
        );
        assert_eq!(unreliable.routing_position, Some(position));
        client.close().unwrap();
    }
}

async fn assert_publish_and_independent_receive(scheme: UrlScheme) {
    let (mut client, mut peer) = fixture(scheme, ClientConfig::default(), 65_536, true).await;
    let payload = vec![42; 40];
    publish(&client, payload.clone()).unwrap();
    let submitted = Codec::default()
        .decode(&peer.recv_datagram().await)
        .unwrap();
    assert_eq!(submitted, state(payload.clone()));

    let mut receiver = client.take_datagram_receiver().unwrap();
    assert!(client.take_datagram_receiver().is_err());
    let control = Envelope::control(
        DeliveryClass::ReliableUnordered,
        ControlPayload::Pong(Pong {
            nonce: 1,
            sender_time_micros: 2,
            responder_time_micros: 3,
        }),
    );
    let control_frame = Codec::default().encode(&control).unwrap();
    peer.write_control_bytes(&control_frame[..4]).await;
    {
        let control_receive = client.recv();
        tokio::pin!(control_receive);
        tokio::select! {
            result = &mut control_receive => panic!("partial control frame completed: {result:?}"),
            () = tokio::time::sleep(POLL_TIMEOUT) => {}
        }
        peer.send_datagram(Codec::default().encode(&state(payload)).unwrap());
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await.unwrap().unwrap(),
            state(vec![42; 40])
        );
        peer.write_control_bytes(&control_frame[4..]).await;
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, control_receive)
                .await
                .unwrap()
                .unwrap(),
            control
        );
    }
    // Publishing must not leave a duplicate/fallback frame on the control stream.
    client.join_session(1, 2).await.unwrap();
    assert_eq!(
        peer.recv_control().await.message_kind(),
        MessageKind::JoinSession
    );
    client.close_gracefully(TEST_TIMEOUT).await.unwrap();
    assert!(receiver.recv_timeout(TEST_TIMEOUT).await.is_err());
}

#[tokio::test]
async fn quic_publish_and_receive_are_independent_of_control_framing() {
    assert_publish_and_independent_receive(UrlScheme::Quic).await;
}

#[tokio::test]
async fn webtransport_publish_and_receive_are_independent_of_control_framing() {
    assert_publish_and_independent_receive(UrlScheme::WebTransport).await;
}

async fn assert_fragmented_control_survives_polling(scheme: UrlScheme) {
    let (mut client, mut peer) = fixture(scheme, ClientConfig::default(), 65_536, true).await;
    let mut datagrams = client.take_datagram_receiver().unwrap();
    let mut reliable = state(vec![0x57; 48]);
    reliable.delivery_class = DeliveryClass::ReliableOrdered;
    reliable.message = MessagePayload::ReliableEvent(OpaquePayload {
        type_id: 8,
        bytes: vec![0x57; 48],
    });
    let codec = Codec::default();
    let frame = codec.encode(&reliable).unwrap();
    let mut offset = 0;
    for end in [1, 3, 4, 13, frame.len() / 2] {
        peer.write_control_bytes(&frame[offset..end]).await;
        let pose = state(vec![42; 40]);
        peer.send_datagram(codec.encode(&pose).unwrap());
        let (control, pose_result) = tokio::join!(
            client.recv_timeout(Duration::from_millis(1)),
            datagrams.recv_timeout(TEST_TIMEOUT),
        );
        assert!(control.unwrap().is_none());
        assert_eq!(pose_result.unwrap().unwrap(), pose);
        offset = end;
    }
    // External cancellation must preserve the same partial frame, too.
    assert!(
        tokio::time::timeout(POLL_TIMEOUT, client.recv())
            .await
            .is_err()
    );
    let mut next = reliable.clone();
    next.sender_sequence += 1;
    peer.write_control_bytes(&frame[offset..]).await;
    peer.send_control(&next).await;
    assert_eq!(
        client.recv_timeout(TEST_TIMEOUT).await.unwrap().unwrap(),
        reliable
    );
    assert_eq!(
        client.recv_timeout(TEST_TIMEOUT).await.unwrap().unwrap(),
        next
    );
    assert!(
        client
            .recv_timeout(Duration::from_millis(1))
            .await
            .unwrap()
            .is_none()
    );
    client.close_gracefully(TEST_TIMEOUT).await.unwrap();
}

#[tokio::test]
async fn quic_control_prefix_and_body_survive_timeout_with_concurrent_datagrams() {
    assert_fragmented_control_survives_polling(UrlScheme::Quic).await;
}

#[tokio::test]
async fn webtransport_control_prefix_and_body_survive_timeout_with_concurrent_datagrams() {
    assert_fragmented_control_survives_polling(UrlScheme::WebTransport).await;
}

#[tokio::test]
async fn receiver_timeout_and_cancellation_do_not_steal_the_next_packet() {
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        let (mut client, peer) = fixture(scheme, ClientConfig::default(), 65_536, true).await;
        let mut receiver = client.take_datagram_receiver().unwrap();
        for _ in 0..3 {
            assert!(receiver.recv_timeout(POLL_TIMEOUT).await.unwrap().is_none());
        }
        tokio::select! {
            result = receiver.recv() => panic!("unexpected datagram: {result:?}"),
            () = tokio::time::sleep(POLL_TIMEOUT) => {}
        }
        let envelope = state(vec![1; 40]);
        peer.send_datagram(Codec::default().encode(&envelope).unwrap());
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await.unwrap().unwrap(),
            envelope
        );
        let (closed, stopped) = tokio::join!(
            client.close_gracefully(TEST_TIMEOUT),
            receiver.recv_timeout(TEST_TIMEOUT),
        );
        closed.unwrap();
        assert!(stopped.is_err());
    }
}

#[tokio::test]
async fn outgoing_payload_caps_and_full_frame_mtu_rejections_leave_control_usable() {
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        for (configured, advertised, expected) in
            [(100_000, 100_000, 65_536), (128, 64, 64), (64, 128, 64)]
        {
            let (mut client, mut peer) = fixture(
                scheme,
                ClientConfig {
                    max_payload_bytes: configured,
                    ..ClientConfig::default()
                },
                advertised,
                true,
            )
            .await;
            assert!(matches!(
                publish(&client, vec![0; expected + 1]),
                Err(ClientError::Protocol(CodecError::PayloadTooLarge { actual, maximum }))
                    if actual == expected + 1 && maximum == expected
            ));
            if expected < 65_536 {
                publish(&client, vec![0; expected]).unwrap();
                assert_eq!(
                    Codec::default()
                        .decode(&peer.recv_datagram().await)
                        .unwrap()
                        .payload_bytes(),
                    vec![0; expected]
                );
            }
            client.join_session(1, 2).await.unwrap();
            assert_eq!(
                peer.recv_control().await.message_kind(),
                MessageKind::JoinSession
            );
            client.close().unwrap();
        }
        let (mut client, mut peer) = fixture(scheme, ClientConfig::default(), 65_536, true).await;
        let maximum = datagram_budget(&client);
        assert!(maximum < 65_536);
        let error = publish(&client, vec![0; maximum]).unwrap_err();
        assert!(matches!(error, ClientError::Transport(_)));
        assert!(
            error
                .to_string()
                .contains(&format!("transport limit {maximum}"))
        );
        assert!(matches!(
            client.publish_unreliable_state(0, 2, 3, 5, 4, 6, 7, 8, vec![0]),
            Err(ClientError::Protocol(CodecError::InvalidSemantics { .. }))
        ));
        publish(&client, vec![9; 40]).unwrap();
        assert_eq!(
            Codec::default()
                .decode(&peer.recv_datagram().await)
                .unwrap(),
            state(vec![9; 40])
        );
        client.join_session(1, 2).await.unwrap();
        assert_eq!(
            peer.recv_control().await.message_kind(),
            MessageKind::JoinSession
        );
        client.close().unwrap();
    }
}

#[tokio::test]
async fn configured_outgoing_frame_limit_is_checked_before_datagram_submission() {
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        let (mut client, mut peer) = fixture(
            scheme,
            ClientConfig {
                max_frame_bytes: 192,
                max_payload_bytes: 128,
                ..ClientConfig::default()
            },
            128,
            true,
        )
        .await;
        assert!(matches!(
            publish(&client, vec![0; 128]),
            Err(ClientError::Protocol(CodecError::FrameTooLarge {
                maximum: 192,
                ..
            }))
        ));
        client.join_session(1, 2).await.unwrap();
        assert_eq!(
            peer.recv_control().await.message_kind(),
            MessageKind::JoinSession
        );
        client.close().unwrap();
    }
}

#[tokio::test]
async fn unsupported_datagrams_do_not_fall_back_to_the_control_stream() {
    let (mut client, mut peer) =
        fixture(UrlScheme::Quic, ClientConfig::default(), 65_536, false).await;
    let error = publish(&client, vec![1; 40]).unwrap_err();
    assert!(error.to_string().contains("unsupported"));
    client.join_session(1, 2).await.unwrap();
    assert_eq!(
        peer.recv_control().await.message,
        MessagePayload::Control(ControlPayload::JoinSession(JoinSession {
            resume_token: vec![]
        }))
    );
    client.close().unwrap();
}

#[tokio::test]
async fn receiver_reports_bad_packets_and_uses_configured_incoming_limits() {
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        let (mut client, peer) = fixture(
            scheme,
            ClientConfig {
                max_frame_bytes: 512,
                max_payload_bytes: 128,
                ..ClientConfig::default()
            },
            32,
            true,
        )
        .await;
        let mut receiver = client.take_datagram_receiver().unwrap();
        let codec = Codec::default();
        let valid = codec.encode(&state(vec![1; 64])).unwrap();
        peer.send_datagram(valid.clone());
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await.unwrap().unwrap(),
            state(vec![1; 64])
        );
        peer.send_datagram(vec![0, 1, 2]);
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Protocol(_))
        ));
        let mut trailing = valid.clone();
        trailing.push(0);
        peer.send_datagram(trailing);
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Protocol(CodecError::TrailingBytes { .. }))
        ));
        peer.send_datagram(codec.encode(&state(vec![0; 129])).unwrap());
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Protocol(CodecError::PayloadTooLarge {
                actual: 129,
                maximum: 128
            }))
        ));
        peer.send_datagram(codec.encode(&state(vec![0; 600])).unwrap());
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Protocol(CodecError::FrameTooLarge {
                maximum: 512,
                ..
            }))
        ));
        let mut wrong_class = state(vec![1]);
        wrong_class.delivery_class = DeliveryClass::LatestValue;
        peer.send_datagram(codec.encode(&wrong_class).unwrap());
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Protocol(CodecError::InvalidSemantics { .. }))
        ));
        let mut wrong_kind = state(vec![1]);
        wrong_kind.delivery_class = DeliveryClass::ReliableOrdered;
        wrong_kind.message = MessagePayload::ReliableEvent(OpaquePayload {
            type_id: 8,
            bytes: vec![1],
        });
        peer.send_datagram(codec.encode(&wrong_kind).unwrap());
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Protocol(CodecError::InvalidSemantics { .. }))
        ));
        let mut invalid_scope = state(vec![1]);
        invalid_scope.namespace_id = 0x1234_5678_1234_5678;
        let mut frame = codec.encode(&invalid_scope).unwrap();
        let offset = frame
            .windows(8)
            .position(|bytes| bytes == invalid_scope.namespace_id.to_le_bytes())
            .unwrap();
        frame[offset..offset + 8].fill(0);
        peer.send_datagram(frame);
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Protocol(CodecError::InvalidSemantics { .. }))
        ));
        peer.send_datagram(valid);
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await.unwrap().unwrap(),
            state(vec![1; 64])
        );
        client.close().unwrap();
    }
}

#[tokio::test]
async fn handout_permanently_blocks_all_managed_exchanges_before_stream_io() {
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        let (mut client, mut peer) = fixture(scheme, ClientConfig::default(), 65_536, true).await;
        let receiver = client.take_datagram_receiver().unwrap();
        let admission = client
            .request_admission(1, 2, 1, "request".to_owned())
            .await;
        assert!(
            admission
                .unwrap_err()
                .to_string()
                .contains("after taking the datagram receiver")
        );
        for result in [
            client.queue_status(1, 2, 2, 1).await,
            client.queue_heartbeat(1, 2, 3, 1).await,
            client.queue_claim(1, 2, 4, 1).await,
            client.queue_cancel(1, 2, 5, 1).await,
        ] {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("after taking the datagram receiver")
            );
        }
        drop(receiver);
        assert!(client.take_datagram_receiver().is_err());
        client.join_session(1, 2).await.unwrap();
        assert_eq!(
            peer.recv_control().await.message_kind(),
            MessageKind::JoinSession
        );
        client.close().unwrap();
    }
}

#[tokio::test]
async fn owned_admission_runner_rejects_handout_and_closes_the_shared_connection() {
    for scheme in [UrlScheme::Quic, UrlScheme::WebTransport] {
        let (mut client, _peer) = fixture(scheme, ClientConfig::default(), 65_536, true).await;
        let mut receiver = client.take_datagram_receiver().unwrap();
        let result = client
            .admit_with_cancellation(1, 2, "request".to_owned(), TEST_TIMEOUT, pending())
            .await;
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("after taking the datagram receiver")
        );
        assert!(matches!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(ClientError::Transport(_))
        ));
    }
}
