//! QUIC/WebTransport transport adapter.
//!
//! Native clients connect directly over QUIC; browser clients connect over WebTransport
//! (HTTP/3 with QUIC underneath). Both share the same envelope codec, the same delivery-class
//! mapping, and the same core worker bridge.
//!
//! Control envelopes and reliable data are carried on the first client-initiated bidirectional
//! stream. `UnreliableSequenced` and `BestEffortEvent` delivery classes are mapped to QUIC
//! unreliable datagrams when they fit within the connection's current datagram budget.

#![deny(unsafe_code)]

pub mod webtransport;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use quinn::{Connection, Endpoint, RecvStream, SendStream, VarInt};
pub use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, trace};
use woven_core::{Command, CommandResult, ConnectionId, Credentials};
use woven_protocol::{
    Authenticated, Capabilities, Codec, ControlPayload, DeliveryClass, Envelope, MessageKind,
    MessagePayload, PROTOCOL_VERSION, ProtocolErrorCode,
};
use woven_transport::{
    MAX_FRAME_BYTES, MAX_PAYLOAD_BYTES, UnroutedControl, WorkerHandle, handle_authenticated,
    outbound_envelope, send_envelope, send_error,
};

const WRITE_CAPACITY: usize = 128;
const MAX_CONNECTION_TASKS: usize = 4_096;
const INITIAL_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_PROTOCOL: u32 = 0x100;
const CLOSE_TRANSPORT: u32 = 0x101;

/// Configuration shared by QUIC connection handlers.
#[derive(Clone)]
pub struct QuicConfig {
    /// Handle to the bounded, single-owner core worker.
    pub worker: WorkerHandle,
    /// Server name reported in the protocol capabilities response.
    pub server_name: Arc<str>,
    /// Server version reported in the protocol capabilities response.
    pub server_version: Arc<str>,
    /// Where client-sent inference control messages are forwarded when the inference plane
    /// is enabled. `None` means the plane is disabled and those messages are rejected.
    pub inference_sink: Option<mpsc::Sender<UnroutedControl>>,
}

impl QuicConfig {
    #[must_use]
    pub fn new(worker: WorkerHandle) -> Self {
        Self {
            worker,
            server_name: Arc::from("woven"),
            server_version: Arc::from(env!("CARGO_PKG_VERSION")),
            inference_sink: None,
        }
    }
}

/// Build a QUIC server configuration from the certificate chain and private key DER supplied by
/// the embedding server. This crate never creates a self-signed production certificate.
pub fn server_config(
    certificate_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<quinn::ServerConfig, rustls::Error> {
    quinn::ServerConfig::with_single_cert(certificate_chain, private_key)
}

/// Bind a local QUIC endpoint for a previously constructed server configuration.
pub fn server_endpoint(
    bind_address: SocketAddr,
    server_config: quinn::ServerConfig,
) -> Result<Endpoint, std::io::Error> {
    Endpoint::server(server_config, bind_address)
}

/// Accept QUIC handshakes until `endpoint` is closed, serving each successful connection.
///
/// The endpoint loop caps active connection tasks at 4,096. It does not create an application-level
/// queue: when all permits are in use, it pauses acceptance and relies on
/// Quinn's configured endpoint limits for backpressure.
pub async fn serve_endpoint(endpoint: Endpoint, config: QuicConfig) {
    let connection_tasks = Arc::new(Semaphore::new(MAX_CONNECTION_TASKS));
    while let Some(incoming) = endpoint.accept().await {
        let Ok(connection) = incoming.await else {
            continue;
        };
        let Ok(permit) = connection_tasks.clone().acquire_owned().await else {
            return;
        };
        let connection_config = config.clone();
        std::mem::drop(tokio::spawn(async move {
            let _permit = permit;
            serve_connection(connection, connection_config).await;
        }));
    }
}

/// Serve one established QUIC connection.
///
/// The first client-initiated bidirectional stream is the Woven control stream. A second
/// client bidirectional stream is a protocol violation and closes the connection.
#[allow(
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::too_many_lines
)]
pub async fn serve_connection(connection: Connection, config: QuicConfig) {
    let codec = Codec::default();
    let core_connection = match config.worker.execute(Command::TransportConnected).await {
        Ok(CommandResult::Connected(connection)) => connection,
        Ok(_) | Err(_) => {
            close(&connection, CLOSE_TRANSPORT, b"core worker unavailable");
            return;
        }
    };

    let (send_stream, mut receive_stream) =
        match tokio::time::timeout(INITIAL_STREAM_TIMEOUT, connection.accept_bi()).await {
            Ok(Ok(streams)) => streams,
            Ok(Err(_)) | Err(_) => {
                config.worker.discard_and_disconnect(core_connection).await;
                close(
                    &connection,
                    CLOSE_PROTOCOL,
                    b"expected a client bidirectional stream",
                );
                return;
            }
        };

    let (write_sender, mut write_receiver) = mpsc::channel::<Envelope>(WRITE_CAPACITY);
    let (shutdown_sender, mut shutdown_receiver) = mpsc::channel::<()>(1);
    let writer_codec = codec.clone();
    let writer_worker = config.worker.clone();
    let writer_shutdown = shutdown_sender.clone();
    let writer_connection = connection.clone();
    let writer_task = tokio::spawn(async move {
        writer_loop(
            send_stream,
            &mut write_receiver,
            writer_codec,
            writer_worker,
            core_connection,
            writer_shutdown,
            writer_connection,
        )
        .await;
    });

    if config
        .worker
        .register_lifecycle(
            core_connection,
            write_sender.clone(),
            shutdown_sender.clone(),
        )
        .await
        .is_err()
    {
        config.worker.discard_and_disconnect(core_connection).await;
        drop(write_sender);
        writer_task.abort();
        close(&connection, CLOSE_TRANSPORT, b"core worker unavailable");
        return;
    }

    let drain_task = spawn_outbound_drain_quic(
        config.worker.clone(),
        core_connection,
        connection.clone(),
        codec.clone(),
        write_sender.clone(),
        shutdown_sender.clone(),
    );

    let mut greeted = false;
    let mut authenticated = false;
    loop {
        tokio::select! {
            _ = shutdown_receiver.recv() => break,
            extra_stream = connection.accept_bi() => {
                if extra_stream.is_ok() {
                    send_error(
                        &write_sender,
                        MessageKind::Unknown,
                        ProtocolErrorCode::UnsupportedMessage,
                        "only one client bidirectional stream is supported".to_owned(),
                    ).await;
                }
                break;
            }
            datagram = connection.read_datagram() => {
                match datagram {
                    Ok(bytes) => {
                        if !authenticated {
                            trace!(?core_connection, "dropping pre-authentication datagram");
                            continue;
                        }
                        match codec.decode(&bytes) {
                            Ok(envelope) => {
                                if handle_authenticated(
                                    &config.worker,
                                    core_connection,
                                    envelope,
                                    &write_sender,
                                    config.inference_sink.as_ref(),
                                ).await.is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                trace!(?error, "dropping malformed datagram");
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            received = read_envelope(&mut receive_stream, &codec) => {
                let envelope = match received {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        send_error(
                            &write_sender,
                            MessageKind::Unknown,
                            ProtocolErrorCode::MalformedFrame,
                            error,
                        ).await;
                        break;
                    }
                };
                let result = if !greeted {
                    handle_hello(&write_sender, &config, &envelope).await.map(|()| greeted = true)
                } else if !authenticated {
                    handle_authenticate(&config, core_connection, &write_sender, envelope)
                        .await
                        .map(|()| authenticated = true)
                } else {
                    handle_authenticated(
                        &config.worker,
                        core_connection,
                        envelope,
                        &write_sender,
                        config.inference_sink.as_ref(),
                    ).await
                };
                if result.is_err() {
                    break;
                }
            }
        }
    }

    drain_task.abort();
    config.worker.discard_and_disconnect(core_connection).await;
    drop(write_sender);
    drop(shutdown_sender);
    let _ = writer_task.await;
    close(&connection, CLOSE_PROTOCOL, b"Woven QUIC connection closed");
    debug!(?core_connection, "QUIC connection closed");
}

fn spawn_outbound_drain_quic(
    worker: WorkerHandle,
    connection: ConnectionId,
    quic_connection: Connection,
    codec: Codec,
    write_sender: mpsc::Sender<Envelope>,
    shutdown_sender: mpsc::Sender<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        loop {
            interval.tick().await;
            if flush_outbound_quic(&worker, connection, &quic_connection, &codec, &write_sender)
                .await
                .is_err()
            {
                let _ = shutdown_sender.try_send(());
                worker.discard_and_disconnect(connection).await;
                return;
            }
        }
    })
}

async fn flush_outbound_quic(
    worker: &WorkerHandle,
    connection: ConnectionId,
    quic_connection: &Connection,
    codec: &Codec,
    write_sender: &mpsc::Sender<Envelope>,
) -> Result<(), ()> {
    if let Ok(CommandResult::Outbound(messages)) =
        worker.execute(Command::DrainOutbound { connection }).await
    {
        for message in messages {
            let envelope = outbound_envelope(message);
            if envelope.delivery_class.is_unreliable() {
                send_datagram_envelope(quic_connection, codec, &envelope);
            } else {
                send_envelope(write_sender, envelope).await?;
            }
        }
    }
    Ok(())
}

fn send_datagram_envelope(connection: &Connection, codec: &Codec, envelope: &Envelope) {
    let frame = match codec.encode(envelope) {
        Ok(frame) => frame,
        Err(error) => {
            trace!(?error, "dropping undeliverable datagram envelope");
            return;
        }
    };
    let max_size = connection.max_datagram_size();
    if max_size.is_some_and(|limit| frame.len() <= limit) {
        if let Err(error) = connection.send_datagram(Bytes::from(frame)) {
            trace!(?error, "dropping datagram that failed to queue");
        }
    } else {
        trace!(
            frame_len = frame.len(),
            ?max_size,
            "dropping datagram exceeding budget"
        );
    }
}

#[allow(clippy::manual_let_else, clippy::single_match_else)]
async fn writer_loop(
    mut stream: SendStream,
    receiver: &mut mpsc::Receiver<Envelope>,
    codec: Codec,
    worker: WorkerHandle,
    core_connection: ConnectionId,
    shutdown: mpsc::Sender<()>,
    connection: Connection,
) {
    while let Some(envelope) = receiver.recv().await {
        let frame = match codec.encode(&envelope) {
            Ok(frame) => frame,
            Err(_) => {
                let _ = shutdown.try_send(());
                worker.discard_and_disconnect(core_connection).await;
                close(
                    &connection,
                    CLOSE_TRANSPORT,
                    b"failed to encode protocol envelope",
                );
                return;
            }
        };
        if stream.write_all(&frame).await.is_err() {
            let _ = shutdown.try_send(());
            worker.discard_and_disconnect(core_connection).await;
            close(&connection, CLOSE_TRANSPORT, b"QUIC stream write failed");
            return;
        }
    }
    let _ = stream.finish();
}

async fn read_envelope(stream: &mut RecvStream, codec: &Codec) -> Result<Envelope, String> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|error| format!("failed to read frame prefix: {error}"))?;
    let frame_len = codec
        .expected_frame_len(&prefix)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "incomplete size prefix".to_owned())?;
    let mut frame = vec![0_u8; frame_len];
    frame[..prefix.len()].copy_from_slice(&prefix);
    stream
        .read_exact(&mut frame[prefix.len()..])
        .await
        .map_err(|error| format!("failed to read frame: {error}"))?;
    codec.decode(&frame).map_err(|error| error.to_string())
}

async fn handle_hello(
    write_sender: &mpsc::Sender<Envelope>,
    config: &QuicConfig,
    envelope: &Envelope,
) -> Result<(), ()> {
    if !matches!(
        envelope.message,
        MessagePayload::Control(ControlPayload::Hello(_))
    ) {
        send_error(
            write_sender,
            envelope.message_kind(),
            ProtocolErrorCode::UnsupportedMessage,
            "expected Hello".to_owned(),
        )
        .await;
        return Err(());
    }
    send_envelope(
        write_sender,
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Capabilities(Capabilities {
                selected_protocol_version: PROTOCOL_VERSION,
                server_name: config.server_name.to_string(),
                server_version: config.server_version.to_string(),
                capability_bits: 0,
                max_frame_size: MAX_FRAME_BYTES,
                max_payload_size: MAX_PAYLOAD_BYTES,
            }),
        ),
    )
    .await
}

#[allow(clippy::manual_let_else, clippy::single_match_else)]
async fn handle_authenticate(
    config: &QuicConfig,
    connection: ConnectionId,
    write_sender: &mpsc::Sender<Envelope>,
    envelope: Envelope,
) -> Result<(), ()> {
    let MessagePayload::Control(ControlPayload::Authenticate(auth)) = envelope.message else {
        send_error(
            write_sender,
            envelope.message_kind(),
            ProtocolErrorCode::AuthenticationRequired,
            "expected Authenticate".to_owned(),
        )
        .await;
        return Err(());
    };
    let token = match String::from_utf8(auth.credentials) {
        Ok(token) => token,
        Err(_) => {
            send_error(
                write_sender,
                MessageKind::Authenticate,
                ProtocolErrorCode::AuthenticationRequired,
                "credentials must be UTF-8".to_owned(),
            )
            .await;
            return Err(());
        }
    };
    match config
        .worker
        .execute(Command::Authenticate {
            connection,
            credentials: Credentials::new(token),
        })
        .await
    {
        Ok(CommandResult::Authenticated(principal)) => {
            send_envelope(
                write_sender,
                Envelope::control(
                    DeliveryClass::ReliableOrdered,
                    ControlPayload::Authenticated(Authenticated {
                        principal_id: principal.get(),
                        assigned_entity_id: None,
                    }),
                ),
            )
            .await
        }
        Ok(_) | Err(_) => {
            send_error(
                write_sender,
                MessageKind::Authenticate,
                ProtocolErrorCode::Unauthorized,
                "authentication failed".to_owned(),
            )
            .await;
            Err(())
        }
    }
}

fn close(connection: &Connection, code: u32, reason: &[u8]) {
    connection.close(VarInt::from_u32(code), reason);
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use quinn::{ClientConfig, Endpoint, crypto::rustls::QuicClientConfig};
    use rustls::{RootCertStore, pki_types::PrivatePkcs8KeyDer};
    use woven_core::{
        AuthenticatedPrincipal, AuthorizationGrants, CoreConfig, DevAuthenticator, PrincipalId,
        TransportIndependentWorker, WovenCore,
    };
    use woven_protocol::{
        Authenticate, AuthenticationScheme, EntityLeaveReason, Hello, OpaquePayload,
    };
    use woven_transport::spawn_worker;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn raw_quinn_datagram_smoke_test() {
        let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate localhost certificate");
        let certificate = certified_key.cert.der().clone();
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified_key.key_pair.serialize_der(),
        ));
        let server_config = server_config(vec![certificate.clone()], private_key)
            .expect("build server configuration");
        let endpoint = server_endpoint(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), server_config)
            .expect("bind QUIC endpoint");
        let server_address = endpoint.local_addr().expect("read server address");

        let mut roots = RootCertStore::empty();
        roots.add(certificate).expect("trust localhost certificate");
        let client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(client_crypto).expect("create QUIC client configuration"),
        ));
        let mut client_endpoint = Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .expect("bind QUIC client endpoint");
        client_endpoint.set_default_client_config(client_config);

        let (client_conn, server_conn) = tokio::join!(
            async {
                client_endpoint
                    .connect(server_address, "localhost")
                    .expect("start QUIC connection")
                    .await
                    .expect("complete QUIC connection")
            },
            async {
                endpoint
                    .accept()
                    .await
                    .expect("accept incoming")
                    .await
                    .expect("complete connection")
            }
        );

        eprintln!(
            "server max_datagram_size: {:?}",
            server_conn.max_datagram_size()
        );
        eprintln!(
            "client max_datagram_size: {:?}",
            client_conn.max_datagram_size()
        );

        server_conn
            .send_datagram(bytes::Bytes::from_static(b"hello"))
            .expect("send datagram");
        let received = tokio::time::timeout(TEST_TIMEOUT, client_conn.read_datagram())
            .await
            .expect("read datagram timed out")
            .expect("read datagram failed");
        assert_eq!(received.as_ref(), b"hello");

        client_conn.close(VarInt::from_u32(0), b"done");
        client_endpoint.close(VarInt::from_u32(0), b"done");
        endpoint.close(VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn loopback_client_completes_hello_and_authenticate() {
        let (endpoint, _worker, server_address, certificate) = setup_server();
        let (connection, mut send, mut receive, codec, client_endpoint) =
            connect_client(server_address, certificate).await;

        perform_handshake(&mut send, &mut receive, &codec).await;

        connection.close(VarInt::from_u32(0), b"test complete");
        client_endpoint.close(VarInt::from_u32(0), b"test complete");
        endpoint.close(VarInt::from_u32(0), b"test complete");
    }

    #[tokio::test]
    async fn reliable_event_fan_out_over_quic() {
        let (endpoint, _worker, server_address, certificate) = setup_server();
        let (alice_conn, mut alice_send, mut alice_recv, codec, client_endpoint) =
            connect_client(server_address, certificate.clone()).await;
        let (bob_conn, mut bob_send, mut bob_recv, _codec2, client_endpoint2) =
            connect_client(server_address, certificate).await;

        for (send, recv) in [
            (&mut alice_send, &mut alice_recv),
            (&mut bob_send, &mut bob_recv),
        ] {
            perform_handshake(send, recv, &codec).await;
            join_and_subscribe(send, recv, &codec, 1, 1).await;
        }

        let alice_entity = recv_subscription_accepted_and_entity(&mut alice_recv, &codec).await;
        let _bob_entity = recv_subscription_accepted_and_entity(&mut bob_recv, &codec).await;

        // Alice publishes a reliable event
        alice_send
            .write_all(
                &codec
                    .encode(&Envelope {
                        protocol_version: PROTOCOL_VERSION,
                        delivery_class: DeliveryClass::ReliableOrdered,
                        namespace_id: 1,
                        session_id: 1,
                        space_id: 1,
                        channel_id: Some(1),
                        entity_id: Some(alice_entity),
                        space_epoch: 1,
                        server_tick: 0,
                        sender_sequence: 1,
                        correlation_id: None,
                        message: MessagePayload::ReliableEvent(OpaquePayload {
                            type_id: 1,
                            bytes: b"hello-quic".to_vec(),
                        }),
                    })
                    .expect("encode event"),
            )
            .await
            .expect("send event");

        let received = tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut bob_recv, &codec))
            .await
            .expect("receive timed out")
            .expect("decode event");
        assert!(
            matches!(received.message, MessagePayload::ReliableEvent(ref payload) if payload.bytes == b"hello-quic")
        );

        alice_conn.close(VarInt::from_u32(0), b"done");
        bob_conn.close(VarInt::from_u32(0), b"done");
        client_endpoint.close(VarInt::from_u32(0), b"done");
        client_endpoint2.close(VarInt::from_u32(0), b"done");
        endpoint.close(VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn disconnect_emits_entity_left_over_quic() {
        let (endpoint, _worker, server_address, certificate) = setup_server();
        let (alice_conn, mut alice_send, mut alice_recv, codec, client_endpoint) =
            connect_client(server_address, certificate.clone()).await;
        let (bob_conn, mut bob_send, mut bob_recv, _codec2, client_endpoint2) =
            connect_client(server_address, certificate).await;

        for (send, recv) in [
            (&mut alice_send, &mut alice_recv),
            (&mut bob_send, &mut bob_recv),
        ] {
            perform_handshake(send, recv, &codec).await;
            join_and_subscribe(send, recv, &codec, 1, 1).await;
        }

        let alice_entity = recv_subscription_accepted_and_entity(&mut alice_recv, &codec).await;
        let _bob_entity = recv_subscription_accepted_and_entity(&mut bob_recv, &codec).await;

        alice_conn.close(VarInt::from_u32(0), b"disconnect");
        client_endpoint.close(VarInt::from_u32(0), b"disconnect");

        let left = tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut bob_recv, &codec))
            .await
            .expect("receive timed out")
            .expect("decode left");
        assert!(matches!(
            left.message,
            MessagePayload::Control(ControlPayload::EntityLeft(ref value))
            if left.entity_id == Some(alice_entity) && value.reason == EntityLeaveReason::Disconnected
        ));

        bob_conn.close(VarInt::from_u32(0), b"done");
        client_endpoint2.close(VarInt::from_u32(0), b"done");
        endpoint.close(VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn quic_datagram_roundtrip_for_unreliable_delivery() {
        let (endpoint, _worker, server_address, certificate) = setup_server();
        let (alice_conn, mut alice_send, mut alice_recv, codec, client_endpoint) =
            connect_client(server_address, certificate.clone()).await;
        let (bob_conn, mut bob_send, mut bob_recv, _codec2, client_endpoint2) =
            connect_client(server_address, certificate).await;

        for (send, recv) in [
            (&mut alice_send, &mut alice_recv),
            (&mut bob_send, &mut bob_recv),
        ] {
            perform_handshake(send, recv, &codec).await;
            join_and_subscribe(send, recv, &codec, 1, 1).await;
        }

        let alice_entity = recv_subscription_accepted_and_entity(&mut alice_recv, &codec).await;
        let _bob_entity = recv_subscription_accepted_and_entity(&mut bob_recv, &codec).await;

        // Send an unreliable sequenced event as a datagram
        let frame = codec
            .encode(&Envelope {
                protocol_version: PROTOCOL_VERSION,
                delivery_class: DeliveryClass::UnreliableSequenced,
                namespace_id: 1,
                session_id: 1,
                space_id: 1,
                channel_id: Some(3),
                entity_id: Some(alice_entity),
                space_epoch: 1,
                server_tick: 0,
                sender_sequence: 1,
                correlation_id: None,
                message: MessagePayload::EntityState(OpaquePayload {
                    type_id: 1,
                    bytes: b"datagram-payload".to_vec(),
                }),
            })
            .expect("encode datagram envelope");
        alice_conn
            .send_datagram(Bytes::from(frame))
            .expect("send datagram");

        // Bob should receive the forwarded message as a datagram from the server
        let datagram = tokio::time::timeout(TEST_TIMEOUT, bob_conn.read_datagram())
            .await
            .expect("datagram receive timed out")
            .expect("read datagram");
        let received = codec.decode(&datagram).expect("decode datagram");
        assert!(
            matches!(received.message, MessagePayload::EntityState(ref payload) if payload.bytes == b"datagram-payload")
        );

        alice_conn.close(VarInt::from_u32(0), b"done");
        bob_conn.close(VarInt::from_u32(0), b"done");
        client_endpoint.close(VarInt::from_u32(0), b"done");
        client_endpoint2.close(VarInt::from_u32(0), b"done");
        endpoint.close(VarInt::from_u32(0), b"done");
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    fn setup_server() -> (Endpoint, WorkerHandle, SocketAddr, CertificateDer<'static>) {
        let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate localhost certificate");
        let certificate = certified_key.cert.der().clone();
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified_key.key_pair.serialize_der(),
        ));
        let server_config = server_config(vec![certificate.clone()], private_key)
            .expect("build server configuration");
        let endpoint = server_endpoint(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), server_config)
            .expect("bind QUIC endpoint");
        let server_address = endpoint.local_addr().expect("read server address");

        let namespace = woven_core::NamespaceId::new(1);
        let session = woven_core::SessionKey {
            namespace,
            session: woven_core::SessionId::new(1),
        };
        let mut grants = AuthorizationGrants::new();
        grants.grant_namespace(namespace, woven_core::AccessGrant::ReadWrite);
        grants.grant_session(session, woven_core::AccessGrant::ReadWrite);
        for space_id in [1, 2] {
            grants.grant_space(
                woven_core::SpaceKey {
                    session,
                    space: woven_core::SpaceId::new(space_id),
                },
                woven_core::AccessGrant::ReadWrite,
            );
        }
        for channel_id in [1, 2, 3] {
            grants.grant_channel(
                woven_core::ChannelScope::new(session, woven_core::ChannelId::new(channel_id)),
                woven_core::AccessGrant::ReadWrite,
            );
        }
        let mut authenticator =
            DevAuthenticator::with_capacity(1).expect("valid identity capacity");
        authenticator
            .insert(
                "dev-token",
                AuthenticatedPrincipal::new(PrincipalId::new(1), grants),
            )
            .expect("insert development identity");
        let mut core = WovenCore::new(authenticator, CoreConfig::default()).expect("create core");
        core.register_channel(woven_core::ChannelDefinition::relay_owned(
            woven_core::ChannelId::new(1),
            woven_core::DeliveryClass::ReliableOrdered,
            woven_core::PersistenceClass::Ephemeral,
            64 * 1024,
        ))
        .expect("register channel 1");
        core.register_channel(woven_core::ChannelDefinition::relay_owned(
            woven_core::ChannelId::new(2),
            woven_core::DeliveryClass::LatestValue,
            woven_core::PersistenceClass::Stateful { ttl: None },
            64 * 1024,
        ))
        .expect("register channel 2");
        core.register_channel(woven_core::ChannelDefinition::relay_owned(
            woven_core::ChannelId::new(3),
            woven_core::DeliveryClass::UnreliableSequenced,
            woven_core::PersistenceClass::Stateful { ttl: None },
            64 * 1024,
        ))
        .expect("register channel 3");
        core.provision_session(session).expect("provision session");
        for space_id in [1, 2] {
            core.install_space(
                session,
                woven_core::SpaceDescriptor {
                    id: woven_core::SpaceId::new(space_id),
                    local_frame: woven_core::CoordinateFrame::Logical,
                    parent: None,
                    epoch: woven_core::SpaceEpoch::new(1),
                    routing: woven_core::RoutingPolicy::BroadcastAll,
                },
            )
            .expect("install space");
        }
        let worker = spawn_worker(TransportIndependentWorker::new(core));
        tokio::spawn(serve_endpoint(
            endpoint.clone(),
            QuicConfig::new(worker.clone()),
        ));
        (endpoint, worker, server_address, certificate)
    }

    async fn connect_client(
        server_address: SocketAddr,
        certificate: CertificateDer<'static>,
    ) -> (
        quinn::Connection,
        quinn::SendStream,
        quinn::RecvStream,
        Codec,
        Endpoint,
    ) {
        let mut roots = RootCertStore::empty();
        roots.add(certificate).expect("trust localhost certificate");
        let client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(client_crypto).expect("create QUIC client configuration"),
        ));
        let mut client_endpoint = Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .expect("bind QUIC client endpoint");
        client_endpoint.set_default_client_config(client_config);
        let connection = tokio::time::timeout(
            TEST_TIMEOUT,
            client_endpoint
                .connect(server_address, "localhost")
                .expect("start QUIC connection"),
        )
        .await
        .expect("QUIC connection timed out")
        .expect("complete QUIC connection");
        let (send, receive) = tokio::time::timeout(TEST_TIMEOUT, connection.open_bi())
            .await
            .expect("open stream timed out")
            .expect("open bidirectional stream");
        (connection, send, receive, Codec::default(), client_endpoint)
    }

    async fn perform_handshake(
        send: &mut quinn::SendStream,
        receive: &mut quinn::RecvStream,
        codec: &Codec,
    ) {
        send.write_all(
            &codec
                .encode(&Envelope::control(
                    DeliveryClass::ReliableOrdered,
                    ControlPayload::Hello(Hello {
                        min_protocol_version: PROTOCOL_VERSION,
                        max_protocol_version: PROTOCOL_VERSION,
                        client_name: "loopback-test".to_owned(),
                        client_version: "0.1.0".to_owned(),
                        capability_bits: 0,
                        max_frame_size: MAX_FRAME_BYTES,
                        max_payload_size: MAX_PAYLOAD_BYTES,
                    }),
                ))
                .expect("encode Hello"),
        )
        .await
        .expect("send Hello");
        let capabilities = tokio::time::timeout(TEST_TIMEOUT, read_envelope(receive, codec))
            .await
            .expect("Capabilities receive timed out")
            .expect("decode Capabilities");
        assert!(matches!(
            capabilities.message,
            MessagePayload::Control(ControlPayload::Capabilities(_))
        ));

        send.write_all(
            &codec
                .encode(&Envelope::control(
                    DeliveryClass::ReliableOrdered,
                    ControlPayload::Authenticate(Authenticate {
                        scheme: AuthenticationScheme::Development,
                        credentials: b"dev-token".to_vec(),
                    }),
                ))
                .expect("encode Authenticate"),
        )
        .await
        .expect("send Authenticate");
        let authenticated = tokio::time::timeout(TEST_TIMEOUT, read_envelope(receive, codec))
            .await
            .expect("Authenticated receive timed out")
            .expect("decode Authenticated");
        assert!(matches!(
            authenticated.message,
            MessagePayload::Control(ControlPayload::Authenticated(Authenticated {
                principal_id: 1,
                assigned_entity_id: None,
            }))
        ));
    }

    async fn join_and_subscribe(
        send: &mut quinn::SendStream,
        _receive: &mut quinn::RecvStream,
        codec: &Codec,
        space_id: u64,
        channel_id: u64,
    ) {
        send.write_all(
            &codec
                .encode(&Envelope {
                    protocol_version: PROTOCOL_VERSION,
                    delivery_class: DeliveryClass::ReliableOrdered,
                    namespace_id: 1,
                    session_id: 1,
                    space_id: 0,
                    channel_id: None,
                    entity_id: None,
                    space_epoch: 0,
                    server_tick: 0,
                    sender_sequence: 0,
                    correlation_id: None,
                    message: MessagePayload::Control(ControlPayload::JoinSession(
                        woven_protocol::JoinSession {
                            resume_token: vec![],
                        },
                    )),
                })
                .expect("encode JoinSession"),
        )
        .await
        .expect("send JoinSession");

        send.write_all(
            &codec
                .encode(&Envelope {
                    protocol_version: PROTOCOL_VERSION,
                    delivery_class: DeliveryClass::ReliableOrdered,
                    namespace_id: 1,
                    session_id: 1,
                    space_id,
                    channel_id: Some(channel_id),
                    entity_id: None,
                    space_epoch: 1,
                    server_tick: 0,
                    sender_sequence: 0,
                    correlation_id: None,
                    message: MessagePayload::Control(ControlPayload::SubscribeSpace(
                        woven_protocol::SubscribeSpace,
                    )),
                })
                .expect("encode SubscribeSpace"),
        )
        .await
        .expect("send SubscribeSpace");
    }

    async fn recv_subscription_accepted_and_entity(
        receive: &mut quinn::RecvStream,
        codec: &Codec,
    ) -> u64 {
        let accepted = tokio::time::timeout(TEST_TIMEOUT, read_envelope(receive, codec))
            .await
            .expect("timeout")
            .expect("decode accepted");
        assert!(matches!(
            accepted.message,
            MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))
        ));
        let entered = tokio::time::timeout(TEST_TIMEOUT, read_envelope(receive, codec))
            .await
            .expect("timeout")
            .expect("decode entered");
        assert!(matches!(
            entered.message,
            MessagePayload::Control(ControlPayload::EntityEntered(_))
        ));
        entered.entity_id.expect("entity id present")
    }
}
