//! WebTransport transport adapter for Woven.
//!
//! Control envelopes and reliable data are carried on the first client-initiated bidirectional
//! stream. `UnreliableSequenced` and `BestEffortEvent` delivery classes are mapped to WebTransport
//! unreliable datagrams when they fit within the connection's current datagram budget.

#![deny(unsafe_code)]

use super::is_client_datagram;
use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, trace};
use woven_core::{Command, CommandResult, ConnectionId, Credentials};
use woven_protocol::{
    Authenticated, AuthenticationScheme, Capabilities, Codec, ControlPayload, DeliveryClass,
    Envelope, MessageKind, MessagePayload, PROTOCOL_VERSION, ProtocolErrorCode,
    SUPPORTED_CAPABILITY_BITS,
};
use woven_transport::{
    MAX_FRAME_BYTES, MAX_PAYLOAD_BYTES, UnroutedControl, WorkerHandle,
    handle_authenticated_with_capabilities, outbound_envelope, send_envelope, send_error,
};
use wtransport::{
    Connection, Endpoint, Identity, RecvStream, SendStream, ServerConfig, VarInt,
    endpoint::{SessionRequest, endpoint_side::Server},
};

const WRITE_CAPACITY: usize = 128;
const MAX_CONNECTION_TASKS: usize = 4_096;
const MAX_ALLOWED_ORIGINS: usize = 64;
const INITIAL_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_PROTOCOL: u32 = 0x100;
const CLOSE_TRANSPORT: u32 = 0x101;

/// A bounded allowlist for browser `Origin` request headers.
///
/// Requests with an `Origin` header must match this list exactly. By default, requests without
/// that header are accepted so native local test clients can connect.
#[derive(Clone, Debug)]
pub struct OriginPolicy {
    allowed_origins: Arc<[Arc<str>]>,
    allow_missing_origin: bool,
}

impl OriginPolicy {
    /// Create an origin policy from at most 64 exact origin values.
    pub fn allowlisted(
        allowed_origins: Vec<Arc<str>>,
        allow_missing_origin: bool,
    ) -> Result<Self, OriginPolicyError> {
        if allowed_origins.len() > MAX_ALLOWED_ORIGINS {
            return Err(OriginPolicyError::TooManyOrigins);
        }
        Ok(Self {
            allowed_origins: allowed_origins.into(),
            allow_missing_origin,
        })
    }

    fn allows(&self, origin: Option<&str>) -> bool {
        match origin {
            Some(origin) => self
                .allowed_origins
                .iter()
                .any(|allowed| allowed.as_ref() == origin),
            None => self.allow_missing_origin,
        }
    }
}

impl Default for OriginPolicy {
    fn default() -> Self {
        Self {
            allowed_origins: Arc::from([]),
            allow_missing_origin: true,
        }
    }
}

/// Error returned when configuring an origin policy beyond its fixed bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OriginPolicyError {
    /// More than 64 origins were supplied.
    TooManyOrigins,
}

impl std::fmt::Display for OriginPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("origin allowlist exceeds its capacity of 64 entries")
    }
}

impl std::error::Error for OriginPolicyError {}

/// Configuration shared by WebTransport session handlers.
#[derive(Clone)]
pub struct WebTransportConfig {
    /// Handle to the bounded, single-owner core worker.
    pub worker: WorkerHandle,
    /// Exact WebTransport request path accepted by this adapter.
    pub path: Arc<str>,
    /// Authentication scheme accepted during the WVN1 handshake.
    pub expected_authentication_scheme: AuthenticationScheme,
    /// Server name reported in the protocol capabilities response.
    pub server_name: Arc<str>,
    /// Server version reported in the protocol capabilities response.
    pub server_version: Arc<str>,
    /// Browser origin authorization policy evaluated before accepting a session.
    pub origin_policy: OriginPolicy,
    /// Where client-sent inference control messages are forwarded when the inference plane
    /// is enabled. `None` means the plane is disabled and those messages are rejected.
    pub inference_sink: Option<mpsc::Sender<UnroutedControl>>,
}

impl WebTransportConfig {
    #[must_use]
    pub fn new(worker: WorkerHandle) -> Self {
        Self {
            worker,
            path: Arc::from("/webtransport"),
            expected_authentication_scheme: AuthenticationScheme::Development,
            server_name: Arc::from("woven"),
            server_version: Arc::from(env!("CARGO_PKG_VERSION")),
            origin_policy: OriginPolicy::default(),
            inference_sink: None,
        }
    }
}

/// Server-side WebTransport endpoint type.
pub type ServerEndpoint = Endpoint<Server>;

/// Bind a WebTransport server endpoint using the supplied TLS identity.
///
/// The embedding server owns certificate lifecycle; this crate does not create self-signed
/// identities or otherwise manage credentials.
pub fn server_endpoint(
    bind_address: SocketAddr,
    identity: Identity,
) -> std::io::Result<ServerEndpoint> {
    Endpoint::server(
        ServerConfig::builder()
            .with_bind_address(bind_address)
            .with_identity(identity)
            .build(),
    )
}

/// Accept WebTransport connection attempts until the endpoint is closed.
///
/// At most 4,096 connection tasks are active. When the limit is reached, acceptance pauses
/// instead of creating an application-level backlog.
pub async fn serve_endpoint(endpoint: ServerEndpoint, config: WebTransportConfig) {
    serve_shared_endpoint(Arc::new(endpoint), config).await;
}

/// Accept WebTransport connection attempts on a retained shared endpoint.
///
/// This form lets an embedding server retain a handle for explicit shutdown while the accept
/// loop is running. At most 4,096 connection tasks are active, matching [`serve_endpoint`].
pub async fn serve_shared_endpoint(endpoint: Arc<ServerEndpoint>, config: WebTransportConfig) {
    let connection_tasks = Arc::new(Semaphore::new(MAX_CONNECTION_TASKS));
    loop {
        let incoming = endpoint.accept().await;
        let Ok(permit) = connection_tasks.clone().acquire_owned().await else {
            return;
        };
        let connection_config = config.clone();
        std::mem::drop(tokio::spawn(async move {
            let _permit = permit;
            let Ok(request) = incoming.await else {
                return;
            };
            serve_request(request, connection_config).await;
        }));
    }
}

async fn serve_request(request: SessionRequest, config: WebTransportConfig) {
    if request.path() != config.path.as_ref() {
        request.not_found().await;
        return;
    }
    if !config.origin_policy.allows(request.origin()) {
        request.forbidden().await;
        return;
    }
    let Ok(connection) = request.accept().await else {
        return;
    };
    serve_connection(connection, config).await;
}

/// Serve one accepted WebTransport session.
///
/// The first client-initiated bidirectional stream is the Woven control stream. Any second
/// client bidirectional stream is a protocol violation and closes the session.
#[allow(
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::too_many_lines
)]
pub async fn serve_connection(connection: Connection, config: WebTransportConfig) {
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

    let drain_task = spawn_outbound_drain_webtransport(
        config.worker.clone(),
        core_connection,
        connection.clone(),
        codec.clone(),
        write_sender.clone(),
        shutdown_sender.clone(),
    );

    let mut greeted = false;
    let mut negotiated_capability_bits = 0;
    let mut authenticated = false;
    'connection: loop {
        let received = {
            // read_exact is not cancellation-safe once a prefix/body has been partly consumed.
            let frame = read_envelope(&mut receive_stream, &codec);
            tokio::pin!(frame);
            loop {
                tokio::select! {
                    _ = shutdown_receiver.recv() => break 'connection,
                    extra_stream = connection.accept_bi() => {
                        if extra_stream.is_ok() {
                            send_error(
                                &write_sender,
                                MessageKind::Unknown,
                                ProtocolErrorCode::UnsupportedMessage,
                                "only one client bidirectional stream is supported".to_owned(),
                            ).await;
                        }
                        break 'connection;
                    }
                    datagram = connection.receive_datagram() => {
                        match datagram {
                            Ok(datagram) => {
                                if !authenticated {
                                    trace!(?core_connection, "dropping pre-authentication datagram");
                                    continue;
                                }
                                match codec.decode(datagram.payload().as_ref()) {
                                    Ok(envelope) if is_client_datagram(&envelope) => {
                                        if handle_authenticated_with_capabilities(
                                            &config.worker,
                                            core_connection,
                                            envelope,
                                            &write_sender,
                                            config.inference_sink.as_ref(),
                                            negotiated_capability_bits,
                                        ).await.is_err() {
                                            break 'connection;
                                        }
                                    }
                                    Ok(_) => trace!("dropping non-state or reliable client datagram"),
                                    Err(error) => {
                                        trace!(?error, "dropping malformed datagram");
                                    }
                                }
                            }
                            Err(_) => break 'connection,
                        }
                    }
                    received = &mut frame => break received,
                }
            }
        };
        let envelope = match received {
            Ok(envelope) => envelope,
            Err(error) => {
                send_error(
                    &write_sender,
                    MessageKind::Unknown,
                    ProtocolErrorCode::MalformedFrame,
                    error,
                )
                .await;
                break;
            }
        };
        let result = if !greeted {
            handle_hello(&write_sender, &config, &envelope)
                .await
                .map(|capabilities| {
                    negotiated_capability_bits = capabilities;
                    greeted = true;
                })
        } else if !authenticated {
            handle_authenticate(&config, core_connection, &write_sender, envelope)
                .await
                .map(|()| authenticated = true)
        } else {
            handle_authenticated_with_capabilities(
                &config.worker,
                core_connection,
                envelope,
                &write_sender,
                config.inference_sink.as_ref(),
                negotiated_capability_bits,
            )
            .await
        };
        if result.is_err() {
            break;
        }
    }

    drain_task.abort();
    config.worker.discard_and_disconnect(core_connection).await;
    drop(write_sender);
    drop(shutdown_sender);
    let _ = writer_task.await;
    close(
        &connection,
        CLOSE_PROTOCOL,
        b"Woven WebTransport session closed",
    );
    debug!(?core_connection, "WebTransport session closed");
}

fn spawn_outbound_drain_webtransport(
    worker: WorkerHandle,
    connection: ConnectionId,
    wt_connection: Connection,
    codec: Codec,
    write_sender: mpsc::Sender<Envelope>,
    shutdown_sender: mpsc::Sender<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        loop {
            interval.tick().await;
            if flush_outbound_webtransport(
                &worker,
                connection,
                &wt_connection,
                &codec,
                &write_sender,
            )
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

async fn flush_outbound_webtransport(
    worker: &WorkerHandle,
    connection: ConnectionId,
    wt_connection: &Connection,
    codec: &Codec,
    write_sender: &mpsc::Sender<Envelope>,
) -> Result<(), ()> {
    if let Ok(CommandResult::Outbound(messages)) =
        worker.execute(Command::DrainOutbound { connection }).await
    {
        for message in messages {
            let envelope = outbound_envelope(message);
            if envelope.delivery_class.is_unreliable() {
                send_datagram_envelope(wt_connection, codec, &envelope);
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
        if let Err(error) = connection.send_datagram(&frame) {
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
        if envelope.delivery_class == DeliveryClass::UnreliableSequenced {
            send_datagram_envelope(&connection, &codec, &envelope);
            continue;
        }
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
            close(
                &connection,
                CLOSE_TRANSPORT,
                b"WebTransport stream write failed",
            );
            return;
        }
    }
    let _ = stream.finish().await;
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
    config: &WebTransportConfig,
    envelope: &Envelope,
) -> Result<u64, ()> {
    let MessagePayload::Control(ControlPayload::Hello(hello)) = &envelope.message else {
        send_error(
            write_sender,
            envelope.message_kind(),
            ProtocolErrorCode::UnsupportedMessage,
            "expected Hello".to_owned(),
        )
        .await;
        return Err(());
    };
    let negotiated = hello.capability_bits & SUPPORTED_CAPABILITY_BITS;
    send_envelope(
        write_sender,
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Capabilities(Capabilities {
                selected_protocol_version: PROTOCOL_VERSION,
                server_name: config.server_name.to_string(),
                server_version: config.server_version.to_string(),
                capability_bits: negotiated,
                max_frame_size: MAX_FRAME_BYTES,
                max_payload_size: MAX_PAYLOAD_BYTES,
            }),
        ),
    )
    .await?;
    Ok(negotiated)
}

#[allow(clippy::manual_let_else, clippy::single_match_else)]
async fn handle_authenticate(
    config: &WebTransportConfig,
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
    if auth.scheme != config.expected_authentication_scheme {
        send_error(
            write_sender,
            MessageKind::Authenticate,
            ProtocolErrorCode::Unauthorized,
            "authentication scheme is not accepted".to_owned(),
        )
        .await;
        return Err(());
    }
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
    use std::{net::Ipv4Addr, time::Duration};

    use woven_core::{
        AccessGrant, AuthenticatedPrincipal, AuthorizationGrants, ChannelDefinition, ChannelId,
        ChannelScope, CoordinateFrame, CoreConfig, DevAuthenticator, NamespaceId, PersistenceClass,
        PrincipalId, RoutingPolicy, SessionId, SessionKey, SpaceDescriptor, SpaceEpoch, SpaceId,
        SpaceKey, TransportIndependentWorker, WovenCore,
    };
    use woven_protocol::{
        Authenticate, AuthenticationScheme, EntityLeaveReason, Hello, OpaquePayload,
    };
    use woven_transport::spawn_worker;
    use wtransport::ClientConfig;

    use super::*;
    use crate::{test_reliable, test_state};

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn loopback_client_completes_hello_and_authenticate() {
        let (_worker, server_address) = setup_server();
        let client = connect_client(server_address).await;
        let (mut send, mut recv) = open_stream(&client).await;

        perform_handshake(&mut send, &mut recv).await;

        client.close(VarInt::from_u32(0), b"test complete");
    }

    #[tokio::test]
    async fn reliable_event_fan_out_over_webtransport() {
        let (_worker, server_address) = setup_server();
        let alice = connect_client(server_address).await;
        let bob = connect_client(server_address).await;
        let (mut alice_send, mut alice_recv) = open_stream(&alice).await;
        let (mut bob_send, mut bob_recv) = open_stream(&bob).await;

        for (send, recv) in [
            (&mut alice_send, &mut alice_recv),
            (&mut bob_send, &mut bob_recv),
        ] {
            perform_handshake(send, recv).await;
            join_and_subscribe(send, recv, 1, 1).await;
        }

        let alice_entity = recv_subscription_accepted_and_entity(&mut alice_recv).await;
        let _bob_entity = recv_subscription_accepted_and_entity(&mut bob_recv).await;

        let codec = Codec::default();
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
                        routing_position: None,
                        message: MessagePayload::ReliableEvent(OpaquePayload {
                            type_id: 1,
                            bytes: b"hello-wt".to_vec(),
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
            matches!(received.message, MessagePayload::ReliableEvent(ref payload) if payload.bytes == b"hello-wt")
        );

        alice.close(VarInt::from_u32(0), b"done");
        bob.close(VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn disconnect_emits_entity_left_over_webtransport() {
        let (_worker, server_address) = setup_server();
        let alice = connect_client(server_address).await;
        let bob = connect_client(server_address).await;
        let (mut alice_send, mut alice_recv) = open_stream(&alice).await;
        let (mut bob_send, mut bob_recv) = open_stream(&bob).await;

        for (send, recv) in [
            (&mut alice_send, &mut alice_recv),
            (&mut bob_send, &mut bob_recv),
        ] {
            perform_handshake(send, recv).await;
            join_and_subscribe(send, recv, 1, 1).await;
        }

        let alice_entity = recv_subscription_accepted_and_entity(&mut alice_recv).await;
        let _bob_entity = recv_subscription_accepted_and_entity(&mut bob_recv).await;

        alice.close(VarInt::from_u32(0), b"disconnect");

        let codec = Codec::default();
        let left = tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut bob_recv, &codec))
            .await
            .expect("receive timed out")
            .expect("decode left");
        assert!(matches!(
            left.message,
            MessagePayload::Control(ControlPayload::EntityLeft(ref value))
            if left.entity_id == Some(alice_entity) && value.reason == EntityLeaveReason::Disconnected
        ));

        bob.close(VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn webtransport_datagram_roundtrip_for_unreliable_delivery() {
        let (_worker, server_address) = setup_server();
        let alice = connect_client(server_address).await;
        let bob = connect_client(server_address).await;
        let (mut alice_send, mut alice_recv) = open_stream(&alice).await;
        let (mut bob_send, mut bob_recv) = open_stream(&bob).await;

        for (send, recv) in [
            (&mut alice_send, &mut alice_recv),
            (&mut bob_send, &mut bob_recv),
        ] {
            perform_handshake(send, recv).await;
            join_and_subscribe(send, recv, 1, 3).await;
        }

        let alice_entity = recv_subscription_accepted_and_entity(&mut alice_recv).await;
        let _bob_entity = recv_subscription_accepted_and_entity(&mut bob_recv).await;

        let codec = Codec::default();
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
                routing_position: None,
                message: MessagePayload::EntityState(OpaquePayload {
                    type_id: 1,
                    bytes: b"wt-datagram".to_vec(),
                }),
            })
            .expect("encode datagram envelope");
        alice.send_datagram(&frame).expect("send datagram");

        let datagram = tokio::time::timeout(TEST_TIMEOUT, bob.receive_datagram())
            .await
            .expect("datagram receive timed out")
            .expect("read datagram");
        let received = codec
            .decode(datagram.payload().as_ref())
            .expect("decode datagram");
        assert!(
            matches!(received.message, MessagePayload::EntityState(ref payload) if payload.bytes == b"wt-datagram")
        );

        alice.close(VarInt::from_u32(0), b"done");
        bob.close(VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one bounded connection exercises writer and timer phases"
    )]
    async fn ephemeral_datagrams_cover_writer_timer_reordering_ingress_and_oversize_drop() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (worker, address) = setup_server();
            let connection = connect_client(address).await;
            let (mut send, mut recv) = open_stream(&connection).await;
            perform_handshake(&mut send, &mut recv).await;
            join_and_subscribe(&mut send, &mut recv, 1, 1).await;
            let entity = recv_subscription_accepted_and_entity(&mut recv).await;
            let codec = Codec::default();
            let core_connection = ConnectionId::new(1);
            let session = SessionKey::new(NamespaceId::new(1), SessionId::new(1));

            let mut leave = Envelope::control(
                DeliveryClass::ReliableOrdered,
                ControlPayload::LeaveSession(woven_protocol::LeaveSession {
                    reason: "must stay joined".into(),
                }),
            );
            leave.namespace_id = 1;
            leave.session_id = 1;
            connection
                .send_datagram(codec.encode(&leave).unwrap())
                .unwrap();
            let mut reliable_state = test_state(entity, 2, 1, 25);
            reliable_state.delivery_class = DeliveryClass::LatestValue;
            connection
                .send_datagram(codec.encode(&reliable_state).unwrap())
                .unwrap();
            connection
                .send_datagram(codec.encode(&test_state(entity, 4, 2, 25)).unwrap())
                .unwrap();
            assert_eq!(recv_datagram(&connection).await.sender_sequence, 2);
            for sequence in [1, 2] {
                connection
                    .send_datagram(codec.encode(&test_state(entity, 4, sequence, 25)).unwrap())
                    .unwrap();
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(100), connection.receive_datagram())
                    .await
                    .is_err()
            );
            connection
                .send_datagram(codec.encode(&test_state(entity, 4, 3, 25)).unwrap())
                .unwrap();
            assert_eq!(recv_datagram(&connection).await.sender_sequence, 3);

            worker
                .execute(Command::Publish(woven_core::PublishRequest {
                    connection: core_connection,
                    session,
                    space: SpaceId::new(1),
                    space_epoch: SpaceEpoch::new(1),
                    entity: Some(woven_core::EntityId::new(entity)),
                    channel: ChannelId::new(4),
                    sequence: 4,
                    delivery: woven_core::DeliveryClass::UnreliableSequenced,
                    persistence: PersistenceClass::Ephemeral,
                    coalesce_key: Some(woven_core::CoalesceKey::new(
                        ChannelId::new(4),
                        Some(woven_core::EntityId::new(entity)),
                        1,
                    )),
                    routing_position: None,
                    payload: vec![7; 25],
                }))
                .await
                .unwrap();
            assert_eq!(recv_datagram(&connection).await.sender_sequence, 4);
            worker
                .send_to_connection(core_connection, test_state(entity, 4, 5, 25))
                .await
                .unwrap();
            assert_eq!(recv_datagram(&connection).await.sender_sequence, 5);
            worker
                .send_to_connection(core_connection, test_state(entity, 4, 6, 65_536))
                .await
                .unwrap();
            let marker = Envelope::control(
                DeliveryClass::ReliableOrdered,
                ControlPayload::ProtocolError(woven_protocol::ProtocolError {
                    code: ProtocolErrorCode::UnsupportedMessage,
                    related_message_kind: MessageKind::EntityState,
                    message: "writer marker".into(),
                }),
            );
            worker
                .send_to_connection(core_connection, marker.clone())
                .await
                .unwrap();
            assert_eq!(
                tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut recv, &codec))
                    .await
                    .unwrap()
                    .unwrap(),
                marker
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(100), connection.receive_datagram())
                    .await
                    .is_err()
            );
            let CommandResult::Snapshot(snapshot) = worker
                .execute(Command::Snapshot {
                    connection: core_connection,
                    session,
                })
                .await
                .unwrap()
            else {
                panic!("expected snapshot")
            };
            assert!(snapshot.state.is_empty());
            assert_eq!(snapshot.state_bytes, 0);
            connection.close(VarInt::from_u32(0), b"done");
        })
        .await
        .expect("bounded datagram regression");
    }

    #[tokio::test]
    async fn split_reliable_frames_survive_interleaved_datagrams_and_timer_drains() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (_worker, address) = setup_server();
            let connection = connect_client(address).await;
            let (mut send, mut recv) = open_stream(&connection).await;
            perform_handshake(&mut send, &mut recv).await;
            join_and_subscribe(&mut send, &mut recv, 1, 1).await;
            let entity = recv_subscription_accepted_and_entity(&mut recv).await;
            let codec = Codec::default();
            let mut datagram_sequence = 0;
            for (sequence, payload) in [(1, b"profile".as_slice()), (2, b"chat".as_slice())] {
                let expected = test_reliable(entity, sequence, payload);
                let frame = codec.encode(&expected).unwrap();
                let mut start = 0;
                for end in [2, 4, frame.len() / 2, frame.len()] {
                    send.write_all(&frame[start..end]).await.unwrap();
                    start = end;
                    if end != frame.len() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        datagram_sequence += 1;
                        connection
                            .send_datagram(
                                codec
                                    .encode(&test_state(entity, 4, datagram_sequence, 25))
                                    .unwrap(),
                            )
                            .unwrap();
                        assert_eq!(
                            recv_datagram(&connection).await.sender_sequence,
                            datagram_sequence
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
                assert_eq!(
                    tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut recv, &codec))
                        .await
                        .unwrap()
                        .unwrap(),
                    expected
                );
            }
            connection.close(VarInt::from_u32(0), b"done");
        })
        .await
        .expect("bounded split-frame regression");
    }

    #[tokio::test]
    async fn startup_replies_precede_early_peer_profile() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (_worker, address) = setup_server();
            let alice = connect_client(address).await;
            let (mut alice_send, mut alice_recv) = open_stream(&alice).await;
            perform_handshake(&mut alice_send, &mut alice_recv).await;
            join_and_subscribe(&mut alice_send, &mut alice_recv, 1, 1).await;
            let alice_entity = recv_subscription_accepted_and_entity(&mut alice_recv).await;
            let bob = connect_client(address).await;
            let (mut bob_send, mut bob_recv) = open_stream(&bob).await;
            perform_handshake(&mut bob_send, &mut bob_recv).await;
            let codec = Codec::default();
            let profile = test_reliable(alice_entity, 1, b"early peer profile");
            let response = async {
                let entered =
                    tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut alice_recv, &codec))
                        .await
                        .unwrap()
                        .unwrap();
                assert!(matches!(
                    entered.message,
                    MessagePayload::Control(ControlPayload::EntityEntered(_))
                ));
                alice_send
                    .write_all(&codec.encode(&profile).unwrap())
                    .await
                    .unwrap();
                assert_eq!(
                    tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut alice_recv, &codec))
                        .await
                        .unwrap()
                        .unwrap(),
                    profile
                );
                entered.entity_id.unwrap()
            };
            let startup = async {
                join_and_subscribe(&mut bob_send, &mut bob_recv, 1, 1).await;
                let entity = recv_subscription_accepted_and_entity(&mut bob_recv).await;
                assert_eq!(
                    tokio::time::timeout(TEST_TIMEOUT, read_envelope(&mut bob_recv, &codec))
                        .await
                        .unwrap()
                        .unwrap(),
                    profile
                );
                entity
            };
            let (announced, assigned) = tokio::join!(response, startup);
            assert_eq!(announced, assigned);
            assert_ne!(assigned, alice_entity);
            alice.close(VarInt::from_u32(0), b"done");
            bob.close(VarInt::from_u32(0), b"done");
        })
        .await
        .expect("bounded startup regression");
    }

    async fn recv_datagram(connection: &Connection) -> Envelope {
        let datagram = tokio::time::timeout(TEST_TIMEOUT, connection.receive_datagram())
            .await
            .unwrap()
            .unwrap();
        let envelope = Codec::default()
            .decode(datagram.payload().as_ref())
            .unwrap();
        assert_eq!(envelope.delivery_class, DeliveryClass::UnreliableSequenced);
        assert_eq!(envelope.channel_id, Some(4));
        assert!(
            matches!(&envelope.message, MessagePayload::EntityState(payload) if payload.type_id == 1 && payload.bytes == vec![7; 25])
        );
        envelope
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    fn setup_server() -> (WorkerHandle, SocketAddr) {
        let identity =
            Identity::self_signed(["localhost", "127.0.0.1"]).expect("generate identity");
        let endpoint = server_endpoint(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), identity)
            .expect("bind WebTransport endpoint");
        let server_address = endpoint.local_addr().expect("read server address");

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
        for channel_id in [1, 2, 3, 4] {
            grants.grant_channel(
                ChannelScope::new(session, ChannelId::new(channel_id)),
                AccessGrant::ReadWrite,
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
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(1),
            woven_core::DeliveryClass::ReliableOrdered,
            PersistenceClass::Ephemeral,
            64 * 1024,
        ))
        .expect("register channel 1");
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(2),
            woven_core::DeliveryClass::LatestValue,
            PersistenceClass::Stateful { ttl: None },
            64 * 1024,
        ))
        .expect("register channel 2");
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(3),
            woven_core::DeliveryClass::UnreliableSequenced,
            PersistenceClass::Stateful { ttl: None },
            64 * 1024,
        ))
        .expect("register channel 3");
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(4),
            woven_core::DeliveryClass::UnreliableSequenced,
            PersistenceClass::Ephemeral,
            64 * 1024,
        ))
        .expect("register channel 4");
        core.provision_session(session).expect("provision session");
        for space_id in [1, 2] {
            core.install_space(
                session,
                SpaceDescriptor {
                    id: SpaceId::new(space_id),
                    local_frame: CoordinateFrame::Logical,
                    bounds: None,
                    parent: None,
                    epoch: SpaceEpoch::new(1),
                    routing: RoutingPolicy::BroadcastAll,
                },
            )
            .expect("install space");
        }
        let worker = spawn_worker(TransportIndependentWorker::new(core));
        let mut config = WebTransportConfig::new(worker.clone());
        config.path = Arc::from("/webtransport");
        tokio::spawn(serve_endpoint(endpoint, config));
        (worker, server_address)
    }

    async fn connect_client(server_address: SocketAddr) -> Connection {
        let client_config = ClientConfig::builder()
            .with_bind_default()
            .with_no_cert_validation()
            .build();
        let client_endpoint = Endpoint::client(client_config).expect("create client endpoint");
        let url = format!("https://127.0.0.1:{}/webtransport", server_address.port());
        tokio::time::timeout(TEST_TIMEOUT, client_endpoint.connect(&url))
            .await
            .expect("connect timed out")
            .expect("connect failed")
    }

    async fn open_stream(connection: &Connection) -> (SendStream, RecvStream) {
        tokio::time::timeout(TEST_TIMEOUT, connection.open_bi())
            .await
            .expect("open_bi timed out")
            .expect("open_bi failed")
            .await
            .expect("stream open failed")
    }

    async fn perform_handshake(send: &mut SendStream, recv: &mut RecvStream) {
        let codec = Codec::default();
        send.write_all(
            &codec
                .encode(&Envelope::control(
                    DeliveryClass::ReliableOrdered,
                    ControlPayload::Hello(Hello {
                        min_protocol_version: PROTOCOL_VERSION,
                        max_protocol_version: PROTOCOL_VERSION,
                        client_name: "wt-test".to_owned(),
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
        let capabilities = tokio::time::timeout(TEST_TIMEOUT, read_envelope(recv, &codec))
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
        let authenticated = tokio::time::timeout(TEST_TIMEOUT, read_envelope(recv, &codec))
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
        send: &mut SendStream,
        _recv: &mut RecvStream,
        space_id: u64,
        channel_id: u64,
    ) {
        let codec = Codec::default();
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
                    routing_position: None,
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
                    routing_position: None,
                    message: MessagePayload::Control(ControlPayload::SubscribeSpace(
                        woven_protocol::SubscribeSpace,
                    )),
                })
                .expect("encode SubscribeSpace"),
        )
        .await
        .expect("send SubscribeSpace");
    }

    async fn recv_subscription_accepted_and_entity(recv: &mut RecvStream) -> u64 {
        let codec = Codec::default();
        let accepted = tokio::time::timeout(TEST_TIMEOUT, read_envelope(recv, &codec))
            .await
            .expect("timeout")
            .expect("decode accepted");
        assert!(matches!(
            accepted.message,
            MessagePayload::Control(ControlPayload::SubscriptionAccepted(_))
        ));
        let entered = tokio::time::timeout(TEST_TIMEOUT, read_envelope(recv, &codec))
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
