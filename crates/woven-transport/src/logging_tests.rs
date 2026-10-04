use super::*;
use crate::{WorkerHandle, handle_authenticated_with_capabilities, spawn_worker};
use tokio::sync::mpsc;
use woven_core::{
    AccessGrant, AuthenticatedPrincipal, AuthorizationGrants, ChannelDefinition, ChannelId,
    Command, CommandResult, CoordinateFrame, CoreConfig, Credentials, DeliveryClass,
    DevAuthenticator, NamespaceId, OutboundQueueConfig, PersistenceClass, PrincipalId,
    PublishRequest, RoutingPolicy, SessionId, SpaceDescriptor, SpaceEpoch, SpaceId, SpaceKey,
    TransportIndependentWorker,
};
use woven_protocol::{CAPABILITY_CLIENT_LOG, ControlPayload, Envelope, MessagePayload};

const SESSION: SessionKey = SessionKey::new(NamespaceId::new(1), SessionId::new(1));
const OTHER: SessionKey = SessionKey::new(NamespaceId::new(2), SessionId::new(1));
const SPACE: SpaceKey = SpaceKey::new(SESSION, SpaceId::new(1));

fn core() -> WovenCore<DevAuthenticator> {
    let mut grants = AuthorizationGrants::new();
    grants.grant_namespace(SESSION.namespace, AccessGrant::ReadWrite);
    grants.grant_session(SESSION, AccessGrant::ReadWrite);
    grants.grant_space(SPACE, AccessGrant::ReadWrite);
    grants.grant_channel(
        woven_core::ChannelScope::new(SESSION, ChannelId::new(1)),
        AccessGrant::ReadWrite,
    );
    let mut auth = DevAuthenticator::new();
    auth.insert(
        "test",
        AuthenticatedPrincipal::new(PrincipalId::new(1), grants),
    )
    .unwrap();
    let mut core = WovenCore::new(
        auth,
        CoreConfig {
            outbound_queue: OutboundQueueConfig {
                total_capacity: 1,
                critical_capacity: 1,
                latest_capacity: 1,
                best_effort_capacity: 1,
            },
            ..CoreConfig::default()
        },
    )
    .unwrap();
    core.provision_session(SESSION).unwrap();
    core.provision_session(OTHER).unwrap();
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(1),
        DeliveryClass::ReliableOrdered,
        PersistenceClass::Ephemeral,
        1024,
    ))
    .unwrap();
    core.install_space(
        SESSION,
        SpaceDescriptor {
            id: SPACE.space,
            local_frame: CoordinateFrame::Logical,
            bounds: None,
            parent: None,
            epoch: SpaceEpoch::new(1),
            routing: RoutingPolicy::BroadcastAll,
        },
    )
    .unwrap();
    core
}

fn message(value: &str) -> ClientLog {
    ClientLog {
        level: LogLevel::Info,
        message: value.to_owned(),
    }
}

async fn connect(worker: &WorkerHandle) -> ConnectionId {
    let CommandResult::Connected(connection) =
        worker.execute(Command::TransportConnected).await.unwrap()
    else {
        panic!("expected connection")
    };
    worker
        .execute(Command::Authenticate {
            connection,
            credentials: Credentials::new("test"),
        })
        .await
        .unwrap();
    connection
}

async fn join(worker: &WorkerHandle, connection: ConnectionId) {
    worker
        .execute(Command::JoinSession {
            connection,
            session: SESSION,
        })
        .await
        .unwrap();
}

#[test]
fn authentication_membership_tenant_rate_and_ring_are_independently_bounded() {
    let mut core = core();
    let connection = core.transport_connected().unwrap();
    let mut capture = LogCapture::default();
    let now = Instant::now();
    assert_eq!(
        capture.client(&core, connection, SESSION, message("no auth"), now),
        Err(ProtocolErrorCode::AuthenticationRequired)
    );
    core.authenticate(connection, &Credentials::new("test"))
        .unwrap();
    assert_eq!(
        capture.client(&core, connection, SESSION, message("not joined"), now),
        Err(ProtocolErrorCode::InvalidScope)
    );
    assert!(core.join_session(connection, OTHER).is_err());
    capture.observe_connection(&core, connection, "session left");
    assert!(capture.entries.is_empty());
    core.join_session(connection, SESSION).unwrap();
    capture.observe_connection(&core, connection, "session left");
    capture.observe_connection(&core, connection, "session left");
    assert_eq!(capture.entries.len(), 1);
    assert_eq!(
        capture.client(&core, connection, OTHER, message("other tenant"), now),
        Err(ProtocolErrorCode::InvalidScope)
    );
    assert_eq!(
        capture.client(&core, connection, SESSION, message(&"é".repeat(513)), now),
        Err(ProtocolErrorCode::PayloadTooLarge)
    );
    for _ in 0..10 {
        capture
            .client(&core, connection, SESSION, message(&"é".repeat(512)), now)
            .unwrap();
    }
    assert_eq!(
        capture.client(
            &core,
            connection,
            SESSION,
            message("limited"),
            now + Duration::from_millis(999)
        ),
        Err(ProtocolErrorCode::RateLimited)
    );
    capture
        .client(
            &core,
            connection,
            SESSION,
            message("new window"),
            now + LOG_WINDOW,
        )
        .unwrap();
    for index in 2..2200 {
        capture
            .client(
                &core,
                connection,
                SESSION,
                message("bounded"),
                now + Duration::from_secs(index),
            )
            .unwrap();
    }
    assert_eq!(capture.entries.len(), LOG_CAPACITY);
    let page = capture.page(0, usize::MAX);
    assert_eq!(page.entries.len(), MAX_LOG_PAGE_ENTRIES);
    assert_eq!(page.entries[0].sequence, page.dropped_through + 1);
    assert_eq!(page.next_sequence, page.entries.last().unwrap().sequence);
    assert!(page.dropped_through > 0);
    let empty = capture.page(u64::MAX, 32);
    assert!(empty.entries.is_empty());
    assert_eq!(empty.next_sequence, u64::MAX);
    core.transport_lost(connection).unwrap();
    capture.observe_connection(&core, connection, "transport lost");
    capture.observe_connection(&core, connection, "transport lost");
    assert!(capture.connections.is_empty());
    assert_eq!(
        capture
            .entries
            .iter()
            .filter(|entry| entry.event == "client.disconnected")
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_managed_join_records_nothing_and_successful_admission_records_once() {
    let mut core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    core.enable_managed(&Credentials::new(
        "local-log-test-admin-credential-0123456789",
    ))
    .unwrap();
    let worker = spawn_worker(TransportIndependentWorker::new(core));
    let token = "a".repeat(64);
    worker
        .manage(woven_core::ManagedRequest::Put {
            session: SESSION,
            revision: 1,
            allocated_ccu: 1,
            tick_rate_hz: None,
            credentials: Credentials::new(token.clone()),
        })
        .await
        .unwrap();
    let CommandResult::Connected(connection) =
        worker.execute(Command::TransportConnected).await.unwrap()
    else {
        panic!("expected connection")
    };
    worker
        .execute(Command::Authenticate {
            connection,
            credentials: Credentials::new(token),
        })
        .await
        .unwrap();
    assert!(
        worker
            .execute(Command::JoinSession {
                connection,
                session: SESSION
            })
            .await
            .is_err()
    );
    assert_eq!(
        worker
            .capture_client_log(connection, SESSION, message("not admitted"))
            .await,
        Err(ProtocolErrorCode::InvalidScope)
    );
    assert!(worker.read_logs(0, 32).await.unwrap().entries.is_empty());
    for _ in 0..2 {
        let result = worker
            .execute(Command::RequestSessionAdmission {
                connection,
                session: SESSION,
                idempotency_key: woven_core::IdempotencyKey::new("managed-log-test").unwrap(),
            })
            .await
            .unwrap();
        assert!(matches!(
            result,
            CommandResult::Admission(woven_core::JoinDecision::Admitted(_))
        ));
    }
    let page = worker.read_logs(0, 32).await.unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].event, "client.connected");
    assert_eq!(page.entries[0].connection, connection);
}

#[test]
fn leaving_and_rejoining_cannot_reset_the_connection_log_rate() {
    let mut core = core();
    let connection = core.transport_connected().unwrap();
    core.authenticate(connection, &Credentials::new("test"))
        .unwrap();
    core.join_session(connection, SESSION).unwrap();
    let mut capture = LogCapture::default();
    let now = Instant::now();
    for _ in 0..10 {
        capture
            .client(&core, connection, SESSION, message("rate window"), now)
            .unwrap();
    }
    core.leave_session(connection, SESSION).unwrap();
    capture.observe_connection(&core, connection, "session left");
    core.join_session(connection, SESSION).unwrap();
    capture.observe_connection(&core, connection, "session left");
    assert_eq!(
        capture.client(&core, connection, SESSION, message("rejoin"), now),
        Err(ProtocolErrorCode::RateLimited)
    );
    capture
        .client(
            &core,
            connection,
            SESSION,
            message("next window"),
            now + LOG_WINDOW,
        )
        .unwrap();
}

#[test]
fn node_aggregate_limit_and_sequence_exhaustion_fail_bounded() {
    let mut core = core();
    let mut capture = LogCapture::default();
    let now = Instant::now();
    for index in 0..103 {
        let connection = core.transport_connected().unwrap();
        core.authenticate(connection, &Credentials::new("test"))
            .unwrap();
        core.join_session(connection, SESSION).unwrap();
        for offset in 0..10 {
            let result = capture.client(&core, connection, SESSION, message("aggregate"), now);
            if index * 10 + offset < NODE_LOG_RATE {
                result.unwrap();
            } else {
                assert_eq!(result, Err(ProtocolErrorCode::RateLimited));
            }
        }
    }
    assert_eq!(capture.accepted.len(), NODE_LOG_RATE);
    capture.last_sequence = u64::MAX;
    let before = capture.entries.len();
    capture.append(
        ConnectionId::new(1),
        SESSION,
        "node",
        "client.connected",
        LogLevel::Info,
        "exhausted".into(),
    );
    assert_eq!(capture.last_sequence, u64::MAX);
    assert_eq!(capture.entries.len(), before);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one lifecycle verifies rejection, non-relay, and cleanup invariants"
)]
async fn bridge_rejections_are_nonfatal_and_logs_never_relay_to_peers() {
    let worker = spawn_worker(TransportIndependentWorker::new(core()));
    let connection = connect(&worker).await;
    let peer = connect(&worker).await;
    join(&worker, connection).await;
    join(&worker, peer).await;
    let (peer_sender, mut peer_receiver) = mpsc::channel(4);
    let (shutdown, _shutdown_receiver) = mpsc::channel(1);
    worker
        .register_lifecycle(peer, peer_sender, shutdown)
        .await
        .unwrap();
    worker
        .execute(Command::Subscribe {
            connection: peer,
            space: SPACE,
        })
        .await
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(4);
    for (session, log, bits, expected) in [
        (
            OTHER,
            message("tenant spoof"),
            CAPABILITY_CLIENT_LOG,
            ProtocolErrorCode::InvalidScope,
        ),
        (
            SESSION,
            message(&"x".repeat(1025)),
            CAPABILITY_CLIENT_LOG,
            ProtocolErrorCode::PayloadTooLarge,
        ),
        (
            SESSION,
            message("not negotiated"),
            0,
            ProtocolErrorCode::UnsupportedMessage,
        ),
    ] {
        let mut envelope = Envelope::control(
            woven_protocol::DeliveryClass::ReliableOrdered,
            ControlPayload::ClientLog(log),
        );
        envelope.namespace_id = session.namespace.get();
        envelope.session_id = session.session.get();
        handle_authenticated_with_capabilities(&worker, connection, envelope, &sender, None, bits)
            .await
            .unwrap();
        assert!(
            matches!(receiver.try_recv().unwrap().message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == expected)
        );
    }
    let mut envelope = Envelope::control(
        woven_protocol::DeliveryClass::ReliableOrdered,
        ControlPayload::ClientLog(message("private log")),
    );
    envelope.namespace_id = 1;
    envelope.session_id = 1;
    for _ in 0..10 {
        handle_authenticated_with_capabilities(
            &worker,
            connection,
            envelope.clone(),
            &sender,
            None,
            CAPABILITY_CLIENT_LOG,
        )
        .await
        .unwrap();
    }
    handle_authenticated_with_capabilities(
        &worker,
        connection,
        envelope,
        &sender,
        None,
        CAPABILITY_CLIENT_LOG,
    )
    .await
    .unwrap();
    assert!(
        matches!(receiver.try_recv().unwrap().message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::RateLimited)
    );
    assert!(receiver.try_recv().is_err());
    assert!(peer_receiver.try_recv().is_err());
    assert!(
        matches!(worker.execute(Command::DrainOutbound { connection: peer }).await.unwrap(), CommandResult::Outbound(messages) if messages.is_empty())
    );
    let page = worker.read_logs(0, 32).await.unwrap();
    assert_eq!(
        page.entries
            .iter()
            .filter(|entry| entry.source == "client")
            .count(),
        10
    );
    assert!(page.entries.iter().all(|entry| entry.session == SESSION));
    worker
        .execute(Command::LeaveSession {
            connection,
            session: SESSION,
        })
        .await
        .unwrap();
    assert!(
        worker
            .execute(Command::LeaveSession {
                connection,
                session: SESSION,
            })
            .await
            .is_err()
    );
    worker.discard_and_disconnect(connection).await;
    worker.discard_and_disconnect(connection).await;
    assert_eq!(
        worker
            .read_logs(0, 32)
            .await
            .unwrap()
            .entries
            .iter()
            .filter(|entry| entry.connection == connection && entry.event == "client.disconnected")
            .count(),
        1
    );
}

#[tokio::test]
async fn internal_core_slow_consumer_disconnects_are_captured_once() {
    let worker = spawn_worker(TransportIndependentWorker::new(core()));
    let publisher = connect(&worker).await;
    let peer = connect(&worker).await;
    join(&worker, publisher).await;
    join(&worker, peer).await;
    worker
        .execute(Command::Subscribe {
            connection: publisher,
            space: SPACE,
        })
        .await
        .unwrap();
    worker
        .execute(Command::Subscribe {
            connection: peer,
            space: SPACE,
        })
        .await
        .unwrap();
    let CommandResult::EntitySpawned(entity) = worker
        .execute(Command::SpawnEntity {
            connection: publisher,
            space: SPACE,
            epoch: SpaceEpoch::new(1),
        })
        .await
        .unwrap()
    else {
        panic!("expected entity")
    };
    let (sender, _receiver) = mpsc::channel(4);
    let (shutdown, mut shutdown_receiver) = mpsc::channel(1);
    worker
        .register_lifecycle(peer, sender, shutdown)
        .await
        .unwrap();
    for sequence in 1..=2 {
        worker
            .execute(Command::Publish(PublishRequest {
                connection: publisher,
                session: SESSION,
                space: SPACE.space,
                space_epoch: SpaceEpoch::new(1),
                entity: Some(entity),
                channel: ChannelId::new(1),
                sequence,
                delivery: DeliveryClass::ReliableOrdered,
                persistence: PersistenceClass::Ephemeral,
                coalesce_key: None,
                routing_position: None,
                payload: b"opaque".to_vec(),
            }))
            .await
            .unwrap();
        worker
            .execute(Command::DrainOutbound {
                connection: publisher,
            })
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), shutdown_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    worker.discard_and_disconnect(peer).await;
    let page = worker.read_logs(0, 32).await.unwrap();
    let events: Vec<_> = page
        .entries
        .iter()
        .filter(|entry| entry.event == "client.disconnected")
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].connection, peer);
    assert_eq!(events[0].message, "slow consumer disconnected");
    assert_eq!(page.entries.len(), 3, "no publication noise");
}

#[tokio::test]
async fn lifecycle_write_failure_also_captures_disconnect_once() {
    let worker = spawn_worker(TransportIndependentWorker::new(core()));
    let connection = connect(&worker).await;
    join(&worker, connection).await;
    let (sender, receiver) = mpsc::channel(1);
    drop(receiver);
    let (shutdown, _shutdown_receiver) = mpsc::channel(1);
    worker
        .register_lifecycle(connection, sender, shutdown)
        .await
        .unwrap();
    assert!(
        worker
            .send_to_connection(
                connection,
                Envelope::control(
                    woven_protocol::DeliveryClass::ReliableUnordered,
                    ControlPayload::Ping(woven_protocol::Ping {
                        nonce: 1,
                        sender_time_micros: 0
                    })
                )
            )
            .await
            .is_err()
    );
    worker.discard_and_disconnect(connection).await;
    let page = worker.read_logs(0, 32).await.unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[1].event, "client.disconnected");
}
