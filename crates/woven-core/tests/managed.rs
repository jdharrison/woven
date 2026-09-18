use std::time::{Duration, Instant};
use woven_core::*;

const ADMIN: &str = "test-admin-credential-independent-0123456789";
fn session(id: u64) -> SessionKey {
    SessionKey::new(NamespaceId::new(id), SessionId::new(id))
}
fn put(id: u64, token: &str, ccu: u32) -> ManagedRequest {
    ManagedRequest::Put {
        session: session(id),
        revision: 1,
        allocated_ccu: ccu,
        credentials: Credentials::new(token),
    }
}
fn core() -> WovenCore<DevAuthenticator> {
    let mut core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    core.enable_managed(&Credentials::new(ADMIN)).unwrap();
    core
}
fn snapshot(core: &mut WovenCore<DevAuthenticator>, id: u64) -> ManagedSnapshot {
    let ManagedOutcome::Snapshot { snapshot, .. } = core
        .manage_at(
            ManagedRequest::Get {
                session: session(id),
            },
            Instant::now(),
        )
        .unwrap()
    else {
        panic!("snapshot");
    };
    snapshot
}
fn connect(core: &mut WovenCore<DevAuthenticator>, token: &str) -> (ConnectionId, PrincipalId) {
    let connection = core.transport_connected().unwrap();
    let principal = core
        .authenticate(connection, &Credentials::new(token))
        .unwrap();
    (connection, principal)
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scoped authentication and queue lifecycle"
)]
fn managed_auth_is_scoped_and_each_connection_has_a_distinct_identity() {
    let mut core = core();
    let now = Instant::now();
    let a = "a".repeat(64);
    let b = "b".repeat(64);
    assert_eq!(core.session_count(), 0);
    for token in [ADMIN, "dev-token", a.as_str(), ""] {
        let connection = core.transport_connected().unwrap();
        assert_eq!(
            core.authenticate(connection, &Credentials::new(token)),
            Err(CoreError::AuthenticationFailed(
                AuthError::InvalidCredentials
            ))
        );
        core.transport_lost(connection).unwrap();
    }
    core.manage_at(put(1, &a, 1), now).unwrap();
    core.manage_at(put(2, &b, 1), now).unwrap();
    let (first, first_principal) = connect(&mut core, &a);
    let (second, second_principal) = connect(&mut core, &a);
    assert_ne!(first_principal, second_principal);
    assert!(
        core.request_session_admission_at(
            first,
            session(2),
            IdempotencyKey::new("x").unwrap(),
            now
        )
        .is_err()
    );
    assert_eq!(
        core.join_session(first, session(1)),
        Err(CoreError::AdmissionLeaseRequired(session(1)))
    );
    let mut worker = TransportIndependentWorker::new(core);
    let request = |connection| Command::RequestSessionAdmission {
        connection,
        session: session(1),
        idempotency_key: IdempotencyKey::new("same").unwrap(),
    };
    assert!(matches!(
        worker.handle(request(first)).unwrap(),
        CommandResult::Admission(JoinDecision::Admitted(_))
    ));
    // No second join command is needed before subscribing.
    worker
        .handle(Command::Subscribe {
            connection: first,
            space: SpaceKey::new(session(1), SpaceId::new(1)),
        })
        .unwrap();
    let CommandResult::Admission(JoinDecision::Queued(ticket)) =
        worker.handle(request(second)).unwrap()
    else {
        panic!("queue");
    };
    for operation in [
        QueueOperation::Status,
        QueueOperation::Heartbeat,
        QueueOperation::Cancel,
        QueueOperation::Claim,
    ] {
        assert_eq!(
            worker
                .core_mut()
                .session_queue_at(
                    first,
                    session(1),
                    ticket.id,
                    operation,
                    now + Duration::from_secs(2)
                )
                .unwrap(),
            QueueStatus::Missing
        );
    }
    assert!(matches!(
        worker.core_mut().session_queue_at(
            first,
            session(1),
            ticket.id,
            QueueOperation::Status,
            now + Duration::from_secs(2)
        ),
        Err(CoreError::AdmissionRateLimited { .. })
    ));
    worker
        .handle(Command::TransportLost { connection: first })
        .unwrap();
    assert_eq!(
        worker
            .handle(Command::SessionQueue {
                connection: second,
                session: session(1),
                ticket: ticket.id,
                operation: QueueOperation::Claim
            })
            .unwrap(),
        CommandResult::Queue(QueueStatus::Admitted)
    );
    assert_eq!(snapshot(worker.core_mut(), 1).admission.active_ccu, 1);
    worker
        .handle(Command::TransportLost { connection: second })
        .unwrap();
    assert_eq!(snapshot(worker.core_mut(), 1).admission.available_slots, 1);
}

#[test]
fn revisions_retries_capacity_and_revocation_preserve_scope_history() {
    let mut core = core();
    let now = Instant::now();
    let a = "a".repeat(64);
    assert!(matches!(
        core.manage_at(put(1, &a, 1), now).unwrap(),
        ManagedOutcome::Snapshot { created: true, .. }
    ));
    assert!(matches!(
        core.manage_at(put(1, &a, 1), now).unwrap(),
        ManagedOutcome::Snapshot { created: false, .. }
    ));
    assert_eq!(
        core.manage_at(put(2, &a, 1), now),
        Err(ManagedError::TokenConflict)
    );
    assert_eq!(
        core.manage_at(put(1, &a, 2), now),
        Err(ManagedError::RevisionConflict)
    );
    let (joined, _) = connect(&mut core, &a);
    let (waiting, _) = connect(&mut core, &a);
    let (idle, _) = connect(&mut core, &a);
    core.admit_and_join_session_at(
        joined,
        session(1),
        IdempotencyKey::new("joined").unwrap(),
        now,
    )
    .unwrap();
    core.admit_and_join_session_at(
        waiting,
        session(1),
        IdempotencyKey::new("waiting").unwrap(),
        now,
    )
    .unwrap();
    let patch = ManagedRequest::Patch {
        session: session(1),
        revision: 2,
        allocated_ccu: 0,
    };
    core.manage_at(patch.clone(), now).unwrap();
    core.manage_at(patch, now).unwrap();
    let value = snapshot(&mut core, 1);
    assert_eq!(value.allocated_ccu, 0);
    assert_eq!(value.admission.pending_target, Some(0));
    assert_eq!(value.admission.available_slots, 0);
    assert_eq!(value.admission.active_ccu, 1);
    assert_eq!(
        core.manage_at(
            ManagedRequest::Delete {
                session: session(1),
                revision: 1
            },
            now
        ),
        Err(ManagedError::RevisionConflict)
    );
    let delete = ManagedRequest::Delete {
        session: session(1),
        revision: 2,
    };
    let ManagedOutcome::Deleted { connections } = core.manage_at(delete.clone(), now).unwrap()
    else {
        panic!("deleted");
    };
    assert_eq!(connections, vec![joined, waiting, idle]);
    assert_eq!(core.connection_count(), 0);
    assert_eq!(core.session_count(), 0);
    core.manage_at(delete, now).unwrap();
    assert_eq!(
        core.manage_at(put(1, &a, 1), now),
        Err(ManagedError::ScopeRetired)
    );
    assert_eq!(
        core.manage_at(put(2, &a, 1), now),
        Err(ManagedError::TokenConflict)
    );
    let connection = core.transport_connected().unwrap();
    assert!(core.authenticate(connection, &Credentials::new(a)).is_err());
    core.maintain_admission_at(now + Duration::from_secs(1000));
}

#[test]
fn invalid_configuration_and_capacity_do_not_partially_provision() {
    let mut core = core();
    for token in ["a".repeat(63), "A".repeat(64), "g".repeat(64)] {
        assert_eq!(
            core.manage_at(put(1, &token, 1), Instant::now()),
            Err(ManagedError::InvalidRequest)
        );
    }
    assert_eq!(
        core.manage_at(put(1, &"a".repeat(64), 4097), Instant::now()),
        Err(ManagedError::InvalidRequest)
    );
    assert_eq!(core.session_count(), 0);
    let admin = "f".repeat(64);
    let mut core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    core.enable_managed(&Credentials::new(&admin)).unwrap();
    assert_eq!(
        core.manage_at(put(1, &admin, 1), Instant::now()),
        Err(ManagedError::TokenConflict)
    );
    let config = CoreConfig {
        max_sessions: 1,
        ..CoreConfig::default()
    };
    let mut core = WovenCore::new(DevAuthenticator::new(), config).unwrap();
    core.enable_managed(&Credentials::new(ADMIN)).unwrap();
    core.manage_at(put(1, &"a".repeat(64), 1), Instant::now())
        .unwrap();
    assert_eq!(
        core.manage_at(put(2, &"b".repeat(64), 1), Instant::now()),
        Err(ManagedError::CapacityExhausted)
    );
    core.manage_at(
        ManagedRequest::Delete {
            session: session(1),
            revision: 1,
        },
        Instant::now(),
    )
    .unwrap();
    core.manage_at(put(2, &"b".repeat(64), 1), Instant::now())
        .unwrap();
}

#[test]
fn managed_mode_requires_only_one_channel_slot() {
    let config = CoreConfig {
        max_channels: 1,
        ..CoreConfig::default()
    };
    let mut core = WovenCore::new(DevAuthenticator::new(), config).unwrap();
    core.enable_managed(&Credentials::new(ADMIN)).unwrap();
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(1),
        DeliveryClass::ReliableOrdered,
        PersistenceClass::Ephemeral,
        65_536,
    ))
    .unwrap();
    assert!(matches!(
        core.manage_at(put(1, &"a".repeat(64), 1), Instant::now()),
        Ok(ManagedOutcome::Snapshot { created: true, .. })
    ));
}

#[test]
fn retired_scope_history_exhaustion_fails_closed() {
    let mut core = core();
    let now = Instant::now();
    for id in 1..=4096 {
        core.manage_at(put(id, &format!("{id:064x}"), 0), now)
            .unwrap();
        core.manage_at(
            ManagedRequest::Delete {
                session: session(id),
                revision: 1,
            },
            now,
        )
        .unwrap();
    }
    assert_eq!(core.session_count(), 0);
    assert_eq!(
        core.manage_at(put(4097, &format!("{:064x}", 4097), 0), now),
        Err(ManagedError::CapacityExhausted)
    );
    assert_eq!(
        core.manage_at(put(1, &format!("{:064x}", 1), 0), now),
        Err(ManagedError::ScopeRetired)
    );
}
