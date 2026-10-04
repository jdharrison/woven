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
        tick_rate_hz: None,
        credentials: Credentials::new(token),
    }
}
fn spatial(id: u64, revision: u64) -> ManagedRequest {
    ManagedRequest::PutSpace {
        session: session(1),
        revision,
        descriptor: SpaceDescriptor {
            id: SpaceId::new(id),
            local_frame: CoordinateFrame::Cartesian3D {
                meters_per_unit: 1.0,
            },
            bounds: Some(SpatialBounds3D {
                min_x: -1000.0,
                min_y: -1000.0,
                min_z: -1000.0,
                max_x: 1000.0,
                max_y: 1000.0,
                max_z: 1000.0,
            }),
            parent: None,
            epoch: SpaceEpoch::new(1),
            routing: RoutingPolicy::SpatialGrid3D {
                cell_size: 10.0,
                interest_radius: 15.0,
                exact_distance: true,
            },
        },
    }
}

fn core() -> WovenCore<DevAuthenticator> {
    let mut core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    core.enable_managed(&Credentials::new(ADMIN)).unwrap();
    core
}
fn rate_patch(revision: u64, allocated_ccu: u32, tick_rate_hz: Option<u32>) -> ManagedRequest {
    ManagedRequest::Patch {
        session: session(1),
        revision,
        allocated_ccu,
        tick_rate_hz,
    }
}

fn rate_put(tick_rate_hz: Option<u32>) -> ManagedRequest {
    ManagedRequest::Put {
        session: session(1),
        revision: 1,
        allocated_ccu: 1,
        tick_rate_hz,
        credentials: Credentials::new("a".repeat(64)),
    }
}

fn event(connection: ConnectionId, entity: EntityId, sequence: u64) -> PublishRequest {
    PublishRequest {
        connection,
        session: session(1),
        space: SpaceId::new(1),
        space_epoch: SpaceEpoch::new(1),
        entity: Some(entity),
        channel: ChannelId::new(1),
        sequence,
        delivery: DeliveryClass::ReliableOrdered,
        persistence: PersistenceClass::Ephemeral,
        coalesce_key: None,
        routing_position: None,
        payload: vec![1],
    }
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
        tick_rate_hz: None,
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
fn managed_spatial_spaces_are_add_only_bounded_and_refresh_exact_grants() {
    let mut core = core();
    let now = Instant::now();
    let token = "a".repeat(64);
    core.manage_at(put(1, &token, 2), now).unwrap();
    let (existing, _) = connect(&mut core, &token);

    assert!(matches!(
        core.manage_at(spatial(3, 2), now).unwrap(),
        ManagedOutcome::Snapshot {
            created: true,
            ref snapshot
        } if snapshot.revision == "2" && snapshot.spaces.len() == 3
    ));
    assert!(matches!(
        core.manage_at(spatial(3, 2), now).unwrap(),
        ManagedOutcome::Snapshot { created: false, .. }
    ));
    assert_eq!(
        core.manage_at(spatial(4, 4), now),
        Err(ManagedError::RevisionConflict)
    );
    let mut conflict = spatial(4, 2);
    assert_eq!(
        core.manage_at(conflict.clone(), now),
        Err(ManagedError::RevisionConflict)
    );
    if let ManagedRequest::PutSpace { revision, .. } = &mut conflict {
        *revision = 3;
    }
    core.manage_at(conflict, now).unwrap();
    let same_revision_capacity = ManagedRequest::Patch {
        session: session(1),
        revision: 3,
        allocated_ccu: 2,
        tick_rate_hz: None,
    };
    assert_eq!(
        core.manage_at(same_revision_capacity, now),
        Err(ManagedError::RevisionConflict)
    );
    let capacity = ManagedRequest::Patch {
        session: session(1),
        revision: 4,
        allocated_ccu: 2,
        tick_rate_hz: None,
    };
    core.manage_at(capacity.clone(), now).unwrap();
    core.manage_at(capacity, now).unwrap();
    assert_eq!(
        core.manage_at(spatial(5, 4), now),
        Err(ManagedError::RevisionConflict)
    );

    core.admit_and_join_session_at(
        existing,
        session(1),
        IdempotencyKey::new("existing").unwrap(),
        now,
    )
    .unwrap();
    core.subscribe(existing, SpaceKey::new(session(1), SpaceId::new(3)))
        .expect("live principal receives exact new grant");
    assert_eq!(
        core.subscribe(existing, SpaceKey::new(session(1), SpaceId::new(999))),
        Err(CoreError::SpaceReadAccessDenied(SpaceKey::new(
            session(1),
            SpaceId::new(999)
        )))
    );

    let (future, _) = connect(&mut core, &token);
    core.admit_and_join_session_at(
        future,
        session(1),
        IdempotencyKey::new("future").unwrap(),
        now,
    )
    .unwrap();
    core.subscribe(future, SpaceKey::new(session(1), SpaceId::new(4)))
        .expect("future principal receives configured grants");

    for id in 5..=66 {
        core.manage_at(spatial(id, id), now).unwrap();
    }
    let value = snapshot(&mut core, 1);
    assert_eq!(value.spaces.len(), MAX_MANAGED_SPACES);
    assert_eq!(value.spaces[0].space_id, "1");
    assert_eq!(value.spaces[1].space_id, "2");
    assert_eq!(
        core.manage_at(spatial(67, 67), now),
        Err(ManagedError::SpaceCapacityExhausted)
    );
}

#[test]
fn managed_rate_configuration_is_validated_atomic_and_revision_identified() {
    let mut core = core();
    let now = Instant::now();
    for hz in [0, 121, u32::MAX] {
        assert_eq!(
            core.manage_at(rate_put(Some(hz)), now),
            Err(ManagedError::InvalidRequest)
        );
        assert_eq!(core.session_count(), 0);
    }
    let created = core.manage_at(rate_put(Some(120)), now).unwrap();
    assert!(
        matches!(&created, ManagedOutcome::Snapshot { snapshot, .. } if snapshot.tick_rate_hz == Some(120))
    );
    core.manage_at(rate_put(Some(120)), now).unwrap();
    for hz in [None, Some(1)] {
        assert_eq!(
            core.manage_at(rate_put(hz), now),
            Err(ManagedError::RevisionConflict)
        );
    }
    let before = snapshot(&mut core, 1);
    for hz in [0, 121] {
        assert_eq!(
            core.manage_at(rate_patch(2, 2, Some(hz)), now),
            Err(ManagedError::InvalidRequest)
        );
        assert_eq!(snapshot(&mut core, 1), before);
    }
    core.manage_at(rate_patch(2, 2, None), now).unwrap();
    assert_eq!(snapshot(&mut core, 1).tick_rate_hz, Some(120));
    core.manage_at(rate_patch(2, 2, None), now).unwrap();
    assert_eq!(
        core.manage_at(rate_patch(2, 2, Some(120)), now),
        Err(ManagedError::RevisionConflict)
    );
    core.manage_at(rate_patch(3, 2, Some(1)), now).unwrap();
    let applied = snapshot(&mut core, 1);
    assert_eq!(applied.tick_rate_hz, Some(1));
    core.manage_at(rate_patch(3, 2, Some(1)), now).unwrap();
    for patch in [
        rate_patch(3, 2, Some(2)),
        rate_patch(3, 2, None),
        rate_patch(2, 2, Some(120)),
    ] {
        assert_eq!(
            core.manage_at(patch, now),
            Err(ManagedError::RevisionConflict)
        );
        assert_eq!(snapshot(&mut core, 1), applied);
    }
    core.manage_at(spatial(3, 4), now).unwrap();
    assert_eq!(snapshot(&mut core, 1).tick_rate_hz, Some(1));
    assert_eq!(
        core.manage_at(rate_patch(4, 2, Some(1)), now),
        Err(ManagedError::RevisionConflict)
    );
}

#[test]
fn managed_patches_and_retries_cannot_replenish_member_publish_budget() {
    let mut core = core();
    let now = Instant::now();
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(1),
        DeliveryClass::ReliableOrdered,
        PersistenceClass::Ephemeral,
        1024,
    ))
    .unwrap();
    core.manage_at(rate_put(None), now).unwrap();
    assert_eq!(snapshot(&mut core, 1).tick_rate_hz, None);
    let (connection, _) = connect(&mut core, &"a".repeat(64));
    core.admit_and_join_session_at(
        connection,
        session(1),
        IdempotencyKey::new("rate").unwrap(),
        now,
    )
    .unwrap();
    core.subscribe(connection, SpaceKey::new(session(1), SpaceId::new(1)))
        .unwrap();
    let entity = core
        .spawn_entity(
            connection,
            SpaceKey::new(session(1), SpaceId::new(1)),
            SpaceEpoch::new(1),
        )
        .unwrap();
    core.publish_at(event(connection, entity, 1), now).unwrap();
    core.publish_at(event(connection, entity, 2), now).unwrap();
    let changed = now + Duration::from_millis(100);
    core.manage_at(rate_patch(2, 1, Some(1)), changed).unwrap();
    for patch in [
        rate_patch(2, 1, Some(1)),
        rate_patch(3, 1, None),
        rate_patch(4, 1, Some(1)),
    ] {
        core.manage_at(patch, changed).unwrap();
        assert_eq!(
            core.publish_at(event(connection, entity, 3), changed),
            Err(CoreError::PublishRateLimited {
                retry_after: Duration::from_millis(900)
            })
        );
    }
    core.manage_at(rate_patch(5, 1, Some(3)), changed).unwrap();
    core.publish_at(event(connection, entity, 3), changed)
        .unwrap();
    assert_eq!(
        core.publish_at(event(connection, entity, 4), changed),
        Err(CoreError::PublishRateLimited {
            retry_after: Duration::from_millis(900)
        })
    );
    core.publish_at(event(connection, entity, 4), now + Duration::from_secs(1))
        .unwrap();
}

#[test]
fn managed_readmission_and_revisions_preserve_publish_window_and_delete_cleans_it() {
    let mut core = core();
    let now = Instant::now();
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(1),
        DeliveryClass::ReliableOrdered,
        PersistenceClass::Ephemeral,
        1024,
    ))
    .unwrap();
    core.manage_at(rate_put(Some(1)), now).unwrap();
    let (connection, _) = connect(&mut core, &"a".repeat(64));
    let root = SpaceKey::new(session(1), SpaceId::new(1));
    core.admit_and_join_session_at(
        connection,
        session(1),
        IdempotencyKey::new("first").unwrap(),
        now,
    )
    .unwrap();
    core.subscribe(connection, root).unwrap();
    let first = core
        .spawn_entity(connection, root, SpaceEpoch::new(1))
        .unwrap();
    core.publish_at(event(connection, first, 1), now).unwrap();
    core.leave_session(connection, session(1)).unwrap();
    let changed = now + Duration::from_millis(100);
    core.manage_at(rate_patch(2, 1, None), changed).unwrap();
    core.manage_at(spatial(3, 3), changed).unwrap();
    core.admit_and_join_session_at(
        connection,
        session(1),
        IdempotencyKey::new("rejoin").unwrap(),
        changed,
    )
    .unwrap();
    core.subscribe(connection, root).unwrap();
    let second = core
        .spawn_entity(connection, root, SpaceEpoch::new(1))
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(
        core.publish_at(event(connection, second, 1), changed),
        Err(CoreError::PublishRateLimited {
            retry_after: Duration::from_millis(900)
        })
    );
    assert_eq!(core.sequence_key_count(session(1)), Some(0));
    core.publish_at(event(connection, second, 1), now + Duration::from_secs(1))
        .unwrap();
    core.leave_session(connection, session(1)).unwrap();
    core.manage_at(
        ManagedRequest::Delete {
            session: session(1),
            revision: 3,
        },
        now + Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(core.connection_count(), 0);
    assert_eq!(core.session_count(), 0);
    assert!(core.session_memberships(connection).is_err());
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
