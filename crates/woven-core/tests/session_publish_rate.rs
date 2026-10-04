//! Deterministic generic session budgets; no transport timing or domain ticks.
use std::time::{Duration, Instant};
use woven_core::*;

fn session(id: u64) -> SessionKey {
    SessionKey::new(NamespaceId::new(1), SessionId::new(id))
}

fn policy(max_publishes: usize) -> PublishRateLimit {
    PublishRateLimit {
        max_publishes,
        window: Duration::from_secs(1),
    }
}

fn core(global_limit: usize) -> WovenCore<DevAuthenticator> {
    core_with_memberships(
        global_limit,
        CoreConfig::default().max_memberships_per_connection,
    )
}

fn core_with_memberships(
    global_limit: usize,
    max_memberships: usize,
) -> WovenCore<DevAuthenticator> {
    let mut grants = AuthorizationGrants::new();
    grants.grant_namespace(NamespaceId::new(1), AccessGrant::ReadWrite);
    for id in 1..=3 {
        grants.grant_session(session(id), AccessGrant::ReadWrite);
        for space in 1..=2 {
            grants.grant_space(
                SpaceKey::new(session(id), SpaceId::new(space)),
                AccessGrant::ReadWrite,
            );
        }
        for channel in 1..=2 {
            grants.grant_channel(
                ChannelScope::new(session(id), ChannelId::new(channel)),
                AccessGrant::ReadWrite,
            );
        }
    }
    let mut auth = DevAuthenticator::new();
    auth.insert(
        "test",
        AuthenticatedPrincipal::new(PrincipalId::new(1), grants.clone()),
    )
    .unwrap();
    auth.insert(
        "other",
        AuthenticatedPrincipal::new(PrincipalId::new(2), grants),
    )
    .unwrap();
    let config = CoreConfig {
        publish_rate_limit: policy(global_limit),
        max_memberships_per_connection: max_memberships,
        ..CoreConfig::default()
    };
    let mut core = WovenCore::new(auth, config).unwrap();
    for channel in 1..=2 {
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(channel),
            DeliveryClass::ReliableOrdered,
            PersistenceClass::Ephemeral,
            1024,
        ))
        .unwrap();
    }
    for id in 1..=3 {
        core.provision_session(session(id)).unwrap();
        for space in 1..=2 {
            core.install_space(
                session(id),
                SpaceDescriptor {
                    id: SpaceId::new(space),
                    local_frame: CoordinateFrame::Logical,
                    bounds: None,
                    parent: None,
                    epoch: SpaceEpoch::new(1),
                    routing: RoutingPolicy::BroadcastAll,
                },
            )
            .unwrap();
        }
    }
    core
}

#[derive(Clone, Copy)]
struct Member {
    connection: ConnectionId,
    entities: [[EntityId; 2]; 3],
}

fn connect(core: &mut WovenCore<DevAuthenticator>) -> Member {
    let connection = core.transport_connected().unwrap();
    core.authenticate(connection, &Credentials::new("test"))
        .unwrap();
    let mut entities = [[EntityId::new(0); 2]; 3];
    for id in 1..=2 {
        core.join_session(connection, session(id)).unwrap();
        for space in 1..=2 {
            let key = SpaceKey::new(session(id), SpaceId::new(space));
            core.subscribe(connection, key).unwrap();
            entities[usize::try_from(id - 1).unwrap()][usize::try_from(space - 1).unwrap()] = core
                .spawn_entity(connection, key, SpaceEpoch::new(1))
                .unwrap();
        }
    }
    Member {
        connection,
        entities,
    }
}

fn request(member: Member, id: u64, space: u64, channel: u64, sequence: u64) -> PublishRequest {
    PublishRequest {
        connection: member.connection,
        session: session(id),
        space: SpaceId::new(space),
        space_epoch: SpaceEpoch::new(1),
        entity: Some(
            member.entities[usize::try_from(id - 1).unwrap()][usize::try_from(space - 1).unwrap()],
        ),
        channel: ChannelId::new(channel),
        sequence,
        delivery: DeliveryClass::ReliableOrdered,
        persistence: PersistenceClass::Ephemeral,
        coalesce_key: None,
        routing_position: None,
        payload: vec![1],
    }
}

fn limited(result: Result<PublishOutcome, CoreError>, retry_after: Duration) {
    assert_eq!(
        result.unwrap_err(),
        CoreError::PublishRateLimited { retry_after }
    );
}

#[test]
fn budget_aggregates_channels_and_spaces_but_isolates_sessions_and_connections() {
    let mut core = core(256);
    core.set_session_publish_rate_limit(session(1), Some(policy(2)))
        .unwrap();
    core.set_session_publish_rate_limit(session(2), Some(policy(2)))
        .unwrap();
    let first = connect(&mut core);
    let second = connect(&mut core);
    let now = Instant::now();
    core.publish_at(request(first, 1, 1, 1, 1), now).unwrap();
    core.publish_at(request(first, 1, 2, 2, 1), now).unwrap();
    limited(
        core.publish_at(request(first, 1, 1, 2, 1), now),
        Duration::from_secs(1),
    );
    core.publish_at(request(first, 2, 1, 1, 1), now).unwrap();
    core.publish_at(request(second, 1, 1, 1, 1), now).unwrap();
    assert_eq!(core.sequence_key_count(session(1)), Some(3));
}

#[test]
fn exact_window_boundary_and_clock_regression_are_deterministic() {
    let mut core = core(256);
    core.set_session_publish_rate_limit(session(1), Some(policy(1)))
        .unwrap();
    let connection = connect(&mut core);
    let now = Instant::now();
    core.publish_at(request(connection, 1, 1, 1, 1), now)
        .unwrap();
    let almost = now + Duration::from_nanos(999_999_999);
    limited(
        core.publish_at(request(connection, 1, 1, 1, 2), almost),
        Duration::from_nanos(1),
    );
    core.publish_at(
        request(connection, 1, 1, 1, 2),
        now + Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        core.publish_at(request(connection, 1, 1, 1, 3), now),
        Err(CoreError::RateLimitClockRegressed)
    );
}

#[test]
fn lowering_raising_and_reenabling_preserve_live_window_and_count() {
    let mut core = core(256);
    let connection = connect(&mut core);
    let now = Instant::now();
    core.publish_at(request(connection, 1, 1, 1, 1), now)
        .unwrap();
    core.publish_at(request(connection, 1, 1, 1, 2), now)
        .unwrap();
    core.set_session_publish_rate_limit(session(1), Some(policy(1)))
        .unwrap();
    let changed = now + Duration::from_millis(100);
    limited(
        core.publish_at(request(connection, 1, 1, 1, 3), changed),
        Duration::from_millis(900),
    );
    core.set_session_publish_rate_limit(session(1), Some(policy(3)))
        .unwrap();
    core.publish_at(request(connection, 1, 1, 1, 3), changed)
        .unwrap();
    limited(
        core.publish_at(request(connection, 1, 1, 1, 4), changed),
        Duration::from_millis(900),
    );
    core.set_session_publish_rate_limit(session(1), None)
        .unwrap();
    core.publish_at(request(connection, 1, 1, 1, 4), changed)
        .unwrap();
    core.set_session_publish_rate_limit(session(1), Some(policy(3)))
        .unwrap();
    limited(
        core.publish_at(request(connection, 1, 1, 1, 5), changed),
        Duration::from_millis(900),
    );
    core.publish_at(
        request(connection, 1, 1, 1, 5),
        now + Duration::from_secs(1),
    )
    .unwrap();
}

#[test]
fn omission_keeps_legacy_budget_and_global_limit_still_wins() {
    let mut core = core(2);
    core.set_session_publish_rate_limit(session(1), Some(policy(120)))
        .unwrap();
    let connection = connect(&mut core);
    let now = Instant::now();
    core.publish_at(request(connection, 1, 1, 1, 1), now)
        .unwrap();
    core.publish_at(request(connection, 2, 1, 1, 1), now)
        .unwrap();
    limited(
        core.publish_at(request(connection, 2, 1, 1, 2), now),
        Duration::from_secs(1),
    );
    core.publish_at(
        request(connection, 2, 1, 1, 2),
        now + Duration::from_secs(1),
    )
    .unwrap();
}

#[test]
fn leave_and_rejoin_cannot_replenish_a_connected_session_budget() {
    let mut core = core(256);
    core.set_session_publish_rate_limit(session(1), Some(policy(1)))
        .unwrap();
    let mut member = connect(&mut core);
    let now = Instant::now();
    core.publish_at(request(member, 1, 1, 1, 1), now).unwrap();
    core.leave_session(member.connection, session(1)).unwrap();
    core.join_session_at(
        member.connection,
        session(1),
        now + Duration::from_millis(100),
    )
    .unwrap();
    let space = SpaceKey::new(session(1), SpaceId::new(1));
    core.subscribe(member.connection, space).unwrap();
    member.entities[0][0] = core
        .spawn_entity(member.connection, space, SpaceEpoch::new(1))
        .unwrap();
    limited(
        core.publish_at(
            request(member, 1, 1, 1, 1),
            now + Duration::from_millis(100),
        ),
        Duration::from_millis(900),
    );
    core.publish_at(request(member, 1, 1, 1, 1), now + Duration::from_secs(1))
        .unwrap();
}

fn rejoin_at(core: &mut WovenCore<DevAuthenticator>, member: &mut Member, id: u64, now: Instant) {
    core.join_session_at(member.connection, session(id), now)
        .unwrap();
    let space = SpaceKey::new(session(id), SpaceId::new(1));
    core.subscribe(member.connection, space).unwrap();
    member.entities[usize::try_from(id - 1).unwrap()][0] = core
        .spawn_entity(member.connection, space, SpaceEpoch::new(1))
        .unwrap();
}

#[test]
fn history_cap_rejects_churn_without_evicting_active_or_detached_budgets() {
    let mut core = core_with_memberships(256, 2);
    for id in 1..=2 {
        core.set_session_publish_rate_limit(session(id), Some(policy(1)))
            .unwrap();
    }
    let mut member = connect(&mut core);
    let now = Instant::now();
    for id in 1..=2 {
        core.publish_at(request(member, id, 1, 1, 1), now).unwrap();
        core.leave_session(member.connection, session(id)).unwrap();
    }
    for _ in 0..32 {
        assert_eq!(
            core.join_session_at(member.connection, session(3), now),
            Err(CoreError::MembershipLimitReached)
        );
        assert!(
            core.session_memberships(member.connection)
                .unwrap()
                .is_empty()
        );
    }
    let changed = now + Duration::from_millis(100);
    rejoin_at(&mut core, &mut member, 1, changed);
    limited(
        core.publish_at(request(member, 1, 1, 1, 1), changed),
        Duration::from_millis(900),
    );
    assert_eq!(
        core.join_session_at(member.connection, session(3), changed),
        Err(CoreError::MembershipLimitReached)
    );
    core.leave_session(member.connection, session(1)).unwrap();
    for before in [
        now.checked_sub(Duration::from_nanos(1)).unwrap(),
        now + Duration::from_nanos(999_999_999),
    ] {
        assert_eq!(
            core.join_session_at(member.connection, session(3), before),
            Err(CoreError::MembershipLimitReached)
        );
    }
    core.join_session_at(member.connection, session(3), now + Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        core.session_memberships(member.connection).unwrap().len(),
        1
    );
    core.transport_lost(member.connection).unwrap();
    let fresh = connect(&mut core);
    core.publish_at(request(fresh, 1, 1, 1, 1), now).unwrap();
}

#[test]
fn history_cap_rejection_rolls_back_admission_before_retry_after_expiry() {
    let mut core = core_with_memberships(256, 2);
    for id in 1..=2 {
        core.set_session_publish_rate_limit(session(id), Some(policy(1)))
            .unwrap();
    }
    let member = connect(&mut core);
    let now = Instant::now();
    for id in 1..=2 {
        core.publish_at(request(member, id, 1, 1, 1), now).unwrap();
        core.leave_session(member.connection, session(id)).unwrap();
    }
    let capacity = CapacityUpdate {
        allocated_ccu: 1,
        revision: 1,
    };
    core.configure_session_admission(
        session(3),
        AdmissionMetadata {
            node_id: NodeId::new(1),
            session: session(3),
        },
        QueuePolicy {
            reconnect_grace: Duration::ZERO,
            ..QueuePolicy::default()
        },
        capacity,
    )
    .unwrap();
    assert_eq!(
        core.admit_and_join_session_at(
            member.connection,
            session(3),
            IdempotencyKey::new("history-full").unwrap(),
            now
        ),
        Err(CoreError::MembershipLimitReached)
    );
    assert!(
        core.session_memberships(member.connection)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.apply_session_capacity_at(session(3), capacity, now)
            .unwrap()
            .active_ccu,
        0
    );
    assert!(matches!(
        core.admit_and_join_session_at(
            member.connection,
            session(3),
            IdempotencyKey::new("history-expired").unwrap(),
            now + Duration::from_secs(1)
        )
        .unwrap(),
        JoinDecision::Admitted(_)
    ));
    assert_eq!(
        core.session_memberships(member.connection).unwrap().len(),
        1
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one queued-claim rollback and retry lifecycle with ownership checks"
)]
fn queued_claim_history_failure_invalidates_ticket_and_allows_fresh_admission() {
    let mut core = core_with_memberships(256, 1);
    core.set_session_publish_rate_limit(session(1), Some(policy(1)))
        .unwrap();
    let capacity = CapacityUpdate {
        allocated_ccu: 1,
        revision: 1,
    };
    core.configure_session_admission(
        session(2),
        AdmissionMetadata {
            node_id: NodeId::new(1),
            session: session(2),
        },
        QueuePolicy {
            reconnect_grace: Duration::ZERO,
            ..QueuePolicy::default()
        },
        capacity,
    )
    .unwrap();
    let now = Instant::now();
    let first = core.transport_connected().unwrap();
    core.authenticate(first, &Credentials::new("test")).unwrap();
    let mut member = Member {
        connection: first,
        entities: [[EntityId::new(0); 2]; 3],
    };
    rejoin_at(&mut core, &mut member, 1, now);
    core.publish_at(request(member, 1, 1, 1, 1), now).unwrap();
    core.leave_session(first, session(1)).unwrap();
    let occupant = core.transport_connected().unwrap();
    core.authenticate(occupant, &Credentials::new("other"))
        .unwrap();
    assert!(matches!(
        core.admit_and_join_session_at(
            occupant,
            session(2),
            IdempotencyKey::new("occupant").unwrap(),
            now
        )
        .unwrap(),
        JoinDecision::Admitted(_)
    ));
    let key = IdempotencyKey::new("retry-the-same-queued-request").unwrap();
    let JoinDecision::Queued(ticket) = core
        .request_session_admission_at(first, session(2), key.clone(), now)
        .unwrap()
    else {
        panic!("second scope must queue while occupied")
    };
    for operation in [
        QueueOperation::Status,
        QueueOperation::Heartbeat,
        QueueOperation::Cancel,
        QueueOperation::Claim,
    ] {
        assert_eq!(
            core.session_queue_at(occupant, session(2), ticket.id, operation, now)
                .unwrap(),
            QueueStatus::Missing
        );
        assert_eq!(
            core.session_queue_at(first, session(3), ticket.id, operation, now)
                .unwrap(),
            QueueStatus::Missing
        );
    }
    assert_eq!(
        core.session_queue_at(first, session(2), ticket.id, QueueOperation::Status, now)
            .unwrap(),
        QueueStatus::Waiting { position: 1 }
    );
    core.leave_session(occupant, session(2)).unwrap();
    let claim_at = now + Duration::from_millis(100);
    assert_eq!(
        core.session_queue_at(
            first,
            session(2),
            ticket.id,
            QueueOperation::Status,
            claim_at
        )
        .unwrap(),
        QueueStatus::Offered
    );
    assert_eq!(
        core.session_queue_at(
            first,
            session(2),
            ticket.id,
            QueueOperation::Claim,
            claim_at
        ),
        Err(CoreError::MembershipLimitReached)
    );
    assert!(core.session_memberships(first).unwrap().is_empty());
    let released = core
        .apply_session_capacity_at(session(2), capacity, claim_at)
        .unwrap();
    assert_eq!(
        (
            released.active_ccu,
            released.offered_slots,
            released.queue_depth,
            released.available_slots
        ),
        (0, 0, 0, 1)
    );
    for operation in [
        QueueOperation::Status,
        QueueOperation::Heartbeat,
        QueueOperation::Cancel,
        QueueOperation::Claim,
    ] {
        assert_eq!(
            core.session_queue_at(first, session(2), ticket.id, operation, claim_at)
                .unwrap(),
            QueueStatus::Missing
        );
    }
    let expired_at = now + Duration::from_secs(1);
    let JoinDecision::Admitted(lease) = core
        .admit_and_join_session_at(first, session(2), key, expired_at)
        .unwrap()
    else {
        panic!("fresh admission must recover after history expiry")
    };
    assert!(
        core.session_memberships(first)
            .unwrap()
            .contains(&session(2))
    );
    assert_eq!(
        core.request_session_admission_at(
            first,
            session(2),
            IdempotencyKey::new("already-bound").unwrap(),
            expired_at
        )
        .unwrap(),
        JoinDecision::Admitted(lease)
    );
    assert_eq!(
        core.session_queue_at(
            first,
            session(2),
            ticket.id,
            QueueOperation::Claim,
            expired_at
        )
        .unwrap(),
        QueueStatus::Missing
    );
    assert_eq!(
        core.session_queue_at(
            occupant,
            session(2),
            ticket.id,
            QueueOperation::Claim,
            expired_at
        )
        .unwrap(),
        QueueStatus::Missing
    );
    assert_eq!(
        core.apply_session_capacity_at(session(2), capacity, expired_at)
            .unwrap()
            .active_ccu,
        1
    );
}

#[test]
fn detached_histories_follow_generic_window_changes_without_resetting_count() {
    let mut core = core_with_memberships(256, 2);
    for id in 1..=2 {
        core.set_session_publish_rate_limit(session(id), Some(policy(1)))
            .unwrap();
    }
    let member = connect(&mut core);
    let now = Instant::now();
    for id in 1..=2 {
        core.publish_at(request(member, id, 1, 1, 1), now).unwrap();
        core.leave_session(member.connection, session(id)).unwrap();
        core.set_session_publish_rate_limit(
            session(id),
            Some(PublishRateLimit {
                max_publishes: 1,
                window: Duration::from_secs(2),
            }),
        )
        .unwrap();
    }
    assert_eq!(
        core.join_session_at(member.connection, session(3), now + Duration::from_secs(1)),
        Err(CoreError::MembershipLimitReached)
    );
    core.join_session_at(member.connection, session(3), now + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn previously_limited_history_survives_unlimited_period_and_another_leave() {
    let mut core = core(256);
    core.set_session_publish_rate_limit(session(1), Some(policy(1)))
        .unwrap();
    let mut member = connect(&mut core);
    let now = Instant::now();
    core.publish_at(request(member, 1, 1, 1, 1), now).unwrap();
    core.leave_session(member.connection, session(1)).unwrap();
    core.set_session_publish_rate_limit(session(1), None)
        .unwrap();
    let changed = now + Duration::from_millis(100);
    rejoin_at(&mut core, &mut member, 1, changed);
    core.publish_at(request(member, 1, 1, 1, 1), changed)
        .unwrap();
    core.leave_session(member.connection, session(1)).unwrap();
    core.set_session_publish_rate_limit(session(1), Some(policy(1)))
        .unwrap();
    rejoin_at(&mut core, &mut member, 1, changed);
    limited(
        core.publish_at(request(member, 1, 1, 1, 1), changed),
        Duration::from_millis(900),
    );
}

#[test]
fn unlimited_only_churn_does_not_acquire_a_recent_history_cooldown() {
    let mut core = core_with_memberships(256, 1);
    let connection = core.transport_connected().unwrap();
    core.authenticate(connection, &Credentials::new("test"))
        .unwrap();
    let now = Instant::now();
    for _ in 0..30 {
        for id in 1..=3 {
            let mut member = Member {
                connection,
                entities: [[EntityId::new(0); 2]; 3],
            };
            rejoin_at(&mut core, &mut member, id, now);
            core.publish_at(request(member, id, 1, 1, 1), now).unwrap();
            core.leave_session(connection, session(id)).unwrap();
        }
    }
    assert!(core.session_memberships(connection).unwrap().is_empty());
}

#[test]
fn invalid_policy_is_atomic_and_membership_lifecycle_bounds_rate_state() {
    let mut core = core(256);
    core.set_session_publish_rate_limit(session(1), Some(policy(1)))
        .unwrap();
    let connection = connect(&mut core);
    let now = Instant::now();
    core.publish_at(request(connection, 1, 1, 1, 1), now)
        .unwrap();
    for invalid in [
        Some(policy(0)),
        Some(PublishRateLimit {
            max_publishes: 2,
            window: Duration::ZERO,
        }),
    ] {
        assert_eq!(
            core.set_session_publish_rate_limit(session(1), invalid),
            Err(CoreError::InvalidConfiguration)
        );
    }
    core.join_session(connection.connection, session(1))
        .unwrap();
    limited(
        core.publish_at(request(connection, 1, 1, 1, 2), now),
        Duration::from_secs(1),
    );
    core.leave_session(connection.connection, session(1))
        .unwrap();
    assert_eq!(
        core.session_memberships(connection.connection)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        core.publish_at(request(connection, 1, 1, 1, 2), now),
        Err(CoreError::SessionMembershipRequired(session(1)))
    );
    core.transport_lost(connection.connection).unwrap();
    assert_eq!(core.connection_count(), 0);
    let fresh = connect(&mut core);
    core.publish_at(request(fresh, 1, 1, 1, 1), now).unwrap();
}
