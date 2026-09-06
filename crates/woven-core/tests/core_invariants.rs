use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use woven_core::{
    AccessGrant, AdmissionMetadata, AuthenticatedPrincipal, AuthorizationGrants, CapacityUpdate,
    ChannelDefinition, ChannelId, ChannelScope, CoalesceKey, Command, CommandResult,
    CoordinateFrame, CoreConfig, CoreError, Credentials, DeliveryClass, DevAuthenticator, EntityId,
    EntityPosition, EntityTransitionRequest, HarnessError, IdKind, IdempotencyKey, JoinDecision,
    JournalRecord, JournalSink, NamespaceId, NodeId, NoopJournalSink, OutboundMessage,
    OutboundQueueConfig, ParentAnchor, PersistenceClass, PrincipalId, PublishRateLimit,
    PublishRequest, QueuePolicy, RoutingPolicy, SessionId, SessionKey, SpaceDescriptor, SpaceEpoch,
    SpaceId, SpaceKey, TransportIndependentWorker, WorkerHarness, WovenCore,
};

const NAMESPACE_A: NamespaceId = NamespaceId::new(10);
const NAMESPACE_B: NamespaceId = NamespaceId::new(20);
const SESSION_ID: SessionId = SessionId::new(100);
const UNPROVISIONED_SESSION_ID: SessionId = SessionId::new(101);
const ROOT: SpaceId = SpaceId::new(1);
const SECONDARY: SpaceId = SpaceId::new(2);
const EVENT_CHANNEL: ChannelId = ChannelId::new(7);
const STATE_CHANNEL: ChannelId = ChannelId::new(8);
const DURABLE_CHANNEL: ChannelId = ChannelId::new(9);
const TTL_CHANNEL: ChannelId = ChannelId::new(10);
const TTL_CHANNEL_TTL: Duration = Duration::from_secs(1);
const EPOCH_ONE: SpaceEpoch = SpaceEpoch::new(1);

fn session_a() -> SessionKey {
    SessionKey::new(NAMESPACE_A, SESSION_ID)
}

fn session_b() -> SessionKey {
    SessionKey::new(NAMESPACE_B, SESSION_ID)
}

fn unprovisioned_session() -> SessionKey {
    SessionKey::new(NAMESPACE_A, UNPROVISIONED_SESSION_ID)
}

fn grants_for(session: SessionKey) -> AuthorizationGrants {
    let mut grants = AuthorizationGrants::new();
    grants.grant_namespace(session.namespace, AccessGrant::ReadWrite);
    grants.grant_session(session, AccessGrant::ReadWrite);
    for space in [ROOT, SECONDARY] {
        grants.grant_space(SpaceKey::new(session, space), AccessGrant::ReadWrite);
    }
    for channel in [EVENT_CHANNEL, STATE_CHANNEL, DURABLE_CHANNEL, TTL_CHANNEL] {
        grants.grant_channel(ChannelScope::new(session, channel), AccessGrant::ReadWrite);
    }
    grants
}

fn authenticator() -> DevAuthenticator {
    let mut alice_grants = grants_for(session_a());
    alice_grants.grant_session(unprovisioned_session(), AccessGrant::ReadWrite);

    let mut authenticator = DevAuthenticator::new();
    authenticator
        .insert(
            "alice",
            AuthenticatedPrincipal::new(PrincipalId::new(1), alice_grants),
        )
        .expect("development identity capacity");
    authenticator
        .insert(
            "bob",
            AuthenticatedPrincipal::new(PrincipalId::new(2), grants_for(session_a())),
        )
        .expect("development identity capacity");
    authenticator
        .insert(
            "other",
            AuthenticatedPrincipal::new(PrincipalId::new(3), grants_for(session_b())),
        )
        .expect("development identity capacity");

    let mut observer_grants = AuthorizationGrants::new();
    observer_grants.grant_namespace(NAMESPACE_A, AccessGrant::Read);
    observer_grants.grant_session(session_a(), AccessGrant::Read);
    observer_grants.grant_space(SpaceKey::new(session_a(), ROOT), AccessGrant::Read);
    observer_grants.grant_channel(
        ChannelScope::new(session_a(), EVENT_CHANNEL),
        AccessGrant::Read,
    );
    authenticator
        .insert(
            "observer",
            AuthenticatedPrincipal::new(PrincipalId::new(4), observer_grants),
        )
        .expect("development identity capacity");
    authenticator
}

fn root_descriptor(id: SpaceId) -> SpaceDescriptor {
    SpaceDescriptor {
        id,
        local_frame: CoordinateFrame::Logical,
        parent: None,
        epoch: EPOCH_ONE,
        routing: RoutingPolicy::BroadcastAll,
    }
}

fn register_channels(core: &mut WovenCore<DevAuthenticator>) {
    for definition in [
        ChannelDefinition::relay_owned(
            EVENT_CHANNEL,
            DeliveryClass::ReliableOrdered,
            PersistenceClass::Ephemeral,
            1_024,
        ),
        ChannelDefinition::relay_owned(
            STATE_CHANNEL,
            DeliveryClass::LatestValue,
            PersistenceClass::Stateful { ttl: None },
            1_024,
        ),
        ChannelDefinition::relay_owned(
            DURABLE_CHANNEL,
            DeliveryClass::ReliableOrdered,
            PersistenceClass::Durable,
            1_024,
        ),
        ChannelDefinition::relay_owned(
            TTL_CHANNEL,
            DeliveryClass::LatestValue,
            PersistenceClass::Stateful {
                ttl: Some(TTL_CHANNEL_TTL),
            },
            1_024,
        ),
    ] {
        core.register_channel(definition)
            .expect("channel registration");
    }
}

fn make_core(config: CoreConfig) -> WovenCore<DevAuthenticator> {
    let mut core = WovenCore::new(authenticator(), config).expect("valid core config");
    register_channels(&mut core);
    core.provision_session(session_a())
        .expect("session provisioning");
    core.install_space(session_a(), root_descriptor(ROOT))
        .expect("root space installation");
    core
}

fn connect_authenticated(
    core: &mut WovenCore<DevAuthenticator>,
    token: &str,
) -> woven_core::ConnectionId {
    let connection = core.transport_connected().expect("connection allocation");
    core.authenticate(connection, &Credentials::new(token))
        .expect("development authentication");
    connection
}

fn join_and_subscribe(
    core: &mut WovenCore<DevAuthenticator>,
    connection: woven_core::ConnectionId,
    space: SpaceId,
) {
    core.join_session(connection, session_a())
        .expect("session join");
    core.subscribe(connection, SpaceKey::new(session_a(), space))
        .expect("space subscription");
}

#[allow(
    clippy::match_same_arms,
    reason = "STATE_CHANNEL and TTL_CHANNEL intentionally return the same message-side value \
              (ttl: None) even though the channels themselves are registered with different \
              TTLs — that's the point being tested, not an oversight"
)]
fn channel_policy(channel: ChannelId) -> (DeliveryClass, PersistenceClass) {
    match channel {
        EVENT_CHANNEL => (DeliveryClass::ReliableOrdered, PersistenceClass::Ephemeral),
        STATE_CHANNEL => (
            DeliveryClass::LatestValue,
            PersistenceClass::Stateful { ttl: None },
        ),
        DURABLE_CHANNEL => (DeliveryClass::ReliableOrdered, PersistenceClass::Durable),
        // Deliberately `ttl: None` here even though the registered channel (see
        // `register_channels`) carries `Some(TTL_CHANNEL_TTL)` — a real publisher never knows
        // or declares the channel's TTL; the channel's registered value is authoritative
        // (`PersistenceClass::same_kind` is what lets this pass channel-policy validation).
        TTL_CHANNEL => (
            DeliveryClass::LatestValue,
            PersistenceClass::Stateful { ttl: None },
        ),
        _ => panic!("test channel must have a registered policy"),
    }
}

fn spatial_descriptor(
    id: SpaceId,
    routing: RoutingPolicy,
    local_frame: CoordinateFrame,
) -> SpaceDescriptor {
    SpaceDescriptor {
        id,
        local_frame,
        parent: None,
        epoch: EPOCH_ONE,
        routing,
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_request(
    connection: woven_core::ConnectionId,
    space: SpaceId,
    epoch: SpaceEpoch,
    entity: EntityId,
    channel: ChannelId,
    sequence: u64,
    component: u64,
    payload: &[u8],
) -> PublishRequest {
    let (delivery, persistence) = channel_policy(channel);
    PublishRequest {
        connection,
        session: session_a(),
        space,
        space_epoch: epoch,
        entity: Some(entity),
        channel,
        sequence,
        delivery,
        persistence,
        coalesce_key: delivery
            .is_replaceable()
            .then(|| CoalesceKey::new(channel, Some(entity), component)),
        payload: payload.to_vec(),
    }
}

#[test]
fn capacity_managed_session_requires_and_releases_admission_lease() {
    let session = session_a();
    let mut core = make_core(CoreConfig::default());
    core.configure_session_admission(
        session,
        AdmissionMetadata {
            node_id: NodeId::new(1),
            session,
        },
        QueuePolicy::default(),
        CapacityUpdate {
            allocated_ccu: 1,
            revision: 1,
        },
    )
    .expect("configure admission");

    let connection = core.transport_connected().expect("connect");
    core.authenticate(connection, &Credentials::new("alice"))
        .expect("authenticate");
    assert_eq!(
        core.join_session(connection, session),
        Err(CoreError::AdmissionLeaseRequired(session))
    );

    let lease = match core
        .request_session_admission_at(
            connection,
            session,
            IdempotencyKey::new("join-1").expect("key"),
            Instant::now(),
        )
        .expect("admission request")
    {
        JoinDecision::Admitted(lease) => lease,
        decision => panic!("unexpected {decision:?}"),
    };
    core.join_session_with_admission(connection, session, lease)
        .expect("admitted join");
    core.leave_session(connection, session)
        .expect("intentional leave");

    let replacement = core.transport_connected().expect("replacement connect");
    core.authenticate(replacement, &Credentials::new("bob"))
        .expect("replacement authenticate");
    assert!(matches!(
        core.request_session_admission_at(
            replacement,
            session,
            IdempotencyKey::new("join-2").expect("key"),
            Instant::now(),
        )
        .expect("replacement admission"),
        JoinDecision::Admitted(_)
    ));
}

#[test]
fn transport_loss_releases_an_unjoined_admission_lease() {
    let session = session_a();
    let mut core = make_core(CoreConfig::default());
    core.configure_session_admission(
        session,
        AdmissionMetadata {
            node_id: NodeId::new(1),
            session,
        },
        QueuePolicy::default(),
        CapacityUpdate {
            allocated_ccu: 1,
            revision: 1,
        },
    )
    .expect("configure admission");

    let first = core.transport_connected().expect("connect");
    core.authenticate(first, &Credentials::new("alice"))
        .expect("authenticate");
    assert!(matches!(
        core.request_session_admission_at(
            first,
            session,
            IdempotencyKey::new("first").expect("key"),
            Instant::now(),
        )
        .expect("admission"),
        JoinDecision::Admitted(_)
    ));
    core.transport_lost(first).expect("disconnect");

    let second = core.transport_connected().expect("connect");
    core.authenticate(second, &Credentials::new("bob"))
        .expect("authenticate");
    assert!(matches!(
        core.request_session_admission_at(
            second,
            session,
            IdempotencyKey::new("second").expect("key"),
            Instant::now(),
        )
        .expect("replacement admission"),
        JoinDecision::Admitted(_)
    ));
}

#[test]
fn authentication_and_server_provisioning_are_required() {
    let mut core = make_core(CoreConfig::default());
    let connection = core.transport_connected().expect("connection allocation");
    assert_eq!(
        core.join_session(connection, session_a()),
        Err(CoreError::AuthenticationRequired)
    );
    assert!(matches!(
        core.authenticate(connection, &Credentials::new("unknown")),
        Err(CoreError::AuthenticationFailed(_))
    ));
    assert_eq!(
        core.authenticate(connection, &Credentials::new("alice")),
        Ok(PrincipalId::new(1))
    );
    assert_eq!(
        core.join_session(connection, unprovisioned_session()),
        Err(CoreError::SessionNotFound(unprovisioned_session()))
    );
    assert_eq!(core.session_count(), 1);
}

#[test]
fn namespace_session_space_and_channel_grants_are_enforced() {
    let mut core = make_core(CoreConfig::default());
    let other = connect_authenticated(&mut core, "other");
    assert_eq!(
        core.join_session(other, session_a()),
        Err(CoreError::NamespaceReadAccessDenied(NAMESPACE_A))
    );

    let observer = connect_authenticated(&mut core, "observer");
    join_and_subscribe(&mut core, observer, ROOT);
    assert_eq!(
        core.spawn_entity(observer, SpaceKey::new(session_a(), ROOT), EPOCH_ONE),
        Err(CoreError::NamespaceWriteAccessDenied(NAMESPACE_A))
    );

    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        STATE_CHANNEL,
        1,
        1,
        b"private state",
    ))
    .expect("state publish");
    assert!(
        core.drain_outbound(observer)
            .expect("observer drain")
            .is_empty()
    );
    assert!(
        core.snapshot(observer, session_a())
            .expect("authorized snapshot")
            .state
            .is_empty()
    );
}

#[test]
fn multiple_subscriptions_and_visible_snapshots_work() {
    let mut core = make_core(CoreConfig::default());
    core.install_space(
        session_a(),
        SpaceDescriptor {
            routing: RoutingPolicy::TopicOnly,
            ..root_descriptor(SECONDARY)
        },
    )
    .expect("secondary space installation");
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    core.subscribe(alice, SpaceKey::new(session_a(), SECONDARY))
        .expect("second subscription");

    assert_eq!(core.subscription_count(alice), Some(2));
    let snapshot = core.snapshot(alice, session_a()).expect("session snapshot");
    assert_eq!(snapshot.subscription_count, 2);
    assert_eq!(snapshot.spaces.len(), 2);
}

#[test]
fn relay_owned_channels_enforce_server_assigned_entity_ownership() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    join_and_subscribe(&mut core, alice, ROOT);
    join_and_subscribe(&mut core, bob, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("server-assigned entity");

    let unauthorized =
        publish_request(bob, ROOT, EPOCH_ONE, entity, EVENT_CHANNEL, 1, 0, b"forged");
    assert!(matches!(
        core.publish(unauthorized),
        Err(CoreError::AuthorityRejected(
            woven_core::AuthorityRejection::EntityNotOwned
        ))
    ));

    let outcome = core
        .publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            EVENT_CHANNEL,
            1,
            0,
            b"owned",
        ))
        .expect("owned publication");
    assert_eq!(outcome.recipient_attempts, 2);
}

#[test]
fn channel_policy_and_stale_sequences_are_rejected() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");

    let mut wrong_policy = publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        EVENT_CHANNEL,
        1,
        0,
        b"event",
    );
    wrong_policy.persistence = PersistenceClass::Durable;
    assert!(matches!(
        core.publish(wrong_policy),
        Err(CoreError::ChannelPolicyMismatch { .. })
    ));

    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        EVENT_CHANNEL,
        2,
        0,
        b"event",
    ))
    .expect("first accepted sequence");
    assert_eq!(
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            EVENT_CHANNEL,
            2,
            0,
            b"duplicate",
        )),
        Err(CoreError::StaleSequence {
            received: 2,
            last: 2
        })
    );
}

#[test]
fn nested_spaces_require_advanced_epochs_after_destruction() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let anchor = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("anchor entity");
    let child = SpaceDescriptor {
        id: SECONDARY,
        local_frame: CoordinateFrame::Cartesian3D {
            meters_per_unit: 0.01,
        },
        parent: Some(ParentAnchor {
            parent_space: ROOT,
            anchor_entity: anchor,
        }),
        epoch: EPOCH_ONE,
        routing: RoutingPolicy::SpatialGrid3D {
            cell_size: 10.0,
            interest_radius: 50.0,
            exact_distance: true,
        },
    };
    core.install_space(session_a(), child.clone())
        .expect("anchored child");
    core.remove_entity(alice, session_a(), anchor)
        .expect("anchor removal destroys descendants");
    assert_eq!(core.space_epoch_tombstone_count(session_a()), Some(1));

    let replacement_anchor = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("replacement anchor");
    let replacement = SpaceDescriptor {
        parent: Some(ParentAnchor {
            parent_space: ROOT,
            anchor_entity: replacement_anchor,
        }),
        ..child
    };
    assert_eq!(
        core.install_space(session_a(), replacement.clone()),
        Err(CoreError::EpochDidNotAdvance {
            current: EPOCH_ONE,
            proposed: EPOCH_ONE
        })
    );
    core.install_space(
        session_a(),
        SpaceDescriptor {
            epoch: SpaceEpoch::new(2),
            ..replacement
        },
    )
    .expect("advanced epoch permits recreation");
}

#[test]
fn latest_state_coalesces_and_is_bounded_in_snapshots() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    join_and_subscribe(&mut core, alice, ROOT);
    join_and_subscribe(&mut core, bob, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");

    for (sequence, payload) in [(1, b"old".as_slice()), (2, b"new".as_slice())] {
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            STATE_CHANNEL,
            sequence,
            42,
            payload,
        ))
        .expect("latest-value publication");
    }

    let outbound = core.drain_outbound(bob).expect("recipient drain");
    assert_eq!(outbound.len(), 1);
    assert_eq!(outbound[0].sequence, 2);
    let snapshot = core.snapshot(bob, session_a()).expect("snapshot");
    assert_eq!(snapshot.state.len(), 1);
    assert_eq!(snapshot.state_bytes, 3);
    assert_eq!(snapshot.state[0].payload, b"new");
}

#[test]
fn critical_capacity_exhaustion_disconnects_without_live_message_loss() {
    let config = CoreConfig {
        outbound_queue: OutboundQueueConfig {
            total_capacity: 2,
            critical_capacity: 1,
            latest_capacity: 1,
            best_effort_capacity: 1,
        },
        ..CoreConfig::default()
    };
    let mut core = make_core(config);
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    join_and_subscribe(&mut core, alice, ROOT);
    join_and_subscribe(&mut core, bob, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");

    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        EVENT_CHANNEL,
        1,
        0,
        b"first",
    ))
    .expect("first publication");
    core.drain_outbound(alice).expect("active sender drain");
    let outcome = core
        .publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            EVENT_CHANNEL,
            2,
            0,
            b"second",
        ))
        .expect("second publication");

    assert_eq!(outcome.disconnected_slow_consumers, vec![bob]);
    assert!(!core.is_connected(bob));
}

#[test]
fn unsubscribe_and_leave_purge_stale_outbound_messages() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    join_and_subscribe(&mut core, alice, ROOT);
    join_and_subscribe(&mut core, bob, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        EVENT_CHANNEL,
        1,
        0,
        b"queued",
    ))
    .expect("publication");

    let cleanup = core
        .unsubscribe(bob, SpaceKey::new(session_a(), ROOT))
        .expect("unsubscribe");
    assert_eq!(cleanup.subscriptions_removed, 1);
    assert_eq!(cleanup.queued_messages_discarded, 1);
    assert!(core.drain_outbound(bob).expect("drain").is_empty());

    let cleanup = core
        .leave_session(alice, session_a())
        .expect("session leave");
    assert!(cleanup.queued_messages_discarded >= 1);
    assert!(core.drain_outbound(alice).expect("drain").is_empty());
}

#[test]
fn transport_loss_removes_owned_entities_and_anchored_descendants() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    join_and_subscribe(&mut core, alice, ROOT);
    core.join_session(bob, session_a()).expect("bob joins");
    let anchor = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("anchor spawn");
    core.install_space(
        session_a(),
        SpaceDescriptor {
            id: SECONDARY,
            local_frame: CoordinateFrame::Cartesian2D {
                meters_per_unit: 1.0,
            },
            parent: Some(ParentAnchor {
                parent_space: ROOT,
                anchor_entity: anchor,
            }),
            epoch: EPOCH_ONE,
            routing: RoutingPolicy::BroadcastAll,
        },
    )
    .expect("child installation");
    core.subscribe(bob, SpaceKey::new(session_a(), SECONDARY))
        .expect("bob child subscription");
    let child_entity = core
        .spawn_entity(bob, SpaceKey::new(session_a(), SECONDARY), EPOCH_ONE)
        .expect("bob child entity");

    let cleanup = core.transport_lost(alice).expect("transport cleanup");
    assert_eq!(cleanup.entities_removed, 2);
    assert_eq!(cleanup.spaces_removed, 1);
    assert_eq!(
        cleanup
            .removed_entities
            .iter()
            .map(|removed| (
                removed.entity,
                removed.session,
                removed.space,
                removed.space_epoch
            ))
            .collect::<Vec<_>>(),
        vec![
            (anchor, session_a(), ROOT, EPOCH_ONE),
            (child_entity, session_a(), SECONDARY, EPOCH_ONE),
        ]
    );
    assert_eq!(core.subscription_count(bob), Some(0));
}

#[test]
fn transition_entity_moves_owned_entity_and_invalidates_source_data() {
    let mut core = make_core(CoreConfig::default());
    core.install_space(session_a(), root_descriptor(SECONDARY))
        .expect("secondary space installation");
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    core.subscribe(alice, SpaceKey::new(session_a(), SECONDARY))
        .expect("secondary subscription");
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        STATE_CHANNEL,
        1,
        0,
        b"source state",
    ))
    .expect("source state publication");
    assert_eq!(core.sequence_key_count(session_a()), Some(1));

    let transition = core
        .transition_entity(EntityTransitionRequest {
            connection: alice,
            session: session_a(),
            entity,
            source_space: ROOT,
            source_epoch: EPOCH_ONE,
            destination_space: SECONDARY,
            destination_epoch: EPOCH_ONE,
        })
        .expect("owned transition");
    assert_eq!(transition.entity, entity);
    assert_eq!(transition.session, session_a());
    assert_eq!(transition.source_space, ROOT);
    assert_eq!(transition.destination_space, SECONDARY);
    assert_eq!(core.sequence_key_count(session_a()), Some(0));
    assert!(
        core.drain_outbound(alice)
            .expect("drain source messages")
            .is_empty()
    );

    let snapshot = core.snapshot(alice, session_a()).expect("session snapshot");
    assert!(snapshot.state.is_empty());
    assert!(
        snapshot
            .entities
            .iter()
            .any(|current| current.id == entity && current.space == SECONDARY)
    );
    assert_eq!(
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            EVENT_CHANNEL,
            1,
            0,
            b"stale source publication",
        )),
        Err(CoreError::EntitySpaceMismatch(entity))
    );
}

#[test]
fn transition_entity_rejects_non_owner_without_partial_update() {
    let mut core = make_core(CoreConfig::default());
    core.install_space(session_a(), root_descriptor(SECONDARY))
        .expect("secondary space installation");
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    for connection in [alice, bob] {
        join_and_subscribe(&mut core, connection, ROOT);
        core.subscribe(connection, SpaceKey::new(session_a(), SECONDARY))
            .expect("secondary subscription");
    }
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");

    assert_eq!(
        core.transition_entity(EntityTransitionRequest {
            connection: bob,
            session: session_a(),
            entity,
            source_space: ROOT,
            source_epoch: EPOCH_ONE,
            destination_space: SECONDARY,
            destination_epoch: EPOCH_ONE,
        }),
        Err(CoreError::EntityNotOwned(entity))
    );
    let snapshot = core.snapshot(alice, session_a()).expect("session snapshot");
    assert!(
        snapshot
            .entities
            .iter()
            .any(|current| current.id == entity && current.space == ROOT)
    );
}

#[test]
fn transition_entity_requires_both_subscriptions_without_partial_update() {
    let mut core = make_core(CoreConfig::default());
    core.install_space(session_a(), root_descriptor(SECONDARY))
        .expect("secondary space installation");
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");

    assert_eq!(
        core.transition_entity(EntityTransitionRequest {
            connection: alice,
            session: session_a(),
            entity,
            source_space: ROOT,
            source_epoch: EPOCH_ONE,
            destination_space: SECONDARY,
            destination_epoch: EPOCH_ONE,
        }),
        Err(CoreError::SubscriptionRequired(SpaceKey::new(
            session_a(),
            SECONDARY
        )))
    );
    let snapshot = core.snapshot(alice, session_a()).expect("session snapshot");
    assert!(
        snapshot
            .entities
            .iter()
            .any(|current| current.id == entity && current.space == ROOT)
    );
}

#[test]
fn payload_connection_and_session_limits_are_enforced() {
    let mut core = make_core(CoreConfig {
        max_connections: 1,
        max_sessions: 1,
        max_payload_bytes: 3,
        ..CoreConfig::default()
    });
    let alice = connect_authenticated(&mut core, "alice");
    assert_eq!(
        core.transport_connected(),
        Err(CoreError::ConnectionLimitReached)
    );
    assert_eq!(
        core.provision_session(session_b()),
        Err(CoreError::SessionLimitReached)
    );
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    assert!(matches!(
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            EVENT_CHANNEL,
            1,
            0,
            b"four",
        )),
        Err(CoreError::PayloadTooLarge { .. })
    ));
}

#[test]
fn sequence_key_capacity_is_enforced() {
    let mut core = make_core(CoreConfig {
        max_sequence_keys_per_session: 1,
        ..CoreConfig::default()
    });
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let first = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("first entity");
    let second = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("second entity");
    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        first,
        STATE_CHANNEL,
        1,
        1,
        b"abc",
    ))
    .expect("first sequence key");
    assert_eq!(
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            second,
            STATE_CHANNEL,
            1,
            1,
            b"x",
        )),
        Err(CoreError::SequenceKeyLimitReached)
    );
}

#[test]
fn state_entry_and_byte_capacities_are_enforced() {
    let mut core = make_core(CoreConfig {
        max_state_entries_per_session: 1,
        max_state_bytes_per_session: 3,
        ..CoreConfig::default()
    });
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("state entity");
    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        STATE_CHANNEL,
        1,
        1,
        b"abc",
    ))
    .expect("state at configured bounds");
    core.drain_outbound(alice).expect("state drain");
    assert_eq!(
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            STATE_CHANNEL,
            2,
            1,
            b"four",
        )),
        Err(CoreError::StateByteLimitReached)
    );
    assert_eq!(
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            STATE_CHANNEL,
            1,
            2,
            b"x",
        )),
        Err(CoreError::StateEntryLimitReached)
    );
}

#[test]
fn publish_rate_limit_is_deterministic_with_injected_time() {
    let mut core = make_core(CoreConfig {
        publish_rate_limit: PublishRateLimit {
            max_publishes: 2,
            window: Duration::from_secs(60),
        },
        ..CoreConfig::default()
    });
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    let now = Instant::now();
    for sequence in 1..=2 {
        core.publish_at(
            publish_request(
                alice,
                ROOT,
                EPOCH_ONE,
                entity,
                EVENT_CHANNEL,
                sequence,
                0,
                b"event",
            ),
            now,
        )
        .expect("within rate limit");
        core.drain_outbound(alice).expect("drain");
    }
    assert!(matches!(
        core.publish_at(
            publish_request(
                alice,
                ROOT,
                EPOCH_ONE,
                entity,
                EVENT_CHANNEL,
                3,
                0,
                b"limited",
            ),
            now,
        ),
        Err(CoreError::PublishRateLimited { .. })
    ));
}

#[test]
fn durable_messages_use_a_bounded_journal_outbox() {
    let mut core = make_core(CoreConfig {
        journal_outbox_capacity: 1,
        ..CoreConfig::default()
    });
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    core.publish(publish_request(
        alice,
        ROOT,
        EPOCH_ONE,
        entity,
        DURABLE_CHANNEL,
        1,
        0,
        b"durable",
    ))
    .expect("durable publication");
    assert_eq!(core.journal_outbox_len(), 1);
    assert_eq!(
        core.publish(publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            DURABLE_CHANNEL,
            2,
            0,
            b"blocked",
        )),
        Err(CoreError::JournalOutboxSaturated)
    );
}

#[test]
fn zero_ids_are_rejected_at_core_boundaries() {
    let mut core = make_core(CoreConfig::default());
    assert_eq!(
        core.provision_session(SessionKey::new(NamespaceId::new(0), SESSION_ID)),
        Err(CoreError::ReservedZeroId(IdKind::Namespace))
    );
    assert_eq!(
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(0),
            DeliveryClass::ReliableOrdered,
            PersistenceClass::Ephemeral,
            10,
        )),
        Err(CoreError::ReservedZeroId(IdKind::Channel))
    );
}

#[test]
fn spatial_grid_2d_filters_replaceable_updates_and_critical_events_bypass_interest() {
    let mut core = make_core(CoreConfig::default());
    core.install_space(
        session_a(),
        spatial_descriptor(
            SECONDARY,
            RoutingPolicy::SpatialGrid2D {
                cell_size: 10.0,
                interest_radius: 10.0,
                exact_distance: true,
            },
            CoordinateFrame::Cartesian2D {
                meters_per_unit: 1.0,
            },
        ),
    )
    .expect("spatial space installation");
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    join_and_subscribe(&mut core, alice, SECONDARY);
    join_and_subscribe(&mut core, bob, SECONDARY);
    let alice_entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), SECONDARY), EPOCH_ONE)
        .expect("alice entity spawn");
    let bob_entity = core
        .spawn_entity(bob, SpaceKey::new(session_a(), SECONDARY), EPOCH_ONE)
        .expect("bob entity spawn");
    core.update_entity_position(
        alice,
        session_a(),
        alice_entity,
        EntityPosition::Cartesian2D { x: 0.0, y: 0.0 },
    )
    .expect("alice position");
    core.update_entity_position(
        bob,
        session_a(),
        bob_entity,
        EntityPosition::Cartesian2D { x: 9.0, y: 0.0 },
    )
    .expect("bob nearby position");

    core.publish(publish_request(
        alice,
        SECONDARY,
        EPOCH_ONE,
        alice_entity,
        STATE_CHANNEL,
        1,
        1,
        b"near",
    ))
    .expect("near state publication");
    assert_eq!(core.drain_outbound(bob).expect("bob outbound").len(), 1);

    core.update_entity_position(
        bob,
        session_a(),
        bob_entity,
        EntityPosition::Cartesian2D { x: 11.0, y: 0.0 },
    )
    .expect("bob distant position");
    core.publish(publish_request(
        alice,
        SECONDARY,
        EPOCH_ONE,
        alice_entity,
        STATE_CHANNEL,
        2,
        1,
        b"far",
    ))
    .expect("far state publication");
    assert!(core.drain_outbound(bob).expect("bob outbound").is_empty());

    core.publish(publish_request(
        alice,
        SECONDARY,
        EPOCH_ONE,
        alice_entity,
        EVENT_CHANNEL,
        1,
        0,
        b"critical",
    ))
    .expect("critical publication");
    assert_eq!(core.drain_outbound(bob).expect("bob outbound").len(), 1);
}

#[test]
fn spatial_grid_3d_rejects_dimension_mismatches_and_routes_nearby_entities() {
    let mut core = make_core(CoreConfig::default());
    core.install_space(
        session_a(),
        spatial_descriptor(
            SECONDARY,
            RoutingPolicy::SpatialGrid3D {
                cell_size: 5.0,
                interest_radius: 5.0,
                exact_distance: true,
            },
            CoordinateFrame::Cartesian3D {
                meters_per_unit: 1.0,
            },
        ),
    )
    .expect("spatial space installation");
    let alice = connect_authenticated(&mut core, "alice");
    let bob = connect_authenticated(&mut core, "bob");
    join_and_subscribe(&mut core, alice, SECONDARY);
    join_and_subscribe(&mut core, bob, SECONDARY);
    let alice_entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), SECONDARY), EPOCH_ONE)
        .expect("alice entity spawn");
    let bob_entity = core
        .spawn_entity(bob, SpaceKey::new(session_a(), SECONDARY), EPOCH_ONE)
        .expect("bob entity spawn");
    assert!(matches!(
        core.update_entity_position(
            alice,
            session_a(),
            alice_entity,
            EntityPosition::Cartesian2D { x: 0.0, y: 0.0 },
        ),
        Err(CoreError::InvalidEntityPosition(_))
    ));
    core.update_entity_position(
        alice,
        session_a(),
        alice_entity,
        EntityPosition::Cartesian3D {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        },
    )
    .expect("alice position");
    core.update_entity_position(
        bob,
        session_a(),
        bob_entity,
        EntityPosition::Cartesian3D {
            x: 3.0,
            y: 4.0,
            z: 0.0,
        },
    )
    .expect("bob position");
    core.publish(publish_request(
        alice,
        SECONDARY,
        EPOCH_ONE,
        alice_entity,
        STATE_CHANNEL,
        1,
        1,
        b"edge",
    ))
    .expect("edge state publication");
    assert_eq!(core.drain_outbound(bob).expect("bob outbound").len(), 1);
}

#[test]
fn worker_handles_entity_transition_command() {
    let mut core = make_core(CoreConfig::default());
    core.install_space(session_a(), root_descriptor(SECONDARY))
        .expect("secondary space installation");
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    core.subscribe(alice, SpaceKey::new(session_a(), SECONDARY))
        .expect("secondary subscription");
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    let mut worker = TransportIndependentWorker::new(core);

    assert!(matches!(
        worker.handle(Command::TransitionEntity(EntityTransitionRequest {
            connection: alice,
            session: session_a(),
            entity,
            source_space: ROOT,
            source_epoch: EPOCH_ONE,
            destination_space: SECONDARY,
            destination_epoch: EPOCH_ONE,
        })),
        Ok(CommandResult::EntityTransitioned(transition))
            if transition.entity == entity
                && transition.source_space == ROOT
                && transition.destination_space == SECONDARY
    ));
}

#[test]
fn no_op_journal_and_bounded_worker_harness_are_runtime_independent() {
    let message = OutboundMessage {
        namespace: NAMESPACE_A,
        session: SESSION_ID,
        space: ROOT,
        space_epoch: EPOCH_ONE,
        entity: None,
        channel: DURABLE_CHANNEL,
        sequence: 1,
        delivery: DeliveryClass::ReliableOrdered,
        persistence: PersistenceClass::Durable,
        coalesce_key: None,
        payload: b"journal".to_vec(),
    };
    let sink = NoopJournalSink;
    let mut future = std::pin::pin!(sink.append(JournalRecord { message }));
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(future.as_mut().poll(&mut context), Poll::Ready(Ok(())));

    let worker = TransportIndependentWorker::new(make_core(CoreConfig::default()));
    let mut harness = WorkerHarness::new(worker, 1).expect("bounded harness");
    harness
        .submit(Command::TransportConnected)
        .expect("first command fits");
    assert_eq!(
        harness.submit(Command::TransportConnected),
        Err(HarnessError::Full)
    );
    assert!(matches!(
        harness
            .step()
            .expect("queued command")
            .expect("worker result"),
        CommandResult::Connected(_)
    ));
}

#[test]
fn stateful_entries_without_ttl_never_expire() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    let now = Instant::now();
    core.publish_at(
        publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            STATE_CHANNEL,
            1,
            0,
            b"forever",
        ),
        now,
    )
    .expect("stateful publish");

    // Advance far past any plausible TTL and sweep: an untouched `ttl: None` entry must survive.
    let far_future = now + Duration::from_secs(365 * 24 * 60 * 60);
    let swept = core.sweep_expired_state(far_future);
    assert_eq!(swept, 0, "ttl: None entries must never be swept");

    let snapshot = core
        .snapshot(alice, session_a())
        .expect("snapshot after sweep");
    assert_eq!(snapshot.state.len(), 1);
    assert_eq!(snapshot.state[0].payload, b"forever");
    assert_eq!(snapshot.state_bytes, "forever".len());
}

#[test]
fn stateful_entries_expire_after_channel_ttl_and_free_bytes() {
    let mut core = make_core(CoreConfig::default());
    let alice = connect_authenticated(&mut core, "alice");
    join_and_subscribe(&mut core, alice, ROOT);
    let entity = core
        .spawn_entity(alice, SpaceKey::new(session_a(), ROOT), EPOCH_ONE)
        .expect("entity spawn");
    let now = Instant::now();
    // TTL_CHANNEL's registered policy carries `ttl: Some(TTL_CHANNEL_TTL)`, even though (like a
    // real publisher) `publish_request` declares `ttl: None` on the message itself — the
    // channel's registered TTL is what actually governs storage.
    core.publish_at(
        publish_request(
            alice,
            ROOT,
            EPOCH_ONE,
            entity,
            TTL_CHANNEL,
            1,
            0,
            b"expires",
        ),
        now,
    )
    .expect("stateful publish onto a TTL-bearing channel");

    let snapshot = core
        .snapshot(alice, session_a())
        .expect("snapshot before expiry");
    assert_eq!(
        snapshot.state.len(),
        1,
        "entry visible before its TTL elapses"
    );

    // Before the TTL elapses: sweeping does nothing.
    let still_fresh = now + TTL_CHANNEL_TTL / 2;
    assert_eq!(core.sweep_expired_state(still_fresh), 0);

    // Past the TTL: an active sweep reclaims it and the byte count comes back down.
    let expired = now + TTL_CHANNEL_TTL * 2;
    assert_eq!(core.sweep_expired_state(expired), 1);

    let snapshot = core
        .snapshot(alice, session_a())
        .expect("snapshot after expiry");
    assert!(snapshot.state.is_empty(), "expired entry must be wiped");
    assert_eq!(snapshot.state_bytes, 0);
}
