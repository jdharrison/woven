use super::*;
use woven_core::{
    AccessGrant, AuthenticatedPrincipal, AuthorizationGrants, ChannelDefinition, ChannelId,
    ChannelScope, CoordinateFrame, CoreConfig, Credentials, DevAuthenticator, NamespaceId,
    PrincipalId, RoutingPolicy, SessionId, SessionKey, SpaceDescriptor, SpaceId, WovenCore,
};

const SESSION: SessionKey = SessionKey::new(NamespaceId::new(1), SessionId::new(1));

fn fixture() -> (WorkerHandle, ConnectionId, u64) {
    let mut grants = AuthorizationGrants::new();
    grants.grant_namespace(SESSION.namespace, AccessGrant::ReadWrite);
    grants.grant_session(SESSION, AccessGrant::ReadWrite);
    for id in [1, 2] {
        grants.grant_space(
            SpaceKey::new(SESSION, SpaceId::new(id)),
            AccessGrant::ReadWrite,
        );
    }
    for id in [1, 2, 3, 4] {
        grants.grant_channel(
            ChannelScope::new(SESSION, ChannelId::new(id)),
            AccessGrant::ReadWrite,
        );
    }
    let mut authenticator = DevAuthenticator::new();
    authenticator
        .insert(
            "test",
            AuthenticatedPrincipal::new(PrincipalId::new(1), grants),
        )
        .unwrap();
    let mut core = WovenCore::new(authenticator, CoreConfig::default()).unwrap();
    for (id, delivery, persistence) in [
        (
            1,
            CoreDelivery::ReliableOrdered,
            PersistenceClass::Ephemeral,
        ),
        (
            2,
            CoreDelivery::LatestValue,
            PersistenceClass::Stateful { ttl: None },
        ),
        (
            3,
            CoreDelivery::UnreliableSequenced,
            PersistenceClass::Stateful { ttl: None },
        ),
        (
            4,
            CoreDelivery::UnreliableSequenced,
            PersistenceClass::Ephemeral,
        ),
    ] {
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(id),
            delivery,
            persistence,
            65_536,
        ))
        .unwrap();
    }
    core.provision_session(SESSION).unwrap();
    for id in [1, 2] {
        core.install_space(
            SESSION,
            SpaceDescriptor {
                id: SpaceId::new(id),
                local_frame: CoordinateFrame::Logical,
                bounds: None,
                parent: None,
                epoch: SpaceEpoch::new(1),
                routing: RoutingPolicy::BroadcastAll,
            },
        )
        .unwrap();
    }
    let connection = core.transport_connected().unwrap();
    core.authenticate(connection, &Credentials::new("test"))
        .unwrap();
    core.join_session(connection, SESSION).unwrap();
    core.subscribe(connection, SpaceKey::new(SESSION, SpaceId::new(1)))
        .unwrap();
    let entity = core
        .spawn_entity(
            connection,
            SpaceKey::new(SESSION, SpaceId::new(1)),
            SpaceEpoch::new(1),
        )
        .unwrap();
    (
        spawn_worker(TransportIndependentWorker::new(core)),
        connection,
        entity.get(),
    )
}

fn state(entity: u64, channel: u64, sequence: u64, delivery: DeliveryClass) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: delivery,
        namespace_id: 1,
        session_id: 1,
        space_id: 1,
        space_epoch: 1,
        channel_id: Some(channel),
        entity_id: Some(entity),
        server_tick: 0,
        sender_sequence: sequence,
        correlation_id: None,
        routing_position: None,
        message: MessagePayload::EntityState(OpaquePayload {
            type_id: 1,
            bytes: vec![7; 25],
        }),
    }
}

async fn snapshot(worker: &WorkerHandle, connection: ConnectionId) -> woven_core::SessionSnapshot {
    let CommandResult::Snapshot(snapshot) = worker
        .execute(Command::Snapshot {
            connection,
            session: SESSION,
        })
        .await
        .unwrap()
    else {
        panic!("expected snapshot")
    };
    snapshot
}

#[tokio::test]
async fn positioned_state_is_rejected_without_negotiated_capability() {
    let (worker, connection, entity) = fixture();
    let (sender, mut receiver) = mpsc::channel(1);
    let mut positioned = state(entity, 2, 1, DeliveryClass::LatestValue);
    positioned.routing_position = Some(woven_protocol::RoutingPosition3D {
        x: 1.0,
        y: 2.0,
        z: 3.0,
    });
    assert!(
        handle_authenticated_with_capabilities(&worker, connection, positioned, &sender, None, 0,)
            .await
            .is_err()
    );
    assert!(matches!(
        receiver.try_recv().unwrap().message,
        MessagePayload::Control(ControlPayload::ProtocolError(error))
            if error.code == ProtocolErrorCode::UnsupportedMessage
    ));

    let mut legacy_positioned = state(entity, 2, 2, DeliveryClass::LatestValue);
    legacy_positioned.routing_position = Some(woven_protocol::RoutingPosition3D {
        x: 1.0,
        y: 2.0,
        z: 3.0,
    });
    assert!(
        handle_authenticated(&worker, connection, legacy_positioned, &sender, None)
            .await
            .is_err()
    );
    assert!(matches!(
        receiver.try_recv().unwrap().message,
        MessagePayload::Control(ControlPayload::ProtocolError(error))
            if error.code == ProtocolErrorCode::UnsupportedMessage
    ));
    assert!(snapshot(&worker, connection).await.state.is_empty());
}

#[tokio::test]
async fn bridge_resolves_registered_persistence_and_keeps_ephemeral_state_out_of_snapshots() {
    let (worker, connection, entity) = fixture();
    let (sender, mut receiver) = mpsc::channel(8);
    for (channel, delivery) in [
        (4, DeliveryClass::UnreliableSequenced),
        (3, DeliveryClass::UnreliableSequenced),
        (2, DeliveryClass::LatestValue),
    ] {
        handle_authenticated(
            &worker,
            connection,
            state(entity, channel, 1, delivery),
            &sender,
            None,
        )
        .await
        .unwrap();
        let envelope = receiver.try_recv().unwrap();
        assert_eq!(
            (envelope.channel_id, envelope.delivery_class),
            (Some(channel), delivery)
        );
        assert!(
            matches!(envelope.message, MessagePayload::EntityState(payload) if payload.type_id == 1 && payload.bytes == vec![7; 25])
        );
    }
    let snapshot = snapshot(&worker, connection).await;
    assert_eq!(snapshot.state.len(), 2);
    assert_eq!(snapshot.state_bytes, 50);
    assert!(
        snapshot
            .state
            .iter()
            .all(|entry| entry.channel != ChannelId::new(4))
    );
    assert_eq!(
        worker
            .channel_persistence(connection, SESSION, ChannelId::new(4))
            .await
            .unwrap(),
        PersistenceClass::Ephemeral
    );
    assert!(matches!(
        worker
            .channel_persistence(connection, SESSION, ChannelId::new(99))
            .await,
        Err(TransportError::Core(CoreError::ChannelWriteAccessDenied(_)))
    ));
}

#[tokio::test]
async fn stale_unreliable_updates_have_no_effects_and_do_not_close_the_bridge() {
    let (worker, connection, entity) = fixture();
    let (sender, mut receiver) = mpsc::channel(8);
    handle_authenticated(
        &worker,
        connection,
        state(entity, 3, 2, DeliveryClass::UnreliableSequenced),
        &sender,
        None,
    )
    .await
    .unwrap();
    assert_eq!(receiver.try_recv().unwrap().sender_sequence, 2);
    for sequence in [1, 2] {
        let mut rejected = state(entity, 3, sequence, DeliveryClass::UnreliableSequenced);
        rejected.message = MessagePayload::EntityState(OpaquePayload {
            type_id: 1,
            bytes: vec![99; 25],
        });
        handle_authenticated(&worker, connection, rejected, &sender, None)
            .await
            .unwrap();
        assert!(receiver.try_recv().is_err());
        let snapshot = snapshot(&worker, connection).await;
        assert_eq!(snapshot.state[0].sequence, 2);
        assert_eq!(snapshot.state[0].payload, vec![7; 25]);
    }
    handle_authenticated(
        &worker,
        connection,
        state(entity, 3, 3, DeliveryClass::UnreliableSequenced),
        &sender,
        None,
    )
    .await
    .unwrap();
    assert_eq!(receiver.try_recv().unwrap().sender_sequence, 3);
    assert_eq!(snapshot(&worker, connection).await.state[0].sequence, 3);

    handle_authenticated(
        &worker,
        connection,
        state(entity, 2, 2, DeliveryClass::LatestValue),
        &sender,
        None,
    )
    .await
    .unwrap();
    receiver.try_recv().unwrap();
    assert!(
        handle_authenticated(
            &worker,
            connection,
            state(entity, 2, 1, DeliveryClass::LatestValue),
            &sender,
            None
        )
        .await
        .is_err()
    );
    assert!(
        matches!(receiver.try_recv().unwrap().message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::SequenceRejected)
    );
}

#[tokio::test]
async fn registered_policy_ownership_epoch_and_payload_limits_remain_enforced() {
    let (worker, connection, entity) = fixture();
    let (sender, mut receiver) = mpsc::channel(8);
    let mut wrong_policy = state(entity, 1, 1, DeliveryClass::UnreliableSequenced);
    assert!(
        handle_authenticated(&worker, connection, wrong_policy.clone(), &sender, None)
            .await
            .is_err()
    );
    receiver.try_recv().unwrap();
    wrong_policy.channel_id = Some(4);
    wrong_policy.space_epoch = 2;
    assert!(
        handle_authenticated(&worker, connection, wrong_policy, &sender, None)
            .await
            .is_err()
    );
    assert!(
        matches!(receiver.try_recv().unwrap().message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::StaleEpoch)
    );
    let mut oversized = state(entity, 4, 1, DeliveryClass::UnreliableSequenced);
    oversized.message = MessagePayload::EntityState(OpaquePayload {
        type_id: 1,
        bytes: vec![0; 65_537],
    });
    assert!(
        handle_authenticated(&worker, connection, oversized, &sender, None)
            .await
            .is_err()
    );
    assert!(
        matches!(receiver.try_recv().unwrap().message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::PayloadTooLarge)
    );
    let CommandResult::EntitySpawned(other) = worker
        .execute(Command::SpawnEntity {
            connection,
            space: SpaceKey::new(SESSION, SpaceId::new(1)),
            epoch: SpaceEpoch::new(1),
        })
        .await
        .unwrap()
    else {
        panic!("expected entity")
    };
    let CommandResult::Connected(other_connection) =
        worker.execute(Command::TransportConnected).await.unwrap()
    else {
        panic!("expected connection")
    };
    worker
        .execute(Command::Authenticate {
            connection: other_connection,
            credentials: Credentials::new("test"),
        })
        .await
        .unwrap();
    worker
        .execute(Command::JoinSession {
            connection: other_connection,
            session: SESSION,
        })
        .await
        .unwrap();
    worker
        .execute(Command::Subscribe {
            connection: other_connection,
            space: SpaceKey::new(SESSION, SpaceId::new(1)),
        })
        .await
        .unwrap();
    assert!(
        handle_authenticated(
            &worker,
            other_connection,
            state(other.get(), 4, 1, DeliveryClass::UnreliableSequenced),
            &sender,
            None
        )
        .await
        .is_err()
    );
    assert!(
        matches!(receiver.try_recv().unwrap().message, MessagePayload::Control(ControlPayload::ProtocolError(error)) if error.code == ProtocolErrorCode::Unauthorized)
    );
    assert!(snapshot(&worker, connection).await.state.is_empty());
    handle_authenticated(
        &worker,
        connection,
        state(entity, 4, 1, DeliveryClass::UnreliableSequenced),
        &sender,
        None,
    )
    .await
    .unwrap();
    assert_eq!(receiver.try_recv().unwrap().sender_sequence, 1);
}

async fn drain(
    worker: &WorkerHandle,
    connection: ConnectionId,
) -> Vec<woven_core::OutboundMessage> {
    let CommandResult::Outbound(messages) = worker
        .execute(Command::DrainOutbound { connection })
        .await
        .unwrap()
    else {
        panic!("expected outbound")
    };
    messages
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one controlled probe exercises startup and per-space activation"
)]
async fn pending_subscriptions_retain_peer_payloads_until_startup_replies_are_queued() {
    let (worker, alice, alice_entity) = fixture();
    let space = SpaceKey::new(SESSION, SpaceId::new(1));
    let (alice_sender, mut alice_receiver) = mpsc::channel(8);
    let (alice_shutdown, _alice_shutdown_receiver) = mpsc::channel(1);
    worker
        .register_lifecycle(alice, alice_sender.clone(), alice_shutdown)
        .await
        .unwrap();
    worker.activate_subscription(alice, space).await.unwrap();
    let CommandResult::Connected(bob) = worker.execute(Command::TransportConnected).await.unwrap()
    else {
        panic!("expected connection")
    };
    worker
        .execute(Command::Authenticate {
            connection: bob,
            credentials: Credentials::new("test"),
        })
        .await
        .unwrap();
    worker
        .execute(Command::JoinSession {
            connection: bob,
            session: SESSION,
        })
        .await
        .unwrap();
    let (bob_sender, mut bob_receiver) = mpsc::channel(8);
    let (bob_shutdown, _bob_shutdown_receiver) = mpsc::channel(1);
    worker
        .register_lifecycle(bob, bob_sender.clone(), bob_shutdown)
        .await
        .unwrap();
    let bob_entity = worker
        .subscribe_and_spawn(bob, space, SpaceEpoch::new(1))
        .await
        .unwrap();
    let entered = alice_receiver.try_recv().unwrap();
    assert_eq!(entered.entity_id, Some(bob_entity.get()));
    assert!(matches!(
        entered.message,
        MessagePayload::Control(ControlPayload::EntityEntered(_))
    ));
    let mut profile = state(alice_entity, 1, 1, DeliveryClass::ReliableOrdered);
    profile.message = MessagePayload::ReliableEvent(OpaquePayload {
        type_id: 1,
        bytes: b"early peer profile".to_vec(),
    });
    handle_authenticated(&worker, alice, profile.clone(), &alice_sender, None)
        .await
        .unwrap();
    handle_authenticated(
        &worker,
        alice,
        state(alice_entity, 4, 1, DeliveryClass::UnreliableSequenced),
        &alice_sender,
        None,
    )
    .await
    .unwrap();
    for _ in 0..3 {
        assert!(drain(&worker, bob).await.is_empty());
    }
    assert!(bob_receiver.try_recv().is_err());
    let mut accepted = Envelope::control(
        DeliveryClass::ReliableOrdered,
        ControlPayload::SubscriptionAccepted(woven_protocol::SubscriptionAccepted {
            subscription_id: 1,
            accepted_space_epoch: 1,
        }),
    );
    accepted.namespace_id = 1;
    accepted.session_id = 1;
    accepted.space_id = 1;
    accepted.space_epoch = 1;
    accepted.channel_id = Some(1);
    send_envelope(&bob_sender, accepted.clone()).await.unwrap();
    let entered = entity_entered_envelope(space, SpaceEpoch::new(1), bob_entity);
    send_envelope(&bob_sender, entered.clone()).await.unwrap();
    worker.activate_subscription(bob, space).await.unwrap();
    flush_outbound(&worker, bob, &bob_sender).await.unwrap();
    assert_eq!(bob_receiver.try_recv().unwrap(), accepted);
    assert_eq!(bob_receiver.try_recv().unwrap(), entered);
    assert_eq!(bob_receiver.try_recv().unwrap(), profile);
    assert_eq!(bob_receiver.try_recv().unwrap().channel_id, Some(4));
    assert!(drain(&worker, bob).await.is_empty());

    // A second pending space must not leak, or stall the already active space.
    let second_space = SpaceKey::new(SESSION, SpaceId::new(2));
    worker
        .execute(Command::Subscribe {
            connection: alice,
            space: second_space,
        })
        .await
        .unwrap();
    let CommandResult::EntitySpawned(second_entity) = worker
        .execute(Command::SpawnEntity {
            connection: alice,
            space: second_space,
            epoch: SpaceEpoch::new(1),
        })
        .await
        .unwrap()
    else {
        panic!("expected entity")
    };
    worker
        .subscribe_and_spawn(bob, second_space, SpaceEpoch::new(1))
        .await
        .unwrap();
    let mut second_profile = profile.clone();
    second_profile.space_id = 2;
    second_profile.entity_id = Some(second_entity.get());
    handle_authenticated(&worker, alice, second_profile.clone(), &alice_sender, None)
        .await
        .unwrap();
    profile.sender_sequence = 2;
    handle_authenticated(&worker, alice, profile.clone(), &alice_sender, None)
        .await
        .unwrap();
    let active = drain(&worker, bob).await;
    assert_eq!(active.len(), 1);
    assert_eq!((active[0].space.get(), active[0].sequence), (1, 2));
    assert!(drain(&worker, bob).await.is_empty());
    worker
        .activate_subscription(bob, second_space)
        .await
        .unwrap();
    let released = drain(&worker, bob).await;
    assert_eq!(released.len(), 1);
    assert_eq!((released[0].space.get(), released[0].sequence), (2, 1));
    assert_eq!(released[0].payload, b"early peer profile");
}
