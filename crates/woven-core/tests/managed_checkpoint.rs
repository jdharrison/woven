use std::time::{Duration, Instant};
use woven_core::*;

const ADMIN: &str = "independent-admin-checkpoint-test-credential";

fn session(id: u64) -> SessionKey {
    SessionKey::new(NamespaceId::new(9), SessionId::new(id))
}

fn core() -> WovenCore<DevAuthenticator> {
    let mut core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    core.enable_managed(&Credentials::new(ADMIN)).unwrap();
    for (id, delivery) in [
        (1, DeliveryClass::ReliableOrdered),
        (4, DeliveryClass::UnreliableSequenced),
    ] {
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(id),
            delivery,
            PersistenceClass::Ephemeral,
            65_536,
        ))
        .unwrap();
    }
    core
}

fn put(id: u64, token: char) -> ManagedRequest {
    ManagedRequest::Put {
        session: session(id),
        revision: 1,
        allocated_ccu: 1,
        tick_rate_hz: Some(1),
        credentials: Credentials::new(token.to_string().repeat(64)),
    }
}

fn space(id: u64, revision: u64) -> ManagedRequest {
    ManagedRequest::PutSpace {
        session: session(id),
        revision,
        descriptor: SpaceDescriptor {
            id: SpaceId::new(17),
            epoch: SpaceEpoch::new(1),
            local_frame: CoordinateFrame::Cartesian3D {
                meters_per_unit: 0.125,
            },
            bounds: Some(SpatialBounds3D {
                min_x: -1024.5,
                min_y: -64.0,
                min_z: -8.0,
                max_x: 2048.25,
                max_y: 128.5,
                max_z: 16.0,
            }),
            parent: None,
            routing: RoutingPolicy::SpatialGrid3D {
                cell_size: 7.5,
                interest_radius: 15.25,
                exact_distance: true,
            },
        },
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
        panic!("snapshot expected")
    };
    snapshot
}

fn connect(core: &mut WovenCore<DevAuthenticator>, token: char) -> ConnectionId {
    let connection = core.transport_connected().unwrap();
    core.authenticate(connection, &Credentials::new(token.to_string().repeat(64)))
        .unwrap();
    connection
}

fn join(core: &mut WovenCore<DevAuthenticator>, connection: ConnectionId, id: u64) -> JoinDecision {
    core.admit_and_join_session_at(
        connection,
        session(id),
        IdempotencyKey::new("join").unwrap(),
        Instant::now(),
    )
    .unwrap()
}

fn event(connection: ConnectionId, entity: EntityId) -> PublishRequest {
    PublishRequest {
        connection,
        session: session(1),
        space: SpaceId::new(1),
        space_epoch: SpaceEpoch::new(1),
        entity: Some(entity),
        channel: ChannelId::new(1),
        sequence: 1,
        delivery: DeliveryClass::ReliableOrdered,
        persistence: PersistenceClass::Ephemeral,
        coalesce_key: None,
        routing_position: None,
        payload: b"NEVER-PERSIST-LIVE-PAYLOAD".to_vec(),
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn crash_reload_preserves_auth_spatial_retirement_and_exact_retry_identity_not_live_state() {
    let now = Instant::now();
    let mut original = core();
    for (id, token) in [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')] {
        original.manage_at(put(id, token), now).unwrap();
    }
    for id in [1, 2, 3] {
        original.manage_at(space(id, 2), now).unwrap();
    }
    let first = connect(&mut original, 'a');
    assert!(matches!(
        join(&mut original, first, 1),
        JoinDecision::Admitted(_)
    ));
    let waiting = connect(&mut original, 'a');
    assert!(matches!(
        join(&mut original, waiting, 1),
        JoinDecision::Queued(_)
    ));
    let system = SpaceKey::new(session(1), SpaceId::new(1));
    original.subscribe(first, system).unwrap();
    let entity = original
        .spawn_entity(first, system, SpaceEpoch::new(1))
        .unwrap();
    original.publish(event(first, entity)).unwrap();
    let patch = ManagedRequest::Patch {
        session: session(1),
        revision: 3,
        allocated_ccu: 0,
        tick_rate_hz: None,
    };
    original.manage_at(patch.clone(), now).unwrap();
    let delete = ManagedRequest::Delete {
        session: session(3),
        revision: 2,
    };
    original.manage_at(delete.clone(), now).unwrap();
    let before = snapshot(&mut original, 1);
    assert_eq!(before.admission.active_ccu, 1);
    assert_eq!(before.admission.pending_target, Some(0));
    assert_eq!(before.admission.queue_depth, 1);

    let checkpoint = original.export_managed_checkpoint().unwrap();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let encoded = std::str::from_utf8(&bytes).unwrap();
    for forbidden in [
        ADMIN,
        &"a".repeat(64),
        "NEVER-PERSIST-LIVE-PAYLOAD",
        "adminVerifier",
        "connections",
        "admission",
        "payload",
        "operations",
    ] {
        assert!(
            !encoded.contains(forbidden),
            "unexpected persisted field/value: {forbidden}"
        );
    }
    let debug = format!("{checkpoint:?} {:?}", checkpoint.scopes[0]);
    for entry in &checkpoint.scopes {
        assert!(!debug.contains(&entry.verifier_sha256));
    }
    drop(original); // Simulated crash: no graceful leave or rate-window cleanup.

    let mut restored = core();
    restored
        .restore_managed_checkpoint(serde_json::from_slice(&bytes).unwrap())
        .unwrap();
    assert_eq!(restored.export_managed_checkpoint().unwrap(), checkpoint);
    assert_eq!(restored.connection_count(), 0);
    assert_eq!(restored.session_count(), 3);
    assert_eq!(restored.sequence_key_count(session(1)), Some(0));
    assert_eq!(restored.journal_outbox_len(), 0);
    let after = snapshot(&mut restored, 1);
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.spaces, before.spaces);
    assert_eq!(after.tick_rate_hz, Some(1));
    assert_eq!(after.allocated_ccu, 0);
    assert_eq!(after.admission.effective_allocated_ccu, 0);
    assert_eq!(after.admission.pending_target, None);
    assert_eq!(after.admission.active_ccu, 0);
    assert_eq!(after.admission.offered_slots, 0);
    assert_eq!(after.admission.queue_depth, 0);
    restored.manage_at(patch, now).unwrap();
    assert_eq!(
        restored.manage_at(
            ManagedRequest::Patch {
                session: session(1),
                revision: 3,
                allocated_ccu: 0,
                tick_rate_hz: Some(1),
            },
            now
        ),
        Err(ManagedError::RevisionConflict)
    );
    restored.manage_at(space(2, 2), now).unwrap();
    let ManagedRequest::PutSpace { mut descriptor, .. } = space(2, 2) else {
        unreachable!()
    };
    descriptor.routing = RoutingPolicy::SpatialGrid3D {
        cell_size: 7.5,
        interest_radius: 15.25,
        exact_distance: false,
    };
    assert_eq!(
        restored.manage_at(
            ManagedRequest::PutSpace {
                session: session(2),
                revision: 2,
                descriptor,
            },
            now
        ),
        Err(ManagedError::RevisionConflict)
    );
    restored.manage_at(put(4, 'd'), now).unwrap();
    restored.manage_at(delete, now).unwrap();
    assert_eq!(
        restored.manage_at(put(3, 'c'), now),
        Err(ManagedError::ScopeRetired)
    );
    assert_eq!(
        restored.manage_at(put(5, 'c'), now),
        Err(ManagedError::TokenConflict)
    );
    for token in ["c".repeat(64), ADMIN.to_owned(), "unknown".to_owned()] {
        let connection = restored.transport_connected().unwrap();
        assert!(
            restored
                .authenticate(connection, &Credentials::new(token))
                .is_err()
        );
        restored.transport_lost(connection).unwrap();
    }

    restored
        .manage_at(
            ManagedRequest::Patch {
                session: session(1),
                revision: 4,
                allocated_ccu: 1,
                tick_rate_hz: None,
            },
            now,
        )
        .unwrap();
    let first = connect(&mut restored, 'a');
    let other = connect(&mut restored, 'b');
    assert!(matches!(
        join(&mut restored, first, 1),
        JoinDecision::Admitted(_)
    ));
    assert!(matches!(
        join(&mut restored, other, 2),
        JoinDecision::Admitted(_)
    ));
    assert!(
        restored
            .subscribe(first, SpaceKey::new(session(2), SpaceId::new(17)))
            .is_err()
    );
    restored
        .subscribe(first, SpaceKey::new(session(1), SpaceId::new(17)))
        .unwrap();
    restored.subscribe(first, system).unwrap();
    let clean = restored.snapshot(first, session(1)).unwrap();
    assert!(clean.entities.is_empty());
    assert!(clean.state.is_empty());
    let spatial = clean
        .spaces
        .iter()
        .find(|entry| entry.descriptor.id == SpaceId::new(17))
        .unwrap();
    let ManagedRequest::PutSpace { descriptor, .. } = space(1, 2) else {
        unreachable!()
    };
    assert_eq!(spatial.descriptor, descriptor);
    let entity = restored
        .spawn_entity(first, system, SpaceEpoch::new(1))
        .unwrap();
    restored.publish(event(first, entity)).unwrap(); // Rate and sequence history did not survive.
    let mut next = event(first, entity);
    next.sequence = 2;
    assert!(matches!(
        restored.publish(next),
        Err(CoreError::PublishRateLimited { .. })
    ));
    restored.maintain_admission_at(now + Duration::from_secs(1000));
}

fn populated_checkpoint() -> ManagedCheckpoint {
    let mut core = core();
    core.manage_at(put(1, 'a'), Instant::now()).unwrap();
    core.manage_at(space(1, 2), Instant::now()).unwrap();
    core.export_managed_checkpoint().unwrap()
}

#[test]
fn spatial_json_roundtrip_preserves_every_float_bit_including_negative_zero() {
    let mut checkpoint = populated_checkpoint();
    let space = &mut checkpoint.scopes[0].spatial_spaces[0];
    space.meters_per_unit = f64::from_bits(0x3fb9_9999_9999_999b);
    space.cell_size = f64::from_bits(0x3ff0_0000_0000_0001);
    space.interest_radius = f64::from_bits(0x400f_ffff_ffff_ffff);
    space.bounds.min.x = -f64::from_bits(0x3fb9_9999_9999_999b);
    space.bounds.min.y = -0.0;
    space.bounds.max.z = f64::from_bits(0x400f_ffff_ffff_ffff);
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let decoded: ManagedCheckpoint = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, checkpoint);
    assert_eq!(
        decoded.scopes[0].spatial_spaces[0].bounds.min.y.to_bits(),
        (-0.0_f64).to_bits()
    );
    let mut target = core();
    target.restore_managed_checkpoint(decoded).unwrap();
    assert_eq!(target.export_managed_checkpoint().unwrap(), checkpoint);
}

#[test]
fn malformed_checkpoints_reject_atomically_and_do_not_poison_a_fresh_core() {
    type Corruption = fn(&mut ManagedCheckpoint);
    let corruptions: &[Corruption] = &[
        |c| c.version = 2,
        |c| c.scopes[0].namespace_id = 0,
        |c| c.scopes[0].session_id = 0,
        |c| c.scopes[0].revision = 0,
        |c| c.scopes[0].allocated_ccu = MAX_MANAGED_CCU + 1,
        |c| c.scopes[0].tick_rate_hz = Some(0),
        |c| c.scopes[0].tick_rate_hz = Some(121),
        |c| c.scopes[0].verifier_sha256 = "short".to_owned(),
        |c| c.scopes[0].verifier_sha256 = "g".repeat(64),
        |c| c.scopes[0].verifier_sha256 = "A".repeat(64),
        |c| c.scopes[0].verifier_sha256 = "0".repeat(64),
        |c| c.scopes.push(c.scopes[0].clone()),
        |c| {
            let mut duplicate = c.scopes[0].clone();
            duplicate.session_id = 2;
            c.scopes.push(duplicate);
        },
        |c| {
            let duplicate = c.scopes[0].spatial_spaces[0].clone();
            c.scopes[0].spatial_spaces.push(duplicate);
        },
        |c| c.scopes[0].spatial_spaces = vec![c.scopes[0].spatial_spaces[0].clone(); 65],
        |c| c.scopes[0].spatial_spaces[0].space_id = 0,
        |c| c.scopes[0].spatial_spaces[0].space_id = 2,
        |c| c.scopes[0].spatial_spaces[0].epoch = 2,
        |c| c.scopes[0].spatial_spaces[0].meters_per_unit = 0.0,
        |c| c.scopes[0].spatial_spaces[0].cell_size = f64::NAN,
        |c| c.scopes[0].spatial_spaces[0].interest_radius = f64::INFINITY,
        |c| c.scopes[0].spatial_spaces[0].bounds.min.x = 3000.0,
        |c| c.scopes[0].spatial_spaces[0].bounds.max.z = f64::NEG_INFINITY,
        |c| c.scopes[0].last_mutation = ManagedCheckpointMutation::Provision,
        |c| c.scopes[0].last_mutation = ManagedCheckpointMutation::Space { space_id: 999 },
        |c| {
            c.scopes[0].last_mutation = ManagedCheckpointMutation::Configuration {
                allocated_ccu: 2,
                tick_rate_hz: None,
            }
        },
        |c| {
            c.scopes[0].last_mutation = ManagedCheckpointMutation::Configuration {
                allocated_ccu: 1,
                tick_rate_hz: Some(2),
            }
        },
    ];
    let valid = populated_checkpoint();
    for (index, corrupt) in corruptions.iter().enumerate() {
        let mut bad = valid.clone();
        corrupt(&mut bad);
        let mut target = core();
        assert!(
            target.restore_managed_checkpoint(bad).is_err(),
            "corruption {index}"
        );
        assert_eq!(target.session_count(), 0);
        assert!(
            target
                .export_managed_checkpoint()
                .unwrap()
                .scopes
                .is_empty()
        );
        target.restore_managed_checkpoint(valid.clone()).unwrap();
    }
}

#[test]
fn restoration_requires_managed_mode_registered_channels_and_a_fresh_empty_core() {
    let checkpoint = populated_checkpoint();
    let mut target = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint.clone()),
        Err(ManagedError::InvalidRequest)
    );
    target.enable_managed(&Credentials::new(ADMIN)).unwrap();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint.clone()),
        Err(ManagedError::InvalidRequest)
    );
    let mut target = core();
    target.provision_session(session(99)).unwrap();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint.clone()),
        Err(ManagedError::InvalidRequest)
    );
    let mut target = core();
    let connection = target.transport_connected().unwrap();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint.clone()),
        Err(ManagedError::InvalidRequest)
    );
    target.transport_lost(connection).unwrap();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint.clone()),
        Err(ManagedError::InvalidRequest)
    );
    let mut target = core();
    target
        .restore_managed_checkpoint(checkpoint.clone())
        .unwrap();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint),
        Err(ManagedError::InvalidRequest)
    );
}

#[test]
fn verifier_cannot_conflict_with_current_admin_even_when_the_scope_is_retired() {
    let token = "a".repeat(64);
    for retired in [false, true] {
        let mut checkpoint = populated_checkpoint();
        checkpoint.scopes[0].retired = retired;
        let mut target = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
        target
            .enable_managed(&Credentials::new(token.clone()))
            .unwrap();
        target
            .register_channel(ChannelDefinition::relay_owned(
                ChannelId::new(1),
                DeliveryClass::ReliableOrdered,
                PersistenceClass::Ephemeral,
                1024,
            ))
            .unwrap();
        assert_eq!(
            target.restore_managed_checkpoint(checkpoint),
            Err(ManagedError::TokenConflict)
        );
        assert_eq!(target.session_count(), 0);
    }
}

#[test]
fn serde_rejects_oversized_lists_invalid_verifiers_and_unknown_privileged_fields() {
    let checkpoint = populated_checkpoint();
    let mut value = serde_json::to_value(&checkpoint).unwrap();
    value["scopes"][0]["spatialSpaces"] =
        serde_json::json!(vec![checkpoint.scopes[0].spatial_spaces[0].clone(); 65]);
    assert!(serde_json::from_value::<ManagedCheckpoint>(value).is_err());
    let mut value = serde_json::to_value(&checkpoint).unwrap();
    value["scopes"] =
        serde_json::json!(vec![checkpoint.scopes[0].clone(); MAX_MANAGED_HISTORY + 1]);
    assert!(serde_json::from_value::<ManagedCheckpoint>(value).is_err());
    for verifier in [
        "x".repeat(64),
        "0".repeat(64),
        "short".to_owned(),
        "f".repeat(4096),
    ] {
        let mut value = serde_json::to_value(&checkpoint).unwrap();
        value["scopes"][0]["verifierSha256"] = verifier.into();
        assert!(serde_json::from_value::<ManagedCheckpoint>(value).is_err());
    }
    for key in ["adminVerifier", "token", "connections"] {
        let mut value = serde_json::to_value(&checkpoint).unwrap();
        value["scopes"][0][key] = "forbidden".into();
        assert!(serde_json::from_value::<ManagedCheckpoint>(value).is_err());
    }
}

#[test]
fn history_active_and_local_configuration_bounds_are_enforced() {
    let mut source = core();
    source.manage_at(put(1, 'a'), Instant::now()).unwrap();
    let base = source.export_managed_checkpoint().unwrap().scopes.remove(0);
    let checkpoint = |count: usize, retired: bool| ManagedCheckpoint {
        version: MANAGED_CHECKPOINT_VERSION,
        migration_baseline: None,
        scopes: (1..=count)
            .map(|id| ManagedCheckpointScope {
                session_id: id as u64,
                verifier_sha256: format!("{id:064x}"),
                retired,
                ..base.clone()
            })
            .collect(),
    };
    let mut target = core();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint(MAX_MANAGED_SCOPES + 1, false)),
        Err(ManagedError::CapacityExhausted)
    );
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint(MAX_MANAGED_HISTORY + 1, true)),
        Err(ManagedError::CapacityExhausted)
    );
    target
        .restore_managed_checkpoint(checkpoint(MAX_MANAGED_HISTORY, true))
        .unwrap();
    assert_eq!(target.session_count(), 0);
    assert_eq!(
        target.export_managed_checkpoint().unwrap().scopes.len(),
        MAX_MANAGED_HISTORY
    );

    let mut target = WovenCore::new(
        DevAuthenticator::new(),
        CoreConfig {
            max_sessions: 1,
            max_connections: 1,
            ..CoreConfig::default()
        },
    )
    .unwrap();
    target.enable_managed(&Credentials::new(ADMIN)).unwrap();
    target
        .register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(1),
            DeliveryClass::ReliableOrdered,
            PersistenceClass::Ephemeral,
            1024,
        ))
        .unwrap();
    assert_eq!(
        target.restore_managed_checkpoint(checkpoint(2, false)),
        Err(ManagedError::CapacityExhausted)
    );
    let mut excessive_ccu = checkpoint(1, false);
    excessive_ccu.scopes[0].allocated_ccu = 2;
    assert_eq!(
        target.restore_managed_checkpoint(excessive_ccu),
        Err(ManagedError::InvalidRequest)
    );
    assert_eq!(target.session_count(), 0);
}
