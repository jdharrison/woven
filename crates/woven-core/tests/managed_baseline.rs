use std::time::Instant;
use woven_core::*;

const ADMIN: &str = "independent-baseline-test-admin-credential";
const HIGH_WATER: u64 = 100;

fn key(id: u64) -> SessionKey {
    SessionKey::new(NamespaceId::new(10 + id), SessionId::new(40 + id))
}

fn credentials(id: u64) -> Credentials {
    Credentials::new(format!("{id:064x}"))
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

fn metadata() -> ManagedMigrationBaseline {
    ManagedMigrationBaseline {
        version: MANAGED_MIGRATION_BASELINE_VERSION,
        migration_id: "9d7d6793-633c-47ae-b53c-df68de7cfec8".to_owned(),
        previous_lineage: "e".repeat(48),
        closed_scope_through: HIGH_WATER,
        unknown_retired_verifier_history: true,
    }
}

fn descriptor(id: u64) -> SpaceDescriptor {
    SpaceDescriptor {
        id: SpaceId::new(id),
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
    }
}

fn inventory(count: u64) -> ManagedCheckpoint {
    let mut source = core();
    for id in 1..=count {
        source
            .manage_at(
                ManagedRequest::Put {
                    session: key(id),
                    revision: 5 + id,
                    allocated_ccu: u32::try_from(id % 4 + 1).unwrap(),
                    tick_rate_hz: Some(u32::try_from(10 + id).unwrap()),
                    credentials: credentials(id),
                },
                Instant::now(),
            )
            .unwrap();
        source
            .manage_at(
                ManagedRequest::PutSpace {
                    session: key(id),
                    revision: 6 + id,
                    descriptor: descriptor(17),
                },
                Instant::now(),
            )
            .unwrap();
    }
    let mut checkpoint = source.export_managed_checkpoint().unwrap();
    checkpoint.migration_baseline = Some(metadata());
    for scope in &mut checkpoint.scopes {
        scope.last_mutation = ManagedCheckpointMutation::MigrationBaseline;
    }
    checkpoint
}

fn get(core: &mut WovenCore<DevAuthenticator>, session: SessionKey) -> ManagedSnapshot {
    let ManagedOutcome::Snapshot { snapshot, .. } = core
        .manage_at(ManagedRequest::Get { session }, Instant::now())
        .unwrap()
    else {
        panic!("snapshot expected")
    };
    snapshot
}

fn patch(scope: &ManagedCheckpointScope, revision: u64) -> ManagedRequest {
    ManagedRequest::Patch {
        session: SessionKey::new(scope.namespace_id.into(), scope.session_id.into()),
        revision,
        allocated_ccu: scope.allocated_ccu,
        tick_rate_hz: None,
    }
}

fn put(session: SessionKey, token_id: u64) -> ManagedRequest {
    ManagedRequest::Put {
        session,
        revision: 1,
        allocated_ccu: 1,
        tick_rate_hz: None,
        credentials: credentials(token_id),
    }
}

#[test]
fn all_twelve_inventory_scopes_keep_ids_revisions_auth_and_spatial_with_empty_worlds() {
    let approved = inventory(12);
    let mut restored = core();
    restored
        .restore_managed_checkpoint(approved.clone())
        .unwrap();
    assert_eq!(restored.session_count(), 12);
    assert_eq!(restored.connection_count(), 0);
    assert_eq!(restored.export_managed_checkpoint().unwrap(), approved);
    for id in 1..=12 {
        let before = &approved.scopes[usize::try_from(id - 1).unwrap()];
        let snapshot = get(&mut restored, key(id));
        assert_eq!(snapshot.namespace_id, before.namespace_id.to_string());
        assert_eq!(snapshot.session_id, before.session_id.to_string());
        assert_eq!(snapshot.revision, before.revision.to_string());
        assert_eq!(snapshot.allocated_ccu, before.allocated_ccu);
        assert_eq!(snapshot.tick_rate_hz, before.tick_rate_hz);
        assert_eq!(snapshot.admission.active_ccu, 0);
        assert_eq!(snapshot.admission.available_slots, before.allocated_ccu);
        let connection = restored.transport_connected().unwrap();
        restored.authenticate(connection, &credentials(id)).unwrap();
        assert!(matches!(
            restored
                .admit_and_join_session_at(
                    connection,
                    key(id),
                    IdempotencyKey::new("baseline-join").unwrap(),
                    Instant::now(),
                )
                .unwrap(),
            JoinDecision::Admitted(_)
        ));
        let space = SpaceKey::new(key(id), SpaceId::new(17));
        restored.subscribe(connection, space).unwrap();
        let world = restored.snapshot(connection, key(id)).unwrap();
        assert_eq!(world.spaces.len(), 1);
        assert_eq!(world.spaces[0].descriptor, descriptor(17));
        assert!(world.entities.is_empty());
        assert!(world.state.is_empty());
        restored
            .spawn_entity(connection, space, SpaceEpoch::new(1))
            .unwrap();
        assert!(
            restored
                .subscribe(
                    connection,
                    SpaceKey::new(key(id % 12 + 1), SpaceId::new(17))
                )
                .is_err()
        );
    }
    let bytes = serde_json::to_vec(&restored.export_managed_checkpoint().unwrap()).unwrap();
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(serde_json::from_slice(&bytes).unwrap())
        .unwrap();
    assert_eq!(restarted.session_count(), 12);
    assert_eq!(restarted.connection_count(), 0);
    assert_eq!(restarted.export_managed_checkpoint().unwrap(), approved);
    for id in 1..=12 {
        assert_eq!(get(&mut restarted, key(id)).admission.active_ccu, 0);
    }
}

#[test]
fn baseline_rejects_historical_same_revision_put_patch_and_spatial_replay_without_changes() {
    let approved = inventory(12);
    let mut restored = core();
    restored
        .restore_managed_checkpoint(approved.clone())
        .unwrap();
    for (index, scope) in approved.scopes.iter().enumerate() {
        let session = key(index as u64 + 1);
        let requests = [
            ManagedRequest::Put {
                session,
                revision: scope.revision,
                allocated_ccu: scope.allocated_ccu,
                tick_rate_hz: scope.tick_rate_hz,
                credentials: credentials(index as u64 + 1),
            },
            patch(scope, scope.revision),
            ManagedRequest::Patch {
                session,
                revision: scope.revision,
                allocated_ccu: scope.allocated_ccu,
                tick_rate_hz: scope.tick_rate_hz,
            },
            ManagedRequest::PutSpace {
                session,
                revision: scope.revision,
                descriptor: descriptor(17),
            },
            ManagedRequest::PutSpace {
                session,
                revision: scope.revision,
                descriptor: descriptor(18),
            },
            patch(scope, scope.revision - 1),
            patch(scope, scope.revision + 2),
            ManagedRequest::PutSpace {
                session,
                revision: scope.revision + 2,
                descriptor: descriptor(18),
            },
        ];
        for request in requests {
            assert_eq!(
                restored.manage_at(request, Instant::now()),
                Err(ManagedError::RevisionConflict)
            );
        }
        assert_eq!(
            get(&mut restored, session).revision,
            scope.revision.to_string()
        );
    }
    assert_eq!(restored.export_managed_checkpoint().unwrap(), approved);
}

#[test]
fn first_next_revision_records_real_identity_and_provenance_survives_all_twelve_edits() {
    let approved = inventory(12);
    let mut restored = core();
    restored
        .restore_managed_checkpoint(approved.clone())
        .unwrap();
    for scope in &approved.scopes {
        let request = patch(scope, scope.revision + 1);
        restored.manage_at(request.clone(), Instant::now()).unwrap();
        restored.manage_at(request, Instant::now()).unwrap();
    }
    let checkpoint = restored.export_managed_checkpoint().unwrap();
    assert_eq!(checkpoint.migration_baseline, approved.migration_baseline);
    assert_eq!(checkpoint.scopes.len(), 12);
    for (after, before) in checkpoint.scopes.iter().zip(&approved.scopes) {
        assert_eq!(after.namespace_id, before.namespace_id);
        assert_eq!(after.session_id, before.session_id);
        assert_eq!(after.revision, before.revision + 1);
        assert_eq!(after.verifier_sha256, before.verifier_sha256);
        assert_eq!(after.spatial_spaces, before.spatial_spaces);
        assert_eq!(
            after.last_mutation,
            ManagedCheckpointMutation::Configuration {
                allocated_ccu: before.allocated_ccu,
                tick_rate_hz: None,
            }
        );
    }
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(
            serde_json::from_slice(&serde_json::to_vec(&checkpoint).unwrap()).unwrap(),
        )
        .unwrap();
    assert_eq!(restarted.export_managed_checkpoint().unwrap(), checkpoint);
    for scope in &checkpoint.scopes {
        restarted
            .manage_at(patch(scope, scope.revision), Instant::now())
            .unwrap();
    }
    let session = SessionKey::new(NamespaceId::new(HIGH_WATER), SessionId::new(HIGH_WATER + 1));
    assert_eq!(
        restarted.manage_at(put(session, 1000), Instant::now()),
        Err(ManagedError::ScopeRetired)
    );
}

#[test]
fn baseline_space_edit_at_next_revision_is_real_and_retriable_after_reload() {
    let approved = inventory(1);
    let mut restored = core();
    restored
        .restore_managed_checkpoint(approved.clone())
        .unwrap();
    let request = ManagedRequest::PutSpace {
        session: key(1),
        revision: approved.scopes[0].revision + 1,
        descriptor: descriptor(18),
    };
    restored.manage_at(request.clone(), Instant::now()).unwrap();
    let checkpoint = restored.export_managed_checkpoint().unwrap();
    assert_eq!(
        checkpoint.scopes[0].last_mutation,
        ManagedCheckpointMutation::Space { space_id: 18 }
    );
    assert_eq!(checkpoint.migration_baseline, approved.migration_baseline);
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(checkpoint.clone())
        .unwrap();
    restarted.manage_at(request, Instant::now()).unwrap();
    assert_eq!(restarted.export_managed_checkpoint().unwrap(), checkpoint);
}

#[test]
fn closed_registry_fence_rejects_all_absent_old_and_mixed_ids_without_synthetic_revocations() {
    let approved = inventory(12);
    let mut restored = core();
    restored.restore_managed_checkpoint(approved).unwrap();
    let mut permitted = 0;
    let ids = [1, HIGH_WATER, HIGH_WATER + 1, u64::MAX];
    for (i, namespace) in ids.into_iter().enumerate() {
        for (j, session) in ids.into_iter().enumerate() {
            let session = SessionKey::new(namespace.into(), session.into());
            let result =
                restored.manage_at(put(session, 1000 + (i * 4 + j) as u64), Instant::now());
            if namespace <= HIGH_WATER || session.session.get() <= HIGH_WATER {
                assert_eq!(result, Err(ManagedError::ScopeRetired));
                for request in [
                    ManagedRequest::Get { session },
                    ManagedRequest::Delete {
                        session,
                        revision: 1,
                    },
                ] {
                    assert_eq!(
                        restored.manage_at(request, Instant::now()),
                        Err(ManagedError::ScopeNotFound)
                    );
                }
            } else {
                assert!(matches!(
                    result,
                    Ok(ManagedOutcome::Snapshot { created: true, .. })
                ));
                permitted += 1;
            }
        }
    }
    assert_eq!(permitted, 4);
    let checkpoint = restored.export_managed_checkpoint().unwrap();
    assert_eq!(checkpoint.scopes.len(), 12 + permitted);
    assert!(checkpoint.scopes.iter().all(|scope| !scope.retired));
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(checkpoint.clone())
        .unwrap();
    assert_eq!(restarted.export_managed_checkpoint().unwrap(), checkpoint);
}

#[test]
fn real_delete_retains_known_verifier_tombstones_and_provenance_even_with_no_active_scopes() {
    let approved = inventory(12);
    let mut restored = core();
    restored
        .restore_managed_checkpoint(approved.clone())
        .unwrap();
    for (index, scope) in approved.scopes.iter().enumerate() {
        let session = key(index as u64 + 1);
        let request = ManagedRequest::Delete {
            session,
            revision: scope.revision,
        };
        assert_eq!(
            restored.manage_at(request.clone(), Instant::now()),
            Ok(ManagedOutcome::Deleted {
                connections: vec![]
            })
        );
        restored.manage_at(request, Instant::now()).unwrap();
        assert_eq!(
            restored.manage_at(put(session, 1000), Instant::now()),
            Err(ManagedError::ScopeRetired)
        );
        let connection = restored.transport_connected().unwrap();
        assert!(
            restored
                .authenticate(connection, &credentials(index as u64 + 1))
                .is_err()
        );
        restored.transport_lost(connection).unwrap();
        let fresh = SessionKey::new(NamespaceId::new(101), SessionId::new(101));
        assert_eq!(
            restored.manage_at(put(fresh, index as u64 + 1), Instant::now()),
            Err(ManagedError::TokenConflict)
        );
    }
    let checkpoint = restored.export_managed_checkpoint().unwrap();
    assert_eq!(checkpoint.migration_baseline, approved.migration_baseline);
    assert_eq!(checkpoint.scopes.len(), 12);
    assert!(checkpoint.scopes.iter().all(|scope| scope.retired
        && scope.last_mutation == ManagedCheckpointMutation::MigrationBaseline));
    let mut restarted = core();
    restarted
        .restore_managed_checkpoint(checkpoint.clone())
        .unwrap();
    assert_eq!(restarted.session_count(), 0);
    assert_eq!(restarted.export_managed_checkpoint().unwrap(), checkpoint);
    assert_eq!(
        restarted.manage_at(put(key(1), 1000), Instant::now()),
        Err(ManagedError::ScopeRetired)
    );
}

#[test]
fn empty_baseline_retains_the_fence_without_claiming_a_lost_verifier_archive() {
    let approved = inventory(0);
    let mut restored = core();
    restored
        .restore_managed_checkpoint(approved.clone())
        .unwrap();
    assert!(
        restored
            .export_managed_checkpoint()
            .unwrap()
            .scopes
            .is_empty()
    );
    assert!(
        restored
            .export_managed_checkpoint()
            .unwrap()
            .migration_baseline
            .unwrap()
            .unknown_retired_verifier_history
    );
    assert_eq!(
        restored.restore_managed_checkpoint(core().export_managed_checkpoint().unwrap()),
        Err(ManagedError::InvalidRequest)
    );
    assert_eq!(
        restored.manage_at(put(key(1), 2000), Instant::now()),
        Err(ManagedError::ScopeRetired)
    );
    let fresh = SessionKey::new(NamespaceId::new(101), SessionId::new(101));
    restored
        .manage_at(put(fresh, 2000), Instant::now())
        .unwrap();
    let checkpoint = restored.export_managed_checkpoint().unwrap();
    assert_eq!(checkpoint.scopes.len(), 1);
    assert_eq!(checkpoint.migration_baseline, approved.migration_baseline);
}

#[test]
fn malformed_or_unapproved_baselines_reject_atomically() {
    type Corruption = fn(&mut ManagedCheckpoint);
    let corruptions: &[Corruption] = &[
        |c| c.migration_baseline = None,
        |c| c.migration_baseline.as_mut().unwrap().version = 0,
        |c| c.migration_baseline.as_mut().unwrap().version = 2,
        |c| c.migration_baseline.as_mut().unwrap().migration_id.clear(),
        |c| {
            c.migration_baseline.as_mut().unwrap().migration_id =
                "x".repeat(MAX_MANAGED_MIGRATION_ID_BYTES + 1);
        },
        |c| c.migration_baseline.as_mut().unwrap().migration_id = "not an identity".to_owned(),
        |c| c.migration_baseline.as_mut().unwrap().migration_id = "control\ncharacter".to_owned(),
        |c| c.migration_baseline.as_mut().unwrap().previous_lineage = "e".repeat(47),
        |c| c.migration_baseline.as_mut().unwrap().previous_lineage = "e".repeat(49),
        |c| c.migration_baseline.as_mut().unwrap().previous_lineage = "g".repeat(48),
        |c| c.migration_baseline.as_mut().unwrap().previous_lineage = "E".repeat(48),
        |c| c.migration_baseline.as_mut().unwrap().closed_scope_through = 0,
        |c| {
            c.migration_baseline
                .as_mut()
                .unwrap()
                .unknown_retired_verifier_history = false;
        },
        |c| c.scopes[0].namespace_id = HIGH_WATER + 1,
        |c| c.scopes[0].session_id = HIGH_WATER + 1,
        |c| {
            c.scopes[0].namespace_id = HIGH_WATER + 1;
            c.scopes[0].session_id = HIGH_WATER + 1;
        },
        |c| c.scopes.push(c.scopes[0].clone()),
        |c| {
            let mut duplicate = c.scopes[0].clone();
            duplicate.namespace_id = 30;
            duplicate.session_id = 60;
            c.scopes.push(duplicate);
        },
    ];
    let approved = inventory(1);
    for (index, corrupt) in corruptions.iter().enumerate() {
        let mut bad = approved.clone();
        corrupt(&mut bad);
        let mut target = core();
        assert!(
            target.restore_managed_checkpoint(bad).is_err(),
            "corruption {index}"
        );
        assert_eq!(target.session_count(), 0);
        let after = target.export_managed_checkpoint().unwrap();
        assert!(after.scopes.is_empty());
        assert!(after.migration_baseline.is_none());
        target.restore_managed_checkpoint(approved.clone()).unwrap();
    }
}

#[test]
fn metadata_is_bounded_explicit_redacted_and_absent_from_legacy_v1_encoding() {
    let approved = inventory(1);
    let encoded = serde_json::to_value(&approved).unwrap();
    assert_eq!(
        encoded["migrationBaseline"]["unknownRetiredVerifierHistory"],
        true
    );
    assert_eq!(
        encoded["scopes"][0]["lastMutation"]["kind"],
        "migrationBaseline"
    );
    let mut missing_flag = encoded.clone();
    missing_flag["migrationBaseline"]
        .as_object_mut()
        .unwrap()
        .remove("unknownRetiredVerifierHistory");
    assert!(serde_json::from_value::<ManagedCheckpoint>(missing_flag).is_err());
    for (field, value) in [
        ("migrationId", "x".repeat(129)),
        ("migrationId", "bad\nidentity".to_owned()),
        ("previousLineage", "e".repeat(49)),
        ("previousLineage", "G".repeat(48)),
    ] {
        let mut bad = encoded.clone();
        bad["migrationBaseline"][field] = value.into();
        assert!(serde_json::from_value::<ManagedCheckpoint>(bad).is_err());
    }
    let baseline = approved.migration_baseline.as_ref().unwrap();
    let debug = format!("{approved:?} {baseline:?}");
    assert!(!debug.contains(&baseline.migration_id));
    assert!(!debug.contains(&baseline.previous_lineage));
    assert!(!debug.contains(&approved.scopes[0].verifier_sha256));
    let mut old_core = core();
    old_core.manage_at(put(key(1), 1), Instant::now()).unwrap();
    let old = old_core.export_managed_checkpoint().unwrap();
    let encoded = serde_json::to_value(&old).unwrap();
    assert!(encoded.get("migrationBaseline").is_none());
    let decoded: ManagedCheckpoint = serde_json::from_value(encoded).unwrap();
    assert!(decoded.migration_baseline.is_none());
    let mut restored = core();
    restored.restore_managed_checkpoint(decoded).unwrap();
    restored.manage_at(put(key(1), 1), Instant::now()).unwrap();
}

#[test]
fn baseline_does_not_invent_pre_migration_mutation_counts_and_next_edit_roundtrips() {
    for spatial_edit in [false, true] {
        let mut approved = inventory(1);
        approved.scopes[0].revision = 1;
        let mut restored = core();
        restored
            .restore_managed_checkpoint(approved.clone())
            .unwrap();
        let request = if spatial_edit {
            ManagedRequest::PutSpace {
                session: key(1),
                revision: 2,
                descriptor: descriptor(18),
            }
        } else {
            patch(&approved.scopes[0], 2)
        };
        restored.manage_at(request.clone(), Instant::now()).unwrap();
        let checkpoint = restored.export_managed_checkpoint().unwrap();
        let mut restarted = core();
        restarted
            .restore_managed_checkpoint(checkpoint.clone())
            .unwrap();
        restarted.manage_at(request, Instant::now()).unwrap();
        assert_eq!(restarted.export_managed_checkpoint().unwrap(), checkpoint);
    }
}

#[test]
fn maximum_fence_and_revision_do_not_wrap_or_reopen_old_allocations() {
    let mut approved = inventory(1);
    approved
        .migration_baseline
        .as_mut()
        .unwrap()
        .closed_scope_through = u64::MAX;
    approved.scopes[0].revision = u64::MAX;
    let mut restored = core();
    restored
        .restore_managed_checkpoint(approved.clone())
        .unwrap();
    assert_eq!(
        restored.manage_at(patch(&approved.scopes[0], u64::MAX), Instant::now()),
        Err(ManagedError::RevisionConflict)
    );
    let absent = SessionKey::new(NamespaceId::new(u64::MAX), SessionId::new(u64::MAX));
    assert_eq!(
        restored.manage_at(put(absent, 2000), Instant::now()),
        Err(ManagedError::ScopeRetired)
    );
    restored
        .manage_at(
            ManagedRequest::Delete {
                session: key(1),
                revision: u64::MAX,
            },
            Instant::now(),
        )
        .unwrap();
    assert_eq!(restored.session_count(), 0);
}
