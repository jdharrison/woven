//! Privileged baseline import is distinct from exact restoration and never approves live adoption.
use super::*;
use woven_core::ManagedMigrationBaseline;
use woven_server::{MANAGED_BASELINE_IMPORT_APPROVAL, import_managed_state_baseline};

const HIGH_WATER: u64 = 100;
fn products() -> impl Iterator<Item = u64> {
    [4, 6].into_iter().chain(11..=20)
}
fn token(id: u64) -> String {
    format!("{id:064x}")
}
fn metadata() -> ManagedMigrationBaseline {
    ManagedMigrationBaseline {
        version: 1,
        migration_id: "local-approved-baseline-12-products-v1".into(),
        previous_lineage: "e".repeat(48),
        closed_scope_through: HIGH_WATER,
        unknown_retired_verifier_history: true,
    }
}
fn inventory() -> ManagedCheckpoint {
    let spatial = checkpoint().scopes[0].spatial_spaces.clone();
    ManagedCheckpoint {
        version: 1,
        migration_baseline: Some(metadata()),
        scopes: products()
            .map(|id| ManagedCheckpointScope {
                namespace_id: id,
                session_id: id,
                revision: if id == 4 || id == 6 { 5 } else { id + 3 },
                last_mutation: ManagedCheckpointMutation::MigrationBaseline,
                allocated_ccu: 4,
                tick_rate_hz: Some(60),
                verifier_sha256: digest(&token(id)),
                retired: false,
                spatial_spaces: spatial.clone(),
            })
            .collect(),
    }
}
fn manifest() -> Value {
    json!({"version":1,"nodeIncarnation":LINEAGE,"checkpoint":inventory(),"approval":MANAGED_BASELINE_IMPORT_APPROVAL})
}
fn status() -> Value {
    json!({"mode":"durable","migrationBaseline":{
        "version":1,"migrationId":metadata().migration_id,"previousLineage":metadata().previous_lineage,
        "closedScopeThrough":HIGH_WATER.to_string(),"unknownRetiredVerifierHistory":true,
    }})
}
async fn import(fixture: &Fixture) {
    let receipt =
        import_managed_state_baseline(fixture.config(), manifest().to_string().into_bytes())
            .await
            .unwrap();
    assert_eq!(receipt.node_incarnation, LINEAGE);
    assert_eq!(receipt.managed_state, Some(status()));
}
async fn cli(
    fixture: &Fixture,
    config: &ManagedServerConfig,
    flag: &str,
    manifest: &Value,
) -> std::process::Output {
    let mut command = tokio::process::Command::from(fixture.command(config));
    command
        .arg(flag)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(manifest.to_string().as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn baseline_approval_requires_exact_json_string_in_library_and_cli() {
    let fixture = Fixture::new();
    let config = fixed_config(&fixture);
    let approved = manifest();
    for approval in [
        json!({(MANAGED_BASELINE_IMPORT_APPROVAL): null}),
        json!({}),
        json!(null),
        json!(true),
        json!(false),
        json!([]),
        json!([MANAGED_BASELINE_IMPORT_APPROVAL]),
        json!(1),
        json!(1.5),
        json!(""),
        json!(" "),
        json!(format!(" {MANAGED_BASELINE_IMPORT_APPROVAL}")),
        json!(format!("{MANAGED_BASELINE_IMPORT_APPROVAL} ")),
        json!(format!("{MANAGED_BASELINE_IMPORT_APPROVAL}\n")),
        json!("not-approved"),
    ] {
        let mut invalid = approved.clone();
        invalid["approval"] = approval;
        assert!(
            import_managed_state_baseline(config.clone(), invalid.to_string().into_bytes())
                .await
                .is_err(),
            "library must reject non-literal approval"
        );
        assert!(!fixture.root.join("state/checkpoint.json").exists());
        assert!(!fixture.root.join("state/checkpoint.pending").exists());
        assert_unbound(&config);
        let result = cli(
            &fixture,
            &config,
            "--managed-state-import-baseline",
            &invalid,
        )
        .await;
        assert!(
            !result.status.success(),
            "CLI must reject non-literal approval"
        );
        assert!(
            result.stdout.is_empty(),
            "rejected import must not emit a receipt"
        );
        assert!(!fixture.root.join("state/checkpoint.json").exists());
        assert!(!fixture.root.join("state/checkpoint.pending").exists());
        assert_unbound(&config);
    }
    let result = cli(
        &fixture,
        &config,
        "--managed-state-import-baseline",
        &approved,
    )
    .await;
    assert!(result.status.success());
    assert!(result.stderr.is_empty());
    let receipt: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        receipt,
        json!({"nodeIncarnation":LINEAGE,"managedState":status()})
    );
    assert_unbound(&config);
}

#[tokio::test]
async fn offline_baseline_requires_its_own_entrypoint_metadata_new_lineage_and_explicit_approval() {
    let fixture = Fixture::new();
    let config = fixed_config(&fixture);
    let approved = manifest();
    let mut no_approval = approved.clone();
    no_approval.as_object_mut().unwrap().remove("approval");
    let mut wrong_approval = approved.clone();
    wrong_approval["approval"] = json!("not-approved");
    let mut old_lineage = approved.clone();
    old_lineage["nodeIncarnation"] = json!(metadata().previous_lineage);
    let mut no_metadata = approved.clone();
    no_metadata["checkpoint"]
        .as_object_mut()
        .unwrap()
        .remove("migrationBaseline");
    let mut invalid_metadata = approved.clone();
    invalid_metadata["checkpoint"]["migrationBaseline"]["unknownRetiredVerifierHistory"] =
        json!(false);
    let mut invalid_core = approved.clone();
    invalid_core["checkpoint"]["scopes"][0]["verifierSha256"] = json!(digest(ADMIN));
    for invalid in [
        &no_approval,
        &wrong_approval,
        &old_lineage,
        &no_metadata,
        &invalid_metadata,
        &invalid_core,
    ] {
        assert!(
            import_managed_state_baseline(config.clone(), invalid.to_string().into_bytes())
                .await
                .is_err()
        );
        assert!(!fixture.root.join("state/checkpoint.json").exists());
        assert_unbound(&config);
    }
    for invalid in [&approved, &no_approval] {
        assert!(
            import_managed_state(config.clone(), invalid.to_string().into_bytes())
                .await
                .is_err()
        );
        let result = cli(&fixture, &config, "--managed-state-import", invalid).await;
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert_unbound(&config);
    }
    let result = cli(
        &fixture,
        &config,
        "--managed-state-import-baseline",
        &no_approval,
    )
    .await;
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert_unbound(&config);
    let result = cli(
        &fixture,
        &config,
        "--managed-state-import-baseline",
        &approved,
    )
    .await;
    assert!(result.status.success());
    assert!(result.stderr.is_empty());
    assert_unbound(&config);
    let receipt: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        receipt,
        json!({"nodeIncarnation":LINEAGE,"managedState":status()})
    );
    for entry in &inventory().scopes {
        assert!(!receipt.to_string().contains(&entry.verifier_sha256));
    }
}

async fn verify_products(fixture: &Fixture, config: &ManagedServerConfig, edited_four: bool) {
    for expected in inventory().scopes {
        let id = expected.session_id;
        let (code, snapshot) = http(config.admin_bind_address, "GET", &path(id), true, "")
            .await
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(
            snapshot["revision"],
            if edited_four && id == 4 {
                "6".into()
            } else {
                expected.revision.to_string()
            }
        );
        assert_eq!(snapshot["tickRateHz"], 60);
        assert_eq!(snapshot["spaces"][2]["spaceId"], "3");
        assert_eq!(
            snapshot["allocatedCCU"],
            if edited_four && id == 4 { 3 } else { 4 }
        );
        let mut client = fixture.client(config.quic_bind_address, &token(id)).await;
        assert_eq!(
            client
                .request_admission(id, id, 1, format!("baseline-{id}"))
                .await
                .unwrap()
                .status,
            woven_protocol::AdmissionStatus::Admitted
        );
        client.subscribe_space(id, id, 3, 1, 2).await.unwrap();
        assert_eq!(
            client.recv().await.unwrap().message_kind(),
            woven_protocol::MessageKind::SubscriptionAccepted
        );
        assert_eq!(
            client.recv().await.unwrap().message_kind(),
            woven_protocol::MessageKind::EntityEntered
        );
        client
            .close_gracefully(Duration::from_secs(2))
            .await
            .unwrap();
    }
}
fn space_body(revision: u64) -> String {
    json!({"revision":revision.to_string(),"metersPerUnit":0.5,"cellSize":f64::from_bits(10.0_f64.to_bits()+1),"interestRadius":20.0,"exactDistance":true,
        "bounds":{"min":{"x":-100.0,"y":-100.0,"z":-100.0},"max":{"x":100.0,"y":100.0,"z":100.0}}}).to_string()
}
async fn verify_replay_and_fence(address: SocketAddr) {
    for id in [4, 6] {
        for (method, target, body) in [
            (
                "PUT",
                path(id),
                json!({"revision":"5","allocatedCCU":4,"tickRateHz":60,"clientToken":token(id)})
                    .to_string(),
            ),
            (
                "PATCH",
                path(id),
                json!({"revision":"5","allocatedCCU":4,"tickRateHz":60}).to_string(),
            ),
            ("PUT", format!("{}/spaces/3", path(id)), space_body(5)),
        ] {
            let (code, error) = http(address, method, &target, true, &body).await.unwrap();
            assert_eq!(code, 409);
            assert_eq!(error["error"]["code"], "revision_conflict");
        }
    }
    for (namespace, session) in [(1, 1), (1, 101), (101, 100)] {
        let (code, error) = http(
            address,
            "PUT",
            &format!("/v1/namespaces/{namespace}/sessions/{session}"),
            true,
            &json!({"revision":"1","allocatedCCU":1,"clientToken":token(1000+namespace+session)})
                .to_string(),
        )
        .await
        .unwrap();
        assert_eq!(code, 409);
        assert_eq!(error["error"]["code"], "scope_retired");
    }
}
fn persisted(fixture: &Fixture) -> ManagedCheckpoint {
    let value: Value =
        serde_json::from_slice(&std::fs::read(fixture.root.join("state/checkpoint.json")).unwrap())
            .unwrap();
    serde_json::from_value(value["payload"]["checkpoint"].clone()).unwrap()
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one bounded twelve-product lifecycle spans import, real bearer traffic, edits and two SIGKILL restarts"
)]
async fn twelve_baseline_products_auto_restore_bearer_spatial_and_revisions_across_sigkill() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let fixture = Fixture::new();
        import(&fixture).await;
        let config = fixed_config(&fixture);
        let mut previous_boot = None;
        for round in 0..2 {
            let mut process = Process(
                fixture
                    .command(&config)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            let identity = node(config.admin_bind_address).await;
            assert_eq!(identity["nodeIncarnation"], LINEAGE);
            assert_eq!(identity["managedState"], status());
            if let Some(boot) = previous_boot {
                assert_ne!(identity["bootId"], boot);
            }
            previous_boot = Some(identity["bootId"].clone());
            assert_eq!(
                http(config.admin_bind_address, "GET", "/v1/node", false, "")
                    .await
                    .unwrap()
                    .0,
                401
            );
            let public = identity.to_string();
            for entry in &inventory().scopes {
                assert!(!public.contains(&entry.verifier_sha256));
            }
            verify_products(&fixture, &config, round > 0).await;
            verify_replay_and_fence(config.admin_bind_address).await;
            let id = if round == 0 { 4 } else { 6 };
            let body = json!({"revision":"6","allocatedCCU":3}).to_string();
            assert_eq!(
                http(config.admin_bind_address, "PATCH", &path(id), true, &body)
                    .await
                    .unwrap()
                    .0,
                200
            );
            assert_eq!(
                http(config.admin_bind_address, "PATCH", &path(id), true, &body)
                    .await
                    .unwrap()
                    .0,
                200
            );
            if round == 0 {
                assert_eq!(
                    http(
                        config.admin_bind_address,
                        "PUT",
                        &path(101),
                        true,
                        &json!({"revision":"1","allocatedCCU":1,"clientToken":token(101)})
                            .to_string()
                    )
                    .await
                    .unwrap()
                    .0,
                    201
                );
            }
            assert_eq!(
                node(config.admin_bind_address).await["managedState"],
                status()
            );
            let checkpoint = persisted(&fixture);
            assert_eq!(checkpoint.migration_baseline, Some(metadata()));
            assert_eq!(checkpoint.scopes.len(), 13);
            for entry in checkpoint
                .scopes
                .iter()
                .filter(|s| s.session_id <= HIGH_WATER)
            {
                assert_eq!(
                    entry.spatial_spaces[0].cell_size.to_bits(),
                    10.0_f64.to_bits() + 1
                );
            }
            process.0.kill().unwrap();
            process.0.wait().unwrap();
            drop(process);
        }
        let server = fixture.start().await;
        assert_eq!(node(server.admin_address).await["managedState"], status());
        assert_eq!(
            server.worker.live_counts().await.unwrap().sessions_active,
            13
        );
        for id in [4, 6] {
            assert_eq!(
                http(server.admin_address, "GET", &path(id), true, "")
                    .await
                    .unwrap()
                    .1["revision"],
                "6"
            );
        }
    })
    .await
    .expect("bounded baseline migration and two abrupt restarts");
}

#[tokio::test]
async fn imports_never_replace_exact_history_baselines_or_corrupt_stores() {
    let exact = Fixture::new();
    exact.import().await;
    let path = exact.root.join("state/checkpoint.json");
    let before = std::fs::read(&path).unwrap();
    assert!(
        import_managed_state_baseline(exact.config(), manifest().to_string().into_bytes())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let fixture = Fixture::new();
    import(&fixture).await;
    let path = fixture.root.join("state/checkpoint.json");
    let before = std::fs::read(&path).unwrap();
    let mut changed = manifest();
    changed["nodeIncarnation"] = json!("d".repeat(48));
    changed["checkpoint"]["migrationBaseline"]["migrationId"] = json!("another-import");
    assert!(
        import_managed_state_baseline(fixture.config(), changed.to_string().into_bytes())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    std::fs::write(&path, b"corrupt baseline store").unwrap();
    let config = fixed_config(&fixture);
    assert!(start_managed(config.clone()).await.is_err());
    assert_unbound(&config);
    assert!(
        import_managed_state_baseline(config, manifest().to_string().into_bytes())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"corrupt baseline store");
}

#[tokio::test]
async fn baseline_status_preserves_full_u64_fence_as_a_safe_decimal_string() {
    let fixture = Fixture::new();
    let mut manifest = manifest();
    manifest["checkpoint"]["migrationBaseline"]["closedScopeThrough"] = json!(u64::MAX);
    let receipt =
        import_managed_state_baseline(fixture.config(), manifest.to_string().into_bytes())
            .await
            .unwrap();
    let server = fixture.start().await;
    let node = node(server.admin_address).await;
    assert_eq!(receipt.managed_state, Some(node["managedState"].clone()));
    assert_eq!(
        node["managedState"]["migrationBaseline"]["closedScopeThrough"],
        u64::MAX.to_string()
    );
}
