use super::*;
use std::{
    fs::OpenOptions,
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "woven-store-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
    fn committed(&self) {
        let store = Store::empty(&self.0).unwrap();
        store.import(state()).unwrap();
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn state() -> StateImport {
    StateImport {
        version: 1,
        node_incarnation: "a".repeat(48),
        checkpoint: ManagedCheckpoint {
            version: 1,
            migration_baseline: None,
            scopes: Vec::new(),
        },
    }
}
#[derive(Default)]
struct CountBytes(usize);
impl Write for CountBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("byte count overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encoded_len(value: &impl Serialize) -> usize {
    let mut count = CountBytes::default();
    serde_json::to_writer(&mut count, value).unwrap();
    count.0
}

#[test]
fn worst_case_bounded_checkpoint_and_checksum_envelope_fit_encoded_limit() {
    use woven_core::{
        MAX_MANAGED_HISTORY, MAX_MANAGED_MIGRATION_ID_BYTES, MAX_MANAGED_SPATIAL_SPACES,
        ManagedBounds3D, ManagedCheckpointMutation, ManagedCheckpointScope, ManagedCheckpointSpace,
        ManagedMigrationBaseline, ManagedPoint3D,
    };
    // This is a conservative structural bound, not a semantically restorable checkpoint:
    // even version/CCU/tick-rate fields use their full integer widths, float bits may be NaN,
    // IDs/verifiers can repeat, and false is longer than the retired tombstone flag true.
    let float = f64::from_bits(u64::MAX);
    let point = ManagedPoint3D {
        x: float,
        y: float,
        z: float,
    };
    let space = ManagedCheckpointSpace {
        space_id: u64::MAX,
        epoch: u64::MAX,
        meters_per_unit: float,
        cell_size: float,
        interest_radius: float,
        exact_distance: false,
        bounds: ManagedBounds3D {
            min: point,
            max: point,
        },
    };
    let mutation = [
        ManagedCheckpointMutation::MigrationBaseline,
        ManagedCheckpointMutation::Provision,
        ManagedCheckpointMutation::Configuration {
            allocated_ccu: u32::MAX,
            tick_rate_hz: Some(u32::MAX),
        },
        ManagedCheckpointMutation::Space { space_id: u64::MAX },
    ]
    .into_iter()
    .max_by_key(encoded_len)
    .unwrap();
    let scope = ManagedCheckpointScope {
        namespace_id: u64::MAX,
        session_id: u64::MAX,
        revision: u64::MAX,
        last_mutation: mutation,
        allocated_ccu: u32::MAX,
        tick_rate_hz: Some(u32::MAX),
        verifier_sha256: "f".repeat(64),
        retired: false,
        spatial_spaces: vec![space; MAX_MANAGED_SPATIAL_SPACES],
    };
    let payload = StateImport {
        version: u32::MAX,
        node_incarnation: "f".repeat(48),
        checkpoint: ManagedCheckpoint {
            version: u32::MAX,
            migration_baseline: Some(ManagedMigrationBaseline {
                version: u32::MAX,
                // Graphic ASCII permits quotes/backslashes; both need two JSON bytes each.
                migration_id: "\"".repeat(MAX_MANAGED_MIGRATION_ID_BYTES),
                previous_lineage: "e".repeat(48),
                closed_scope_through: u64::MAX,
                unknown_retired_verifier_history: false,
            }),
            scopes: vec![scope; MAX_MANAGED_HISTORY],
        },
    };
    // Count actual serde output without allocating either the encoded manifest or envelope.
    let manifest_bytes = encoded_len(&payload);
    let approved_manifest_bytes = manifest_bytes
        + ",\"approval\":".len()
        + encoded_len(&crate::MANAGED_BASELINE_IMPORT_APPROVAL);
    let envelope_bytes = encoded_len(&Envelope {
        version: u32::MAX,
        payload,
        sha256: "f".repeat(64),
    });
    assert!(
        manifest_bytes > 64 * 1024 * 1024,
        "fixture must exceed the old limit"
    );
    assert!(
        envelope_bytes > manifest_bytes + 64,
        "count checksum and envelope overhead"
    );
    assert!(
        manifest_bytes <= MAX_STATE_BYTES,
        "manifest requires {manifest_bytes} bytes"
    );
    assert!(
        approved_manifest_bytes <= MAX_STATE_BYTES,
        "approved baseline manifest requires {approved_manifest_bytes} bytes"
    );
    assert!(
        envelope_bytes <= MAX_STATE_BYTES,
        "envelope requires {envelope_bytes} bytes"
    );
    println!(
        "worst-case structural manifest: {manifest_bytes} bytes; approved baseline manifest: {approved_manifest_bytes} bytes; envelope: {envelope_bytes} bytes; limit: {MAX_STATE_BYTES} bytes"
    );
}

#[test]
fn exact_v1_without_baseline_keeps_legacy_json_and_checksum() {
    let payload = state();
    let legacy = format!(
        "{{\"version\":1,\"nodeIncarnation\":\"{}\",\"checkpoint\":{{\"version\":1,\"scopes\":[]}}}}",
        "a".repeat(48)
    );
    assert_eq!(encode(&payload).unwrap(), legacy.as_bytes());
    let directory = Directory::new();
    directory.committed();
    let (_, restored) = Store::load(&directory.0).unwrap();
    assert!(restored.checkpoint.migration_baseline.is_none());
    assert_eq!(encode(&restored).unwrap(), legacy.as_bytes());
}

#[test]
fn baseline_reused_previous_lineage_is_rejected_even_with_a_valid_checksum() {
    let directory = Directory::new();
    directory.committed();
    let mut payload = state();
    payload.checkpoint.migration_baseline = Some(woven_core::ManagedMigrationBaseline {
        version: 1,
        migration_id: "synthetic-reused-lineage".into(),
        previous_lineage: payload.node_incarnation.clone(),
        closed_scope_through: 100,
        unknown_retired_verifier_history: true,
    });
    assert!(payload.validate().is_err());
    let checksum = sha256_hex(&encode(&payload).unwrap());
    let bytes = encode(&Envelope {
        version: 1,
        payload,
        sha256: checksum,
    })
    .unwrap();
    std::fs::write(directory.0.join(CHECKPOINT), bytes).unwrap();
    assert!(Store::load(&directory.0).is_err());
}

#[test]
fn permissions_exclusive_lock_and_atomic_replacement() {
    let directory = Directory::new();
    let store = Store::empty(&directory.0).unwrap();
    assert!(Store::empty(&directory.0).is_err());
    store.import(state()).unwrap();
    let file = directory.0.join(CHECKPOINT);
    assert_eq!(std::fs::metadata(&file).unwrap().mode() & 0o7777, 0o600);
    assert_eq!(
        std::fs::metadata(directory.0.join(LOCK)).unwrap().mode() & 0o7777,
        0o600
    );
    let before = std::fs::metadata(&file).unwrap().ino();
    store.replace(state()).unwrap();
    assert_ne!(std::fs::metadata(&file).unwrap().ino(), before);
    assert!(!directory.0.join(PENDING).exists());
    drop(store);
    let (store, restored) = Store::load(&directory.0).unwrap();
    assert_eq!(restored.node_incarnation, "a".repeat(48));
    assert!(Store::load(&directory.0).is_err());
    drop(store);
    assert!(Store::load(&directory.0).is_ok());
}
#[test]
fn crash_leftover_never_replaces_verified_checkpoint_or_initializes_missing_state() {
    let directory = Directory::new();
    directory.committed();
    let store = Store::acquire(&directory.0).unwrap();
    let mut temp = pending(&store.directory).unwrap();
    temp.write_all(b"uncommitted partial bytes").unwrap();
    temp.sync_all().unwrap();
    drop(temp);
    drop(store);
    let (store, restored) = Store::load(&directory.0).unwrap();
    assert_eq!(restored.checkpoint, state().checkpoint);
    assert!(!directory.0.join(PENDING).exists());
    drop(store);
    std::fs::rename(directory.0.join(CHECKPOINT), directory.0.join(PENDING)).unwrap();
    assert!(Store::load(&directory.0).is_err());
    assert!(Store::empty(&directory.0).is_err());
}
#[test]
fn schema_checksum_versions_and_size_are_bounded_and_fail_closed() {
    let directory = Directory::new();
    directory.committed();
    let path = directory.0.join(CHECKPOINT);
    let original = std::fs::read(&path).unwrap();
    let mut wrong_version: Envelope = serde_json::from_slice(&original).unwrap();
    wrong_version.version = 2;
    std::fs::write(&path, encode(&wrong_version).unwrap()).unwrap();
    assert!(Store::load(&directory.0).is_err());
    let mut wrong_checksum: Envelope = serde_json::from_slice(&original).unwrap();
    wrong_checksum.payload.node_incarnation = "b".repeat(48);
    std::fs::write(&path, encode(&wrong_checksum).unwrap()).unwrap();
    assert!(Store::load(&directory.0).is_err());
    let mut unknown: serde_json::Value = serde_json::from_slice(&original).unwrap();
    unknown["unexpected"] = serde_json::json!(true);
    std::fs::write(&path, serde_json::to_vec(&unknown).unwrap()).unwrap();
    assert!(Store::load(&directory.0).is_err());
    // Sparse file exercises the bound without allocating a giant manifest in the test.
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(MAX_STATE_BYTES as u64 + 1).unwrap();
    drop(file);
    assert!(Store::load(&directory.0).is_err());
    // Exercise the same writer boundary without allocating the production byte ceiling.
    let mut bytes = BoundedBytes::<8>(Vec::new());
    bytes.write_all(&[0; 7]).unwrap();
    assert!(bytes.write_all(&[0; 2]).is_err());
    assert_eq!(bytes.0.len(), 7);
    bytes.write_all(&[0]).unwrap();
    assert!(bytes.write_all(&[0]).is_err());
    assert_eq!(bytes.0.len(), 8);
}
#[test]
fn symlinks_hardlinks_permissive_directories_and_special_files_are_rejected() {
    let directory = Directory::new();
    directory.committed();
    let alias = Directory::new();
    std::os::unix::fs::symlink(&directory.0, alias.0.join("link")).unwrap();
    assert!(Store::load(&alias.0.join("link")).is_err());
    std::fs::set_permissions(&directory.0, std::fs::Permissions::from_mode(0o750)).unwrap();
    assert!(Store::load(&directory.0).is_err());
    std::fs::set_permissions(&directory.0, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::hard_link(directory.0.join(CHECKPOINT), alias.0.join("copy")).unwrap();
    assert!(Store::load(&directory.0).is_err());
    std::fs::remove_file(alias.0.join("copy")).unwrap();
    std::fs::remove_file(directory.0.join(LOCK)).unwrap();
    std::os::unix::fs::symlink(alias.0.join("target"), directory.0.join(LOCK)).unwrap();
    assert!(Store::load(&directory.0).is_err());
}
#[test]
fn interrupted_replace_preserves_old_checkpoint_and_refuses_retry_into_uncertain_store() {
    let directory = Directory::new();
    directory.committed();
    let (store, _) = Store::load(&directory.0).unwrap();
    let before = std::fs::read(directory.0.join(CHECKPOINT)).unwrap();
    std::fs::create_dir(directory.0.join(PENDING)).unwrap();
    assert!(store.replace(state()).is_err());
    assert_eq!(std::fs::read(directory.0.join(CHECKPOINT)).unwrap(), before);
    drop(store);
    assert!(Store::load(&directory.0).is_err());
    assert!(Store::empty(&directory.0).is_err());
}
