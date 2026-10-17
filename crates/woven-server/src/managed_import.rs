//! Offline privileged initialization/import; never starts a worker or binds a listener.
use super::{
    ManagedServerConfig, ServerError,
    durable::{MAX_STATE_BYTES, StateImport, Store},
    managed_core, read_bounded, state_error, validate_token,
};
use serde::Deserialize;
use serde_json::{Value, json};
use woven_core::{ManagedCheckpoint, ManagedMigrationBaseline};

/// Maximum encoded import manifest and on-disk envelope size (128 MiB).
/// Covers every bounded compact v1 checkpoint, including lineage and checksum overhead.
pub const MAX_MANAGED_STATE_BYTES: usize = MAX_STATE_BYTES;
/// Explicit acknowledgement of a new lineage and unrecoverable retired-verifier history.
pub const MANAGED_BASELINE_IMPORT_APPROVAL: &str =
    "approve-new-lineage-with-unknown-retired-verifier-history-v1";

/// Safe import receipt. The checkpoint and its verifiers are deliberately never returned.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedStateImportResult {
    pub node_incarnation: String,
    /// Only baseline import adds provenance; the exact-import receipt remains unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_state: Option<Value>,
}

struct BaselineApproval;
impl<'de> Deserialize<'de> for BaselineApproval {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ApprovalVisitor;
        impl serde::de::Visitor<'_> for ApprovalVisitor {
            type Value = BaselineApproval;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("the exact baseline approval string")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value == MANAGED_BASELINE_IMPORT_APPROVAL {
                    Ok(BaselineApproval)
                } else {
                    Err(E::custom("the exact baseline approval string is required"))
                }
            }
        }
        // A derived unit enum also accepts an externally tagged object, not just a string.
        deserializer.deserialize_str(ApprovalVisitor)
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BaselineImport {
    version: u32,
    node_incarnation: String,
    checkpoint: ManagedCheckpoint,
    #[serde(rename = "approval")]
    _approval: BaselineApproval,
}

#[derive(Clone, Copy)]
enum ImportMode {
    Exact,
    Baseline,
}

/// Exact offline import into an existing empty private directory. Rejects migration-baseline
/// metadata; a missing history archive must not masquerade as exact restoration. The current
/// configured admin credential validates verifier separation. All work is blocking-thread local.
pub async fn import_managed_state(
    config: ManagedServerConfig,
    manifest: Vec<u8>,
) -> Result<ManagedStateImportResult, ServerError> {
    import(config, manifest, ImportMode::Exact).await
}

/// Explicit offline lost-history baseline import. Requires the approval string, baseline
/// metadata and a new serving lineage different from `previousLineage`. This does not prove
/// that the external inventory/high-water mark is complete or approve Host adoption.
/// Never replaces an existing/uncertain store, starts a worker, or binds a listener.
pub async fn import_managed_state_baseline(
    config: ManagedServerConfig,
    manifest: Vec<u8>,
) -> Result<ManagedStateImportResult, ServerError> {
    import(config, manifest, ImportMode::Baseline).await
}

async fn import(
    config: ManagedServerConfig,
    manifest: Vec<u8>,
    mode: ImportMode,
) -> Result<ManagedStateImportResult, ServerError> {
    config.validate()?;
    if manifest.len() > MAX_STATE_BYTES {
        return Err(state_error());
    }
    let path = config.state_directory.ok_or_else(state_error)?;
    tokio::task::spawn_blocking(move || {
        let payload: StateImport = match mode {
            ImportMode::Exact => {
                let payload: StateImport =
                    serde_json::from_slice(&manifest).map_err(|_| state_error())?;
                if payload.checkpoint.migration_baseline.is_some() {
                    return Err(state_error());
                }
                payload
            }
            ImportMode::Baseline => {
                let payload: BaselineImport =
                    serde_json::from_slice(&manifest).map_err(|_| state_error())?;
                if payload.checkpoint.migration_baseline.is_none() {
                    return Err(state_error());
                }
                StateImport {
                    version: payload.version,
                    node_incarnation: payload.node_incarnation,
                    checkpoint: payload.checkpoint,
                }
            }
        };
        payload.validate().map_err(|_| state_error())?;
        let bytes = read_bounded(&config.admin_token_file, 4098, true)?;
        let admin = validate_token(&bytes)?;
        let mut core = managed_core(admin)?;
        core.restore_managed_checkpoint(payload.checkpoint.clone())
            .map_err(|_| state_error())?;
        // Project only after the core has validated the complete checkpoint and provenance.
        let receipt = ManagedStateImportResult {
            node_incarnation: payload.node_incarnation.clone(),
            managed_state: payload
                .checkpoint
                .migration_baseline
                .as_ref()
                .map(|baseline| durable_status(Some(baseline))),
        };
        let store = Store::empty(&path).map_err(|_| state_error())?;
        store.import(payload).map_err(|_| state_error())?;
        Ok(receipt)
    })
    .await
    .map_err(|_| state_error())?
}

/// Safe immutable projection, called only after successful core checkpoint validation.
pub(super) fn durable_status(baseline: Option<&ManagedMigrationBaseline>) -> Value {
    let mut status = json!({"mode": "durable"});
    if let Some(baseline) = baseline {
        status["migrationBaseline"] = json!({
            "version": baseline.version,
            "migrationId": baseline.migration_id,
            "previousLineage": baseline.previous_lineage,
            "closedScopeThrough": baseline.closed_scope_through.to_string(),
            "unknownRetiredVerifierHistory": baseline.unknown_retired_verifier_history,
        });
    }
    status
}
