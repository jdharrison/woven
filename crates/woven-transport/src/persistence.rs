use std::{future::Future, pin::Pin, time::Duration};
use woven_core::ManagedCheckpoint;

/// A stalled store is a failed store: never leave readiness serving indefinitely without ACKs.
pub const MANAGED_PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Deliberately opaque: store errors must not accidentally log privileged checkpoint contents.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedPersistenceError;

impl std::fmt::Display for ManagedPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("managed checkpoint persistence failed")
    }
}

impl std::error::Error for ManagedPersistenceError {}

/// Privileged persistence adapter implemented by the server, never by the core or protocol.
/// `Ok(())` MUST mean the complete checkpoint was atomically and durably replaced (including
/// directory durability where required). Errors must not leave a partially readable checkpoint.
/// Protect verifier-bearing bytes with restricted permissions; never log or expose them.
/// The owning worker calls this once at a time and awaits it before serving any further work.
/// Cancellation/timeout stops the worker; adapters must tolerate a dropped save future.
pub trait ManagedPersistence: Send + Sync + 'static {
    fn save(
        &self,
        checkpoint: ManagedCheckpoint,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagedPersistenceError>> + Send + '_>>;
}
