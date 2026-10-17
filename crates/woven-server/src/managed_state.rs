//! Private, bounded Linux VM store. No verifier-bearing value implements unredacted Debug.
use super::{new_node_incarnation, sha256_hex};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    future::Future,
    io::{Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
use woven_core::ManagedCheckpoint;
use woven_transport::{ManagedPersistence, ManagedPersistenceError};

#[cfg(all(test, target_os = "linux"))]
#[path = "managed_state_tests.rs"]
mod tests;

type Result<T> = std::result::Result<T, ManagedPersistenceError>;
// Covers the maximum bounded v1 DTO plus lineage/checksum envelope; the structural
// worst-case serialization test must remain below this explicit encoded-size ceiling.
pub(super) const MAX_STATE_BYTES: usize = 128 * 1024 * 1024;
const CHECKPOINT: &str = "checkpoint.json";
const PENDING: &str = "checkpoint.pending";
const LOCK: &str = "managed.lock";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StateImport {
    pub version: u32,
    pub node_incarnation: String,
    pub checkpoint: ManagedCheckpoint,
}

impl StateImport {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.node_incarnation.len() != 48
            || !self
                .node_incarnation
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ManagedPersistenceError);
        }
        if self
            .checkpoint
            .migration_baseline
            .as_ref()
            .is_some_and(|baseline| baseline.previous_lineage == self.node_incarnation)
        {
            return Err(ManagedPersistenceError);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    payload: StateImport,
    sha256: String,
}

struct BoundedBytes<const LIMIT: usize>(Vec<u8>);
impl<const LIMIT: usize> Write for BoundedBytes<LIMIT> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > LIMIT.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("managed state size limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut bytes = BoundedBytes::<MAX_STATE_BYTES>(Vec::new());
    serde_json::to_writer(&mut bytes, value).map_err(|_| ManagedPersistenceError)?;
    Ok(bytes.0)
}

pub(super) struct Store {
    directory: File,
    // Retained through blocking writes, even if their async caller times out or is cancelled.
    _lock: File,
    lineage: String,
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{File, LOCK, ManagedPersistenceError, PENDING, Path, PathBuf, Result};
    use std::{
        fs::OpenOptions,
        os::{
            fd::AsRawFd,
            unix::fs::{MetadataExt, OpenOptionsExt},
        },
        path::Component,
    };

    pub fn child(directory: &File, name: &str) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(name)
    }
    fn options(directory: bool) -> OpenOptions {
        let mut options = OpenOptions::new();
        options.read(true).custom_flags(
            libc::O_NOFOLLOW | libc::O_NONBLOCK | if directory { libc::O_DIRECTORY } else { 0 },
        );
        options
    }
    pub fn directory(path: &Path) -> Result<File> {
        if !path.is_absolute() {
            return Err(ManagedPersistenceError);
        }
        let mut current = options(true)
            .open("/")
            .map_err(|_| ManagedPersistenceError)?;
        for component in path.components() {
            match component {
                Component::RootDir => {}
                Component::Normal(name) => {
                    current = options(true)
                        .open(child(
                            &current,
                            name.to_str().ok_or(ManagedPersistenceError)?,
                        ))
                        .map_err(|_| ManagedPersistenceError)?;
                }
                _ => return Err(ManagedPersistenceError),
            }
        }
        secure(&current, true)?;
        Ok(current)
    }
    pub fn secure(file: &File, directory: bool) -> Result<()> {
        let metadata = file.metadata().map_err(|_| ManagedPersistenceError)?;
        let owner = std::fs::metadata("/proc/self")
            .map_err(|_| ManagedPersistenceError)?
            .uid();
        if metadata.uid() != owner
            || metadata.mode() & 0o7777 != if directory { 0o700 } else { 0o600 }
            || if directory {
                !metadata.is_dir()
            } else {
                !metadata.is_file() || metadata.nlink() != 1
            }
        {
            return Err(ManagedPersistenceError);
        }
        Ok(())
    }
    pub fn open(directory: &File, name: &str) -> Result<File> {
        let file = options(false)
            .open(child(directory, name))
            .map_err(|_| ManagedPersistenceError)?;
        secure(&file, false)?;
        Ok(file)
    }
    pub fn lock(directory: &File) -> Result<File> {
        let file = options(false)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(child(directory, LOCK))
            .map_err(|_| ManagedPersistenceError)?;
        secure(&file, false)?;
        file.try_lock().map_err(|_| ManagedPersistenceError)?;
        directory.sync_all().map_err(|_| ManagedPersistenceError)?;
        Ok(file)
    }
    pub fn pending(directory: &File) -> Result<File> {
        let file = options(false)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(child(directory, PENDING))
            .map_err(|_| ManagedPersistenceError)?;
        secure(&file, false)?;
        Ok(file)
    }
}
#[cfg(target_os = "linux")]
use platform::{child, directory, lock, open, pending, secure};

// This store deliberately requires Linux local filesystem semantics; no weaker fallback.
#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{File, ManagedPersistenceError, Path, PathBuf, Result};
    pub fn child(_: &File, _: &str) -> PathBuf {
        PathBuf::new()
    }
    pub fn directory(_: &Path) -> Result<File> {
        Err(ManagedPersistenceError)
    }
    pub fn secure(_: &File, _: bool) -> Result<()> {
        Err(ManagedPersistenceError)
    }
    pub fn open(_: &File, _: &str) -> Result<File> {
        Err(ManagedPersistenceError)
    }
    pub fn lock(_: &File) -> Result<File> {
        Err(ManagedPersistenceError)
    }
    pub fn pending(_: &File) -> Result<File> {
        Err(ManagedPersistenceError)
    }
}
#[cfg(not(target_os = "linux"))]
use platform::{child, directory, lock, open, pending, secure};

impl Store {
    fn acquire(path: &Path) -> Result<Self> {
        let directory = directory(path)?;
        let lock = lock(&directory)?;
        Ok(Self {
            directory,
            _lock: lock,
            lineage: String::new(),
        })
    }
    pub fn load(path: &Path) -> Result<(Arc<Self>, StateImport)> {
        let mut store = Self::acquire(path)?;
        let file = open(&store.directory, CHECKPOINT)?;
        if file.metadata().map_err(|_| ManagedPersistenceError)?.len() > MAX_STATE_BYTES as u64 {
            return Err(ManagedPersistenceError);
        }
        let mut bytes = Vec::new();
        file.take(MAX_STATE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ManagedPersistenceError)?;
        if bytes.len() > MAX_STATE_BYTES {
            return Err(ManagedPersistenceError);
        }
        let envelope: Envelope =
            serde_json::from_slice(&bytes).map_err(|_| ManagedPersistenceError)?;
        envelope.payload.validate()?;
        if envelope.version != 1 || envelope.sha256 != sha256_hex(&encode(&envelope.payload)?) {
            return Err(ManagedPersistenceError);
        }
        store.lineage.clone_from(&envelope.payload.node_incarnation);
        // A crash before rename can leave an uncommitted temporary file. Only a verified main
        // checkpoint is authoritative; never promote the temporary file or initialize empty.
        match std::fs::symlink_metadata(child(&store.directory, PENDING)) {
            Ok(_) => {
                open(&store.directory, PENDING)?;
                std::fs::remove_file(child(&store.directory, PENDING))
                    .map_err(|_| ManagedPersistenceError)?;
                store
                    .directory
                    .sync_all()
                    .map_err(|_| ManagedPersistenceError)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(ManagedPersistenceError),
        }
        Ok((Arc::new(store), envelope.payload))
    }
    pub fn empty(path: &Path) -> Result<Arc<Self>> {
        let store = Self::acquire(path)?;
        let entries =
            std::fs::read_dir(child(&store.directory, ".")).map_err(|_| ManagedPersistenceError)?;
        for entry in entries {
            if entry.map_err(|_| ManagedPersistenceError)?.file_name() != LOCK {
                return Err(ManagedPersistenceError);
            }
        }
        Ok(Arc::new(store))
    }
    pub fn import(&self, payload: StateImport) -> Result<()> {
        payload.validate()?;
        // Import is offline and lock-protected; an existing or uncertain state is never replaced.
        match std::fs::symlink_metadata(child(&self.directory, CHECKPOINT)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(ManagedPersistenceError),
        }
        self.replace(payload)
    }
    fn replace(&self, payload: StateImport) -> Result<()> {
        secure(&self.directory, true)?;
        if !self.lineage.is_empty() {
            // A running durable writer must not recreate a missing or unsafe main checkpoint.
            open(&self.directory, CHECKPOINT)?;
        }
        let checksum = sha256_hex(&encode(&payload)?);
        let bytes = encode(&Envelope {
            version: 1,
            payload,
            sha256: checksum,
        })?;
        let mut file = pending(&self.directory)?;
        file.write_all(&bytes)
            .map_err(|_| ManagedPersistenceError)?;
        file.sync_all().map_err(|_| ManagedPersistenceError)?;
        std::fs::rename(
            child(&self.directory, PENDING),
            child(&self.directory, CHECKPOINT),
        )
        .map_err(|_| ManagedPersistenceError)?;
        self.directory
            .sync_all()
            .map_err(|_| ManagedPersistenceError)?;
        Ok(())
    }
    pub fn persistence(store: &Arc<Self>) -> Arc<dyn ManagedPersistence> {
        Arc::new(Persistence(store.clone()))
    }
}
struct Persistence(Arc<Store>);
impl ManagedPersistence for Persistence {
    fn save(
        &self,
        checkpoint: ManagedCheckpoint,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let store = self.0.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store.replace(StateImport {
                    version: 1,
                    node_incarnation: store.lineage.clone(),
                    checkpoint,
                })
            })
            .await
            .map_err(|_| ManagedPersistenceError)?
        })
    }
}

/// Generated per invocation; never loaded from the checkpoint or configured by an operator.
pub(super) fn boot_id() -> std::result::Result<String, crate::ServerError> {
    new_node_incarnation()
}
