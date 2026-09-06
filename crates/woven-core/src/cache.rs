//! Backing store for `Stateful`-class channel/entity state, with optional per-entry TTL.
//!
//! [`CacheService`] is a seam, not a working pluggable backend yet — mirrors [`crate::JournalSink`]
//! for the same reason (ADR-0008): it lets a future `CloudCacheService` (Redis, later replicated to
//! a `NoSQL` store) replace [`InMemoryCacheService`] without touching routing or authority code. Like
//! `JournalSink`/`JournalOutbox`, `WovenCore`'s actual hot path talks to `InMemoryCacheService`'s
//! concrete, synchronous inherent methods directly (no futures to poll, no executor needed inside
//! core) — the trait exists to define the contract a real backend would have to satisfy, not because
//! anything awaits it today.

use std::collections::BTreeMap;
use std::future::{Future, Ready, ready};
use std::time::{Duration, Instant};

use crate::{ChannelId, EntityId, SpaceEpoch, SpaceId};

/// Identifies one piece of `Stateful` state: a single component of a single entity (or
/// space-scoped value, when `entity` is `None`) on one channel, within one space epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CacheKey {
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    pub entity: Option<EntityId>,
    pub channel: ChannelId,
    pub component: u64,
}

/// The value half of a cache entry — everything a reader actually needs back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheEntry {
    pub sequence: u64,
    pub payload: Vec<u8>,
}

/// Failure from a [`CacheService`] operation. The in-memory backend never produces one; this
/// exists for a future backend that talks over a network (e.g. Redis) and can genuinely fail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheError {
    pub message: String,
}

/// Async contract a future backend (e.g. `CloudCacheService`) must satisfy to replace
/// [`InMemoryCacheService`]. Not called on `WovenCore`'s hot path today — see the module docs.
/// Writes take `&mut self` rather than requiring implementors to fake interior mutability; a
/// networked backend (Redis client, etc.) is just as able to satisfy that as an in-memory map.
pub trait CacheService {
    type GetFuture<'a>: Future<Output = Option<CacheEntry>> + Send + 'a
    where
        Self: 'a;
    type SetFuture<'a>: Future<Output = Result<(), CacheError>> + Send + 'a
    where
        Self: 'a;
    type RemoveFuture<'a>: Future<Output = ()> + Send + 'a
    where
        Self: 'a;

    fn get(&self, key: CacheKey) -> Self::GetFuture<'_>;
    fn set(
        &mut self,
        key: CacheKey,
        entry: CacheEntry,
        ttl: Option<Duration>,
    ) -> Self::SetFuture<'_>;
    fn remove(&mut self, key: CacheKey) -> Self::RemoveFuture<'_>;
}

#[derive(Clone, Debug)]
struct StoredEntry {
    entry: CacheEntry,
    ttl: Option<Duration>,
    last_touched: Instant,
}

/// In-memory `CacheService` backend. Entries with `ttl: None` behave exactly like the plain
/// `BTreeMap` this replaced — they live until explicitly evicted (session/space/entity removal).
/// Entries with a TTL are lazily hidden once expired (`get_fresh` won't return them) and reclaimed
/// on the next [`sweep_expired`](Self::sweep_expired) call, which `woven-transport`'s worker loop
/// runs on a timer so idle state is actually freed, not just hidden.
#[derive(Debug, Default)]
pub struct InMemoryCacheService {
    entries: BTreeMap<CacheKey, StoredEntry>,
}

impl InMemoryCacheService {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    fn is_expired(stored: &StoredEntry, now: Instant) -> bool {
        stored
            .ttl
            .is_some_and(|ttl| now.saturating_duration_since(stored.last_touched) >= ttl)
    }

    /// Read a value, treating an expired-but-not-yet-swept entry as absent.
    #[must_use]
    pub fn get_fresh(&self, key: &CacheKey, now: Instant) -> Option<&CacheEntry> {
        self.entries.get(key).and_then(|stored| {
            if Self::is_expired(stored, now) {
                None
            } else {
                Some(&stored.entry)
            }
        })
    }

    /// Raw lookup, ignoring TTL freshness — for capacity accounting, where a not-yet-swept
    /// expired entry still occupies real memory and should still count.
    #[must_use]
    pub fn get(&self, key: &CacheKey) -> Option<&CacheEntry> {
        self.entries.get(key).map(|stored| &stored.entry)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Write a value, resetting its inactivity clock. Returns the previous entry, if any
    /// (mirrors `BTreeMap::insert`'s return semantics, for callers doing their own byte-accounting).
    pub fn put(
        &mut self,
        key: CacheKey,
        entry: CacheEntry,
        ttl: Option<Duration>,
        now: Instant,
    ) -> Option<CacheEntry> {
        self.entries
            .insert(
                key,
                StoredEntry {
                    entry,
                    ttl,
                    last_touched: now,
                },
            )
            .map(|stored| stored.entry)
    }

    /// Remove a value outright (e.g. entity/space/session teardown), returning it if present.
    pub fn evict(&mut self, key: &CacheKey) -> Option<CacheEntry> {
        self.entries.remove(key).map(|stored| stored.entry)
    }

    /// Actively reclaim every entry past its TTL. Returns the number of entries removed.
    pub fn sweep_expired(&mut self, now: Instant) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_, stored| !Self::is_expired(stored, now));
        before - self.entries.len()
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.entries
            .values()
            .map(|stored| stored.entry.payload.len())
            .sum()
    }

    /// Iterates every stored entry, **not** filtering by TTL freshness (unlike [`get_fresh`](
    /// Self::get_fresh)). An entry past its TTL but not yet reclaimed by
    /// [`sweep_expired`](Self::sweep_expired) still appears here — acceptable for its current use
    /// (session snapshots), since TTLs are measured in hours and the sweep interval in seconds.
    pub fn iter(&self) -> impl Iterator<Item = (&CacheKey, &CacheEntry)> {
        self.entries
            .iter()
            .map(|(key, stored)| (key, &stored.entry))
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&CacheKey) -> bool) {
        self.entries.retain(|key, _| keep(key));
    }
}

impl CacheService for InMemoryCacheService {
    type GetFuture<'a> = Ready<Option<CacheEntry>>;
    type SetFuture<'a> = Ready<Result<(), CacheError>>;
    type RemoveFuture<'a> = Ready<()>;

    fn get(&self, key: CacheKey) -> Self::GetFuture<'_> {
        ready(self.get_fresh(&key, Instant::now()).cloned())
    }

    fn set(
        &mut self,
        key: CacheKey,
        entry: CacheEntry,
        ttl: Option<Duration>,
    ) -> Self::SetFuture<'_> {
        self.put(key, entry, ttl, Instant::now());
        ready(Ok(()))
    }

    fn remove(&mut self, key: CacheKey) -> Self::RemoveFuture<'_> {
        self.evict(&key);
        ready(())
    }
}
