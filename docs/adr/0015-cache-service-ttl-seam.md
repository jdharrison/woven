# ADR 0015: `CacheService` Seam for TTL-Based Ephemeral Persistence

## Status

Accepted

## Context

`Stateful` channel state persisted in memory for a session's entire lifetime with no way to reclaim it earlier — every write lived until the owning entity, space, or session was explicitly torn down. Product tiers need a middle ground between "gone the instant it's published" (`Ephemeral`) and "kept forever in-process" (today's `Stateful`): state that survives while actively used, but is wiped after a configurable period of inactivity, without requiring a real external persistence backend yet. ADR-0008 already established the pattern for this kind of gap — define the seam ahead of any real backend, and let the hot path talk to a concrete in-memory type directly rather than through the trait.

## Decision

`PersistenceClass::Stateful` now carries `ttl: Option<Duration>`, configured once per channel at registration (`ttl: None` preserves today's behavior exactly — the default, and what every existing channel uses). The channel's registered TTL is authoritative; a publisher's own message never declares a meaningful TTL, so channel-policy validation (`PersistenceClass::same_kind`) compares by variant only, ignoring the TTL payload.

`InMemoryCacheService` (`crates/woven-core/src/cache.rs`) replaces the bare `BTreeMap` that used to back `SessionState.state`, tracking a `last_touched` timestamp per entry. `WovenCore` calls its synchronous inherent methods (`put`, `get`, `sweep_expired`) directly — no futures to poll, no executor needed inside `woven-core`, matching the existing constraint that core stays synchronous and free of a real `tokio` dependency. A `CacheService` trait (async, mirroring `JournalSink`) is defined alongside it and is what a future `CloudCacheService` (Redis, later replicated to a NoSQL store) would implement to replace `InMemoryCacheService` without touching routing or authority code — not because anything calls it via the trait, or its own `get_fresh`, on the hot path today.

Lazy expiry alone (hiding stale reads) isn't sufficient: state nobody ever touches again needs to be actively reclaimed. `woven-transport`'s worker loop runs `WovenCore::sweep_expired_state` on a 60-second timer (`tokio::select!` alongside the existing command channel), which is coarse enough to cost nothing meaningful against TTLs measured in hours. Capacity accounting (`validate_state_capacity`) additionally sweeps its own session on every publish before counting, specifically so a burst of short-lived writes that has since gone quiet can't occupy a session's entry/byte caps indefinitely and spuriously reject unrelated new publishes — that check can't wait for the global timer, since it gates admission of new state.

## Consequences

- Existing behavior is unchanged for every channel registered today (`ttl: None`); no observable regression.
- Snapshots may show an entry for up to one sweep interval (60s) after its TTL technically elapses, since reading (`SessionSnapshot`) doesn't itself filter by freshness — acceptable given TTLs are measured in hours, not seconds. Capacity accounting does not share this gap (see above): it always reflects the true live set at publish time.
- No wire protocol or client API changes: TTL is a channel/service-level configuration choice, not something a publisher requests per message.
- A real backend (Redis now, NoSQL later for long-term simulations) can replace `InMemoryCacheService` by implementing `CacheService`, without changing `WovenCore`'s routing or authority logic.
