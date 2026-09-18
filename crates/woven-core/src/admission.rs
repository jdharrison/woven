#![allow(clippy::missing_panics_doc)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{ConnectionHandle, NodeId, PrincipalId, SessionKey, UsageCounters};

#[allow(clippy::cast_possible_truncation)]
fn count(value: usize) -> u32 {
    value.min(u32::MAX as usize) as u32
}

/// Opaque client-provided idempotency key. Length is bounded by the controller to avoid
/// unbounded memory growth from attacker-controlled input.
#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub const MAX_LEN: usize = 256;

    /// Returns `None` when the key exceeds the maximum length.
    #[must_use]
    pub fn new(key: impl Into<String>) -> Option<Self> {
        let key = key.into();
        if key.len() > Self::MAX_LEN {
            return None;
        }
        Some(Self(key))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Admission policy for a virtual server. All durations use monotonic time internally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuePolicy {
    pub enabled: bool,
    pub max_depth: usize,
    pub ticket_ttl: Duration,
    pub offer_ttl: Duration,
    pub heartbeat_timeout: Duration,
    pub reconnect_grace: Duration,
}

impl Default for QueuePolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: 1_024,
            ticket_ttl: Duration::from_secs(15 * 60),
            offer_ttl: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(30),
            reconnect_grace: Duration::from_secs(20),
        }
    }
}

/// A capacity update delivered by the control plane. Revisions must increase monotonically.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CapacityUpdate {
    pub allocated_ccu: u32,
    pub revision: u64,
}

/// Why a join request was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RejectionReason {
    ServerPaused,
    QueueFull,
    QueueDisabled,
    AlreadyQueued,
    InvalidIdempotencyKey,
}

/// Opaque ticket identifier exposed to queued clients.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct QueueTicketId(u64);

impl QueueTicketId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Opaque resume token used to reclaim a slot during reconnect grace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResumeToken(u64);

impl ResumeToken {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A queued client entry.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueueTicket {
    pub id: QueueTicketId,
    pub session: SessionKey,
    pub principal: PrincipalId,
    pub idempotency_key: IdempotencyKey,
}

/// An active admission lease. Dropping the lease does *not* release it; callers must call
/// `AdmissionController::release` to keep lifecycle explicit and record usage telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AdmissionLease {
    pub id: u64,
    pub session: SessionKey,
    pub principal: PrincipalId,
    pub resume_token: ResumeToken,
}

/// The outcome of a join request.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JoinDecision {
    Admitted(AdmissionLease),
    Queued(QueueTicket),
    Paused,
    Rejected(RejectionReason),
}

/// Current state of a queue ticket.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum QueueStatus {
    Waiting { position: usize },
    Offered,
    Admitted,
    Expired,
    Cancelled,
    Missing,
}

/// A connection-owned queue action executed by the core worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueOperation {
    Status,
    Heartbeat,
    Claim,
    Cancel,
}

/// Result of cancelling a queue ticket.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CancelResult {
    Cancelled,
    Missing,
    AlreadyAdmitted,
    AlreadyExpired,
    AlreadyOffered,
}

/// Result of claiming an admission offer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ClaimError {
    NotOffered,
    Missing,
    Expired,
    Cancelled,
    AlreadyAdmitted,
}

/// Why a lease is being released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseReason {
    Intentional,
    Unexpected,
}

/// Admission-scoped operational snapshot. Contains no player PII or ticket secrets.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AdmissionSnapshot {
    pub allocated_ccu: u32,
    pub active_ccu: u32,
    pub reconnect_reservations: u32,
    pub offered_slots: u32,
    pub available_slots: u32,
    pub queue_depth: usize,
    pub pending_target: Option<u32>,
    pub capacity_revision: u64,
}

/// Per-session metadata used by usage windows.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AdmissionMetadata {
    pub node_id: NodeId,
    pub session: SessionKey,
}

#[derive(Clone, Copy, Debug)]
enum TicketState {
    Waiting,
    Offered(Instant),
    Expired,
    Cancelled,
    Admitted,
}

#[derive(Clone, Debug)]
struct TicketEntry {
    ticket: QueueTicket,
    created: Instant,
    last_heartbeat: Instant,
    state: TicketState,
    terminal_at: Option<Instant>,
}

#[derive(Clone, Copy, Debug)]
struct Reservation {
    principal: PrincipalId,
    expires: Instant,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct ActiveLease {
    principal: PrincipalId,
    started: Instant,
    usage_handle: ConnectionHandle,
}

/// Bounded, transport-neutral admission state for one provisioned session.
///
/// The controller is intentionally single-owner: callers must serialize access through the
/// core worker or another single executor. It performs no I/O and no async work.
pub struct AdmissionController {
    metadata: AdmissionMetadata,
    policy: QueuePolicy,
    allocated_ccu: u32,
    pending_target: Option<u32>,
    revision: u64,
    next_ticket: u64,
    next_lease: u64,
    active: BTreeMap<u64, ActiveLease>,
    reservations: BTreeMap<u64, Reservation>,
    tickets: BTreeMap<QueueTicketId, TicketEntry>,
    waiting: VecDeque<QueueTicketId>,
    idempotency_index: BTreeMap<(PrincipalId, IdempotencyKey), QueueTicketId>,
    counters: Arc<UsageCounters>,
}

impl AdmissionController {
    #[must_use]
    pub fn new(
        metadata: AdmissionMetadata,
        policy: QueuePolicy,
        capacity: CapacityUpdate,
        counters: Arc<UsageCounters>,
    ) -> Self {
        Self {
            metadata,
            policy,
            allocated_ccu: capacity.allocated_ccu,
            pending_target: None,
            revision: capacity.revision,
            next_ticket: 1,
            next_lease: 1,
            active: BTreeMap::new(),
            reservations: BTreeMap::new(),
            tickets: BTreeMap::new(),
            waiting: VecDeque::with_capacity(policy.max_depth),
            idempotency_index: BTreeMap::new(),
            counters,
        }
    }

    #[must_use]
    pub fn metadata(&self) -> AdmissionMetadata {
        self.metadata
    }

    /// Request admission for a principal. Idempotent for valid queue tickets.
    pub fn request_join_at(&mut self, request: JoinRequest, now: Instant) -> JoinDecision {
        self.counters.increment_join_attempts();
        self.expire_at(now);

        let Some(idempotency_key) = request.idempotency_key else {
            self.counters.increment_auth_rejections();
            return JoinDecision::Rejected(RejectionReason::InvalidIdempotencyKey);
        };

        if self.next_lease == u64::MAX {
            return JoinDecision::Rejected(RejectionReason::QueueFull);
        }
        if self.admission_limit() == 0 {
            self.counters.increment_paused_rejections();
            return JoinDecision::Paused;
        }

        if let Some(ticket_id) = self
            .idempotency_index
            .get(&(request.principal, idempotency_key.clone()))
            && let Some(entry) = self.tickets.get(ticket_id)
        {
            match entry.state {
                TicketState::Waiting | TicketState::Offered(_) => {
                    return JoinDecision::Queued(entry.ticket.clone());
                }
                TicketState::Admitted | TicketState::Expired | TicketState::Cancelled => {}
            }
        }

        if self.has_queued_principal(request.principal) {
            self.counters.increment_queue_full_rejections();
            return JoinDecision::Rejected(RejectionReason::AlreadyQueued);
        }

        if self.occupied() < self.admission_limit() {
            self.counters.increment_immediate_admissions();
            return JoinDecision::Admitted(self.admit(request.principal, now));
        }

        if !self.policy.enabled {
            self.counters.increment_queue_full_rejections();
            return JoinDecision::Rejected(RejectionReason::QueueDisabled);
        }

        if self
            .tickets
            .values()
            .filter(|entry| entry.terminal_at.is_none())
            .count()
            >= self.policy.max_depth
            || self.next_ticket == u64::MAX
            || self.next_lease == u64::MAX
        {
            self.counters.increment_queue_full_rejections();
            return JoinDecision::Rejected(RejectionReason::QueueFull);
        }

        self.counters.increment_queued_joins();
        self.counters.set_queue_depth(self.queue_depth() + 1);
        let ticket = self.enqueue(idempotency_key, request.principal, now);
        JoinDecision::Queued(ticket)
    }

    pub fn queue_status_at(&mut self, id: QueueTicketId, now: Instant) -> QueueStatus {
        self.expire_at(now);
        self.status(id)
    }

    pub fn heartbeat_at(&mut self, id: QueueTicketId, now: Instant) -> QueueStatus {
        self.expire_at(now);
        if let Some(entry) = self.tickets.get_mut(&id)
            && matches!(entry.state, TicketState::Waiting)
        {
            entry.last_heartbeat = now;
        }
        self.status(id)
    }

    pub fn cancel_at(&mut self, id: QueueTicketId, now: Instant) -> CancelResult {
        self.expire_at(now);
        let Some(entry) = self.tickets.get_mut(&id) else {
            return CancelResult::Missing;
        };
        match entry.state {
            TicketState::Waiting | TicketState::Offered(_) => {
                entry.state = TicketState::Cancelled;
                entry.terminal_at = Some(now);
                self.idempotency_index
                    .remove(&(entry.ticket.principal, entry.ticket.idempotency_key.clone()));
                self.waiting.retain(|candidate| *candidate != id);
                self.counters.increment_cancelled_tickets();
                self.prune_terminal_at(now);
                self.apply_pending();
                self.promote_at(now);
                CancelResult::Cancelled
            }
            TicketState::Admitted => CancelResult::AlreadyAdmitted,
            TicketState::Expired | TicketState::Cancelled => CancelResult::AlreadyExpired,
        }
    }

    pub fn claim_offer_at(
        &mut self,
        id: QueueTicketId,
        now: Instant,
    ) -> Result<AdmissionLease, ClaimError> {
        self.expire_at(now);
        let principal = {
            let entry = self.tickets.get_mut(&id).ok_or(ClaimError::Missing)?;
            match entry.state {
                TicketState::Offered(_) => {
                    entry.state = TicketState::Admitted;
                    entry.terminal_at = Some(now);
                    self.idempotency_index
                        .remove(&(entry.ticket.principal, entry.ticket.idempotency_key.clone()));
                    entry.ticket.principal
                }
                TicketState::Waiting => return Err(ClaimError::NotOffered),
                TicketState::Expired => return Err(ClaimError::Expired),
                TicketState::Cancelled => return Err(ClaimError::Cancelled),
                TicketState::Admitted => return Err(ClaimError::AlreadyAdmitted),
            }
        };
        self.counters.increment_promoted_players();
        self.prune_terminal_at(now);
        Ok(self.admit(principal, now))
    }

    pub fn release_at(&mut self, lease: AdmissionLease, reason: ReleaseReason, now: Instant) {
        if lease.session != self.metadata.session
            || self
                .active
                .get(&lease.id)
                .is_none_or(|active| active.principal != lease.principal)
        {
            return;
        }
        let Some(removed) = self.active.remove(&lease.id) else {
            return;
        };
        self.counters.end_connection(removed.usage_handle, now);
        self.counters.set_active_ccu(count(self.active.len()));
        if matches!(reason, ReleaseReason::Unexpected) && !self.policy.reconnect_grace.is_zero() {
            let expires = now.checked_add(self.policy.reconnect_grace).unwrap_or(now);
            self.reservations.insert(
                lease.id,
                Reservation {
                    principal: lease.principal,
                    expires,
                },
            );
            self.counters.increment_reconnect_reservations();
        }
        self.apply_pending();
        self.promote_at(now);
    }

    pub fn resume_at(
        &mut self,
        token: ResumeToken,
        principal: PrincipalId,
        now: Instant,
    ) -> Option<AdmissionLease> {
        self.expire_at(now);
        let lease_id = token.get();
        let reservation = self.reservations.remove(&lease_id)?;
        if reservation.principal != principal {
            self.reservations.insert(lease_id, reservation);
            return None;
        }
        self.counters.increment_successful_resumes();
        let usage_handle = self.counters.start_connection(principal, now);
        self.active.insert(
            lease_id,
            ActiveLease {
                principal,
                started: now,
                usage_handle,
            },
        );
        self.counters.set_active_ccu(count(self.active.len()));
        Some(AdmissionLease {
            id: lease_id,
            session: self.metadata.session,
            principal,
            resume_token: ResumeToken(lease_id),
        })
    }

    pub fn apply_capacity_at(&mut self, update: CapacityUpdate, now: Instant) -> AdmissionSnapshot {
        if update.revision <= self.revision {
            return self.snapshot();
        }
        self.revision = update.revision;
        self.counters.increment_capacity_allocations();
        if update.allocated_ccu < self.occupied() {
            self.pending_target = Some(update.allocated_ccu);
        } else {
            self.allocated_ccu = update.allocated_ccu;
            self.pending_target = None;
            self.promote_at(now);
        }
        self.snapshot()
    }

    #[must_use]
    pub fn snapshot(&self) -> AdmissionSnapshot {
        let occupied = self.occupied();
        let available = self.admission_limit().saturating_sub(occupied);
        AdmissionSnapshot {
            allocated_ccu: self.allocated_ccu,
            active_ccu: count(self.active.len()),
            reconnect_reservations: count(self.reservations.len()),
            offered_slots: count(
                self.tickets
                    .values()
                    .filter(|entry| matches!(entry.state, TicketState::Offered(_)))
                    .count(),
            ),
            available_slots: available,
            queue_depth: self.queue_depth(),
            pending_target: self.pending_target,
            capacity_revision: self.revision,
        }
    }

    #[must_use]
    pub fn counters(&self) -> Arc<UsageCounters> {
        self.counters.clone()
    }

    /// Returns true while this controller retains the ticket.
    #[must_use]
    pub fn has_ticket(&self, id: QueueTicketId) -> bool {
        self.tickets.contains_key(&id)
    }

    /// Verifies that a lease was issued by this session and has not been released.
    #[must_use]
    pub fn has_active_lease(&self, lease: AdmissionLease) -> bool {
        lease.session == self.metadata.session
            && self
                .active
                .get(&lease.id)
                .is_some_and(|active| active.principal == lease.principal)
    }

    fn admit(&mut self, principal: PrincipalId, now: Instant) -> AdmissionLease {
        let id = self.next_lease;
        self.next_lease += 1;
        let usage_handle = self.counters.start_connection(principal, now);
        self.active.insert(
            id,
            ActiveLease {
                principal,
                started: now,
                usage_handle,
            },
        );
        self.counters.set_active_ccu(count(self.active.len()));
        AdmissionLease {
            id,
            session: self.metadata.session,
            principal,
            resume_token: ResumeToken(id),
        }
    }

    fn enqueue(
        &mut self,
        idempotency_key: IdempotencyKey,
        principal: PrincipalId,
        now: Instant,
    ) -> QueueTicket {
        let id = QueueTicketId(self.next_ticket);
        self.next_ticket += 1;
        let ticket = QueueTicket {
            id,
            session: self.metadata.session,
            principal,
            idempotency_key: idempotency_key.clone(),
        };
        self.tickets.insert(
            id,
            TicketEntry {
                ticket: ticket.clone(),
                created: now,
                last_heartbeat: now,
                state: TicketState::Waiting,
                terminal_at: None,
            },
        );
        self.idempotency_index
            .insert((principal, idempotency_key), id);
        self.waiting.push_back(id);
        ticket
    }

    fn occupied(&self) -> u32 {
        count(self.active.len())
            + count(self.reservations.len())
            + count(
                self.tickets
                    .values()
                    .filter(|entry| matches!(entry.state, TicketState::Offered(_)))
                    .count(),
            )
    }

    fn has_queued_principal(&self, principal: PrincipalId) -> bool {
        self.tickets.values().any(|entry| {
            entry.ticket.principal == principal
                && matches!(entry.state, TicketState::Waiting | TicketState::Offered(_))
        })
    }

    fn queue_depth(&self) -> usize {
        self.waiting
            .iter()
            .filter(|id| {
                matches!(
                    self.tickets.get(id).map(|entry| entry.state),
                    Some(TicketState::Waiting)
                )
            })
            .count()
    }

    fn status(&self, id: QueueTicketId) -> QueueStatus {
        let Some(entry) = self.tickets.get(&id) else {
            return QueueStatus::Missing;
        };
        match entry.state {
            TicketState::Waiting => QueueStatus::Waiting {
                position: self
                    .waiting
                    .iter()
                    .filter(|candidate| {
                        matches!(
                            self.tickets.get(candidate).map(|candidate| candidate.state),
                            Some(TicketState::Waiting)
                        )
                    })
                    .position(|candidate| *candidate == id)
                    .map_or(0, |position| position + 1),
            },
            TicketState::Offered(_) => QueueStatus::Offered,
            TicketState::Admitted => QueueStatus::Admitted,
            TicketState::Expired => QueueStatus::Expired,
            TicketState::Cancelled => QueueStatus::Cancelled,
        }
    }

    fn expire_at(&mut self, now: Instant) {
        let mut expired_reservations = 0;
        self.reservations.retain(|_, value| {
            let keep = value.expires > now;
            if !keep {
                expired_reservations += 1;
            }
            keep
        });
        if expired_reservations > 0 {
            self.counters
                .increment_expired_tickets_by(expired_reservations);
        }

        let mut expired_tickets = 0;
        let mut abandoned_offers = 0;
        for entry in self.tickets.values_mut() {
            let was_live = matches!(entry.state, TicketState::Waiting | TicketState::Offered(_));
            if matches!(entry.state, TicketState::Waiting)
                && (now.saturating_duration_since(entry.created) >= self.policy.ticket_ttl
                    || now.saturating_duration_since(entry.last_heartbeat)
                        >= self.policy.heartbeat_timeout)
            {
                entry.state = TicketState::Expired;
                expired_tickets += 1;
            }
            if let TicketState::Offered(expires) = entry.state
                && expires <= now
            {
                entry.state = TicketState::Expired;
                abandoned_offers += 1;
            }
            if was_live && matches!(entry.state, TicketState::Expired) {
                entry.terminal_at = Some(now);
                self.idempotency_index
                    .remove(&(entry.ticket.principal, entry.ticket.idempotency_key.clone()));
            }
        }
        if expired_tickets > 0 {
            self.counters.increment_expired_tickets_by(expired_tickets);
        }
        if abandoned_offers > 0 {
            self.counters
                .increment_abandoned_offers_by(abandoned_offers);
        }
        self.waiting.retain(|id| {
            matches!(
                self.tickets.get(id).map(|entry| entry.state),
                Some(TicketState::Waiting)
            )
        });
        self.prune_terminal_at(now);
        self.counters.set_queue_depth(self.queue_depth());
        self.apply_pending();
        self.promote_at(now);
    }

    /// Advances bounded expiry, retention and promotion without client traffic.
    pub fn maintain_at(&mut self, now: Instant) {
        self.expire_at(now);
    }

    fn prune_terminal_at(&mut self, now: Instant) {
        self.tickets.retain(|_, entry| {
            entry
                .terminal_at
                .is_none_or(|at| now.saturating_duration_since(at) < Duration::from_secs(60))
        });
        let mut terminal = self
            .tickets
            .iter()
            .filter_map(|(id, entry)| entry.terminal_at.map(|at| (at, *id)))
            .collect::<Vec<_>>();
        terminal.sort_unstable();
        let excess = terminal.len().saturating_sub(1024);
        for (_, id) in terminal.into_iter().take(excess) {
            self.tickets.remove(&id);
        }
        self.idempotency_index
            .retain(|_, id| self.tickets.contains_key(id));
    }

    /// Ownership must be checked before any operation that can mutate a ticket.
    #[must_use]
    pub fn ticket_owned_by(&self, id: QueueTicketId, principal: PrincipalId) -> bool {
        self.tickets
            .get(&id)
            .is_some_and(|entry| entry.ticket.principal == principal)
    }

    /// Cancels all live tickets for a departing identity.
    pub fn cancel_principal_at(&mut self, principal: PrincipalId, now: Instant) {
        let ids = self
            .tickets
            .iter()
            .filter_map(|(id, entry)| {
                (entry.ticket.principal == principal && entry.terminal_at.is_none()).then_some(*id)
            })
            .collect::<Vec<_>>();
        for id in ids {
            self.cancel_at(id, now);
        }
    }

    fn admission_limit(&self) -> u32 {
        self.pending_target.unwrap_or(self.allocated_ccu)
    }

    fn apply_pending(&mut self) {
        if let Some(target) = self.pending_target
            && self.occupied() <= target
        {
            self.allocated_ccu = target;
            self.pending_target = None;
        }
    }

    fn promote_at(&mut self, now: Instant) {
        while self.admission_limit() > 0 && self.occupied() < self.admission_limit() {
            let Some(id) = self.waiting.pop_front() else {
                break;
            };
            let Some(entry) = self.tickets.get_mut(&id) else {
                continue;
            };
            if matches!(entry.state, TicketState::Waiting) {
                let expires = now.checked_add(self.policy.offer_ttl).unwrap_or(now);
                entry.state = TicketState::Offered(expires);
            }
        }
        self.counters.set_queue_depth(self.queue_depth());
    }
}

/// Request to join a virtual server.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JoinRequest {
    pub principal: PrincipalId,
    pub idempotency_key: Option<IdempotencyKey>,
}

impl JoinRequest {
    #[must_use]
    pub fn new(principal: PrincipalId, idempotency_key: IdempotencyKey) -> Self {
        Self {
            principal,
            idempotency_key: Some(idempotency_key),
        }
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    #![allow(clippy::manual_let_else)]

    use super::*;

    fn metadata() -> AdmissionMetadata {
        AdmissionMetadata {
            node_id: NodeId::new(1),
            session: SessionKey::new(crate::NamespaceId::new(1), crate::SessionId::new(1)),
        }
    }

    fn policy() -> QueuePolicy {
        QueuePolicy {
            max_depth: 16,
            heartbeat_timeout: Duration::from_secs(60),
            ..QueuePolicy::default()
        }
    }

    fn controller(capacity: u32) -> AdmissionController {
        AdmissionController::new(
            metadata(),
            policy(),
            CapacityUpdate {
                allocated_ccu: capacity,
                revision: 1,
            },
            Arc::new(UsageCounters::new(metadata())),
        )
    }

    fn join(principal: u64, key: &str) -> JoinRequest {
        JoinRequest::new(
            PrincipalId::new(principal),
            IdempotencyKey::new(key).unwrap(),
        )
    }

    #[test]
    fn ten_slots_admit_ten_and_queue_five() {
        let now = Instant::now();
        let mut controller = controller(10);
        let mut admitted = 0;
        let mut queued = 0;
        for value in 1..=15 {
            match controller.request_join_at(join(value, "key"), now) {
                JoinDecision::Admitted(_) => admitted += 1,
                JoinDecision::Queued(_) => queued += 1,
                decision => panic!("unexpected {decision:?}"),
            }
        }
        assert_eq!((admitted, queued), (10, 5));
        assert_eq!(controller.snapshot().active_ccu, 10);
    }

    #[test]
    fn release_offers_oldest_waiter() {
        let now = Instant::now();
        let mut controller = controller(1);
        let lease = match controller.request_join_at(join(1, "a"), now) {
            JoinDecision::Admitted(lease) => lease,
            _ => panic!(),
        };
        let first = match controller.request_join_at(join(2, "b"), now) {
            JoinDecision::Queued(ticket) => ticket,
            _ => panic!(),
        };
        let second = match controller.request_join_at(join(3, "c"), now) {
            JoinDecision::Queued(ticket) => ticket,
            _ => panic!(),
        };
        controller.release_at(lease, ReleaseReason::Intentional, now);
        assert_eq!(
            controller.queue_status_at(first.id, now),
            QueueStatus::Offered
        );
        assert_eq!(
            controller.queue_status_at(second.id, now),
            QueueStatus::Waiting { position: 1 }
        );
    }

    #[test]
    fn reconnect_grace_blocks_promotion_until_expiry() {
        let now = Instant::now();
        let mut controller = controller(1);
        let lease = match controller.request_join_at(join(1, "a"), now) {
            JoinDecision::Admitted(lease) => lease,
            _ => panic!(),
        };
        let ticket = match controller.request_join_at(join(2, "b"), now) {
            JoinDecision::Queued(ticket) => ticket,
            _ => panic!(),
        };
        controller.release_at(lease, ReleaseReason::Unexpected, now);
        assert_eq!(
            controller.queue_status_at(ticket.id, now),
            QueueStatus::Waiting { position: 1 }
        );
        assert_eq!(
            controller.queue_status_at(ticket.id, now + policy().reconnect_grace),
            QueueStatus::Offered
        );
    }

    #[test]
    fn allocation_increase_promotes_waiters_and_stale_update_is_ignored() {
        let now = Instant::now();
        let mut controller = controller(1);
        let _ = controller.request_join_at(join(1, "a"), now);
        let first = match controller.request_join_at(join(2, "b"), now) {
            JoinDecision::Queued(ticket) => ticket,
            _ => panic!(),
        };
        let second = match controller.request_join_at(join(3, "c"), now) {
            JoinDecision::Queued(ticket) => ticket,
            _ => panic!(),
        };
        controller.apply_capacity_at(
            CapacityUpdate {
                allocated_ccu: 3,
                revision: 2,
            },
            now,
        );
        assert_eq!(
            controller.queue_status_at(first.id, now),
            QueueStatus::Offered
        );
        assert_eq!(
            controller.queue_status_at(second.id, now),
            QueueStatus::Offered
        );
        assert_eq!(
            controller
                .apply_capacity_at(
                    CapacityUpdate {
                        allocated_ccu: 1,
                        revision: 1,
                    },
                    now,
                )
                .allocated_ccu,
            3
        );
    }

    #[test]
    fn decrease_drains_without_disconnect_and_paused_rejects() {
        let now = Instant::now();
        let mut controller = controller(2);
        let first = match controller.request_join_at(join(1, "a"), now) {
            JoinDecision::Admitted(lease) => lease,
            _ => panic!(),
        };
        let _ = controller.request_join_at(join(2, "b"), now);
        let snapshot = controller.apply_capacity_at(
            CapacityUpdate {
                allocated_ccu: 1,
                revision: 2,
            },
            now,
        );
        assert_eq!(snapshot.pending_target, Some(1));
        assert_eq!(snapshot.active_ccu, 2);
        controller.release_at(first, ReleaseReason::Intentional, now);
        assert_eq!(controller.snapshot().allocated_ccu, 1);
        let paused = controller.apply_capacity_at(
            CapacityUpdate {
                allocated_ccu: 0,
                revision: 3,
            },
            now,
        );
        assert_eq!(paused.pending_target, Some(0));
    }

    #[test]
    fn paused_and_full_queue_are_structured_rejections() {
        let now = Instant::now();
        let mut paused = controller(0);
        assert_eq!(
            paused.request_join_at(join(1, "a"), now),
            JoinDecision::Paused
        );
        let mut full = AdmissionController::new(
            metadata(),
            QueuePolicy {
                max_depth: 0,
                ..policy()
            },
            CapacityUpdate {
                allocated_ccu: 1,
                revision: 1,
            },
            Arc::new(UsageCounters::new(metadata())),
        );
        let _ = full.request_join_at(join(1, "a"), now);
        assert_eq!(
            full.request_join_at(join(2, "b"), now),
            JoinDecision::Rejected(RejectionReason::QueueFull)
        );
    }

    #[test]
    fn terminal_history_and_indexes_remain_bounded_under_churn() {
        let now = Instant::now();
        let mut controller = controller(1);
        let _ = controller.request_join_at(join(1, "active"), now);
        for principal in 2..4096 {
            let JoinDecision::Queued(ticket) =
                controller.request_join_at(join(principal, "same"), now)
            else {
                panic!("queued");
            };
            assert!(controller.ticket_owned_by(ticket.id, PrincipalId::new(principal)));
            controller.cancel_at(ticket.id, now);
            assert!(controller.tickets.len() <= 1024);
            assert!(controller.idempotency_index.is_empty());
            assert!(controller.waiting.is_empty());
        }
        controller.maintain_at(now + Duration::from_secs(60));
        assert!(controller.tickets.is_empty());
        assert_eq!(controller.snapshot().active_ccu, 1);
    }

    #[test]
    fn offered_tickets_count_toward_live_queue_bound() {
        let now = Instant::now();
        let mut controller = controller(1);
        controller.policy.max_depth = 1;
        let JoinDecision::Admitted(lease) = controller.request_join_at(join(1, "a"), now) else {
            panic!("admitted");
        };
        let JoinDecision::Queued(ticket) = controller.request_join_at(join(2, "b"), now) else {
            panic!("queued");
        };
        controller.release_at(lease, ReleaseReason::Intentional, now);
        assert_eq!(
            controller.queue_status_at(ticket.id, now),
            QueueStatus::Offered
        );
        assert_eq!(
            controller.request_join_at(join(3, "c"), now),
            JoinDecision::Rejected(RejectionReason::QueueFull)
        );
    }

    #[test]
    fn idempotency_keys_are_principal_scoped() {
        let now = Instant::now();
        let mut controller = controller(1);
        let _ = controller.request_join_at(join(1, "a"), now);
        let first = match controller.request_join_at(join(2, "b"), now) {
            JoinDecision::Queued(ticket) => ticket,
            _ => panic!(),
        };
        let second = match controller.request_join_at(join(3, "b"), now) {
            JoinDecision::Queued(ticket) => ticket,
            _ => panic!(),
        };
        assert_ne!(first.id, second.id);
        assert_ne!(first.principal, second.principal);
        assert_eq!(
            controller.request_join_at(join(2, "b"), now),
            JoinDecision::Queued(first)
        );
    }

    #[test]
    fn same_principal_cannot_hold_two_queue_positions() {
        let now = Instant::now();
        let mut controller = controller(1);
        let _ = controller.request_join_at(join(1, "a"), now);
        let _ = controller.request_join_at(join(2, "b"), now);
        assert!(matches!(
            controller.request_join_at(join(2, "c"), now),
            JoinDecision::Rejected(RejectionReason::AlreadyQueued)
        ));
    }

    #[test]
    fn resume_reclaims_reservation() {
        let now = Instant::now();
        let mut controller = controller(1);
        let lease = match controller.request_join_at(join(1, "a"), now) {
            JoinDecision::Admitted(lease) => lease,
            _ => panic!(),
        };
        controller.release_at(lease, ReleaseReason::Unexpected, now);
        let resumed = controller
            .resume_at(lease.resume_token, PrincipalId::new(1), now)
            .expect("valid resume");
        assert_eq!(resumed.id, lease.id);
        assert!(
            controller
                .resume_at(lease.resume_token, PrincipalId::new(1), now)
                .is_none()
        );
    }
}
