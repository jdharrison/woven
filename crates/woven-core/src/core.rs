use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crate::{
    AccessGrant, AdmissionController, AdmissionLease, AdmissionMetadata, AuthError,
    AuthenticatedPrincipal, Authenticator, AuthorityContext, AuthorityEmission, AuthorityOutcome,
    AuthorityRejection, CacheEntry, CacheKey, CapacityUpdate, ChannelDefinition, ChannelId,
    ChannelScope, CoalesceKey, ConnectionId, Credentials, DeliveryClass, EntityId, EntityPosition,
    EntitySnapshot, IdempotencyKey, InMemoryCacheService, JoinDecision, JournalOutbox,
    JournalRecord, NamespaceId, OutboundMessage, OutboundQueue, OutboundQueueConfig, ParentAnchor,
    PersistenceClass, PositionValidationError, PrincipalId, ProposedMessage, QueueConfigError,
    QueueError, QueueEviction, QueuePolicy, QueuePush, RoutingPolicy, SessionKey, SessionSnapshot,
    SpaceDescriptor, SpaceEpoch, SpaceId, SpaceKey, SpaceSnapshot, SpaceValidationError,
    StateSnapshot, UsageCounters,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishRateLimit {
    pub max_publishes: usize,
    pub window: Duration,
}

impl Default for PublishRateLimit {
    fn default() -> Self {
        Self {
            max_publishes: 256,
            window: Duration::from_secs(1),
        }
    }
}

impl PublishRateLimit {
    fn validate(self) -> Result<Self, CoreError> {
        if self.max_publishes == 0 || self.window.is_zero() {
            return Err(CoreError::InvalidConfiguration);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoreConfig {
    pub outbound_queue: OutboundQueueConfig,
    pub publish_rate_limit: PublishRateLimit,
    pub max_connections: usize,
    pub max_sessions: usize,
    pub max_channels: usize,
    pub max_memberships_per_connection: usize,
    pub max_subscriptions_per_connection: usize,
    pub max_owned_entities_per_connection: usize,
    pub max_payload_bytes: usize,
    pub max_spaces_per_session: usize,
    pub max_space_epoch_tombstones_per_session: usize,
    pub max_entities_per_session: usize,
    pub max_state_entries_per_session: usize,
    pub max_state_bytes_per_session: usize,
    pub max_sequence_keys_per_session: usize,
    pub max_authority_emissions: usize,
    pub journal_outbox_capacity: usize,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            outbound_queue: OutboundQueueConfig::default(),
            publish_rate_limit: PublishRateLimit::default(),
            max_connections: 4_096,
            max_sessions: 1_024,
            max_channels: 1_024,
            max_memberships_per_connection: 32,
            max_subscriptions_per_connection: 128,
            max_owned_entities_per_connection: 256,
            max_payload_bytes: 64 * 1024,
            max_spaces_per_session: 1_024,
            max_space_epoch_tombstones_per_session: 4_096,
            max_entities_per_session: 16_384,
            max_state_entries_per_session: 65_536,
            max_state_bytes_per_session: 64 * 1024 * 1024,
            max_sequence_keys_per_session: 131_072,
            max_authority_emissions: 16,
            journal_outbox_capacity: 1_024,
        }
    }
}

impl CoreConfig {
    fn validate(self) -> Result<Self, CoreError> {
        self.outbound_queue
            .validate()
            .map_err(CoreError::InvalidQueueConfig)?;
        self.publish_rate_limit.validate()?;
        if self.max_connections == 0
            || self.max_sessions == 0
            || self.max_channels == 0
            || self.max_memberships_per_connection == 0
            || self.max_subscriptions_per_connection == 0
            || self.max_owned_entities_per_connection == 0
            || self.max_payload_bytes == 0
            || self.max_spaces_per_session == 0
            || self.max_space_epoch_tombstones_per_session == 0
            || self.max_entities_per_session == 0
            || self.max_state_entries_per_session == 0
            || self.max_state_bytes_per_session == 0
            || self.max_sequence_keys_per_session == 0
            || self.max_authority_emissions == 0
            || self.max_spaces_per_session > self.max_space_epoch_tombstones_per_session
        {
            return Err(CoreError::InvalidConfiguration);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdKind {
    Namespace,
    Session,
    Space,
    Entity,
    Connection,
    Principal,
    Channel,
    SpaceEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoreError {
    InvalidConfiguration,
    InvalidQueueConfig(QueueConfigError),
    ReservedZeroId(IdKind),
    AuthenticationFailed(AuthError),
    AuthenticationRequired,
    AlreadyAuthenticated,
    UnknownConnection(ConnectionId),
    ConnectionLimitReached,
    NamespaceReadAccessDenied(NamespaceId),
    NamespaceWriteAccessDenied(NamespaceId),
    SessionReadAccessDenied(SessionKey),
    SessionWriteAccessDenied(SessionKey),
    SpaceReadAccessDenied(SpaceKey),
    SpaceWriteAccessDenied(SpaceKey),
    ChannelWriteAccessDenied(ChannelScope),
    UnknownChannel(ChannelId),
    ChannelAlreadyRegistered(ChannelId),
    ChannelLimitReached,
    InvalidChannelLimit(ChannelId),
    ChannelPolicyMismatch {
        channel: ChannelId,
        expected_delivery: DeliveryClass,
        received_delivery: DeliveryClass,
        expected_persistence: PersistenceClass,
        received_persistence: PersistenceClass,
    },
    SessionNotFound(SessionKey),
    SessionAlreadyProvisioned(SessionKey),
    SessionLimitReached,
    SessionMembershipRequired(SessionKey),
    MembershipLimitReached,
    SpaceNotFound(SpaceKey),
    SpaceAlreadyExists(SpaceKey),
    SpaceLimitReached,
    SpaceIdHistoryLimitReached,
    InvalidSpace(SpaceValidationError),
    ParentSpaceNotFound(SpaceId),
    ParentAnchorNotFound(EntityId),
    ParentAnchorInWrongSpace {
        entity: EntityId,
        expected: SpaceId,
    },
    EpochDidNotAdvance {
        current: SpaceEpoch,
        proposed: SpaceEpoch,
    },
    SpaceEpochMismatch {
        expected: SpaceEpoch,
        received: SpaceEpoch,
    },
    SubscriptionRequired(SpaceKey),
    SubscriptionLimitReached,
    EntityLimitReached,
    OwnedEntityLimitReached,
    EntityNotFound(EntityId),
    EntityNotOwned(EntityId),
    EntitySpaceMismatch(EntityId),
    InvalidEntityPosition(PositionValidationError),
    SpatialCellOutOfRange,
    StateEntryLimitReached,
    StateByteLimitReached,
    SequenceKeyLimitReached,
    PayloadTooLarge {
        actual: usize,
        limit: usize,
    },
    MissingCoalesceKey,
    InvalidCoalesceKey,
    StaleSequence {
        received: u64,
        last: u64,
    },
    PublishRateLimited {
        retry_after: Duration,
    },
    RateLimitClockRegressed,
    AuthorityRejected(AuthorityRejection),
    AuthorityEmissionLimitExceeded,
    JournalOutboxSaturated,
    IdExhausted,
    AdmissionAlreadyConfigured(SessionKey),
    AdmissionLeaseRequired(SessionKey),
    InvalidAdmissionLease(SessionKey),
    Queue(QueueError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishRequest {
    pub connection: ConnectionId,
    pub session: SessionKey,
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    pub entity: Option<EntityId>,
    pub channel: ChannelId,
    pub sequence: u64,
    pub delivery: DeliveryClass,
    pub persistence: PersistenceClass,
    pub coalesce_key: Option<CoalesceKey>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueueActivity {
    pub queued: usize,
    pub critical_evictions: usize,
    pub replaced_latest: usize,
    pub evicted_latest: usize,
    pub dropped_latest: usize,
    pub dropped_best_effort: usize,
    pub critical_capacity_exhausted: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PublishOutcome {
    pub authorized_messages: usize,
    pub recipient_attempts: usize,
    pub queues: QueueActivity,
    pub disconnected_slow_consumers: Vec<ConnectionId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemovedEntity {
    pub entity: EntityId,
    pub session: SessionKey,
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityTransitionRequest {
    pub connection: ConnectionId,
    pub session: SessionKey,
    pub entity: EntityId,
    pub source_space: SpaceId,
    pub source_epoch: SpaceEpoch,
    pub destination_space: SpaceId,
    pub destination_epoch: SpaceEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityTransition {
    pub entity: EntityId,
    pub session: SessionKey,
    pub source_space: SpaceId,
    pub source_epoch: SpaceEpoch,
    pub destination_space: SpaceId,
    pub destination_epoch: SpaceEpoch,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CleanupSummary {
    pub memberships_removed: usize,
    pub subscriptions_removed: usize,
    pub entities_removed: usize,
    pub spaces_removed: usize,
    pub queued_messages_discarded: usize,
    pub removed_entities: Vec<RemovedEntity>,
}

impl CleanupSummary {
    fn add(&mut self, mut other: Self) {
        self.memberships_removed += other.memberships_removed;
        self.subscriptions_removed += other.subscriptions_removed;
        self.entities_removed += other.entities_removed;
        self.spaces_removed += other.spaces_removed;
        self.queued_messages_discarded += other.queued_messages_discarded;
        self.removed_entities.append(&mut other.removed_entities);
    }
}

#[derive(Debug, Default)]
struct ConnectionRateLimiter {
    window_started: Option<Instant>,
    publishes: usize,
}

impl ConnectionRateLimiter {
    fn admit(&mut self, policy: PublishRateLimit, now: Instant) -> Result<(), CoreError> {
        let Some(started) = self.window_started else {
            self.window_started = Some(now);
            self.publishes = 1;
            return Ok(());
        };
        let elapsed = now
            .checked_duration_since(started)
            .ok_or(CoreError::RateLimitClockRegressed)?;
        if elapsed >= policy.window {
            self.window_started = Some(now);
            self.publishes = 1;
            return Ok(());
        }
        if self.publishes == policy.max_publishes {
            return Err(CoreError::PublishRateLimited {
                retry_after: policy.window.saturating_sub(elapsed),
            });
        }
        self.publishes += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct ConnectionState {
    authenticated: Option<AuthenticatedPrincipal>,
    memberships: BTreeSet<SessionKey>,
    subscriptions: BTreeSet<SpaceKey>,
    owned_entities: usize,
    rate_limiter: ConnectionRateLimiter,
    outbound: OutboundQueue,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct EntityRecord {
    id: EntityId,
    owner_connection: ConnectionId,
    owner_principal: PrincipalId,
    space: SpaceId,
    space_epoch: SpaceEpoch,
    position: Option<EntityPosition>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum SpatialCell {
    Cartesian2D { x: i64, y: i64 },
    Cartesian3D { x: i64, y: i64, z: i64 },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SpatialCellKey {
    space: SpaceId,
    space_epoch: SpaceEpoch,
    cell: SpatialCell,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SequenceKey {
    connection: ConnectionId,
    space: SpaceId,
    space_epoch: SpaceEpoch,
    entity: Option<EntityId>,
    channel: ChannelId,
    component: u64,
}

#[derive(Debug)]
struct SessionState {
    members: BTreeSet<ConnectionId>,
    spaces: BTreeMap<SpaceId, SpaceDescriptor>,
    space_epoch_tombstones: BTreeMap<SpaceId, SpaceEpoch>,
    subscribers: BTreeMap<SpaceId, BTreeSet<ConnectionId>>,
    entities: BTreeMap<EntityId, EntityRecord>,
    entity_cells: BTreeMap<EntityId, SpatialCellKey>,
    cell_entities: BTreeMap<SpatialCellKey, BTreeSet<EntityId>>,
    sequences: BTreeMap<SequenceKey, u64>,
    state: InMemoryCacheService,
    state_bytes: usize,
}

impl SessionState {
    fn new() -> Self {
        Self {
            members: BTreeSet::new(),
            spaces: BTreeMap::new(),
            space_epoch_tombstones: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            entities: BTreeMap::new(),
            entity_cells: BTreeMap::new(),
            cell_entities: BTreeMap::new(),
            sequences: BTreeMap::new(),
            state: InMemoryCacheService::new(),
            state_bytes: 0,
        }
    }

    fn recompute_state_bytes(&mut self) {
        self.state_bytes = self.state.byte_len();
    }

    fn remove_entity_from_cell(&mut self, entity: EntityId) {
        let Some(cell) = self.entity_cells.remove(&entity) else {
            return;
        };
        let remove_cell = self.cell_entities.get_mut(&cell).is_some_and(|entities| {
            entities.remove(&entity);
            entities.is_empty()
        });
        if remove_cell {
            self.cell_entities.remove(&cell);
        }
    }

    fn insert_entity_into_cell(&mut self, entity: EntityId, cell: SpatialCellKey) {
        self.entity_cells.insert(entity, cell);
        self.cell_entities.entry(cell).or_default().insert(entity);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorizedMessage {
    space: SpaceId,
    space_epoch: SpaceEpoch,
    entity: Option<EntityId>,
    channel: ChannelId,
    sequence: u64,
    delivery: DeliveryClass,
    persistence: PersistenceClass,
    coalesce_key: Option<CoalesceKey>,
    payload: Vec<u8>,
}

impl AuthorizedMessage {
    fn from_request(request: &PublishRequest) -> Self {
        Self {
            space: request.space,
            space_epoch: request.space_epoch,
            entity: request.entity,
            channel: request.channel,
            sequence: request.sequence,
            delivery: request.delivery,
            persistence: request.persistence,
            coalesce_key: request.coalesce_key,
            payload: request.payload.clone(),
        }
    }

    fn from_emission(emission: AuthorityEmission) -> Self {
        Self {
            space: emission.space,
            space_epoch: emission.space_epoch,
            entity: emission.entity,
            channel: emission.channel,
            sequence: emission.sequence,
            delivery: emission.delivery,
            persistence: emission.persistence,
            coalesce_key: emission.coalesce_key,
            payload: emission.payload,
        }
    }

    fn to_outbound(&self, session: SessionKey) -> OutboundMessage {
        OutboundMessage {
            namespace: session.namespace,
            session: session.session,
            space: self.space,
            space_epoch: self.space_epoch,
            entity: self.entity,
            channel: self.channel,
            sequence: self.sequence,
            delivery: self.delivery,
            persistence: self.persistence,
            coalesce_key: self.coalesce_key,
            payload: self.payload.clone(),
        }
    }
}

pub struct WovenCore<A> {
    authenticator: A,
    config: CoreConfig,
    next_connection_id: u64,
    next_entity_id: u64,
    connections: BTreeMap<ConnectionId, ConnectionState>,
    sessions: BTreeMap<SessionKey, SessionState>,
    channels: BTreeMap<ChannelId, ChannelDefinition>,
    admissions: BTreeMap<SessionKey, AdmissionController>,
    pending_admissions: BTreeMap<(ConnectionId, SessionKey), AdmissionLease>,
    admission_leases: BTreeMap<(ConnectionId, SessionKey), AdmissionLease>,
    journal_outbox: JournalOutbox,
}

impl<A: Authenticator> WovenCore<A> {
    pub fn new(authenticator: A, config: CoreConfig) -> Result<Self, CoreError> {
        let config = config.validate()?;
        Ok(Self {
            authenticator,
            config,
            next_connection_id: 1,
            next_entity_id: 1,
            connections: BTreeMap::new(),
            sessions: BTreeMap::new(),
            channels: BTreeMap::new(),
            admissions: BTreeMap::new(),
            pending_admissions: BTreeMap::new(),
            admission_leases: BTreeMap::new(),
            journal_outbox: JournalOutbox::new(config.journal_outbox_capacity),
        })
    }

    pub fn register_channel(&mut self, channel: ChannelDefinition) -> Result<(), CoreError> {
        require_nonzero(channel.id.get(), IdKind::Channel)?;
        if channel.max_payload_bytes == 0 {
            return Err(CoreError::InvalidChannelLimit(channel.id));
        }
        if self.channels.contains_key(&channel.id) {
            return Err(CoreError::ChannelAlreadyRegistered(channel.id));
        }
        if self.channels.len() == self.config.max_channels {
            return Err(CoreError::ChannelLimitReached);
        }
        self.channels.insert(channel.id, channel);
        Ok(())
    }

    pub fn provision_session(&mut self, key: SessionKey) -> Result<(), CoreError> {
        validate_session_key(key)?;
        if self.sessions.contains_key(&key) {
            return Err(CoreError::SessionAlreadyProvisioned(key));
        }
        if self.sessions.len() == self.config.max_sessions {
            return Err(CoreError::SessionLimitReached);
        }
        self.sessions.insert(key, SessionState::new());
        Ok(())
    }

    /// Enables capacity admission for an already provisioned session.
    ///
    /// The caller is the trusted deployment/control-plane adapter. Core stores no account,
    /// billing, or capacity-pool identity; it only enforces the supplied per-session limit.
    pub fn configure_session_admission(
        &mut self,
        session_key: SessionKey,
        metadata: AdmissionMetadata,
        policy: QueuePolicy,
        capacity: CapacityUpdate,
    ) -> Result<(), CoreError> {
        validate_session_key(session_key)?;
        if !self.sessions.contains_key(&session_key) {
            return Err(CoreError::SessionNotFound(session_key));
        }
        if metadata.session != session_key {
            return Err(CoreError::InvalidAdmissionLease(session_key));
        }
        if self.admissions.contains_key(&session_key) {
            return Err(CoreError::AdmissionAlreadyConfigured(session_key));
        }
        let counters = std::sync::Arc::new(UsageCounters::new(metadata));
        self.admissions.insert(
            session_key,
            AdmissionController::new(metadata, policy, capacity, counters),
        );
        Ok(())
    }

    /// Applies a trusted, monotonic capacity update to a provisioned session.
    pub fn apply_session_capacity_at(
        &mut self,
        session_key: SessionKey,
        update: CapacityUpdate,
        now: Instant,
    ) -> Result<crate::AdmissionSnapshot, CoreError> {
        validate_session_key(session_key)?;
        self.admissions
            .get_mut(&session_key)
            .map(|controller| controller.apply_capacity_at(update, now))
            .ok_or(CoreError::AdmissionLeaseRequired(session_key))
    }

    /// Requests admission for an authenticated connection to a capacity-managed session.
    pub fn request_session_admission_at(
        &mut self,
        connection: ConnectionId,
        session_key: SessionKey,
        idempotency_key: IdempotencyKey,
        now: Instant,
    ) -> Result<JoinDecision, CoreError> {
        validate_connection_id(connection)?;
        validate_session_key(session_key)?;
        let principal = self.authenticated_principal(connection)?;
        require_namespace_read(&principal, session_key.namespace)?;
        require_session_read(&principal, session_key)?;
        let controller = self
            .admissions
            .get_mut(&session_key)
            .ok_or(CoreError::AdmissionLeaseRequired(session_key))?;
        let decision = controller.request_join_at(
            crate::JoinRequest::new(principal.principal_id, idempotency_key),
            now,
        );
        if let JoinDecision::Admitted(lease) = decision {
            self.pending_admissions
                .insert((connection, session_key), lease);
            Ok(JoinDecision::Admitted(lease))
        } else {
            Ok(decision)
        }
    }

    pub fn install_space(
        &mut self,
        session_key: SessionKey,
        descriptor: SpaceDescriptor,
    ) -> Result<(), CoreError> {
        validate_session_key(session_key)?;
        descriptor.validate().map_err(CoreError::InvalidSpace)?;
        let session = self
            .sessions
            .get(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        let space_key = SpaceKey::new(session_key, descriptor.id);
        if session.spaces.contains_key(&descriptor.id) {
            return Err(CoreError::SpaceAlreadyExists(space_key));
        }
        if session.spaces.len() == self.config.max_spaces_per_session {
            return Err(CoreError::SpaceLimitReached);
        }
        if let Some(previous_epoch) = session.space_epoch_tombstones.get(&descriptor.id) {
            if descriptor.epoch <= *previous_epoch {
                return Err(CoreError::EpochDidNotAdvance {
                    current: *previous_epoch,
                    proposed: descriptor.epoch,
                });
            }
        } else if session.spaces.len() + session.space_epoch_tombstones.len()
            == self.config.max_space_epoch_tombstones_per_session
        {
            return Err(CoreError::SpaceIdHistoryLimitReached);
        }
        if let Some(parent) = descriptor.parent {
            Self::validate_parent_anchor(session, parent)?;
        }

        let session = self
            .sessions
            .get_mut(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        session.space_epoch_tombstones.remove(&descriptor.id);
        session.subscribers.insert(descriptor.id, BTreeSet::new());
        session.spaces.insert(descriptor.id, descriptor);
        Ok(())
    }

    fn validate_parent_anchor(
        session: &SessionState,
        parent: ParentAnchor,
    ) -> Result<(), CoreError> {
        if !session.spaces.contains_key(&parent.parent_space) {
            return Err(CoreError::ParentSpaceNotFound(parent.parent_space));
        }
        let anchor = session
            .entities
            .get(&parent.anchor_entity)
            .ok_or(CoreError::ParentAnchorNotFound(parent.anchor_entity))?;
        if anchor.space != parent.parent_space {
            return Err(CoreError::ParentAnchorInWrongSpace {
                entity: parent.anchor_entity,
                expected: parent.parent_space,
            });
        }
        Ok(())
    }

    pub fn advance_space_epoch(
        &mut self,
        session_key: SessionKey,
        space: SpaceId,
        proposed: SpaceEpoch,
    ) -> Result<CleanupSummary, CoreError> {
        validate_session_key(session_key)?;
        require_nonzero(space.get(), IdKind::Space)?;
        require_nonzero(proposed.get(), IdKind::SpaceEpoch)?;
        let session = self
            .sessions
            .get(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        let descriptor = session
            .spaces
            .get(&space)
            .ok_or(CoreError::SpaceNotFound(SpaceKey::new(session_key, space)))?;
        if proposed <= descriptor.epoch {
            return Err(CoreError::EpochDidNotAdvance {
                current: descriptor.epoch,
                proposed,
            });
        }
        let entities = session
            .entities
            .values()
            .filter(|entity| entity.space == space)
            .map(|entity| entity.id)
            .collect::<BTreeSet<_>>();

        let mut summary = self.remove_entities_and_anchored_spaces(session_key, entities);
        for connection in self.connections.values_mut() {
            summary.queued_messages_discarded += connection.outbound.purge(|message| {
                message.namespace == session_key.namespace
                    && message.session == session_key.session
                    && message.space == space
            });
        }
        let session = self
            .sessions
            .get_mut(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        let descriptor = session
            .spaces
            .get_mut(&space)
            .ok_or(CoreError::SpaceNotFound(SpaceKey::new(session_key, space)))?;
        descriptor.epoch = proposed;
        session.state.retain(|key| key.space != space);
        session.recompute_state_bytes();
        session.sequences.retain(|key, _| key.space != space);
        Ok(summary)
    }

    pub fn transport_connected(&mut self) -> Result<ConnectionId, CoreError> {
        if self.connections.len() == self.config.max_connections {
            return Err(CoreError::ConnectionLimitReached);
        }
        let id = ConnectionId::new(self.next_connection_id);
        self.next_connection_id = self
            .next_connection_id
            .checked_add(1)
            .ok_or(CoreError::IdExhausted)?;
        let outbound = OutboundQueue::new(self.config.outbound_queue)
            .map_err(CoreError::InvalidQueueConfig)?;
        self.connections.insert(
            id,
            ConnectionState {
                authenticated: None,
                memberships: BTreeSet::new(),
                subscriptions: BTreeSet::new(),
                owned_entities: 0,
                rate_limiter: ConnectionRateLimiter::default(),
                outbound,
            },
        );
        Ok(id)
    }

    pub fn authenticate(
        &mut self,
        connection: ConnectionId,
        credentials: &Credentials,
    ) -> Result<PrincipalId, CoreError> {
        validate_connection_id(connection)?;
        let state = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        if state.authenticated.is_some() {
            return Err(CoreError::AlreadyAuthenticated);
        }
        let principal = self
            .authenticator
            .authenticate(credentials)
            .map_err(CoreError::AuthenticationFailed)?;
        require_nonzero(principal.principal_id.get(), IdKind::Principal)?;
        let principal_id = principal.principal_id;
        self.connections
            .get_mut(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .authenticated = Some(principal);
        Ok(principal_id)
    }

    pub fn join_session(
        &mut self,
        connection: ConnectionId,
        key: SessionKey,
    ) -> Result<(), CoreError> {
        if self.admissions.contains_key(&key) {
            return Err(CoreError::AdmissionLeaseRequired(key));
        }
        self.join_session_unchecked_admission(connection, key)
    }

    /// Joins a capacity-managed session after the transport-neutral admission controller has
    /// issued a lease for this authenticated principal.
    pub fn join_session_with_admission(
        &mut self,
        connection: ConnectionId,
        key: SessionKey,
        lease: AdmissionLease,
    ) -> Result<(), CoreError> {
        validate_connection_id(connection)?;
        validate_session_key(key)?;
        let principal = self.authenticated_principal(connection)?;
        if lease.session != key || lease.principal != principal.principal_id {
            return Err(CoreError::InvalidAdmissionLease(key));
        }
        if let Some(bound) = self.admission_leases.get(&(connection, key)) {
            return if *bound == lease {
                Ok(())
            } else {
                Err(CoreError::InvalidAdmissionLease(key))
            };
        }
        if self.pending_admissions.get(&(connection, key)) != Some(&lease) {
            return Err(CoreError::InvalidAdmissionLease(key));
        }
        let controller = self
            .admissions
            .get(&key)
            .ok_or(CoreError::AdmissionLeaseRequired(key))?;
        if !controller.has_active_lease(lease)
            || self.admission_leases.values().any(|bound| *bound == lease)
        {
            return Err(CoreError::InvalidAdmissionLease(key));
        }
        self.join_session_unchecked_admission(connection, key)?;
        self.pending_admissions.remove(&(connection, key));
        self.admission_leases.insert((connection, key), lease);
        Ok(())
    }

    fn join_session_unchecked_admission(
        &mut self,
        connection: ConnectionId,
        key: SessionKey,
    ) -> Result<(), CoreError> {
        validate_connection_id(connection)?;
        validate_session_key(key)?;
        let principal = self.authenticated_principal(connection)?;
        require_namespace_read(&principal, key.namespace)?;
        require_session_read(&principal, key)?;
        if !self.sessions.contains_key(&key) {
            return Err(CoreError::SessionNotFound(key));
        }
        let connection_state = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        if connection_state.memberships.contains(&key) {
            return Ok(());
        }
        if connection_state.memberships.len() == self.config.max_memberships_per_connection {
            return Err(CoreError::MembershipLimitReached);
        }
        self.connections
            .get_mut(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .memberships
            .insert(key);
        self.sessions
            .get_mut(&key)
            .ok_or(CoreError::SessionNotFound(key))?
            .members
            .insert(connection);
        Ok(())
    }

    pub fn leave_session(
        &mut self,
        connection: ConnectionId,
        key: SessionKey,
    ) -> Result<CleanupSummary, CoreError> {
        validate_connection_id(connection)?;
        validate_session_key(key)?;
        self.require_membership(connection, key)?;
        self.release_session_admission(connection, key, crate::ReleaseReason::Intentional);
        Ok(self.detach_connection_from_session(connection, key))
    }

    pub fn subscribe(
        &mut self,
        connection: ConnectionId,
        space_key: SpaceKey,
    ) -> Result<(), CoreError> {
        validate_connection_id(connection)?;
        validate_space_key(space_key)?;
        let principal = self.authenticated_principal(connection)?;
        require_namespace_read(&principal, space_key.session.namespace)?;
        require_session_read(&principal, space_key.session)?;
        require_space_read(&principal, space_key)?;
        self.require_membership(connection, space_key.session)?;
        let connection_state = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        if connection_state.subscriptions.contains(&space_key) {
            return Ok(());
        }
        if connection_state.subscriptions.len() == self.config.max_subscriptions_per_connection {
            return Err(CoreError::SubscriptionLimitReached);
        }
        let session = self
            .sessions
            .get(&space_key.session)
            .ok_or(CoreError::SessionNotFound(space_key.session))?;
        if !session.spaces.contains_key(&space_key.space) {
            return Err(CoreError::SpaceNotFound(space_key));
        }

        self.connections
            .get_mut(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .subscriptions
            .insert(space_key);
        self.sessions
            .get_mut(&space_key.session)
            .ok_or(CoreError::SessionNotFound(space_key.session))?
            .subscribers
            .get_mut(&space_key.space)
            .ok_or(CoreError::SpaceNotFound(space_key))?
            .insert(connection);
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        connection: ConnectionId,
        space_key: SpaceKey,
    ) -> Result<CleanupSummary, CoreError> {
        validate_connection_id(connection)?;
        validate_space_key(space_key)?;
        self.authenticated_principal(connection)?;
        let removed = self
            .connections
            .get_mut(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .subscriptions
            .remove(&space_key);
        let mut summary = CleanupSummary::default();
        if removed {
            summary.subscriptions_removed = 1;
            if let Some(subscribers) = self
                .sessions
                .get_mut(&space_key.session)
                .and_then(|session| session.subscribers.get_mut(&space_key.space))
            {
                subscribers.remove(&connection);
            }
            summary.queued_messages_discarded = self
                .connections
                .get_mut(&connection)
                .ok_or(CoreError::UnknownConnection(connection))?
                .outbound
                .purge(|message| {
                    message.namespace == space_key.session.namespace
                        && message.session == space_key.session.session
                        && message.space == space_key.space
                });
        }
        Ok(summary)
    }

    pub fn spawn_entity(
        &mut self,
        connection: ConnectionId,
        space_key: SpaceKey,
        epoch: SpaceEpoch,
    ) -> Result<EntityId, CoreError> {
        validate_connection_id(connection)?;
        validate_space_key(space_key)?;
        require_nonzero(epoch.get(), IdKind::SpaceEpoch)?;
        let principal = self.authenticated_principal(connection)?;
        require_namespace_write(&principal, space_key.session.namespace)?;
        require_session_write(&principal, space_key.session)?;
        require_space_write(&principal, space_key)?;
        self.require_membership(connection, space_key.session)?;
        self.require_subscription(connection, space_key)?;
        let connection_state = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        if connection_state.owned_entities == self.config.max_owned_entities_per_connection {
            return Err(CoreError::OwnedEntityLimitReached);
        }
        let session = self
            .sessions
            .get(&space_key.session)
            .ok_or(CoreError::SessionNotFound(space_key.session))?;
        let descriptor = session
            .spaces
            .get(&space_key.space)
            .ok_or(CoreError::SpaceNotFound(space_key))?;
        if descriptor.epoch != epoch {
            return Err(CoreError::SpaceEpochMismatch {
                expected: descriptor.epoch,
                received: epoch,
            });
        }
        if session.entities.len() == self.config.max_entities_per_session {
            return Err(CoreError::EntityLimitReached);
        }

        let id = EntityId::new(self.next_entity_id);
        self.next_entity_id = self
            .next_entity_id
            .checked_add(1)
            .ok_or(CoreError::IdExhausted)?;
        self.sessions
            .get_mut(&space_key.session)
            .ok_or(CoreError::SessionNotFound(space_key.session))?
            .entities
            .insert(
                id,
                EntityRecord {
                    id,
                    owner_connection: connection,
                    owner_principal: principal.principal_id,
                    space: space_key.space,
                    space_epoch: epoch,
                    position: None,
                },
            );
        self.connections
            .get_mut(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .owned_entities += 1;
        Ok(id)
    }

    pub fn update_entity_position(
        &mut self,
        connection: ConnectionId,
        session_key: SessionKey,
        entity: EntityId,
        position: EntityPosition,
    ) -> Result<(), CoreError> {
        validate_connection_id(connection)?;
        validate_session_key(session_key)?;
        require_nonzero(entity.get(), IdKind::Entity)?;
        let principal = self.authenticated_principal(connection)?;
        require_namespace_write(&principal, session_key.namespace)?;
        require_session_write(&principal, session_key)?;
        self.require_membership(connection, session_key)?;

        let session = self
            .sessions
            .get(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        let record = session
            .entities
            .get(&entity)
            .ok_or(CoreError::EntityNotFound(entity))?;
        if record.owner_connection != connection {
            return Err(CoreError::EntityNotOwned(entity));
        }
        let space_key = SpaceKey::new(session_key, record.space);
        require_space_write(&principal, space_key)?;
        self.require_subscription(connection, space_key)?;
        let descriptor = session
            .spaces
            .get(&record.space)
            .ok_or(CoreError::SpaceNotFound(space_key))?;
        position
            .validate_for_frame(descriptor.local_frame)
            .map_err(CoreError::InvalidEntityPosition)?;
        let cell = spatial_cell_for(position, descriptor, record.space_epoch)?;

        let session = self
            .sessions
            .get_mut(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        if session.entity_cells.get(&entity).copied() != cell {
            session.remove_entity_from_cell(entity);
            if let Some(cell) = cell {
                session.insert_entity_into_cell(entity, cell);
            }
        }
        session
            .entities
            .get_mut(&entity)
            .ok_or(CoreError::EntityNotFound(entity))?
            .position = Some(position);
        Ok(())
    }

    pub fn remove_entity(
        &mut self,
        connection: ConnectionId,
        session_key: SessionKey,
        entity: EntityId,
    ) -> Result<CleanupSummary, CoreError> {
        validate_connection_id(connection)?;
        validate_session_key(session_key)?;
        require_nonzero(entity.get(), IdKind::Entity)?;
        self.authenticated_principal(connection)?;
        self.require_membership(connection, session_key)?;
        let record = self
            .sessions
            .get(&session_key)
            .and_then(|session| session.entities.get(&entity))
            .ok_or(CoreError::EntityNotFound(entity))?;
        if record.owner_connection != connection {
            return Err(CoreError::EntityNotOwned(entity));
        }
        Ok(self.remove_entities_and_anchored_spaces(session_key, BTreeSet::from([entity])))
    }

    pub fn transition_entity(
        &mut self,
        request: EntityTransitionRequest,
    ) -> Result<EntityTransition, CoreError> {
        validate_connection_id(request.connection)?;
        validate_session_key(request.session)?;
        require_nonzero(request.entity.get(), IdKind::Entity)?;
        require_nonzero(request.source_space.get(), IdKind::Space)?;
        require_nonzero(request.source_epoch.get(), IdKind::SpaceEpoch)?;
        require_nonzero(request.destination_space.get(), IdKind::Space)?;
        require_nonzero(request.destination_epoch.get(), IdKind::SpaceEpoch)?;

        let principal = self.authenticated_principal(request.connection)?;
        require_namespace_write(&principal, request.session.namespace)?;
        require_session_write(&principal, request.session)?;
        let source = SpaceKey::new(request.session, request.source_space);
        let destination = SpaceKey::new(request.session, request.destination_space);
        require_space_write(&principal, source)?;
        require_space_write(&principal, destination)?;
        self.require_membership(request.connection, request.session)?;
        self.require_subscription(request.connection, source)?;
        self.require_subscription(request.connection, destination)?;

        let session = self
            .sessions
            .get(&request.session)
            .ok_or(CoreError::SessionNotFound(request.session))?;
        let source_descriptor = session
            .spaces
            .get(&request.source_space)
            .ok_or(CoreError::SpaceNotFound(source))?;
        if source_descriptor.epoch != request.source_epoch {
            return Err(CoreError::SpaceEpochMismatch {
                expected: source_descriptor.epoch,
                received: request.source_epoch,
            });
        }
        let destination_descriptor = session
            .spaces
            .get(&request.destination_space)
            .ok_or(CoreError::SpaceNotFound(destination))?;
        if destination_descriptor.epoch != request.destination_epoch {
            return Err(CoreError::SpaceEpochMismatch {
                expected: destination_descriptor.epoch,
                received: request.destination_epoch,
            });
        }
        let record = session
            .entities
            .get(&request.entity)
            .ok_or(CoreError::EntityNotFound(request.entity))?;
        if record.owner_connection != request.connection {
            return Err(CoreError::EntityNotOwned(request.entity));
        }
        if record.space != request.source_space || record.space_epoch != request.source_epoch {
            return Err(CoreError::EntitySpaceMismatch(request.entity));
        }

        let transition = EntityTransition {
            entity: request.entity,
            session: request.session,
            source_space: request.source_space,
            source_epoch: request.source_epoch,
            destination_space: request.destination_space,
            destination_epoch: request.destination_epoch,
        };
        if request.source_space == request.destination_space
            && request.source_epoch == request.destination_epoch
        {
            return Ok(transition);
        }

        let session = self
            .sessions
            .get_mut(&request.session)
            .ok_or(CoreError::SessionNotFound(request.session))?;
        session.remove_entity_from_cell(request.entity);
        let record = session
            .entities
            .get_mut(&request.entity)
            .ok_or(CoreError::EntityNotFound(request.entity))?;
        record.space = request.destination_space;
        record.space_epoch = request.destination_epoch;
        record.position = None;
        session.state.retain(|key| {
            key.space != request.source_space
                || key.space_epoch != request.source_epoch
                || key.entity != Some(request.entity)
        });
        session.recompute_state_bytes();
        session.sequences.retain(|key, _| {
            key.space != request.source_space
                || key.space_epoch != request.source_epoch
                || key.entity != Some(request.entity)
        });
        for connection in self.connections.values_mut() {
            connection.outbound.purge(|message| {
                message.namespace == request.session.namespace
                    && message.session == request.session.session
                    && message.space == request.source_space
                    && message.space_epoch == request.source_epoch
                    && message.entity == Some(request.entity)
            });
        }
        Ok(transition)
    }

    pub fn publish(&mut self, request: PublishRequest) -> Result<PublishOutcome, CoreError> {
        self.publish_at(request, Instant::now())
    }

    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    pub fn publish_at(
        &mut self,
        request: PublishRequest,
        now: Instant,
    ) -> Result<PublishOutcome, CoreError> {
        validate_connection_id(request.connection)?;
        self.connections
            .get_mut(&request.connection)
            .ok_or(CoreError::UnknownConnection(request.connection))?
            .rate_limiter
            .admit(self.config.publish_rate_limit, now)?;
        validate_publish_request_ids(&request)?;
        let principal = self.authenticated_principal(request.connection)?;
        require_namespace_write(&principal, request.session.namespace)?;
        require_session_write(&principal, request.session)?;
        let target = SpaceKey::new(request.session, request.space);
        require_space_write(&principal, target)?;
        let channel_scope = ChannelScope::new(request.session, request.channel);
        if !principal.grants().can_write_channel(channel_scope) {
            return Err(CoreError::ChannelWriteAccessDenied(channel_scope));
        }
        self.require_membership(request.connection, request.session)?;
        self.require_subscription(request.connection, target)?;

        let channel = self
            .channels
            .get(&request.channel)
            .cloned()
            .ok_or(CoreError::UnknownChannel(request.channel))?;
        validate_channel_policy(&channel, request.delivery, request.persistence)?;
        let session = self
            .sessions
            .get(&request.session)
            .ok_or(CoreError::SessionNotFound(request.session))?;
        let owner = request
            .entity
            .and_then(|entity| session.entities.get(&entity))
            .map(|entity| entity.owner_connection);
        let proposed_authorized = AuthorizedMessage::from_request(&request);
        self.validate_message(
            request.connection,
            request.session,
            &proposed_authorized,
            None,
        )?;

        let context = AuthorityContext {
            connection: request.connection,
            principal: principal.principal_id,
            is_session_member: true,
            is_space_subscriber: true,
            entity_owner: owner,
        };
        let proposed = ProposedMessage {
            space: request.space,
            space_epoch: request.space_epoch,
            entity: request.entity,
            channel: request.channel,
            sequence: request.sequence,
            delivery: request.delivery,
            persistence: request.persistence,
            coalesce_key: request.coalesce_key,
            payload: &request.payload,
        };
        let authority_outcome = channel.authority.evaluate(&context, proposed);

        let mut planned_sequences = BTreeMap::new();
        let messages = match authority_outcome {
            AuthorityOutcome::Accept => vec![proposed_authorized],
            AuthorityOutcome::Reject(reason) => {
                return Err(CoreError::AuthorityRejected(reason));
            }
            AuthorityOutcome::Transform(transformed) => vec![AuthorizedMessage {
                coalesce_key: transformed.coalesce_key,
                payload: transformed.payload,
                ..proposed_authorized
            }],
            AuthorityOutcome::Emit(emissions) => {
                if emissions.len() > self.config.max_authority_emissions {
                    return Err(CoreError::AuthorityEmissionLimitExceeded);
                }
                planned_sequences.insert(
                    sequence_key(request.connection, &proposed_authorized),
                    proposed_authorized.sequence,
                );
                emissions
                    .into_vec()
                    .into_iter()
                    .map(AuthorizedMessage::from_emission)
                    .collect()
            }
        };

        for message in &messages {
            self.validate_message(
                request.connection,
                request.session,
                message,
                Some(&planned_sequences),
            )?;
            planned_sequences.insert(sequence_key(request.connection, message), message.sequence);
        }
        self.validate_sequence_capacity(request.session, &planned_sequences)?;
        self.validate_state_capacity(request.session, &messages)?;

        let durable_count = messages
            .iter()
            .filter(|message| message.persistence == PersistenceClass::Durable)
            .count();
        if durable_count > self.journal_outbox.remaining_capacity() {
            return Err(CoreError::JournalOutboxSaturated);
        }

        let outbound_messages = messages
            .iter()
            .map(|message| message.to_outbound(request.session))
            .collect::<Vec<_>>();
        for outbound in outbound_messages
            .iter()
            .filter(|message| message.persistence == PersistenceClass::Durable)
        {
            self.journal_outbox
                .push(JournalRecord {
                    message: outbound.clone(),
                })
                .map_err(|_| CoreError::JournalOutboxSaturated)?;
        }

        let session = self
            .sessions
            .get_mut(&request.session)
            .ok_or(CoreError::SessionNotFound(request.session))?;
        for (key, sequence) in planned_sequences {
            session.sequences.insert(key, sequence);
        }
        for message in &messages {
            if matches!(message.persistence, PersistenceClass::Stateful { .. }) {
                // TTL is a channel-level config choice, not something the publisher declares —
                // `message.persistence`'s own `ttl` is a placeholder (always `None` from the
                // transport layer, see `PersistenceClass::same_kind`); the channel's registered
                // TTL is authoritative.
                let ttl = self.channels.get(&message.channel).and_then(|channel| {
                    match channel.persistence {
                        PersistenceClass::Stateful { ttl } => ttl,
                        _ => None,
                    }
                });
                let key = state_key(message);
                let entry = CacheEntry {
                    sequence: message.sequence,
                    payload: message.payload.clone(),
                };
                if let Some(previous) = session.state.put(key, entry, ttl, now) {
                    session.state_bytes =
                        session.state_bytes.saturating_sub(previous.payload.len());
                }
                session.state_bytes += message.payload.len();
            }
        }

        let mut outcome = PublishOutcome {
            authorized_messages: messages.len(),
            ..PublishOutcome::default()
        };
        let mut disconnected = BTreeSet::new();
        for outbound in outbound_messages {
            let recipients = self
                .sessions
                .get(&request.session)
                .map(|session| routing_recipients(session, &outbound))
                .unwrap_or_default();
            for recipient in recipients {
                if disconnected.contains(&recipient) {
                    continue;
                }
                let Some(connection) = self.connections.get_mut(&recipient) else {
                    continue;
                };
                let Some(recipient_principal) = connection.authenticated.as_ref() else {
                    continue;
                };
                if !can_receive(
                    recipient_principal,
                    request.session,
                    outbound.space,
                    outbound.channel,
                ) {
                    continue;
                }
                outcome.recipient_attempts += 1;
                let queue_result = connection
                    .outbound
                    .push(outbound.clone())
                    .map_err(CoreError::Queue)?;
                record_queue_result(&mut outcome.queues, queue_result);
                if queue_result == QueuePush::CriticalCapacityExhausted {
                    disconnected.insert(recipient);
                }
            }
        }

        for connection in disconnected {
            if self.connections.contains_key(&connection) {
                self.transport_lost(connection)?;
                outcome.disconnected_slow_consumers.push(connection);
            }
        }
        Ok(outcome)
    }

    fn validate_message(
        &self,
        connection: ConnectionId,
        session_key: SessionKey,
        message: &AuthorizedMessage,
        planned: Option<&BTreeMap<SequenceKey, u64>>,
    ) -> Result<(), CoreError> {
        validate_authorized_message_ids(message)?;
        let channel = self
            .channels
            .get(&message.channel)
            .ok_or(CoreError::UnknownChannel(message.channel))?;
        validate_channel_policy(channel, message.delivery, message.persistence)?;
        let payload_limit = self.config.max_payload_bytes.min(channel.max_payload_bytes);
        if message.payload.len() > payload_limit {
            return Err(CoreError::PayloadTooLarge {
                actual: message.payload.len(),
                limit: payload_limit,
            });
        }
        validate_coalesce_key(message)?;

        let session = self
            .sessions
            .get(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        let descriptor = session
            .spaces
            .get(&message.space)
            .ok_or(CoreError::SpaceNotFound(SpaceKey::new(
                session_key,
                message.space,
            )))?;
        if descriptor.epoch != message.space_epoch {
            return Err(CoreError::SpaceEpochMismatch {
                expected: descriptor.epoch,
                received: message.space_epoch,
            });
        }
        if let Some(entity_id) = message.entity {
            let entity = session
                .entities
                .get(&entity_id)
                .ok_or(CoreError::EntityNotFound(entity_id))?;
            if entity.space != message.space || entity.space_epoch != message.space_epoch {
                return Err(CoreError::EntitySpaceMismatch(entity_id));
            }
        }

        let key = sequence_key(connection, message);
        let stored_last = session.sequences.get(&key).copied();
        let planned_last = planned.and_then(|values| values.get(&key).copied());
        if let Some(last) = stored_last.into_iter().chain(planned_last).max()
            && message.sequence <= last
        {
            return Err(CoreError::StaleSequence {
                received: message.sequence,
                last,
            });
        }
        Ok(())
    }

    fn validate_sequence_capacity(
        &self,
        session_key: SessionKey,
        planned: &BTreeMap<SequenceKey, u64>,
    ) -> Result<(), CoreError> {
        let session = self
            .sessions
            .get(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        let new_keys = planned
            .keys()
            .filter(|key| !session.sequences.contains_key(key))
            .count();
        if session.sequences.len().saturating_add(new_keys)
            > self.config.max_sequence_keys_per_session
        {
            return Err(CoreError::SequenceKeyLimitReached);
        }
        Ok(())
    }

    fn validate_state_capacity(
        &self,
        session_key: SessionKey,
        messages: &[AuthorizedMessage],
    ) -> Result<(), CoreError> {
        let session = self
            .sessions
            .get(&session_key)
            .ok_or(CoreError::SessionNotFound(session_key))?;
        let mut entries = session.state.len();
        let mut bytes = session.state_bytes;
        let mut projected = BTreeMap::new();
        for message in messages
            .iter()
            .filter(|message| matches!(message.persistence, PersistenceClass::Stateful { .. }))
        {
            let key = state_key(message);
            let previous_len = projected
                .get(&key)
                .copied()
                .or_else(|| session.state.get(&key).map(|entry| entry.payload.len()));
            if let Some(previous_len) = previous_len {
                bytes = bytes.saturating_sub(previous_len);
            } else {
                entries += 1;
            }
            bytes = bytes.saturating_add(message.payload.len());
            projected.insert(key, message.payload.len());
        }
        if entries > self.config.max_state_entries_per_session {
            return Err(CoreError::StateEntryLimitReached);
        }
        if bytes > self.config.max_state_bytes_per_session {
            return Err(CoreError::StateByteLimitReached);
        }
        Ok(())
    }

    pub fn snapshot(
        &self,
        connection: ConnectionId,
        key: SessionKey,
    ) -> Result<SessionSnapshot, CoreError> {
        validate_connection_id(connection)?;
        validate_session_key(key)?;
        let principal = self.authenticated_principal(connection)?;
        require_namespace_read(&principal, key.namespace)?;
        require_session_read(&principal, key)?;
        self.require_membership(connection, key)?;
        let connection_state = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        let session = self
            .sessions
            .get(&key)
            .ok_or(CoreError::SessionNotFound(key))?;
        let visible_spaces = connection_state
            .subscriptions
            .iter()
            .filter(|space| {
                space.session == key
                    && principal.grants().can_read_space(**space)
                    && session.spaces.contains_key(&space.space)
            })
            .map(|space| space.space)
            .collect::<BTreeSet<_>>();
        let spaces = visible_spaces
            .iter()
            .filter_map(|space| session.spaces.get(space))
            .cloned()
            .map(|mut descriptor| {
                if descriptor
                    .parent
                    .is_some_and(|parent| !visible_spaces.contains(&parent.parent_space))
                {
                    descriptor.parent = None;
                }
                SpaceSnapshot { descriptor }
            })
            .collect();
        let entities = session
            .entities
            .values()
            .filter(|entity| visible_spaces.contains(&entity.space))
            .map(|entity| EntitySnapshot {
                id: entity.id,
                owner_connection: entity.owner_connection,
                owner_principal: entity.owner_principal,
                space: entity.space,
                space_epoch: entity.space_epoch,
            })
            .collect();
        let state = session
            .state
            .iter()
            .filter(|(state_key, _)| {
                visible_spaces.contains(&state_key.space)
                    && principal
                        .grants()
                        .can_read_channel(ChannelScope::new(key, state_key.channel))
            })
            .map(|(state_key, record)| StateSnapshot {
                space: state_key.space,
                space_epoch: state_key.space_epoch,
                entity: state_key.entity,
                channel: state_key.channel,
                component: state_key.component,
                sequence: record.sequence,
                payload: record.payload.clone(),
            })
            .collect::<Vec<_>>();
        let state_bytes = state.iter().map(|record| record.payload.len()).sum();
        let subscription_count = visible_spaces
            .iter()
            .filter_map(|space| session.subscribers.get(space))
            .map(BTreeSet::len)
            .sum();
        Ok(SessionSnapshot {
            key,
            member_count: session.members.len(),
            subscription_count,
            state_bytes,
            spaces,
            entities,
            state,
        })
    }

    pub fn drain_outbound(
        &mut self,
        connection: ConnectionId,
    ) -> Result<Vec<OutboundMessage>, CoreError> {
        validate_connection_id(connection)?;
        Ok(self
            .connections
            .get_mut(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .outbound
            .drain())
    }

    pub fn pop_journal_record(&mut self) -> Option<JournalRecord> {
        self.journal_outbox.pop()
    }

    #[must_use]
    pub fn journal_outbox_len(&self) -> usize {
        self.journal_outbox.len()
    }

    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Actively reclaims every `Stateful` entry, across every session, past its configured TTL.
    /// Entries with `ttl: None` are never touched. Returns the total number of entries removed.
    /// Lazy expiry alone (hiding stale reads via `get_fresh`) isn't enough to actually free memory
    /// for state nobody touches again, so callers (`woven-transport`'s worker loop) should run
    /// this on a timer, not only in response to activity.
    pub fn sweep_expired_state(&mut self, now: Instant) -> usize {
        let mut swept = 0;
        for session in self.sessions.values_mut() {
            swept += session.state.sweep_expired(now);
            session.recompute_state_bytes();
        }
        swept
    }

    #[must_use]
    pub fn is_connected(&self, connection: ConnectionId) -> bool {
        self.connections.contains_key(&connection)
    }

    #[must_use]
    pub fn subscription_count(&self, connection: ConnectionId) -> Option<usize> {
        self.connections
            .get(&connection)
            .map(|state| state.subscriptions.len())
    }

    #[must_use]
    pub fn owned_entity_count(&self, connection: ConnectionId) -> Option<usize> {
        self.connections
            .get(&connection)
            .map(|state| state.owned_entities)
    }

    #[must_use]
    pub fn sequence_key_count(&self, session: SessionKey) -> Option<usize> {
        self.sessions
            .get(&session)
            .map(|state| state.sequences.len())
    }

    #[must_use]
    pub fn space_epoch_tombstone_count(&self, session: SessionKey) -> Option<usize> {
        self.sessions
            .get(&session)
            .map(|state| state.space_epoch_tombstones.len())
    }

    pub fn transport_lost(
        &mut self,
        connection: ConnectionId,
    ) -> Result<CleanupSummary, CoreError> {
        validate_connection_id(connection)?;
        let memberships = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .memberships
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut summary = CleanupSummary::default();
        for key in memberships {
            self.release_session_admission(connection, key, crate::ReleaseReason::Unexpected);
            summary.add(self.detach_connection_from_session(connection, key));
        }
        let pending_sessions = self
            .pending_admissions
            .keys()
            .filter_map(|(pending_connection, session)| {
                (*pending_connection == connection).then_some(*session)
            })
            .collect::<Vec<_>>();
        for session in pending_sessions {
            if let Some(lease) = self.pending_admissions.remove(&(connection, session))
                && let Some(controller) = self.admissions.get_mut(&session)
            {
                controller.release_at(lease, crate::ReleaseReason::Intentional, Instant::now());
            }
        }
        let state = self
            .connections
            .remove(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        summary.queued_messages_discarded += state.outbound.len();
        Ok(summary)
    }

    fn release_session_admission(
        &mut self,
        connection: ConnectionId,
        session: SessionKey,
        reason: crate::ReleaseReason,
    ) {
        let Some(lease) = self.admission_leases.remove(&(connection, session)) else {
            return;
        };
        if let Some(controller) = self.admissions.get_mut(&session) {
            controller.release_at(lease, reason, Instant::now());
        }
    }

    fn authenticated_principal(
        &self,
        connection: ConnectionId,
    ) -> Result<AuthenticatedPrincipal, CoreError> {
        self.connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?
            .authenticated
            .clone()
            .ok_or(CoreError::AuthenticationRequired)
    }

    fn require_membership(
        &self,
        connection: ConnectionId,
        session: SessionKey,
    ) -> Result<(), CoreError> {
        let state = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        if state.memberships.contains(&session) {
            Ok(())
        } else {
            Err(CoreError::SessionMembershipRequired(session))
        }
    }

    fn require_subscription(
        &self,
        connection: ConnectionId,
        space: SpaceKey,
    ) -> Result<(), CoreError> {
        let state = self
            .connections
            .get(&connection)
            .ok_or(CoreError::UnknownConnection(connection))?;
        if state.subscriptions.contains(&space) {
            Ok(())
        } else {
            Err(CoreError::SubscriptionRequired(space))
        }
    }

    fn detach_connection_from_session(
        &mut self,
        connection: ConnectionId,
        key: SessionKey,
    ) -> CleanupSummary {
        let owned = self
            .sessions
            .get(&key)
            .map(|session| {
                session
                    .entities
                    .values()
                    .filter(|entity| entity.owner_connection == connection)
                    .map(|entity| entity.id)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let mut summary = self.remove_entities_and_anchored_spaces(key, owned);

        if let Some(session) = self.sessions.get_mut(&key) {
            if session.members.remove(&connection) {
                summary.memberships_removed += 1;
            }
            for subscribers in session.subscribers.values_mut() {
                if subscribers.remove(&connection) {
                    summary.subscriptions_removed += 1;
                }
            }
            session
                .sequences
                .retain(|sequence_key, _| sequence_key.connection != connection);
        }
        if let Some(state) = self.connections.get_mut(&connection) {
            state.memberships.remove(&key);
            state.subscriptions.retain(|space| space.session != key);
            summary.queued_messages_discarded += state.outbound.purge(|message| {
                message.namespace == key.namespace && message.session == key.session
            });
        }
        summary
    }

    fn remove_entities_and_anchored_spaces(
        &mut self,
        session_key: SessionKey,
        mut removed_entities: BTreeSet<EntityId>,
    ) -> CleanupSummary {
        let mut removed_spaces = BTreeSet::new();
        while let Some(session) = self.sessions.get(&session_key) {
            let spaces_before = removed_spaces.len();
            for descriptor in session.spaces.values() {
                if descriptor.parent.is_some_and(|parent| {
                    removed_entities.contains(&parent.anchor_entity)
                        || removed_spaces.contains(&parent.parent_space)
                }) {
                    removed_spaces.insert(descriptor.id);
                }
            }
            let entities_before = removed_entities.len();
            for entity in session.entities.values() {
                if removed_spaces.contains(&entity.space) {
                    removed_entities.insert(entity.id);
                }
            }
            if spaces_before == removed_spaces.len() && entities_before == removed_entities.len() {
                break;
            }
        }

        let removed_owners = self
            .sessions
            .get(&session_key)
            .map(|session| {
                session
                    .entities
                    .values()
                    .filter(|entity| removed_entities.contains(&entity.id))
                    .fold(BTreeMap::new(), |mut owners, entity| {
                        *owners.entry(entity.owner_connection).or_insert(0) += 1;
                        owners
                    })
            })
            .unwrap_or_default();

        let mut summary = CleanupSummary::default();
        if let Some(session) = self.sessions.get_mut(&session_key) {
            summary.removed_entities = session
                .entities
                .values()
                .filter(|entity| removed_entities.contains(&entity.id))
                .map(|entity| RemovedEntity {
                    entity: entity.id,
                    session: session_key,
                    space: entity.space,
                    space_epoch: entity.space_epoch,
                })
                .collect();
            summary.entities_removed = summary.removed_entities.len();
            for entity in &removed_entities {
                session.remove_entity_from_cell(*entity);
            }
            session
                .entities
                .retain(|id, _| !removed_entities.contains(id));

            for space in &removed_spaces {
                if let Some(descriptor) = session.spaces.remove(space) {
                    session
                        .space_epoch_tombstones
                        .insert(descriptor.id, descriptor.epoch);
                }
                if let Some(subscribers) = session.subscribers.remove(space) {
                    summary.subscriptions_removed += subscribers.len();
                }
            }
            summary.spaces_removed = removed_spaces.len();
            session.state.retain(|key| {
                !removed_spaces.contains(&key.space)
                    && key
                        .entity
                        .is_none_or(|entity| !removed_entities.contains(&entity))
            });
            session.recompute_state_bytes();
            session.sequences.retain(|key, _| {
                !removed_spaces.contains(&key.space)
                    && key
                        .entity
                        .is_none_or(|entity| !removed_entities.contains(&entity))
            });
        }

        for (owner, count) in removed_owners {
            if let Some(connection) = self.connections.get_mut(&owner) {
                connection.owned_entities = connection.owned_entities.saturating_sub(count);
            }
        }
        for connection in self.connections.values_mut() {
            connection.subscriptions.retain(|space| {
                space.session != session_key || !removed_spaces.contains(&space.space)
            });
            summary.queued_messages_discarded += connection.outbound.purge(|message| {
                message.namespace == session_key.namespace
                    && message.session == session_key.session
                    && (removed_spaces.contains(&message.space)
                        || message
                            .entity
                            .is_some_and(|entity| removed_entities.contains(&entity)))
            });
        }
        summary
    }
}

fn spatial_cell_for(
    position: EntityPosition,
    descriptor: &SpaceDescriptor,
    epoch: SpaceEpoch,
) -> Result<Option<SpatialCellKey>, CoreError> {
    let cell = match (position, descriptor.routing) {
        (EntityPosition::Cartesian2D { x, y }, RoutingPolicy::SpatialGrid2D { cell_size, .. }) => {
            SpatialCell::Cartesian2D {
                x: grid_coordinate(x, cell_size)?,
                y: grid_coordinate(y, cell_size)?,
            }
        }
        (
            EntityPosition::Cartesian3D { x, y, z },
            RoutingPolicy::SpatialGrid3D { cell_size, .. },
        ) => SpatialCell::Cartesian3D {
            x: grid_coordinate(x, cell_size)?,
            y: grid_coordinate(y, cell_size)?,
            z: grid_coordinate(z, cell_size)?,
        },
        (_, RoutingPolicy::BroadcastAll | RoutingPolicy::TopicOnly) => return Ok(None),
        _ => {
            return Err(CoreError::InvalidEntityPosition(
                PositionValidationError::DimensionMismatch,
            ));
        }
    };
    Ok(Some(SpatialCellKey {
        space: descriptor.id,
        space_epoch: epoch,
        cell,
    }))
}

// Coordinates are finite and cell sizes positive before conversion; the range check prevents
// a truncated value from entering the index.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn grid_coordinate(value: f64, cell_size: f64) -> Result<i64, CoreError> {
    let coordinate = (value / cell_size).floor();
    if !coordinate.is_finite() || coordinate < i64::MIN as f64 || coordinate >= i64::MAX as f64 {
        return Err(CoreError::SpatialCellOutOfRange);
    }
    Ok(coordinate as i64)
}

fn routing_recipients(
    session: &SessionState,
    outbound: &OutboundMessage,
) -> BTreeSet<ConnectionId> {
    let subscribers = session
        .subscribers
        .get(&outbound.space)
        .cloned()
        .unwrap_or_default();
    let Some(descriptor) = session.spaces.get(&outbound.space) else {
        return BTreeSet::new();
    };
    if outbound.delivery.is_critical()
        || matches!(
            descriptor.routing,
            RoutingPolicy::BroadcastAll | RoutingPolicy::TopicOnly
        )
    {
        return subscribers;
    }

    let (cell_size, interest_radius, exact_distance) = match descriptor.routing {
        RoutingPolicy::SpatialGrid2D {
            cell_size,
            interest_radius,
            exact_distance,
        }
        | RoutingPolicy::SpatialGrid3D {
            cell_size,
            interest_radius,
            exact_distance,
        } => (cell_size, interest_radius, exact_distance),
        RoutingPolicy::BroadcastAll | RoutingPolicy::TopicOnly => return subscribers,
    };
    let Some(source_entity) = outbound.entity else {
        return BTreeSet::new();
    };
    let Some(source) = session.entities.get(&source_entity) else {
        return BTreeSet::new();
    };
    let Some(source_position) = source.position else {
        return BTreeSet::new();
    };
    let Some(source_cell) = session.entity_cells.get(&source_entity).copied() else {
        return BTreeSet::new();
    };
    if source_cell.space != outbound.space || source_cell.space_epoch != outbound.space_epoch {
        return BTreeSet::new();
    }

    let radius_in_cells = interest_radius / cell_size;
    let mut recipients = BTreeSet::new();
    for (candidate_cell, candidate_entities) in &session.cell_entities {
        if candidate_cell.space != outbound.space
            || candidate_cell.space_epoch != outbound.space_epoch
            || !cells_are_near(source_cell.cell, candidate_cell.cell, radius_in_cells)
        {
            continue;
        }
        for candidate_entity in candidate_entities {
            let Some(candidate) = session.entities.get(candidate_entity) else {
                continue;
            };
            let Some(candidate_position) = candidate.position else {
                continue;
            };
            if exact_distance
                && !squared_distance_within(source_position, candidate_position, interest_radius)
            {
                continue;
            }
            if subscribers.contains(&candidate.owner_connection) {
                recipients.insert(candidate.owner_connection);
            }
        }
    }
    recipients
}

// Cell distances are compared with a finite, validated radius; precision beyond f64 cannot
// improve the grid's f64 coordinate semantics.
#[allow(clippy::cast_precision_loss)]
fn cells_are_near(source: SpatialCell, candidate: SpatialCell, radius_in_cells: f64) -> bool {
    match (source, candidate) {
        (
            SpatialCell::Cartesian2D {
                x: source_x,
                y: source_y,
            },
            SpatialCell::Cartesian2D {
                x: candidate_x,
                y: candidate_y,
            },
        ) => {
            source_x.abs_diff(candidate_x) as f64 <= radius_in_cells
                && source_y.abs_diff(candidate_y) as f64 <= radius_in_cells
        }
        (
            SpatialCell::Cartesian3D {
                x: source_x,
                y: source_y,
                z: source_z,
            },
            SpatialCell::Cartesian3D {
                x: candidate_x,
                y: candidate_y,
                z: candidate_z,
            },
        ) => {
            source_x.abs_diff(candidate_x) as f64 <= radius_in_cells
                && source_y.abs_diff(candidate_y) as f64 <= radius_in_cells
                && source_z.abs_diff(candidate_z) as f64 <= radius_in_cells
        }
        _ => false,
    }
}

fn squared_distance_within(source: EntityPosition, candidate: EntityPosition, radius: f64) -> bool {
    let components: &[f64] = match (source, candidate) {
        (
            EntityPosition::Cartesian2D {
                x: source_x,
                y: source_y,
            },
            EntityPosition::Cartesian2D {
                x: candidate_x,
                y: candidate_y,
            },
        ) => &[source_x - candidate_x, source_y - candidate_y],
        (
            EntityPosition::Cartesian3D {
                x: source_x,
                y: source_y,
                z: source_z,
            },
            EntityPosition::Cartesian3D {
                x: candidate_x,
                y: candidate_y,
                z: candidate_z,
            },
        ) => &[
            source_x - candidate_x,
            source_y - candidate_y,
            source_z - candidate_z,
        ],
        _ => return false,
    };
    let scale = components
        .iter()
        .fold(radius, |largest, component| largest.max(component.abs()));
    if !scale.is_finite() {
        return false;
    }
    let squared_distance = components
        .iter()
        .map(|component| (component / scale).powi(2))
        .sum::<f64>();
    squared_distance <= (radius / scale).powi(2)
}

fn validate_session_key(key: SessionKey) -> Result<(), CoreError> {
    require_nonzero(key.namespace.get(), IdKind::Namespace)?;
    require_nonzero(key.session.get(), IdKind::Session)
}

fn validate_space_key(key: SpaceKey) -> Result<(), CoreError> {
    validate_session_key(key.session)?;
    require_nonzero(key.space.get(), IdKind::Space)
}

fn validate_connection_id(connection: ConnectionId) -> Result<(), CoreError> {
    require_nonzero(connection.get(), IdKind::Connection)
}

fn validate_publish_request_ids(request: &PublishRequest) -> Result<(), CoreError> {
    validate_session_key(request.session)?;
    require_nonzero(request.space.get(), IdKind::Space)?;
    require_nonzero(request.space_epoch.get(), IdKind::SpaceEpoch)?;
    require_nonzero(request.channel.get(), IdKind::Channel)?;
    if let Some(entity) = request.entity {
        require_nonzero(entity.get(), IdKind::Entity)?;
    }
    if let Some(key) = request.coalesce_key {
        require_nonzero(key.channel.get(), IdKind::Channel)?;
        if let Some(entity) = key.entity {
            require_nonzero(entity.get(), IdKind::Entity)?;
        }
    }
    Ok(())
}

fn validate_authorized_message_ids(message: &AuthorizedMessage) -> Result<(), CoreError> {
    require_nonzero(message.space.get(), IdKind::Space)?;
    require_nonzero(message.space_epoch.get(), IdKind::SpaceEpoch)?;
    require_nonzero(message.channel.get(), IdKind::Channel)?;
    if let Some(entity) = message.entity {
        require_nonzero(entity.get(), IdKind::Entity)?;
    }
    if let Some(key) = message.coalesce_key {
        require_nonzero(key.channel.get(), IdKind::Channel)?;
        if let Some(entity) = key.entity {
            require_nonzero(entity.get(), IdKind::Entity)?;
        }
    }
    Ok(())
}

fn require_nonzero(value: u64, kind: IdKind) -> Result<(), CoreError> {
    if value == 0 {
        Err(CoreError::ReservedZeroId(kind))
    } else {
        Ok(())
    }
}

fn require_namespace_read(
    principal: &AuthenticatedPrincipal,
    namespace: NamespaceId,
) -> Result<(), CoreError> {
    if principal.grants().can_read_namespace(namespace) {
        Ok(())
    } else {
        Err(CoreError::NamespaceReadAccessDenied(namespace))
    }
}

fn require_namespace_write(
    principal: &AuthenticatedPrincipal,
    namespace: NamespaceId,
) -> Result<(), CoreError> {
    if principal.grants().can_write_namespace(namespace) {
        Ok(())
    } else {
        Err(CoreError::NamespaceWriteAccessDenied(namespace))
    }
}

fn require_session_read(
    principal: &AuthenticatedPrincipal,
    session: SessionKey,
) -> Result<(), CoreError> {
    if principal.grants().can_read_session(session) {
        Ok(())
    } else {
        Err(CoreError::SessionReadAccessDenied(session))
    }
}

fn require_session_write(
    principal: &AuthenticatedPrincipal,
    session: SessionKey,
) -> Result<(), CoreError> {
    if principal.grants().can_write_session(session) {
        Ok(())
    } else {
        Err(CoreError::SessionWriteAccessDenied(session))
    }
}

fn require_space_read(
    principal: &AuthenticatedPrincipal,
    space: SpaceKey,
) -> Result<(), CoreError> {
    if principal.grants().can_read_space(space) {
        Ok(())
    } else {
        Err(CoreError::SpaceReadAccessDenied(space))
    }
}

fn require_space_write(
    principal: &AuthenticatedPrincipal,
    space: SpaceKey,
) -> Result<(), CoreError> {
    if principal.grants().can_write_space(space) {
        Ok(())
    } else {
        Err(CoreError::SpaceWriteAccessDenied(space))
    }
}

fn can_receive(
    principal: &AuthenticatedPrincipal,
    session: SessionKey,
    space: SpaceId,
    channel: ChannelId,
) -> bool {
    principal.grants().can_read_namespace(session.namespace)
        && principal.grants().can_read_session(session)
        && principal
            .grants()
            .can_read_space(SpaceKey::new(session, space))
        && principal
            .grants()
            .can_read_channel(ChannelScope::new(session, channel))
}

fn validate_channel_policy(
    channel: &ChannelDefinition,
    delivery: DeliveryClass,
    persistence: PersistenceClass,
) -> Result<(), CoreError> {
    if channel.delivery == delivery && channel.persistence.same_kind(persistence) {
        Ok(())
    } else {
        Err(CoreError::ChannelPolicyMismatch {
            channel: channel.id,
            expected_delivery: channel.delivery,
            received_delivery: delivery,
            expected_persistence: channel.persistence,
            received_persistence: persistence,
        })
    }
}

fn validate_coalesce_key(message: &AuthorizedMessage) -> Result<(), CoreError> {
    if message.delivery.is_replaceable() {
        let key = message.coalesce_key.ok_or(CoreError::MissingCoalesceKey)?;
        if key.channel != message.channel || key.entity != message.entity {
            return Err(CoreError::InvalidCoalesceKey);
        }
    } else if message
        .coalesce_key
        .is_some_and(|key| key.channel != message.channel || key.entity != message.entity)
    {
        return Err(CoreError::InvalidCoalesceKey);
    }
    Ok(())
}

fn sequence_key(connection: ConnectionId, message: &AuthorizedMessage) -> SequenceKey {
    SequenceKey {
        connection,
        space: message.space,
        space_epoch: message.space_epoch,
        entity: message.entity,
        channel: message.channel,
        component: message.coalesce_key.map_or(0, |key| key.component),
    }
}

fn state_key(message: &AuthorizedMessage) -> CacheKey {
    CacheKey {
        space: message.space,
        space_epoch: message.space_epoch,
        entity: message.entity,
        channel: message.channel,
        component: message.coalesce_key.map_or(0, |key| key.component),
    }
}

fn record_queue_result(activity: &mut QueueActivity, result: QueuePush) {
    match result {
        QueuePush::Queued => activity.queued += 1,
        QueuePush::QueuedCriticalAfterEviction(eviction) => {
            activity.queued += 1;
            activity.critical_evictions += 1;
            match eviction {
                QueueEviction::Latest { .. } => activity.evicted_latest += 1,
                QueueEviction::BestEffort => activity.dropped_best_effort += 1,
            }
        }
        QueuePush::ReplacedLatest => activity.replaced_latest += 1,
        QueuePush::EvictedLatest { .. } => activity.evicted_latest += 1,
        QueuePush::EvictedBestEffortForLatest | QueuePush::DroppedBestEffort => {
            activity.dropped_best_effort += 1;
        }
        QueuePush::DroppedLatest => activity.dropped_latest += 1,
        QueuePush::CriticalCapacityExhausted => activity.critical_capacity_exhausted += 1,
    }
}

#[allow(dead_code)]
const _: AccessGrant = AccessGrant::Read;
