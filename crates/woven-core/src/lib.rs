#![forbid(unsafe_code)]

mod admission;
mod auth;
mod authority;
mod cache;
mod core;
mod ids;
mod journal;
mod model;
mod queue;
mod usage;
mod worker;

pub use admission::{
    AdmissionController, AdmissionLease, AdmissionMetadata, AdmissionSnapshot, CancelResult,
    CapacityUpdate, ClaimError, IdempotencyKey, JoinDecision, JoinRequest, QueueOperation,
    QueuePolicy, QueueStatus, QueueTicket, QueueTicketId, RejectionReason, ReleaseReason,
    ResumeToken,
};
pub use auth::{
    AccessGrant, AuthError, AuthenticatedPrincipal, Authenticator, AuthorizationGrants,
    ChannelScope, Credentials, DevAuthenticator, DevAuthenticatorError,
};
pub use authority::{
    AuthorityContext, AuthorityEmission, AuthorityOutcome, AuthorityPolicy, AuthorityRejection,
    AuthorityTransform, ChannelDefinition, ProposedMessage, RelayOwned,
};
pub use cache::{CacheEntry, CacheError, CacheKey, CacheService, InMemoryCacheService};
pub use core::managed::{
    MANAGED_CHECKPOINT_VERSION, MANAGED_MIGRATION_BASELINE_VERSION, MAX_MANAGED_CCU,
    MAX_MANAGED_HISTORY, MAX_MANAGED_MIGRATION_ID_BYTES, MAX_MANAGED_SCOPES, MAX_MANAGED_SPACES,
    MAX_MANAGED_SPATIAL_SPACES, ManagedAdmissionSnapshot, ManagedBounds3D, ManagedCheckpoint,
    ManagedCheckpointMutation, ManagedCheckpointScope, ManagedCheckpointSpace, ManagedError,
    ManagedMigrationBaseline, ManagedOutcome, ManagedPoint3D, ManagedRequest, ManagedSnapshot,
    ManagedSpaceSnapshot,
};
pub use core::{
    CleanupSummary, CoreConfig, CoreError, EntityTransition, EntityTransitionRequest, IdKind,
    PublishOutcome, PublishRateLimit, PublishRequest, QueueActivity, RemovedEntity, WovenCore,
};
pub use ids::{
    ChannelId, ConnectionId, EntityId, NamespaceId, NodeId, PrincipalId, SessionId, SessionKey,
    SpaceEpoch, SpaceId, SpaceKey,
};
pub use journal::{
    JournalError, JournalOutbox, JournalOutboxError, JournalRecord, JournalSink, NoopJournalSink,
};
pub use model::{
    CoalesceKey, CoordinateFrame, DeliveryClass, EntityPosition, EntitySnapshot, OutboundMessage,
    ParentAnchor, PersistenceClass, PositionValidationError, RoutingPolicy, ScopedCoalesceKey,
    SessionSnapshot, SpaceDescriptor, SpaceSnapshot, SpaceValidationError, SpatialBounds3D,
    StateSnapshot,
};
pub use queue::{
    OutboundQueue, OutboundQueueConfig, QueueConfigError, QueueError, QueueEviction, QueuePush,
};
pub use usage::{
    ConnectionHandle, DEFAULT_USAGE_WINDOW, JsonlFileSink, MemoryUsageSink, NoopUsageSink,
    SinkHealth, SpoolingUsageSink, USAGE_SCHEMA_VERSION, UsageAggregator, UsageCounters,
    UsageMetrics, UsageSink, UsageSinkError, UsageWindow,
};
pub use worker::{Command, CommandResult, HarnessError, TransportIndependentWorker, WorkerHarness};
