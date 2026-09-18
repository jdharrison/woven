use crate::PROTOCOL_VERSION;

macro_rules! numeric_enum {
    ($name:ident, $repr:ty, { $($variant:ident = $value:expr),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr($repr)]
        pub enum $name {
            $($variant = $value),+
        }

        impl $name {
            #[must_use]
            pub const fn value(self) -> $repr {
                self as $repr
            }

            pub(crate) fn from_wire(value: $repr) -> Option<Self> {
                match value {
                    $($value => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

numeric_enum!(MessageKind, u8, {
    Unknown = 0,
    Hello = 1,
    Capabilities = 2,
    Authenticate = 3,
    Authenticated = 4,
    JoinSession = 5,
    LeaveSession = 6,
    SubscribeSpace = 7,
    UnsubscribeSpace = 8,
    SubscriptionAccepted = 9,
    SubscriptionRejected = 10,
    EntityEntered = 11,
    EntityLeft = 12,
    EntityState = 13,
    ReliableEvent = 14,
    SnapshotRequest = 15,
    Snapshot = 16,
    SpaceTransition = 17,
    Ping = 18,
    Pong = 19,
    ProtocolError = 20,
    InferenceRequested = 21,
    InferenceAccepted = 22,
    InferenceProgress = 23,
    InferenceStreamChunk = 24,
    InferenceCompleted = 25,
    InferenceFailed = 26,
    InferenceCancelled = 27,
    InferenceExpired = 28,
    ToolCallProposed = 29,
    ToolCallAccepted = 30,
    ToolCallRejected = 31,
    ToolCallCompleted = 32,
    RequestAdmission = 33,
    AdmissionResult = 34,
    QueueStatusRequest = 35,
    QueueHeartbeat = 36,
    QueueClaim = 37,
    QueueCancel = 38,
    QueueUpdate = 39,
});

numeric_enum!(DeliveryClass, u8, {
    Unknown = 0,
    ReliableOrdered = 1,
    ReliableUnordered = 2,
    LatestValue = 3,
    UnreliableSequenced = 4,
    BestEffortEvent = 5,
});

impl DeliveryClass {
    /// Returns `true` for delivery classes that should be carried on
    /// unreliable datagram transports rather than reliable streams.
    #[must_use]
    pub const fn is_unreliable(self) -> bool {
        matches!(self, Self::UnreliableSequenced | Self::BestEffortEvent)
    }
}

numeric_enum!(AuthenticationScheme, u8, {
    Unknown = 0,
    Bearer = 1,
    Development = 2,
});

numeric_enum!(SubscriptionRejectionCode, u16, {
    Unknown = 0,
    Unauthorized = 1,
    SpaceNotFound = 2,
    CapacityExceeded = 3,
    EpochMismatch = 4,
});

numeric_enum!(EntityLeaveReason, u8, {
    Unknown = 0,
    Unsubscribed = 1,
    Disconnected = 2,
    Transitioned = 3,
    Removed = 4,
});

numeric_enum!(ProtocolErrorCode, u16, {
    Unknown = 0,
    MalformedFrame = 1,
    UnsupportedVersion = 2,
    UnsupportedMessage = 3,
    AuthenticationRequired = 4,
    Unauthorized = 5,
    InvalidScope = 6,
    StaleEpoch = 7,
    SequenceRejected = 8,
    PayloadTooLarge = 9,
    RateLimited = 10,
    Internal = 11,
});

numeric_enum!(ToolCallRejectionCode, u8, {
    Unknown = 0,
    Stale = 1,
    Unauthorized = 2,
    InvalidArguments = 3,
    PolicyDenied = 4,
});

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hello {
    pub min_protocol_version: u16,
    pub max_protocol_version: u16,
    pub client_name: String,
    pub client_version: String,
    pub capability_bits: u64,
    pub max_frame_size: u32,
    pub max_payload_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Capabilities {
    pub selected_protocol_version: u16,
    pub server_name: String,
    pub server_version: String,
    pub capability_bits: u64,
    pub max_frame_size: u32,
    pub max_payload_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authenticate {
    pub scheme: AuthenticationScheme,
    pub credentials: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Authenticated {
    pub principal_id: u64,
    pub assigned_entity_id: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinSession {
    pub resume_token: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaveSession {
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscribeSpace;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsubscribeSpace {
    pub subscription_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionAccepted {
    pub subscription_id: u64,
    pub accepted_space_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionRejected {
    pub code: SubscriptionRejectionCode,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityEntered {
    pub owner_entity_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityLeft {
    pub reason: EntityLeaveReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotRequest {
    pub after_server_tick: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpaceTransition {
    pub from_space_id: u64,
    pub to_space_id: u64,
    pub to_space_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ping {
    pub nonce: u64,
    pub sender_time_micros: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pong {
    pub nonce: u64,
    pub sender_time_micros: u64,
    pub responder_time_micros: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolError {
    pub code: ProtocolErrorCode,
    pub related_message_kind: MessageKind,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceRequested {
    pub capability: String,
    pub deadline_ms: u64,
    pub input: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InferenceAccepted {
    pub queued_position: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InferenceProgress {
    pub percent: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceStreamChunk {
    pub sequence: u32,
    pub chunk: Vec<u8>,
    pub is_final: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceCompleted {
    pub result: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceFailed {
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceCancelled {
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceExpired {
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolCallProposed {
    pub tool_id: String,
    pub tool_version: u32,
    pub arguments: Vec<u8>,
    pub expected_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolCallAccepted {
    pub tool_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolCallRejected {
    pub code: ToolCallRejectionCode,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolCallCompleted {
    pub new_revision: u64,
    pub result: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlPayload {
    RequestAdmission(RequestAdmission),
    AdmissionResult(AdmissionResult),
    QueueStatusRequest(QueueStatusRequest),
    QueueHeartbeat(QueueHeartbeat),
    QueueClaim(QueueClaim),
    QueueCancel(QueueCancel),
    QueueUpdate(QueueUpdate),
    Hello(Hello),
    Capabilities(Capabilities),
    Authenticate(Authenticate),
    Authenticated(Authenticated),
    JoinSession(JoinSession),
    LeaveSession(LeaveSession),
    SubscribeSpace(SubscribeSpace),
    UnsubscribeSpace(UnsubscribeSpace),
    SubscriptionAccepted(SubscriptionAccepted),
    SubscriptionRejected(SubscriptionRejected),
    EntityEntered(EntityEntered),
    EntityLeft(EntityLeft),
    SnapshotRequest(SnapshotRequest),
    SpaceTransition(SpaceTransition),
    Ping(Ping),
    Pong(Pong),
    ProtocolError(ProtocolError),
    InferenceRequested(InferenceRequested),
    InferenceAccepted(InferenceAccepted),
    InferenceProgress(InferenceProgress),
    InferenceStreamChunk(InferenceStreamChunk),
    InferenceCompleted(InferenceCompleted),
    InferenceFailed(InferenceFailed),
    InferenceCancelled(InferenceCancelled),
    InferenceExpired(InferenceExpired),
    ToolCallProposed(ToolCallProposed),
    ToolCallAccepted(ToolCallAccepted),
    ToolCallRejected(ToolCallRejected),
    ToolCallCompleted(ToolCallCompleted),
}

impl ControlPayload {
    #[must_use]
    pub const fn message_kind(&self) -> MessageKind {
        match self {
            Self::RequestAdmission(_) => MessageKind::RequestAdmission,
            Self::AdmissionResult(_) => MessageKind::AdmissionResult,
            Self::QueueStatusRequest(_) => MessageKind::QueueStatusRequest,
            Self::QueueHeartbeat(_) => MessageKind::QueueHeartbeat,
            Self::QueueClaim(_) => MessageKind::QueueClaim,
            Self::QueueCancel(_) => MessageKind::QueueCancel,
            Self::QueueUpdate(_) => MessageKind::QueueUpdate,
            Self::Hello(_) => MessageKind::Hello,
            Self::Capabilities(_) => MessageKind::Capabilities,
            Self::Authenticate(_) => MessageKind::Authenticate,
            Self::Authenticated(_) => MessageKind::Authenticated,
            Self::JoinSession(_) => MessageKind::JoinSession,
            Self::LeaveSession(_) => MessageKind::LeaveSession,
            Self::SubscribeSpace(_) => MessageKind::SubscribeSpace,
            Self::UnsubscribeSpace(_) => MessageKind::UnsubscribeSpace,
            Self::SubscriptionAccepted(_) => MessageKind::SubscriptionAccepted,
            Self::SubscriptionRejected(_) => MessageKind::SubscriptionRejected,
            Self::EntityEntered(_) => MessageKind::EntityEntered,
            Self::EntityLeft(_) => MessageKind::EntityLeft,
            Self::SnapshotRequest(_) => MessageKind::SnapshotRequest,
            Self::SpaceTransition(_) => MessageKind::SpaceTransition,
            Self::Ping(_) => MessageKind::Ping,
            Self::Pong(_) => MessageKind::Pong,
            Self::ProtocolError(_) => MessageKind::ProtocolError,
            Self::InferenceRequested(_) => MessageKind::InferenceRequested,
            Self::InferenceAccepted(_) => MessageKind::InferenceAccepted,
            Self::InferenceProgress(_) => MessageKind::InferenceProgress,
            Self::InferenceStreamChunk(_) => MessageKind::InferenceStreamChunk,
            Self::InferenceCompleted(_) => MessageKind::InferenceCompleted,
            Self::InferenceFailed(_) => MessageKind::InferenceFailed,
            Self::InferenceCancelled(_) => MessageKind::InferenceCancelled,
            Self::InferenceExpired(_) => MessageKind::InferenceExpired,
            Self::ToolCallProposed(_) => MessageKind::ToolCallProposed,
            Self::ToolCallAccepted(_) => MessageKind::ToolCallAccepted,
            Self::ToolCallRejected(_) => MessageKind::ToolCallRejected,
            Self::ToolCallCompleted(_) => MessageKind::ToolCallCompleted,
        }
    }

    pub(crate) fn variable_len(&self) -> usize {
        match self {
            Self::Hello(value) => value
                .client_name
                .len()
                .saturating_add(value.client_version.len()),
            Self::Capabilities(value) => value
                .server_name
                .len()
                .saturating_add(value.server_version.len()),
            Self::RequestAdmission(value) => value.idempotency_key.len(),

            Self::Authenticate(value) => value.credentials.len(),
            Self::JoinSession(value) => value.resume_token.len(),
            Self::LeaveSession(value) => value.reason.len(),
            Self::SubscriptionRejected(value) => value.reason.len(),
            Self::ProtocolError(value) => value.message.len(),
            Self::InferenceRequested(value) => {
                value.capability.len().saturating_add(value.input.len())
            }
            Self::InferenceStreamChunk(value) => value.chunk.len(),
            Self::InferenceCompleted(value) => value.result.len(),
            Self::InferenceFailed(value) => value.reason.len(),
            Self::InferenceCancelled(value) => value.reason.len(),
            Self::InferenceExpired(value) => value.reason.len(),
            Self::ToolCallProposed(value) => {
                value.tool_id.len().saturating_add(value.arguments.len())
            }
            Self::ToolCallAccepted(value) => value.tool_id.len(),
            Self::ToolCallRejected(value) => value.reason.len(),
            Self::ToolCallCompleted(value) => value.result.len(),
            Self::AdmissionResult(_)
            | Self::QueueStatusRequest(_)
            | Self::QueueHeartbeat(_)
            | Self::QueueClaim(_)
            | Self::QueueCancel(_)
            | Self::QueueUpdate(_)
            | Self::Authenticated(_)
            | Self::SubscribeSpace(_)
            | Self::UnsubscribeSpace(_)
            | Self::SubscriptionAccepted(_)
            | Self::EntityEntered(_)
            | Self::EntityLeft(_)
            | Self::SnapshotRequest(_)
            | Self::SpaceTransition(_)
            | Self::Ping(_)
            | Self::Pong(_)
            | Self::InferenceAccepted(_)
            | Self::InferenceProgress(_) => 0,
        }
    }
}

numeric_enum!(AdmissionStatus, u8, {
    Unknown = 0, Admitted = 1, Queued = 2, Paused = 3, Rejected = 4,
});
numeric_enum!(AdmissionRejectionCode, u8, {
    None = 0, ServerPaused = 1, QueueFull = 2, QueueDisabled = 3,
    AlreadyQueued = 4, InvalidIdempotencyKey = 5,
});
numeric_enum!(QueueState, u8, {
    Unknown = 0, Waiting = 1, Offered = 2, Admitted = 3,
    Cancelled = 4, Expired = 5, Missing = 6,
});

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestAdmission {
    pub idempotency_key: String,
}

/// Sanitized result: admitted means the worker has already joined the session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionResult {
    pub status: AdmissionStatus,
    pub rejection_code: AdmissionRejectionCode,
    pub ticket_id: Option<u64>,
    /// Advisory only; never a permit or a promise of capacity.
    pub poll_after_ms: u32,
    /// Zero means unavailable; never synthesize a lifetime from configured TTL.
    pub ticket_remaining_ms: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueStatusRequest {
    pub ticket_id: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueHeartbeat {
    pub ticket_id: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueClaim {
    pub ticket_id: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueCancel {
    pub ticket_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueUpdate {
    pub ticket_id: u64,
    pub state: QueueState,
    /// One-based, present only while waiting.
    pub position: u32,
    pub poll_after_ms: u32,
    /// Zero means unavailable.
    pub ticket_remaining_ms: u32,
    /// Zero means unavailable.
    pub offer_remaining_ms: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpaquePayload {
    pub type_id: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MessagePayload {
    Control(ControlPayload),
    EntityState(OpaquePayload),
    ReliableEvent(OpaquePayload),
    Snapshot(OpaquePayload),
}

impl MessagePayload {
    #[must_use]
    pub const fn message_kind(&self) -> MessageKind {
        match self {
            Self::Control(control) => control.message_kind(),
            Self::EntityState(_) => MessageKind::EntityState,
            Self::ReliableEvent(_) => MessageKind::ReliableEvent,
            Self::Snapshot(_) => MessageKind::Snapshot,
        }
    }

    #[must_use]
    pub const fn opaque(&self) -> Option<&OpaquePayload> {
        match self {
            Self::Control(_) => None,
            Self::EntityState(payload) | Self::ReliableEvent(payload) | Self::Snapshot(payload) => {
                Some(payload)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub protocol_version: u16,
    pub delivery_class: DeliveryClass,
    pub namespace_id: u64,
    pub session_id: u64,
    pub space_id: u64,
    pub channel_id: Option<u64>,
    pub entity_id: Option<u64>,
    pub space_epoch: u64,
    pub server_tick: u64,
    pub sender_sequence: u64,
    pub correlation_id: Option<u64>,
    pub message: MessagePayload,
}

impl Envelope {
    #[must_use]
    pub const fn new(delivery_class: DeliveryClass, message: MessagePayload) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            delivery_class,
            namespace_id: 0,
            session_id: 0,
            space_id: 0,
            channel_id: None,
            entity_id: None,
            space_epoch: 0,
            server_tick: 0,
            sender_sequence: 0,
            correlation_id: None,
            message,
        }
    }

    #[must_use]
    pub const fn control(delivery_class: DeliveryClass, payload: ControlPayload) -> Self {
        Self::new(delivery_class, MessagePayload::Control(payload))
    }

    #[must_use]
    pub const fn entity_state(delivery_class: DeliveryClass, payload: OpaquePayload) -> Self {
        Self::new(delivery_class, MessagePayload::EntityState(payload))
    }

    #[must_use]
    pub const fn reliable_event(delivery_class: DeliveryClass, payload: OpaquePayload) -> Self {
        Self::new(delivery_class, MessagePayload::ReliableEvent(payload))
    }

    #[must_use]
    pub const fn snapshot(delivery_class: DeliveryClass, payload: OpaquePayload) -> Self {
        Self::new(delivery_class, MessagePayload::Snapshot(payload))
    }

    #[must_use]
    pub const fn message_kind(&self) -> MessageKind {
        self.message.message_kind()
    }

    #[must_use]
    pub const fn payload_type_id(&self) -> Option<u64> {
        match self.message.opaque() {
            Some(payload) => Some(payload.type_id),
            None => None,
        }
    }

    #[must_use]
    pub fn payload_bytes(&self) -> &[u8] {
        self.message
            .opaque()
            .map_or(&[], |payload| payload.bytes.as_slice())
    }
}
