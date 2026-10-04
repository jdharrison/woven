#![deny(unsafe_code)]

mod codec;
mod model;
mod semantics;

// FlatBuffers' generated accessors contain its audited low-level unsafe code.
// It remains private; all untrusted input enters through the verified Codec API.
#[allow(unsafe_code, warnings)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/mod.rs"));
}

pub use codec::{Codec, CodecError, CodecLimits};
pub use model::{
    AdmissionRejectionCode, AdmissionResult, AdmissionStatus, Authenticate, Authenticated,
    AuthenticationScheme, Capabilities, ClientLog, ControlPayload, DeliveryClass, EntityEntered,
    EntityLeaveReason, EntityLeft, Envelope, Hello, InferenceAccepted, InferenceCancelled,
    InferenceCompleted, InferenceExpired, InferenceFailed, InferenceProgress, InferenceRequested,
    InferenceStreamChunk, JoinSession, LeaveSession, LogLevel, MessageKind, MessagePayload,
    OpaquePayload, Ping, Pong, ProtocolError, ProtocolErrorCode, QueueCancel, QueueClaim,
    QueueHeartbeat, QueueState, QueueStatusRequest, QueueUpdate, RequestAdmission,
    RoutingPosition3D, SnapshotRequest, SpaceTransition, SubscribeSpace, SubscriptionAccepted,
    SubscriptionRejected, SubscriptionRejectionCode, ToolCallAccepted, ToolCallCompleted,
    ToolCallProposed, ToolCallRejected, ToolCallRejectionCode, UnsubscribeSpace,
};

pub const PROTOCOL_VERSION: u16 = 1;
pub const FILE_IDENTIFIER: &str = "WVN1";
/// Negotiated support for optional 3D routing positions on `EntityState` envelopes.
pub const CAPABILITY_POSITIONED_ENTITY_STATE: u64 = 1 << 0;
/// Optional session-scoped client logging; sending does not acknowledge persistence.
pub const CAPABILITY_CLIENT_LOG: u64 = 1 << 1;
/// Maximum UTF-8 byte length of a nonempty client log message.
pub const MAX_LOG_MESSAGE_BYTES: usize = 1024;
/// Capability bits implemented by this protocol/runtime version.
pub const SUPPORTED_CAPABILITY_BITS: u64 =
    CAPABILITY_POSITIONED_ENTITY_STATE | CAPABILITY_CLIENT_LOG;
