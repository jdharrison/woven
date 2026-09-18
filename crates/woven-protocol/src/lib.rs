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
    AuthenticationScheme, Capabilities, ControlPayload, DeliveryClass, EntityEntered,
    EntityLeaveReason, EntityLeft, Envelope, Hello, InferenceAccepted, InferenceCancelled,
    InferenceCompleted, InferenceExpired, InferenceFailed, InferenceProgress, InferenceRequested,
    InferenceStreamChunk, JoinSession, LeaveSession, MessageKind, MessagePayload, OpaquePayload,
    Ping, Pong, ProtocolError, ProtocolErrorCode, QueueCancel, QueueClaim, QueueHeartbeat,
    QueueState, QueueStatusRequest, QueueUpdate, RequestAdmission, SnapshotRequest,
    SpaceTransition, SubscribeSpace, SubscriptionAccepted, SubscriptionRejected,
    SubscriptionRejectionCode, ToolCallAccepted, ToolCallCompleted, ToolCallProposed,
    ToolCallRejected, ToolCallRejectionCode, UnsubscribeSpace,
};

pub const PROTOCOL_VERSION: u16 = 1;
pub const FILE_IDENTIFIER: &str = "WVN1";
