use crate::{
    AdmissionRejectionCode, AdmissionResult, AdmissionStatus, QueueCancel, QueueClaim,
    QueueHeartbeat, QueueState, QueueStatusRequest, QueueUpdate, RequestAdmission,
};
use std::fmt;

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::generated::woven::protocol::v1 as wire;
use crate::{
    Authenticate, Authenticated, AuthenticationScheme, Capabilities, ControlPayload, DeliveryClass,
    EntityEntered, EntityLeaveReason, EntityLeft, Envelope, FILE_IDENTIFIER, Hello,
    InferenceAccepted, InferenceCancelled, InferenceCompleted, InferenceExpired, InferenceFailed,
    InferenceProgress, InferenceRequested, InferenceStreamChunk, JoinSession, LeaveSession,
    MessageKind, MessagePayload, OpaquePayload, PROTOCOL_VERSION, Ping, Pong, ProtocolError,
    ProtocolErrorCode, SnapshotRequest, SpaceTransition, SubscribeSpace, SubscriptionAccepted,
    SubscriptionRejected, SubscriptionRejectionCode, ToolCallAccepted, ToolCallCompleted,
    ToolCallProposed, ToolCallRejected, ToolCallRejectionCode, UnsubscribeSpace,
};

const SIZE_PREFIX_LEN: usize = 4;
const MIN_ENCODED_LEN: u32 = 8;
const MIN_FRAME_LEN: usize = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CodecLimits {
    pub max_frame_len: usize,
    pub max_payload_len: usize,
}

impl CodecLimits {
    pub fn new(max_frame_len: usize, max_payload_len: usize) -> Result<Self, CodecError> {
        let max_representable_frame = (u32::MAX as usize).saturating_add(SIZE_PREFIX_LEN);
        if max_frame_len < MIN_FRAME_LEN
            || max_frame_len > max_representable_frame
            || max_payload_len > max_frame_len
        {
            return Err(CodecError::InvalidLimits {
                max_frame_len,
                max_payload_len,
            });
        }

        Ok(Self {
            max_frame_len,
            max_payload_len,
        })
    }
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame_len: 1024 * 1024,
            max_payload_len: 256 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    InvalidLimits {
        max_frame_len: usize,
        max_payload_len: usize,
    },
    FrameTooLarge {
        actual: usize,
        maximum: usize,
    },
    PayloadTooLarge {
        actual: usize,
        maximum: usize,
    },
    TruncatedFrame {
        expected: usize,
        actual: usize,
    },
    TrailingBytes {
        expected: usize,
        actual: usize,
    },
    InvalidSizePrefix(u32),
    InvalidFileIdentifier,
    InvalidFlatbuffer(String),
    UnsupportedProtocolVersion(u16),
    UnknownMessageKind(u8),
    UnknownDeliveryClass(u8),
    UnsupportedEnumValue {
        name: &'static str,
        value: u64,
    },
    MessageControlMismatch {
        message_kind: u8,
        control_kind: u8,
    },
    MissingPayloadType(MessageKind),
    UnexpectedDomainPayload(MessageKind),
    InvalidSemantics {
        message_kind: MessageKind,
        reason: &'static str,
    },
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits {
                max_frame_len,
                max_payload_len,
            } => write!(
                formatter,
                "invalid codec limits: frame={max_frame_len}, payload={max_payload_len}"
            ),
            Self::FrameTooLarge { actual, maximum } => {
                write!(formatter, "frame length {actual} exceeds limit {maximum}")
            }
            Self::PayloadTooLarge { actual, maximum } => {
                write!(formatter, "payload length {actual} exceeds limit {maximum}")
            }
            Self::TruncatedFrame { expected, actual } => write!(
                formatter,
                "truncated frame: expected {expected} bytes, received {actual}"
            ),
            Self::TrailingBytes { expected, actual } => write!(
                formatter,
                "frame has trailing bytes: expected {expected} bytes, received {actual}"
            ),
            Self::InvalidSizePrefix(prefix) => {
                write!(formatter, "invalid FlatBuffers size prefix {prefix}")
            }
            Self::InvalidFileIdentifier => {
                write!(
                    formatter,
                    "frame does not have the {FILE_IDENTIFIER} identifier"
                )
            }
            Self::InvalidFlatbuffer(error) => {
                write!(formatter, "FlatBuffers verification failed: {error}")
            }
            Self::UnsupportedProtocolVersion(version) => {
                write!(formatter, "unsupported protocol version {version}")
            }
            Self::UnknownMessageKind(value) => write!(formatter, "unknown message kind {value}"),
            Self::UnknownDeliveryClass(value) => {
                write!(formatter, "unknown delivery class {value}")
            }
            Self::UnsupportedEnumValue { name, value } => {
                write!(formatter, "unsupported {name} value {value}")
            }
            Self::MessageControlMismatch {
                message_kind,
                control_kind,
            } => write!(
                formatter,
                "message kind {message_kind} does not match control union kind {control_kind}"
            ),
            Self::MissingPayloadType(kind) => {
                write!(formatter, "{kind:?} requires a non-zero payload type ID")
            }
            Self::UnexpectedDomainPayload(kind) => {
                write!(
                    formatter,
                    "control message {kind:?} carries domain payload fields"
                )
            }
            Self::InvalidSemantics {
                message_kind,
                reason,
            } => write!(formatter, "invalid {message_kind:?} semantics: {reason}"),
        }
    }
}

impl std::error::Error for CodecError {}

#[derive(Clone, Debug, Default)]
pub struct Codec {
    limits: CodecLimits,
}

impl Codec {
    pub fn new(limits: CodecLimits) -> Result<Self, CodecError> {
        CodecLimits::new(limits.max_frame_len, limits.max_payload_len)?;
        Ok(Self { limits })
    }

    #[must_use]
    pub const fn limits(&self) -> CodecLimits {
        self.limits
    }

    pub fn expected_frame_len(&self, prefix: &[u8]) -> Result<Option<usize>, CodecError> {
        if prefix.len() < SIZE_PREFIX_LEN {
            return Ok(None);
        }

        let mut encoded_len = [0_u8; SIZE_PREFIX_LEN];
        encoded_len.copy_from_slice(&prefix[..SIZE_PREFIX_LEN]);
        let encoded_len = u32::from_le_bytes(encoded_len);
        if encoded_len < MIN_ENCODED_LEN {
            return Err(CodecError::InvalidSizePrefix(encoded_len));
        }

        let frame_len = (encoded_len as usize).saturating_add(SIZE_PREFIX_LEN);
        self.enforce_frame_limit(frame_len)?;
        Ok(Some(frame_len))
    }

    pub fn encode(&self, envelope: &Envelope) -> Result<Vec<u8>, CodecError> {
        self.validate_outbound(envelope)?;

        let mut builder = FlatBufferBuilder::new();
        let (control_type, control) = match &envelope.message {
            MessagePayload::Control(payload) => {
                let (kind, offset) =
                    encode_control(&mut builder, payload, envelope.channel_id.unwrap_or(0));
                (kind, Some(offset))
            }
            MessagePayload::EntityState(_)
            | MessagePayload::ReliableEvent(_)
            | MessagePayload::Snapshot(_) => (wire::ControlPayload::NONE, None),
        };
        let payload = envelope
            .message
            .opaque()
            .map(|payload| builder.create_vector(payload.bytes.as_slice()));
        let payload_type_id = envelope
            .message
            .opaque()
            .map_or(0, |payload| payload.type_id);
        let root = wire::Envelope::create(
            &mut builder,
            &wire::EnvelopeArgs {
                protocol_version: envelope.protocol_version,
                message_kind: wire::MessageKind(envelope.message_kind().value()),
                delivery_class: wire::DeliveryClass(envelope.delivery_class.value()),
                namespace_id: envelope.namespace_id,
                session_id: envelope.session_id,
                space_id: envelope.space_id,
                entity_id: envelope.entity_id.unwrap_or(0),
                space_epoch: envelope.space_epoch,
                server_tick: envelope.server_tick,
                sender_sequence: envelope.sender_sequence,
                correlation_id: envelope.correlation_id.unwrap_or(0),
                payload_type_id,
                payload,
                control_type,
                control,
                channel_id: envelope.channel_id.unwrap_or(0),
            },
        );
        wire::finish_size_prefixed_envelope_buffer(&mut builder, root);
        let encoded = builder.finished_data().to_vec();
        self.enforce_frame_limit(encoded.len())?;
        Ok(encoded)
    }

    pub fn decode(&self, frame: &[u8]) -> Result<Envelope, CodecError> {
        let expected = self
            .expected_frame_len(frame)?
            .ok_or(CodecError::TruncatedFrame {
                expected: SIZE_PREFIX_LEN,
                actual: frame.len(),
            })?;
        match frame.len().cmp(&expected) {
            std::cmp::Ordering::Less => {
                return Err(CodecError::TruncatedFrame {
                    expected,
                    actual: frame.len(),
                });
            }
            std::cmp::Ordering::Greater => {
                return Err(CodecError::TrailingBytes {
                    expected,
                    actual: frame.len(),
                });
            }
            std::cmp::Ordering::Equal => {}
        }

        if !wire::envelope_size_prefixed_buffer_has_identifier(frame) {
            return Err(CodecError::InvalidFileIdentifier);
        }
        let envelope = wire::size_prefixed_root_as_envelope(frame)
            .map_err(|error| CodecError::InvalidFlatbuffer(error.to_string()))?;
        self.decode_verified(envelope)
    }

    fn validate_outbound(&self, envelope: &Envelope) -> Result<(), CodecError> {
        if envelope.protocol_version != PROTOCOL_VERSION {
            return Err(CodecError::UnsupportedProtocolVersion(
                envelope.protocol_version,
            ));
        }
        if envelope.delivery_class == DeliveryClass::Unknown {
            return Err(CodecError::UnknownDeliveryClass(
                envelope.delivery_class.value(),
            ));
        }
        crate::semantics::validate(envelope)?;

        match &envelope.message {
            MessagePayload::Control(control) => {
                self.enforce_payload_limit(control.variable_len())?;
            }
            MessagePayload::EntityState(payload)
            | MessagePayload::ReliableEvent(payload)
            | MessagePayload::Snapshot(payload) => {
                if payload.type_id == 0 {
                    return Err(CodecError::MissingPayloadType(envelope.message_kind()));
                }
                self.enforce_payload_limit(payload.bytes.len())?;
            }
        }
        Ok(())
    }

    fn decode_verified(&self, envelope: wire::Envelope<'_>) -> Result<Envelope, CodecError> {
        let protocol_version = envelope.protocol_version();
        if protocol_version != PROTOCOL_VERSION {
            return Err(CodecError::UnsupportedProtocolVersion(protocol_version));
        }

        let message_kind = MessageKind::from_wire(envelope.message_kind().0)
            .filter(|kind| *kind != MessageKind::Unknown)
            .ok_or(CodecError::UnknownMessageKind(envelope.message_kind().0))?;
        let delivery_class = DeliveryClass::from_wire(envelope.delivery_class().0)
            .filter(|class| *class != DeliveryClass::Unknown)
            .ok_or(CodecError::UnknownDeliveryClass(
                envelope.delivery_class().0,
            ))?;

        let message = match message_kind {
            MessageKind::EntityState | MessageKind::ReliableEvent | MessageKind::Snapshot => {
                self.decode_opaque(&envelope, message_kind)?
            }
            MessageKind::Unknown => return Err(CodecError::UnknownMessageKind(0)),
            _ => {
                if envelope.payload_type_id() != 0 || envelope.payload().is_some() {
                    return Err(CodecError::UnexpectedDomainPayload(message_kind));
                }
                MessagePayload::Control(decode_control(self, &envelope, message_kind)?)
            }
        };

        let decoded = Envelope {
            protocol_version,
            delivery_class,
            namespace_id: envelope.namespace_id(),
            session_id: envelope.session_id(),
            space_id: envelope.space_id(),
            channel_id: nonzero(envelope.channel_id()),
            entity_id: nonzero(envelope.entity_id()),
            space_epoch: envelope.space_epoch(),
            server_tick: envelope.server_tick(),
            sender_sequence: envelope.sender_sequence(),
            correlation_id: nonzero(envelope.correlation_id()),
            message,
        };
        crate::semantics::validate(&decoded)?;
        Ok(decoded)
    }

    fn decode_opaque(
        &self,
        envelope: &wire::Envelope<'_>,
        kind: MessageKind,
    ) -> Result<MessagePayload, CodecError> {
        if envelope.control_type() != wire::ControlPayload::NONE || envelope.control().is_some() {
            return Err(control_mismatch(envelope, kind));
        }
        if envelope.payload_type_id() == 0 {
            return Err(CodecError::MissingPayloadType(kind));
        }

        let wire_payload = envelope.payload();
        self.enforce_payload_limit(wire_payload.map_or(0, |value| value.len()))?;
        let bytes = vector_bytes(wire_payload);
        let payload = OpaquePayload {
            type_id: envelope.payload_type_id(),
            bytes,
        };
        Ok(match kind {
            MessageKind::EntityState => MessagePayload::EntityState(payload),
            MessageKind::ReliableEvent => MessagePayload::ReliableEvent(payload),
            MessageKind::Snapshot => MessagePayload::Snapshot(payload),
            _ => return Err(control_mismatch(envelope, kind)),
        })
    }

    fn enforce_frame_limit(&self, actual: usize) -> Result<(), CodecError> {
        if actual > self.limits.max_frame_len {
            Err(CodecError::FrameTooLarge {
                actual,
                maximum: self.limits.max_frame_len,
            })
        } else {
            Ok(())
        }
    }

    fn enforce_payload_limit(&self, actual: usize) -> Result<(), CodecError> {
        if actual > self.limits.max_payload_len {
            Err(CodecError::PayloadTooLarge {
                actual,
                maximum: self.limits.max_payload_len,
            })
        } else {
            Ok(())
        }
    }
}

#[allow(clippy::too_many_lines)]
fn encode_control(
    builder: &mut FlatBufferBuilder<'_>,
    payload: &ControlPayload,
    channel_id: u64,
) -> (wire::ControlPayload, WIPOffset<UnionWIPOffset>) {
    match payload {
        ControlPayload::Hello(value) => {
            let client_name = builder.create_string(&value.client_name);
            let client_version = builder.create_string(&value.client_version);
            let offset = wire::HelloPayload::create(
                builder,
                &wire::HelloPayloadArgs {
                    min_protocol_version: value.min_protocol_version,
                    max_protocol_version: value.max_protocol_version,
                    client_name: Some(client_name),
                    client_version: Some(client_version),
                    capability_bits: value.capability_bits,
                    max_frame_size: value.max_frame_size,
                    max_payload_size: value.max_payload_size,
                },
            );
            (wire::ControlPayload::HelloPayload, offset.as_union_value())
        }
        ControlPayload::Capabilities(value) => {
            let server_name = builder.create_string(&value.server_name);
            let server_version = builder.create_string(&value.server_version);
            let offset = wire::CapabilitiesPayload::create(
                builder,
                &wire::CapabilitiesPayloadArgs {
                    selected_protocol_version: value.selected_protocol_version,
                    server_name: Some(server_name),
                    server_version: Some(server_version),
                    capability_bits: value.capability_bits,
                    max_frame_size: value.max_frame_size,
                    max_payload_size: value.max_payload_size,
                },
            );
            (
                wire::ControlPayload::CapabilitiesPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::Authenticate(value) => {
            let credentials = builder.create_vector(&value.credentials);
            let offset = wire::AuthenticatePayload::create(
                builder,
                &wire::AuthenticatePayloadArgs {
                    scheme: wire::AuthenticationScheme(value.scheme.value()),
                    credentials: Some(credentials),
                },
            );
            (
                wire::ControlPayload::AuthenticatePayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::Authenticated(value) => {
            let offset = wire::AuthenticatedPayload::create(
                builder,
                &wire::AuthenticatedPayloadArgs {
                    principal_id: value.principal_id,
                    assigned_entity_id: value.assigned_entity_id.unwrap_or(0),
                },
            );
            (
                wire::ControlPayload::AuthenticatedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::JoinSession(value) => {
            let resume_token = builder.create_vector(&value.resume_token);
            let offset = wire::JoinSessionPayload::create(
                builder,
                &wire::JoinSessionPayloadArgs {
                    resume_token: Some(resume_token),
                },
            );
            (
                wire::ControlPayload::JoinSessionPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::LeaveSession(value) => {
            let reason = builder.create_string(&value.reason);
            let offset = wire::LeaveSessionPayload::create(
                builder,
                &wire::LeaveSessionPayloadArgs {
                    reason: Some(reason),
                },
            );
            (
                wire::ControlPayload::LeaveSessionPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::SubscribeSpace(_) => {
            let offset = wire::SubscribeSpacePayload::create(
                builder,
                &wire::SubscribeSpacePayloadArgs { channel_id },
            );
            (
                wire::ControlPayload::SubscribeSpacePayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::UnsubscribeSpace(value) => {
            let offset = wire::UnsubscribeSpacePayload::create(
                builder,
                &wire::UnsubscribeSpacePayloadArgs {
                    subscription_id: value.subscription_id,
                },
            );
            (
                wire::ControlPayload::UnsubscribeSpacePayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::SubscriptionAccepted(value) => {
            let offset = wire::SubscriptionAcceptedPayload::create(
                builder,
                &wire::SubscriptionAcceptedPayloadArgs {
                    subscription_id: value.subscription_id,
                    accepted_space_epoch: value.accepted_space_epoch,
                },
            );
            (
                wire::ControlPayload::SubscriptionAcceptedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::SubscriptionRejected(value) => {
            let reason = builder.create_string(&value.reason);
            let offset = wire::SubscriptionRejectedPayload::create(
                builder,
                &wire::SubscriptionRejectedPayloadArgs {
                    code: wire::SubscriptionRejectionCode(value.code.value()),
                    reason: Some(reason),
                },
            );
            (
                wire::ControlPayload::SubscriptionRejectedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::EntityEntered(value) => {
            let offset = wire::EntityEnteredPayload::create(
                builder,
                &wire::EntityEnteredPayloadArgs {
                    owner_entity_id: value.owner_entity_id.unwrap_or(0),
                },
            );
            (
                wire::ControlPayload::EntityEnteredPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::EntityLeft(value) => {
            let offset = wire::EntityLeftPayload::create(
                builder,
                &wire::EntityLeftPayloadArgs {
                    reason: wire::EntityLeaveReason(value.reason.value()),
                },
            );
            (
                wire::ControlPayload::EntityLeftPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::SnapshotRequest(value) => {
            let offset = wire::SnapshotRequestPayload::create(
                builder,
                &wire::SnapshotRequestPayloadArgs {
                    channel_id,
                    after_server_tick: value.after_server_tick.unwrap_or(0),
                },
            );
            (
                wire::ControlPayload::SnapshotRequestPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::SpaceTransition(value) => {
            let offset = wire::SpaceTransitionPayload::create(
                builder,
                &wire::SpaceTransitionPayloadArgs {
                    from_space_id: value.from_space_id,
                    to_space_id: value.to_space_id,
                    to_space_epoch: value.to_space_epoch,
                },
            );
            (
                wire::ControlPayload::SpaceTransitionPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::Ping(value) => {
            let offset = wire::PingPayload::create(
                builder,
                &wire::PingPayloadArgs {
                    nonce: value.nonce,
                    sender_time_micros: value.sender_time_micros,
                },
            );
            (wire::ControlPayload::PingPayload, offset.as_union_value())
        }
        ControlPayload::Pong(value) => {
            let offset = wire::PongPayload::create(
                builder,
                &wire::PongPayloadArgs {
                    nonce: value.nonce,
                    sender_time_micros: value.sender_time_micros,
                    responder_time_micros: value.responder_time_micros,
                },
            );
            (wire::ControlPayload::PongPayload, offset.as_union_value())
        }
        ControlPayload::ProtocolError(value) => {
            let message = builder.create_string(&value.message);
            let offset = wire::ProtocolErrorPayload::create(
                builder,
                &wire::ProtocolErrorPayloadArgs {
                    code: wire::ProtocolErrorCode(value.code.value()),
                    related_message_kind: wire::MessageKind(value.related_message_kind.value()),
                    message: Some(message),
                },
            );
            (
                wire::ControlPayload::ProtocolErrorPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceRequested(value) => {
            let capability = builder.create_string(&value.capability);
            let input = builder.create_vector(&value.input);
            let offset = wire::InferenceRequestedPayload::create(
                builder,
                &wire::InferenceRequestedPayloadArgs {
                    capability: Some(capability),
                    deadline_ms: value.deadline_ms,
                    input: Some(input),
                },
            );
            (
                wire::ControlPayload::InferenceRequestedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceAccepted(value) => {
            let offset = wire::InferenceAcceptedPayload::create(
                builder,
                &wire::InferenceAcceptedPayloadArgs {
                    queued_position: value.queued_position,
                },
            );
            (
                wire::ControlPayload::InferenceAcceptedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceProgress(value) => {
            let offset = wire::InferenceProgressPayload::create(
                builder,
                &wire::InferenceProgressPayloadArgs {
                    percent: value.percent,
                },
            );
            (
                wire::ControlPayload::InferenceProgressPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceStreamChunk(value) => {
            let chunk = builder.create_vector(&value.chunk);
            let offset = wire::InferenceStreamChunkPayload::create(
                builder,
                &wire::InferenceStreamChunkPayloadArgs {
                    sequence: value.sequence,
                    chunk: Some(chunk),
                    is_final: value.is_final,
                },
            );
            (
                wire::ControlPayload::InferenceStreamChunkPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceCompleted(value) => {
            let result = builder.create_vector(&value.result);
            let offset = wire::InferenceCompletedPayload::create(
                builder,
                &wire::InferenceCompletedPayloadArgs {
                    result: Some(result),
                },
            );
            (
                wire::ControlPayload::InferenceCompletedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceFailed(value) => {
            let reason = builder.create_string(&value.reason);
            let offset = wire::InferenceFailedPayload::create(
                builder,
                &wire::InferenceFailedPayloadArgs {
                    reason: Some(reason),
                },
            );
            (
                wire::ControlPayload::InferenceFailedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceCancelled(value) => {
            let reason = builder.create_string(&value.reason);
            let offset = wire::InferenceCancelledPayload::create(
                builder,
                &wire::InferenceCancelledPayloadArgs {
                    reason: Some(reason),
                },
            );
            (
                wire::ControlPayload::InferenceCancelledPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::InferenceExpired(value) => {
            let reason = builder.create_string(&value.reason);
            let offset = wire::InferenceExpiredPayload::create(
                builder,
                &wire::InferenceExpiredPayloadArgs {
                    reason: Some(reason),
                },
            );
            (
                wire::ControlPayload::InferenceExpiredPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::ToolCallProposed(value) => {
            let tool_id = builder.create_string(&value.tool_id);
            let arguments = builder.create_vector(&value.arguments);
            let offset = wire::ToolCallProposedPayload::create(
                builder,
                &wire::ToolCallProposedPayloadArgs {
                    tool_id: Some(tool_id),
                    tool_version: value.tool_version,
                    arguments: Some(arguments),
                    expected_revision: value.expected_revision,
                },
            );
            (
                wire::ControlPayload::ToolCallProposedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::ToolCallAccepted(value) => {
            let tool_id = builder.create_string(&value.tool_id);
            let offset = wire::ToolCallAcceptedPayload::create(
                builder,
                &wire::ToolCallAcceptedPayloadArgs {
                    tool_id: Some(tool_id),
                },
            );
            (
                wire::ControlPayload::ToolCallAcceptedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::ToolCallRejected(value) => {
            let reason = builder.create_string(&value.reason);
            let offset = wire::ToolCallRejectedPayload::create(
                builder,
                &wire::ToolCallRejectedPayloadArgs {
                    code: wire::ToolCallRejectionCode(value.code.value()),
                    reason: Some(reason),
                },
            );
            (
                wire::ControlPayload::ToolCallRejectedPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::RequestAdmission(value) => {
            let idempotency_key = builder.create_string(&value.idempotency_key);
            let offset = wire::RequestAdmissionPayload::create(
                builder,
                &wire::RequestAdmissionPayloadArgs {
                    idempotency_key: Some(idempotency_key),
                },
            );
            (
                wire::ControlPayload::RequestAdmissionPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::AdmissionResult(value) => {
            let offset = wire::AdmissionResultPayload::create(
                builder,
                &wire::AdmissionResultPayloadArgs {
                    status: wire::AdmissionStatus(value.status.value()),
                    rejection_code: wire::AdmissionRejectionCode(value.rejection_code.value()),
                    ticket_id: value.ticket_id.unwrap_or(0),
                    poll_after_ms: value.poll_after_ms,
                    ticket_remaining_ms: value.ticket_remaining_ms,
                },
            );
            (
                wire::ControlPayload::AdmissionResultPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::QueueStatusRequest(value) => {
            let offset = wire::QueueStatusRequestPayload::create(
                builder,
                &wire::QueueStatusRequestPayloadArgs {
                    ticket_id: value.ticket_id,
                },
            );
            (
                wire::ControlPayload::QueueStatusRequestPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::QueueHeartbeat(value) => {
            let offset = wire::QueueHeartbeatPayload::create(
                builder,
                &wire::QueueHeartbeatPayloadArgs {
                    ticket_id: value.ticket_id,
                },
            );
            (
                wire::ControlPayload::QueueHeartbeatPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::QueueClaim(value) => {
            let offset = wire::QueueClaimPayload::create(
                builder,
                &wire::QueueClaimPayloadArgs {
                    ticket_id: value.ticket_id,
                },
            );
            (
                wire::ControlPayload::QueueClaimPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::QueueCancel(value) => {
            let offset = wire::QueueCancelPayload::create(
                builder,
                &wire::QueueCancelPayloadArgs {
                    ticket_id: value.ticket_id,
                },
            );
            (
                wire::ControlPayload::QueueCancelPayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::QueueUpdate(value) => {
            let offset = wire::QueueUpdatePayload::create(
                builder,
                &wire::QueueUpdatePayloadArgs {
                    ticket_id: value.ticket_id,
                    state: wire::QueueState(value.state.value()),
                    position: value.position,
                    poll_after_ms: value.poll_after_ms,
                    ticket_remaining_ms: value.ticket_remaining_ms,
                    offer_remaining_ms: value.offer_remaining_ms,
                },
            );
            (
                wire::ControlPayload::QueueUpdatePayload,
                offset.as_union_value(),
            )
        }
        ControlPayload::ToolCallCompleted(value) => {
            let result = builder.create_vector(&value.result);
            let offset = wire::ToolCallCompletedPayload::create(
                builder,
                &wire::ToolCallCompletedPayloadArgs {
                    new_revision: value.new_revision,
                    result: Some(result),
                },
            );
            (
                wire::ControlPayload::ToolCallCompletedPayload,
                offset.as_union_value(),
            )
        }
    }
}

#[allow(clippy::too_many_lines)]
fn decode_control(
    codec: &Codec,
    envelope: &wire::Envelope<'_>,
    kind: MessageKind,
) -> Result<ControlPayload, CodecError> {
    Ok(match kind {
        MessageKind::Hello => {
            require_control(envelope, kind, wire::ControlPayload::HelloPayload)?;
            let value = envelope
                .control_as_hello_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            let (client_name, client_version) = checked_control_strings(
                codec,
                value.client_name().unwrap_or_default(),
                value.client_version().unwrap_or_default(),
            )?;
            ControlPayload::Hello(Hello {
                min_protocol_version: value.min_protocol_version(),
                max_protocol_version: value.max_protocol_version(),
                client_name,
                client_version,
                capability_bits: value.capability_bits(),
                max_frame_size: value.max_frame_size(),
                max_payload_size: value.max_payload_size(),
            })
        }
        MessageKind::Capabilities => {
            require_control(envelope, kind, wire::ControlPayload::CapabilitiesPayload)?;
            let value = envelope
                .control_as_capabilities_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            let (server_name, server_version) = checked_control_strings(
                codec,
                value.server_name().unwrap_or_default(),
                value.server_version().unwrap_or_default(),
            )?;
            ControlPayload::Capabilities(Capabilities {
                selected_protocol_version: value.selected_protocol_version(),
                server_name,
                server_version,
                capability_bits: value.capability_bits(),
                max_frame_size: value.max_frame_size(),
                max_payload_size: value.max_payload_size(),
            })
        }
        MessageKind::Authenticate => {
            require_control(envelope, kind, wire::ControlPayload::AuthenticatePayload)?;
            let value = envelope
                .control_as_authenticate_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::Authenticate(Authenticate {
                scheme: AuthenticationScheme::from_wire(value.scheme().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "authentication scheme",
                        value: u64::from(value.scheme().0),
                    },
                )?,
                credentials: checked_control_vector(codec, value.credentials())?,
            })
        }
        MessageKind::Authenticated => {
            require_control(envelope, kind, wire::ControlPayload::AuthenticatedPayload)?;
            let value = envelope
                .control_as_authenticated_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::Authenticated(Authenticated {
                principal_id: value.principal_id(),
                assigned_entity_id: nonzero(value.assigned_entity_id()),
            })
        }
        MessageKind::JoinSession => {
            require_control(envelope, kind, wire::ControlPayload::JoinSessionPayload)?;
            let value = envelope
                .control_as_join_session_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::JoinSession(JoinSession {
                resume_token: checked_control_vector(codec, value.resume_token())?,
            })
        }
        MessageKind::LeaveSession => {
            require_control(envelope, kind, wire::ControlPayload::LeaveSessionPayload)?;
            let value = envelope
                .control_as_leave_session_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::LeaveSession(LeaveSession {
                reason: checked_control_string(codec, value.reason())?,
            })
        }
        MessageKind::SubscribeSpace => {
            require_control(envelope, kind, wire::ControlPayload::SubscribeSpacePayload)?;
            let value = envelope
                .control_as_subscribe_space_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            require_channel_mirror(envelope, kind, value.channel_id())?;
            ControlPayload::SubscribeSpace(SubscribeSpace)
        }
        MessageKind::UnsubscribeSpace => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::UnsubscribeSpacePayload,
            )?;
            let value = envelope
                .control_as_unsubscribe_space_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::UnsubscribeSpace(UnsubscribeSpace {
                subscription_id: value.subscription_id(),
            })
        }
        MessageKind::SubscriptionAccepted => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::SubscriptionAcceptedPayload,
            )?;
            let value = envelope
                .control_as_subscription_accepted_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::SubscriptionAccepted(SubscriptionAccepted {
                subscription_id: value.subscription_id(),
                accepted_space_epoch: value.accepted_space_epoch(),
            })
        }
        MessageKind::SubscriptionRejected => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::SubscriptionRejectedPayload,
            )?;
            let value = envelope
                .control_as_subscription_rejected_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::SubscriptionRejected(SubscriptionRejected {
                code: SubscriptionRejectionCode::from_wire(value.code().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "subscription rejection code",
                        value: u64::from(value.code().0),
                    },
                )?,
                reason: checked_control_string(codec, value.reason())?,
            })
        }
        MessageKind::EntityEntered => {
            require_control(envelope, kind, wire::ControlPayload::EntityEnteredPayload)?;
            let value = envelope
                .control_as_entity_entered_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::EntityEntered(EntityEntered {
                owner_entity_id: nonzero(value.owner_entity_id()),
            })
        }
        MessageKind::EntityLeft => {
            require_control(envelope, kind, wire::ControlPayload::EntityLeftPayload)?;
            let value = envelope
                .control_as_entity_left_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::EntityLeft(EntityLeft {
                reason: EntityLeaveReason::from_wire(value.reason().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "entity leave reason",
                        value: u64::from(value.reason().0),
                    },
                )?,
            })
        }
        MessageKind::SnapshotRequest => {
            require_control(envelope, kind, wire::ControlPayload::SnapshotRequestPayload)?;
            let value = envelope
                .control_as_snapshot_request_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            require_channel_mirror(envelope, kind, value.channel_id())?;
            ControlPayload::SnapshotRequest(SnapshotRequest {
                after_server_tick: nonzero(value.after_server_tick()),
            })
        }
        MessageKind::SpaceTransition => {
            require_control(envelope, kind, wire::ControlPayload::SpaceTransitionPayload)?;
            let value = envelope
                .control_as_space_transition_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::SpaceTransition(SpaceTransition {
                from_space_id: value.from_space_id(),
                to_space_id: value.to_space_id(),
                to_space_epoch: value.to_space_epoch(),
            })
        }
        MessageKind::Ping => {
            require_control(envelope, kind, wire::ControlPayload::PingPayload)?;
            let value = envelope
                .control_as_ping_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::Ping(Ping {
                nonce: value.nonce(),
                sender_time_micros: value.sender_time_micros(),
            })
        }
        MessageKind::Pong => {
            require_control(envelope, kind, wire::ControlPayload::PongPayload)?;
            let value = envelope
                .control_as_pong_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::Pong(Pong {
                nonce: value.nonce(),
                sender_time_micros: value.sender_time_micros(),
                responder_time_micros: value.responder_time_micros(),
            })
        }
        MessageKind::ProtocolError => {
            require_control(envelope, kind, wire::ControlPayload::ProtocolErrorPayload)?;
            let value = envelope
                .control_as_protocol_error_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::ProtocolError(ProtocolError {
                code: ProtocolErrorCode::from_wire(value.code().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "protocol error code",
                        value: u64::from(value.code().0),
                    },
                )?,
                related_message_kind: MessageKind::from_wire(value.related_message_kind().0)
                    .ok_or(CodecError::UnsupportedEnumValue {
                        name: "related message kind",
                        value: u64::from(value.related_message_kind().0),
                    })?,
                message: checked_control_string(codec, value.message())?,
            })
        }
        MessageKind::InferenceRequested => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::InferenceRequestedPayload,
            )?;
            let value = envelope
                .control_as_inference_requested_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceRequested(InferenceRequested {
                capability: checked_control_string(codec, value.capability())?,
                deadline_ms: value.deadline_ms(),
                input: checked_control_vector(codec, value.input())?,
            })
        }
        MessageKind::InferenceAccepted => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::InferenceAcceptedPayload,
            )?;
            let value = envelope
                .control_as_inference_accepted_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceAccepted(InferenceAccepted {
                queued_position: value.queued_position(),
            })
        }
        MessageKind::InferenceProgress => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::InferenceProgressPayload,
            )?;
            let value = envelope
                .control_as_inference_progress_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceProgress(InferenceProgress {
                percent: value.percent(),
            })
        }
        MessageKind::InferenceStreamChunk => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::InferenceStreamChunkPayload,
            )?;
            let value = envelope
                .control_as_inference_stream_chunk_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceStreamChunk(InferenceStreamChunk {
                sequence: value.sequence(),
                chunk: checked_control_vector(codec, value.chunk())?,
                is_final: value.is_final(),
            })
        }
        MessageKind::InferenceCompleted => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::InferenceCompletedPayload,
            )?;
            let value = envelope
                .control_as_inference_completed_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceCompleted(InferenceCompleted {
                result: checked_control_vector(codec, value.result())?,
            })
        }
        MessageKind::InferenceFailed => {
            require_control(envelope, kind, wire::ControlPayload::InferenceFailedPayload)?;
            let value = envelope
                .control_as_inference_failed_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceFailed(InferenceFailed {
                reason: checked_control_string(codec, value.reason())?,
            })
        }
        MessageKind::InferenceCancelled => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::InferenceCancelledPayload,
            )?;
            let value = envelope
                .control_as_inference_cancelled_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceCancelled(InferenceCancelled {
                reason: checked_control_string(codec, value.reason())?,
            })
        }
        MessageKind::InferenceExpired => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::InferenceExpiredPayload,
            )?;
            let value = envelope
                .control_as_inference_expired_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::InferenceExpired(InferenceExpired {
                reason: checked_control_string(codec, value.reason())?,
            })
        }
        MessageKind::ToolCallProposed => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::ToolCallProposedPayload,
            )?;
            let value = envelope
                .control_as_tool_call_proposed_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::ToolCallProposed(ToolCallProposed {
                tool_id: checked_control_string(codec, value.tool_id())?,
                tool_version: value.tool_version(),
                arguments: checked_control_vector(codec, value.arguments())?,
                expected_revision: value.expected_revision(),
            })
        }
        MessageKind::ToolCallAccepted => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::ToolCallAcceptedPayload,
            )?;
            let value = envelope
                .control_as_tool_call_accepted_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::ToolCallAccepted(ToolCallAccepted {
                tool_id: checked_control_string(codec, value.tool_id())?,
            })
        }
        MessageKind::ToolCallRejected => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::ToolCallRejectedPayload,
            )?;
            let value = envelope
                .control_as_tool_call_rejected_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::ToolCallRejected(ToolCallRejected {
                code: ToolCallRejectionCode::from_wire(value.code().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "tool call rejection code",
                        value: u64::from(value.code().0),
                    },
                )?,
                reason: checked_control_string(codec, value.reason())?,
            })
        }
        MessageKind::ToolCallCompleted => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::ToolCallCompletedPayload,
            )?;
            let value = envelope
                .control_as_tool_call_completed_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::ToolCallCompleted(ToolCallCompleted {
                new_revision: value.new_revision(),
                result: checked_control_vector(codec, value.result())?,
            })
        }
        MessageKind::RequestAdmission => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::RequestAdmissionPayload,
            )?;
            let value = envelope
                .control_as_request_admission_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::RequestAdmission(RequestAdmission {
                idempotency_key: checked_control_string(codec, value.idempotency_key())?,
            })
        }
        MessageKind::AdmissionResult => {
            require_control(envelope, kind, wire::ControlPayload::AdmissionResultPayload)?;
            let value = envelope
                .control_as_admission_result_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::AdmissionResult(AdmissionResult {
                status: AdmissionStatus::from_wire(value.status().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "AdmissionStatus",
                        value: u64::from(value.status().0),
                    },
                )?,
                rejection_code: AdmissionRejectionCode::from_wire(value.rejection_code().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "AdmissionRejectionCode",
                        value: u64::from(value.rejection_code().0),
                    },
                )?,
                ticket_id: nonzero(value.ticket_id()),
                poll_after_ms: value.poll_after_ms(),
                ticket_remaining_ms: value.ticket_remaining_ms(),
            })
        }
        MessageKind::QueueStatusRequest => {
            require_control(
                envelope,
                kind,
                wire::ControlPayload::QueueStatusRequestPayload,
            )?;
            let value = envelope
                .control_as_queue_status_request_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::QueueStatusRequest(QueueStatusRequest {
                ticket_id: value.ticket_id(),
            })
        }
        MessageKind::QueueHeartbeat => {
            require_control(envelope, kind, wire::ControlPayload::QueueHeartbeatPayload)?;
            let value = envelope
                .control_as_queue_heartbeat_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::QueueHeartbeat(QueueHeartbeat {
                ticket_id: value.ticket_id(),
            })
        }
        MessageKind::QueueClaim => {
            require_control(envelope, kind, wire::ControlPayload::QueueClaimPayload)?;
            let value = envelope
                .control_as_queue_claim_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::QueueClaim(QueueClaim {
                ticket_id: value.ticket_id(),
            })
        }
        MessageKind::QueueCancel => {
            require_control(envelope, kind, wire::ControlPayload::QueueCancelPayload)?;
            let value = envelope
                .control_as_queue_cancel_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::QueueCancel(QueueCancel {
                ticket_id: value.ticket_id(),
            })
        }
        MessageKind::QueueUpdate => {
            require_control(envelope, kind, wire::ControlPayload::QueueUpdatePayload)?;
            let value = envelope
                .control_as_queue_update_payload()
                .ok_or_else(|| control_mismatch(envelope, kind))?;
            ControlPayload::QueueUpdate(QueueUpdate {
                ticket_id: value.ticket_id(),
                state: QueueState::from_wire(value.state().0).ok_or(
                    CodecError::UnsupportedEnumValue {
                        name: "QueueState",
                        value: u64::from(value.state().0),
                    },
                )?,
                position: value.position(),
                poll_after_ms: value.poll_after_ms(),
                ticket_remaining_ms: value.ticket_remaining_ms(),
                offer_remaining_ms: value.offer_remaining_ms(),
            })
        }
        MessageKind::Unknown
        | MessageKind::EntityState
        | MessageKind::ReliableEvent
        | MessageKind::Snapshot => return Err(control_mismatch(envelope, kind)),
    })
}

fn require_control(
    envelope: &wire::Envelope<'_>,
    kind: MessageKind,
    expected: wire::ControlPayload,
) -> Result<(), CodecError> {
    if envelope.control_type() != expected || envelope.control().is_none() {
        Err(control_mismatch(envelope, kind))
    } else {
        Ok(())
    }
}

fn control_mismatch(envelope: &wire::Envelope<'_>, kind: MessageKind) -> CodecError {
    CodecError::MessageControlMismatch {
        message_kind: kind.value(),
        control_kind: envelope.control_type().0,
    }
}

fn checked_control_vector(
    codec: &Codec,
    vector: Option<flatbuffers::Vector<'_, u8>>,
) -> Result<Vec<u8>, CodecError> {
    codec.enforce_payload_limit(vector.map_or(0, |value| value.len()))?;
    Ok(vector_bytes(vector))
}

fn checked_control_string(codec: &Codec, value: Option<&str>) -> Result<String, CodecError> {
    let value = value.unwrap_or_default();
    codec.enforce_payload_limit(value.len())?;
    Ok(value.to_owned())
}

fn checked_control_strings(
    codec: &Codec,
    first: &str,
    second: &str,
) -> Result<(String, String), CodecError> {
    codec.enforce_payload_limit(first.len().saturating_add(second.len()))?;
    Ok((first.to_owned(), second.to_owned()))
}

fn require_channel_mirror(
    envelope: &wire::Envelope<'_>,
    kind: MessageKind,
    mirrored_channel_id: u64,
) -> Result<(), CodecError> {
    if mirrored_channel_id == envelope.channel_id() {
        Ok(())
    } else {
        Err(CodecError::InvalidSemantics {
            message_kind: kind,
            reason: "control channel_id must match envelope channel_id",
        })
    }
}

fn vector_bytes(vector: Option<flatbuffers::Vector<'_, u8>>) -> Vec<u8> {
    vector.map_or_else(Vec::new, |value| value.bytes().to_vec())
}

const fn nonzero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_decode_rejects_invalid_wire_semantics_and_enums() {
        for invalid in 0..5 {
            let mut builder = FlatBufferBuilder::new();
            let payload = wire::QueueUpdatePayload::create(
                &mut builder,
                &wire::QueueUpdatePayloadArgs {
                    ticket_id: u64::from(invalid != 0),
                    state: if invalid == 1 {
                        wire::QueueState(255)
                    } else {
                        wire::QueueState::Offered
                    },
                    ..Default::default()
                },
            );
            let root = wire::Envelope::create(
                &mut builder,
                &wire::EnvelopeArgs {
                    namespace_id: u64::from(invalid != 2),
                    session_id: 2,
                    correlation_id: if invalid == 3 { 0 } else { 3 },
                    message_kind: if invalid == 4 {
                        wire::MessageKind::AdmissionResult
                    } else {
                        wire::MessageKind::QueueUpdate
                    },
                    delivery_class: wire::DeliveryClass::ReliableOrdered,
                    control_type: wire::ControlPayload::QueueUpdatePayload,
                    control: Some(payload.as_union_value()),
                    ..Default::default()
                },
            );
            wire::finish_size_prefixed_envelope_buffer(&mut builder, root);
            assert!(Codec::default().decode(builder.finished_data()).is_err());
        }
    }

    fn raw_frame(protocol_version: u16, message_kind: u8, with_ping_control: bool) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let (control_type, control) = if with_ping_control {
            let ping = wire::PingPayload::create(
                &mut builder,
                &wire::PingPayloadArgs {
                    nonce: 41,
                    sender_time_micros: 42,
                },
            );
            (
                wire::ControlPayload::PingPayload,
                Some(ping.as_union_value()),
            )
        } else {
            (wire::ControlPayload::NONE, None)
        };
        let root = wire::Envelope::create(
            &mut builder,
            &wire::EnvelopeArgs {
                protocol_version,
                message_kind: wire::MessageKind(message_kind),
                delivery_class: wire::DeliveryClass::ReliableOrdered,
                control_type,
                control,
                ..wire::EnvelopeArgs::default()
            },
        );
        wire::finish_size_prefixed_envelope_buffer(&mut builder, root);
        builder.finished_data().to_vec()
    }

    #[test]
    fn verifier_rejects_malformed_flatbuffer() {
        let codec = Codec::default();
        let mut frame = raw_frame(PROTOCOL_VERSION, MessageKind::Ping.value(), true);
        frame[SIZE_PREFIX_LEN..SIZE_PREFIX_LEN * 2].copy_from_slice(&u32::MAX.to_le_bytes());

        assert!(matches!(
            codec.decode(&frame),
            Err(CodecError::InvalidFlatbuffer(_))
        ));
    }

    #[test]
    fn framing_rejects_truncated_and_oversized_frames() {
        let frame = raw_frame(PROTOCOL_VERSION, MessageKind::Ping.value(), true);
        let codec = Codec::default();
        assert!(matches!(
            codec.decode(&frame[..frame.len() - 1]),
            Err(CodecError::TruncatedFrame { .. })
        ));

        let small_codec =
            Codec::new(CodecLimits::new(32, 16).expect("valid limits")).expect("valid codec");
        assert!(matches!(
            small_codec.decode(&frame),
            Err(CodecError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn payload_limits_apply_to_encode_and_decode() {
        let large_codec = Codec::default();
        let mut envelope = Envelope::entity_state(
            DeliveryClass::LatestValue,
            OpaquePayload {
                type_id: 7,
                bytes: vec![1, 2, 3],
            },
        );
        envelope.namespace_id = 1;
        envelope.session_id = 2;
        envelope.space_id = 3;
        envelope.channel_id = Some(4);
        envelope.entity_id = Some(5);
        envelope.space_epoch = 6;
        let frame = large_codec.encode(&envelope).expect("valid frame");
        let small_codec =
            Codec::new(CodecLimits::new(512, 2).expect("valid limits")).expect("valid codec");

        assert!(matches!(
            small_codec.encode(&envelope),
            Err(CodecError::PayloadTooLarge {
                actual: 3,
                maximum: 2
            })
        ));
        assert!(matches!(
            small_codec.decode(&frame),
            Err(CodecError::PayloadTooLarge {
                actual: 3,
                maximum: 2
            })
        ));
    }

    #[test]
    fn unsupported_version_is_rejected_after_verification() {
        let frame = raw_frame(PROTOCOL_VERSION + 1, MessageKind::Ping.value(), true);
        assert_eq!(
            Codec::default().decode(&frame),
            Err(CodecError::UnsupportedProtocolVersion(PROTOCOL_VERSION + 1))
        );
    }

    #[test]
    fn unknown_message_kind_is_rejected_after_verification() {
        let frame = raw_frame(PROTOCOL_VERSION, 250, false);
        assert_eq!(
            Codec::default().decode(&frame),
            Err(CodecError::UnknownMessageKind(250))
        );
    }

    #[test]
    fn message_and_control_union_must_match() {
        let frame = raw_frame(PROTOCOL_VERSION, MessageKind::Hello.value(), true);
        assert_eq!(
            Codec::default().decode(&frame),
            Err(CodecError::MessageControlMismatch {
                message_kind: MessageKind::Hello.value(),
                control_kind: wire::ControlPayload::PingPayload.0,
            })
        );
    }
}
