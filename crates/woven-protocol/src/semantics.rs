use crate::{
    Authenticate, Capabilities, CodecError, ControlPayload, DeliveryClass, Envelope, Hello,
    InferenceRequested, MessageKind, MessagePayload, ProtocolError, SnapshotRequest,
    SpaceTransition, ToolCallProposed,
};

pub(crate) fn validate(envelope: &Envelope) -> Result<(), CodecError> {
    reject_zero_optional_ids(envelope)?;
    validate_delivery(envelope)?;

    match &envelope.message {
        MessagePayload::Control(control) => validate_control(envelope, control),
        MessagePayload::EntityState(payload) => {
            require_space_scope(envelope, true, true)?;
            require(
                payload.type_id,
                envelope,
                "payload_type_id must be non-zero",
            )
        }
        MessagePayload::ReliableEvent(payload) | MessagePayload::Snapshot(payload) => {
            require_space_scope(envelope, true, false)?;
            require(
                payload.type_id,
                envelope,
                "payload_type_id must be non-zero",
            )
        }
    }
}

#[allow(clippy::too_many_lines)]
fn validate_control(envelope: &Envelope, control: &ControlPayload) -> Result<(), CodecError> {
    match control {
        managed @ (ControlPayload::RequestAdmission(_)
        | ControlPayload::AdmissionResult(_)
        | ControlPayload::QueueStatusRequest(_)
        | ControlPayload::QueueHeartbeat(_)
        | ControlPayload::QueueClaim(_)
        | ControlPayload::QueueCancel(_)
        | ControlPayload::QueueUpdate(_)) => validate_managed(envelope, managed),
        ControlPayload::Hello(value) => {
            require_unscoped(envelope)?;
            validate_hello(envelope, value)
        }
        ControlPayload::Capabilities(value) => {
            require_unscoped(envelope)?;
            validate_capabilities(envelope, value)
        }
        ControlPayload::Authenticate(value) => {
            require_unscoped(envelope)?;
            validate_authenticate(envelope, value)
        }
        ControlPayload::Authenticated(value) => {
            require_unscoped(envelope)?;
            require(
                value.principal_id,
                envelope,
                "authenticated principal_id must be non-zero",
            )?;
            reject_zero_option(
                value.assigned_entity_id,
                envelope,
                "assigned_entity_id cannot be Some(0)",
            )
        }
        ControlPayload::JoinSession(_) | ControlPayload::LeaveSession(_) => {
            require_session_scope(envelope)
        }
        ControlPayload::SubscribeSpace(_) => require_space_scope(envelope, true, false),
        ControlPayload::UnsubscribeSpace(value) => {
            require_space_scope(envelope, true, false)?;
            require(
                value.subscription_id,
                envelope,
                "subscription_id must be non-zero",
            )
        }
        ControlPayload::SubscriptionAccepted(value) => {
            require_space_scope(envelope, true, false)?;
            require(
                value.subscription_id,
                envelope,
                "subscription_id must be non-zero",
            )?;
            require(
                value.accepted_space_epoch,
                envelope,
                "accepted_space_epoch must be non-zero",
            )?;
            if value.accepted_space_epoch != envelope.space_epoch {
                return invalid(
                    envelope,
                    "accepted_space_epoch must match envelope space_epoch",
                );
            }
            Ok(())
        }
        ControlPayload::SubscriptionRejected(value) => {
            require_space_scope(envelope, true, false)?;
            if value.code == crate::SubscriptionRejectionCode::Unknown {
                return invalid(envelope, "subscription rejection code cannot be Unknown");
            }
            Ok(())
        }
        ControlPayload::EntityEntered(value) => {
            require_space_scope(envelope, false, true)?;
            reject_zero_option(
                value.owner_entity_id,
                envelope,
                "owner_entity_id cannot be Some(0)",
            )
        }
        ControlPayload::EntityLeft(value) => {
            require_space_scope(envelope, false, true)?;
            if value.reason == crate::EntityLeaveReason::Unknown {
                return invalid(envelope, "entity leave reason cannot be Unknown");
            }
            Ok(())
        }
        ControlPayload::SnapshotRequest(value) => {
            require_space_scope(envelope, true, false)?;
            validate_snapshot_request(envelope, value)
        }
        ControlPayload::SpaceTransition(value) => validate_transition(envelope, value),
        ControlPayload::Ping(value) => {
            require_unscoped(envelope)?;
            require(value.nonce, envelope, "ping nonce must be non-zero")
        }
        ControlPayload::Pong(value) => {
            require_unscoped(envelope)?;
            require(value.nonce, envelope, "pong nonce must be non-zero")
        }
        ControlPayload::ProtocolError(value) => validate_protocol_error(envelope, value),
        inference @ (ControlPayload::InferenceRequested(_)
        | ControlPayload::InferenceAccepted(_)
        | ControlPayload::InferenceProgress(_)
        | ControlPayload::InferenceStreamChunk(_)
        | ControlPayload::InferenceCompleted(_)
        | ControlPayload::InferenceFailed(_)
        | ControlPayload::InferenceCancelled(_)
        | ControlPayload::InferenceExpired(_)
        | ControlPayload::ToolCallProposed(_)
        | ControlPayload::ToolCallAccepted(_)
        | ControlPayload::ToolCallRejected(_)
        | ControlPayload::ToolCallCompleted(_)) => validate_inference_control(envelope, inference),
    }
}

fn validate_managed(envelope: &Envelope, control: &ControlPayload) -> Result<(), CodecError> {
    use crate::{AdmissionRejectionCode as Rejection, AdmissionStatus as Admission, QueueState};
    require_session_scope(envelope)?;
    require(
        envelope.correlation_id.unwrap_or(0),
        envelope,
        "managed controls require correlation_id",
    )?;
    let valid = match control {
        ControlPayload::RequestAdmission(value) => {
            !value.idempotency_key.is_empty() && value.idempotency_key.len() <= 256
        }
        ControlPayload::QueueStatusRequest(value) => value.ticket_id != 0,
        ControlPayload::QueueHeartbeat(value) => value.ticket_id != 0,
        ControlPayload::QueueClaim(value) => value.ticket_id != 0,
        ControlPayload::QueueCancel(value) => value.ticket_id != 0,
        ControlPayload::AdmissionResult(value) => {
            let queued = value.status == Admission::Queued;
            value.status != Admission::Unknown
                && (value.status == Admission::Rejected)
                    == (value.rejection_code != Rejection::None)
                && queued == value.ticket_id.is_some()
                && value.ticket_id != Some(0)
                && value.poll_after_ms <= 30_000
                && value.ticket_remaining_ms <= 900_000
                && (queued || value.ticket_remaining_ms == 0)
                && (matches!(value.status, Admission::Queued | Admission::Paused)
                    || value.poll_after_ms == 0)
        }
        ControlPayload::QueueUpdate(value) => {
            let live = matches!(value.state, QueueState::Waiting | QueueState::Offered);
            value.ticket_id != 0
                && value.state != QueueState::Unknown
                && (value.state == QueueState::Waiting) == (value.position != 0)
                && value.poll_after_ms <= 30_000
                && value.ticket_remaining_ms <= 900_000
                && value.offer_remaining_ms <= 30_000
                && (value.state == QueueState::Offered || value.offer_remaining_ms == 0)
                && (live || (value.poll_after_ms == 0 && value.ticket_remaining_ms == 0))
        }
        _ => unreachable!("managed control dispatch"),
    };
    if valid {
        Ok(())
    } else {
        invalid(envelope, "invalid managed admission fields")
    }
}

fn validate_inference_control(
    envelope: &Envelope,
    control: &ControlPayload,
) -> Result<(), CodecError> {
    require_space_scope(envelope, false, true)?;
    match control {
        ControlPayload::InferenceRequested(value) => validate_inference_requested(envelope, value),
        ControlPayload::InferenceProgress(value) => {
            if value.percent > 100 {
                return invalid(envelope, "inference progress percent cannot exceed 100");
            }
            Ok(())
        }
        ControlPayload::InferenceFailed(value) => {
            if value.reason.is_empty() {
                return invalid(envelope, "inference failure reason cannot be empty");
            }
            Ok(())
        }
        ControlPayload::ToolCallProposed(value) => validate_tool_call_proposed(envelope, value),
        ControlPayload::ToolCallAccepted(value) => {
            if value.tool_id.is_empty() {
                return invalid(envelope, "tool_id cannot be empty");
            }
            Ok(())
        }
        ControlPayload::ToolCallRejected(value) => {
            if value.code == crate::ToolCallRejectionCode::Unknown {
                return invalid(envelope, "tool call rejection code cannot be Unknown");
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_inference_requested(
    envelope: &Envelope,
    value: &InferenceRequested,
) -> Result<(), CodecError> {
    if value.capability.is_empty() {
        return invalid(envelope, "inference capability cannot be empty");
    }
    Ok(())
}

fn validate_tool_call_proposed(
    envelope: &Envelope,
    value: &ToolCallProposed,
) -> Result<(), CodecError> {
    if value.tool_id.is_empty() {
        return invalid(envelope, "tool_id cannot be empty");
    }
    if value.tool_version == 0 {
        return invalid(envelope, "tool_version must be non-zero");
    }
    Ok(())
}

fn validate_hello(envelope: &Envelope, value: &Hello) -> Result<(), CodecError> {
    if value.min_protocol_version == 0
        || value.min_protocol_version > value.max_protocol_version
        || !(value.min_protocol_version..=value.max_protocol_version)
            .contains(&crate::PROTOCOL_VERSION)
    {
        return invalid(
            envelope,
            "Hello version range must be non-zero, ordered, and include protocol v1",
        );
    }
    validate_advertised_limits(
        envelope,
        value.max_frame_size,
        value.max_payload_size,
        "Hello",
    )
}

fn validate_capabilities(envelope: &Envelope, value: &Capabilities) -> Result<(), CodecError> {
    if value.selected_protocol_version != crate::PROTOCOL_VERSION {
        return invalid(
            envelope,
            "Capabilities selected_protocol_version must be protocol v1",
        );
    }
    validate_advertised_limits(
        envelope,
        value.max_frame_size,
        value.max_payload_size,
        "Capabilities",
    )
}

fn validate_advertised_limits(
    envelope: &Envelope,
    max_frame_size: u32,
    max_payload_size: u32,
    message: &'static str,
) -> Result<(), CodecError> {
    if max_frame_size == 0 || max_payload_size == 0 {
        return invalid(
            envelope,
            match message {
                "Hello" => "Hello advertised limits must be non-zero",
                _ => "Capabilities advertised limits must be non-zero",
            },
        );
    }
    if max_payload_size > max_frame_size {
        return invalid(
            envelope,
            match message {
                "Hello" => "Hello max_payload_size cannot exceed max_frame_size",
                _ => "Capabilities max_payload_size cannot exceed max_frame_size",
            },
        );
    }
    Ok(())
}

fn validate_authenticate(envelope: &Envelope, value: &Authenticate) -> Result<(), CodecError> {
    if value.scheme == crate::AuthenticationScheme::Unknown {
        return invalid(envelope, "authentication scheme cannot be Unknown");
    }
    if value.credentials.is_empty() {
        return invalid(envelope, "authentication credentials cannot be empty");
    }
    Ok(())
}

fn validate_snapshot_request(
    envelope: &Envelope,
    value: &SnapshotRequest,
) -> Result<(), CodecError> {
    reject_zero_option(
        value.after_server_tick,
        envelope,
        "after_server_tick cannot be Some(0)",
    )
}

fn validate_transition(envelope: &Envelope, value: &SpaceTransition) -> Result<(), CodecError> {
    require_space_scope(envelope, false, true)?;
    require(
        value.from_space_id,
        envelope,
        "transition from_space_id must be non-zero",
    )?;
    require(
        value.to_space_id,
        envelope,
        "transition to_space_id must be non-zero",
    )?;
    require(
        value.to_space_epoch,
        envelope,
        "transition to_space_epoch must be non-zero",
    )?;
    if value.from_space_id != envelope.space_id {
        return invalid(
            envelope,
            "transition from_space_id must match envelope space_id",
        );
    }
    if value.from_space_id == value.to_space_id {
        return invalid(envelope, "transition spaces must differ");
    }
    Ok(())
}

fn validate_protocol_error(envelope: &Envelope, value: &ProtocolError) -> Result<(), CodecError> {
    if value.code == crate::ProtocolErrorCode::Unknown {
        return invalid(envelope, "protocol error code cannot be Unknown");
    }
    if value.related_message_kind == MessageKind::Unknown {
        return invalid(envelope, "related message kind cannot be Unknown");
    }
    validate_optional_scope(envelope)
}

fn validate_delivery(envelope: &Envelope) -> Result<(), CodecError> {
    let kind = envelope.message_kind();
    let valid = match kind {
        MessageKind::EntityState => matches!(
            envelope.delivery_class,
            DeliveryClass::LatestValue | DeliveryClass::UnreliableSequenced
        ),
        MessageKind::ReliableEvent => matches!(
            envelope.delivery_class,
            DeliveryClass::ReliableOrdered | DeliveryClass::ReliableUnordered
        ),
        MessageKind::Ping | MessageKind::Pong => {
            envelope.delivery_class == DeliveryClass::ReliableUnordered
        }
        MessageKind::InferenceProgress | MessageKind::InferenceStreamChunk => {
            envelope.delivery_class == DeliveryClass::BestEffortEvent
        }
        MessageKind::Unknown => false,
        _ => envelope.delivery_class == DeliveryClass::ReliableOrdered,
    };
    if valid {
        Ok(())
    } else {
        invalid(
            envelope,
            "delivery class is incompatible with the message kind",
        )
    }
}

fn require_unscoped(envelope: &Envelope) -> Result<(), CodecError> {
    if envelope.namespace_id != 0
        || envelope.session_id != 0
        || envelope.space_id != 0
        || envelope.channel_id.is_some()
        || envelope.entity_id.is_some()
        || envelope.space_epoch != 0
    {
        invalid(
            envelope,
            "connection-level message must not carry namespace, session, space, channel, entity, or epoch scope",
        )
    } else {
        Ok(())
    }
}

fn require_session_scope(envelope: &Envelope) -> Result<(), CodecError> {
    require(
        envelope.namespace_id,
        envelope,
        "namespace_id must be non-zero",
    )?;
    require(envelope.session_id, envelope, "session_id must be non-zero")?;
    if envelope.space_id != 0
        || envelope.channel_id.is_some()
        || envelope.entity_id.is_some()
        || envelope.space_epoch != 0
    {
        invalid(
            envelope,
            "session-level message must not carry space, channel, entity, or epoch scope",
        )
    } else {
        Ok(())
    }
}

fn require_space_scope(
    envelope: &Envelope,
    require_channel: bool,
    require_entity: bool,
) -> Result<(), CodecError> {
    require(
        envelope.namespace_id,
        envelope,
        "namespace_id must be non-zero",
    )?;
    require(envelope.session_id, envelope, "session_id must be non-zero")?;
    require(envelope.space_id, envelope, "space_id must be non-zero")?;
    require(
        envelope.space_epoch,
        envelope,
        "space_epoch must be non-zero",
    )?;

    if require_channel {
        require(
            envelope.channel_id.unwrap_or(0),
            envelope,
            "channel_id must be non-zero",
        )?;
    } else if envelope.channel_id.is_some() {
        return invalid(envelope, "message kind must not carry channel_id");
    }
    if require_entity {
        require(
            envelope.entity_id.unwrap_or(0),
            envelope,
            "entity_id must be non-zero",
        )?;
    }
    Ok(())
}

fn validate_optional_scope(envelope: &Envelope) -> Result<(), CodecError> {
    let has_space_detail = envelope.space_id != 0
        || envelope.channel_id.is_some()
        || envelope.entity_id.is_some()
        || envelope.space_epoch != 0;
    if envelope.namespace_id == 0 && (envelope.session_id != 0 || has_space_detail) {
        return invalid(envelope, "scoped protocol error requires namespace_id");
    }
    if envelope.session_id == 0 && has_space_detail {
        return invalid(envelope, "space-scoped protocol error requires session_id");
    }
    if envelope.space_id == 0
        && (envelope.channel_id.is_some()
            || envelope.entity_id.is_some()
            || envelope.space_epoch != 0)
    {
        return invalid(envelope, "space-scoped protocol error requires space_id");
    }
    Ok(())
}

fn reject_zero_optional_ids(envelope: &Envelope) -> Result<(), CodecError> {
    reject_zero_option(
        envelope.channel_id,
        envelope,
        "channel_id cannot be Some(0)",
    )?;
    reject_zero_option(envelope.entity_id, envelope, "entity_id cannot be Some(0)")?;
    reject_zero_option(
        envelope.correlation_id,
        envelope,
        "correlation_id cannot be Some(0)",
    )
}

fn require(value: u64, envelope: &Envelope, reason: &'static str) -> Result<(), CodecError> {
    if value == 0 {
        invalid(envelope, reason)
    } else {
        Ok(())
    }
}

fn reject_zero_option(
    value: Option<u64>,
    envelope: &Envelope,
    reason: &'static str,
) -> Result<(), CodecError> {
    if value == Some(0) {
        invalid(envelope, reason)
    } else {
        Ok(())
    }
}

fn invalid<T>(envelope: &Envelope, reason: &'static str) -> Result<T, CodecError> {
    Err(CodecError::InvalidSemantics {
        message_kind: envelope.message_kind(),
        reason,
    })
}
