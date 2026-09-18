use woven_protocol::{
    Authenticate, Authenticated, AuthenticationScheme, Capabilities, Codec, CodecError,
    CodecLimits, ControlPayload, DeliveryClass, EntityEntered, EntityLeaveReason, EntityLeft,
    Envelope, Hello, InferenceAccepted, InferenceCancelled, InferenceCompleted, InferenceExpired,
    InferenceFailed, InferenceProgress, InferenceRequested, InferenceStreamChunk, JoinSession,
    LeaveSession, MessageKind, MessagePayload, OpaquePayload, Ping, Pong, ProtocolError,
    ProtocolErrorCode, SnapshotRequest, SpaceTransition, SubscribeSpace, SubscriptionAccepted,
    SubscriptionRejected, SubscriptionRejectionCode, ToolCallAccepted, ToolCallCompleted,
    ToolCallProposed, ToolCallRejected, ToolCallRejectionCode, UnsubscribeSpace,
};

fn envelope_for_control(payload: ControlPayload) -> Envelope {
    let kind = payload.message_kind();
    let delivery = if matches!(kind, MessageKind::Ping | MessageKind::Pong) {
        DeliveryClass::ReliableUnordered
    } else if matches!(
        kind,
        MessageKind::InferenceProgress | MessageKind::InferenceStreamChunk
    ) {
        DeliveryClass::BestEffortEvent
    } else {
        DeliveryClass::ReliableOrdered
    };
    let mut envelope = Envelope::control(delivery, payload);

    match kind {
        MessageKind::Hello
        | MessageKind::Capabilities
        | MessageKind::Authenticate
        | MessageKind::Authenticated
        | MessageKind::Ping
        | MessageKind::Pong
        | MessageKind::ProtocolError => {}
        MessageKind::RequestAdmission
        | MessageKind::AdmissionResult
        | MessageKind::QueueStatusRequest
        | MessageKind::QueueHeartbeat
        | MessageKind::QueueClaim
        | MessageKind::QueueCancel
        | MessageKind::QueueUpdate => {
            envelope.namespace_id = 10;
            envelope.session_id = 20;
            envelope.correlation_id = Some(1);
        }
        MessageKind::JoinSession | MessageKind::LeaveSession => {
            envelope.namespace_id = 10;
            envelope.session_id = 20;
        }
        MessageKind::SubscribeSpace
        | MessageKind::UnsubscribeSpace
        | MessageKind::SubscriptionAccepted
        | MessageKind::SubscriptionRejected
        | MessageKind::SnapshotRequest => {
            envelope.namespace_id = 10;
            envelope.session_id = 20;
            envelope.space_id = 30;
            envelope.channel_id = Some(35);
            envelope.space_epoch = match &envelope.message {
                MessagePayload::Control(ControlPayload::SubscriptionAccepted(value)) => {
                    value.accepted_space_epoch
                }
                _ => 50,
            };
        }
        MessageKind::EntityEntered
        | MessageKind::EntityLeft
        | MessageKind::InferenceRequested
        | MessageKind::InferenceAccepted
        | MessageKind::InferenceProgress
        | MessageKind::InferenceStreamChunk
        | MessageKind::InferenceCompleted
        | MessageKind::InferenceFailed
        | MessageKind::InferenceCancelled
        | MessageKind::InferenceExpired
        | MessageKind::ToolCallProposed
        | MessageKind::ToolCallAccepted
        | MessageKind::ToolCallRejected
        | MessageKind::ToolCallCompleted => {
            envelope.namespace_id = 10;
            envelope.session_id = 20;
            envelope.space_id = 30;
            envelope.entity_id = Some(40);
            envelope.space_epoch = 50;
        }
        MessageKind::SpaceTransition => {
            let MessagePayload::Control(ControlPayload::SpaceTransition(value)) = &envelope.message
            else {
                unreachable!("kind and control payload must agree");
            };
            envelope.namespace_id = 10;
            envelope.session_id = 20;
            envelope.space_id = value.from_space_id;
            envelope.entity_id = Some(40);
            envelope.space_epoch = 50;
        }
        MessageKind::Unknown
        | MessageKind::EntityState
        | MessageKind::ReliableEvent
        | MessageKind::Snapshot => unreachable!("not a control message"),
    }
    envelope
}

#[test]
#[allow(clippy::too_many_lines)]
fn all_typed_control_payloads_roundtrip_with_semantic_scopes() {
    let controls = vec![
        ControlPayload::Hello(Hello {
            min_protocol_version: 1,
            max_protocol_version: 1,
            client_name: "woven-test".to_owned(),
            client_version: "0.1.0".to_owned(),
            capability_bits: 0b101,
            max_frame_size: 65_536,
            max_payload_size: 16_384,
        }),
        ControlPayload::Capabilities(Capabilities {
            selected_protocol_version: 1,
            server_name: "node-a".to_owned(),
            server_version: "0.1.0".to_owned(),
            capability_bits: 0b111,
            max_frame_size: 1_048_576,
            max_payload_size: 262_144,
        }),
        ControlPayload::Authenticate(Authenticate {
            scheme: AuthenticationScheme::Bearer,
            credentials: b"opaque-token".to_vec(),
        }),
        ControlPayload::Authenticated(Authenticated {
            principal_id: 91,
            assigned_entity_id: Some(92),
        }),
        ControlPayload::JoinSession(JoinSession {
            resume_token: b"resume".to_vec(),
        }),
        ControlPayload::LeaveSession(LeaveSession {
            reason: "client request".to_owned(),
        }),
        ControlPayload::SubscribeSpace(SubscribeSpace),
        ControlPayload::UnsubscribeSpace(UnsubscribeSpace {
            subscription_id: 102,
        }),
        ControlPayload::SubscriptionAccepted(SubscriptionAccepted {
            subscription_id: 103,
            accepted_space_epoch: 104,
        }),
        ControlPayload::SubscriptionRejected(SubscriptionRejected {
            code: SubscriptionRejectionCode::Unauthorized,
            reason: "denied".to_owned(),
        }),
        ControlPayload::EntityEntered(EntityEntered {
            owner_entity_id: Some(105),
        }),
        ControlPayload::EntityLeft(EntityLeft {
            reason: EntityLeaveReason::Transitioned,
        }),
        ControlPayload::SnapshotRequest(SnapshotRequest {
            after_server_tick: Some(107),
        }),
        ControlPayload::SpaceTransition(SpaceTransition {
            from_space_id: 108,
            to_space_id: 109,
            to_space_epoch: 110,
        }),
        ControlPayload::Ping(Ping {
            nonce: 111,
            sender_time_micros: 112,
        }),
        ControlPayload::Pong(Pong {
            nonce: 111,
            sender_time_micros: 112,
            responder_time_micros: 113,
        }),
        ControlPayload::ProtocolError(ProtocolError {
            code: ProtocolErrorCode::InvalidScope,
            related_message_kind: MessageKind::SubscribeSpace,
            message: "unknown space".to_owned(),
        }),
        ControlPayload::InferenceRequested(InferenceRequested {
            capability: "language.dialogue".to_owned(),
            deadline_ms: 2_000,
            input: b"hello companion".to_vec(),
        }),
        ControlPayload::InferenceAccepted(InferenceAccepted { queued_position: 1 }),
        ControlPayload::InferenceProgress(InferenceProgress { percent: 50 }),
        ControlPayload::InferenceStreamChunk(InferenceStreamChunk {
            sequence: 1,
            chunk: b"partial".to_vec(),
            is_final: false,
        }),
        ControlPayload::InferenceCompleted(InferenceCompleted {
            result: b"final answer".to_vec(),
        }),
        ControlPayload::InferenceFailed(InferenceFailed {
            reason: "provider unavailable".to_owned(),
        }),
        ControlPayload::InferenceCancelled(InferenceCancelled {
            reason: "user cancelled".to_owned(),
        }),
        ControlPayload::InferenceExpired(InferenceExpired {
            reason: "deadline passed".to_owned(),
        }),
        ControlPayload::ToolCallProposed(ToolCallProposed {
            tool_id: "diagnostics.report".to_owned(),
            tool_version: 1,
            arguments: b"{}".to_vec(),
            expected_revision: 3,
        }),
        ControlPayload::ToolCallAccepted(ToolCallAccepted {
            tool_id: "diagnostics.report".to_owned(),
        }),
        ControlPayload::ToolCallRejected(ToolCallRejected {
            code: ToolCallRejectionCode::Stale,
            reason: "expected_revision is stale".to_owned(),
        }),
        ControlPayload::ToolCallCompleted(ToolCallCompleted {
            new_revision: 4,
            result: b"ok".to_vec(),
        }),
    ];
    let codec = Codec::default();

    for control in controls {
        let expected = envelope_for_control(control);
        let frame = codec.encode(&expected).expect("control should encode");
        let actual = codec.decode(&frame).expect("control should decode");
        assert_eq!(actual, expected);
        assert_eq!(actual.payload_type_id(), None);
        assert!(actual.payload_bytes().is_empty());
    }
}

#[test]
fn opaque_domain_payloads_roundtrip_with_required_channel_scope() {
    let messages = [
        (
            DeliveryClass::LatestValue,
            MessagePayload::EntityState(OpaquePayload {
                type_id: 201,
                bytes: vec![1, 2, 3],
            }),
            Some(4),
        ),
        (
            DeliveryClass::ReliableOrdered,
            MessagePayload::ReliableEvent(OpaquePayload {
                type_id: 202,
                bytes: vec![4, 5, 6],
            }),
            Some(4),
        ),
        (
            DeliveryClass::ReliableOrdered,
            MessagePayload::Snapshot(OpaquePayload {
                type_id: 203,
                bytes: vec![7, 8, 9],
            }),
            None,
        ),
    ];
    let codec = Codec::default();

    for (delivery, message, entity_id) in messages {
        let mut expected = Envelope::new(delivery, message);
        expected.namespace_id = 1;
        expected.session_id = 2;
        expected.space_id = 3;
        expected.channel_id = Some(9);
        expected.entity_id = entity_id;
        expected.space_epoch = 5;
        expected.server_tick = 6;
        expected.sender_sequence = 7;
        expected.correlation_id = Some(8);

        let frame = codec
            .encode(&expected)
            .expect("domain message should encode");
        assert_eq!(codec.decode(&frame), Ok(expected));
    }
}

#[test]
fn inbound_control_variable_data_respects_payload_limit() {
    let envelope = Envelope::control(
        DeliveryClass::ReliableOrdered,
        ControlPayload::Hello(Hello {
            min_protocol_version: 1,
            max_protocol_version: 1,
            client_name: "sixteen-byte-name".to_owned(),
            client_version: "sixteen-byte-vers".to_owned(),
            capability_bits: 0,
            max_frame_size: 512,
            max_payload_size: 128,
        }),
    );
    let frame = Codec::default()
        .encode(&envelope)
        .expect("large-limit codec should encode");
    let codec = Codec::new(CodecLimits::new(512, 8).expect("valid limits")).expect("valid codec");

    assert!(matches!(
        codec.decode(&frame),
        Err(CodecError::PayloadTooLarge { maximum: 8, .. })
    ));
}

#[test]
fn outbound_semantic_invalidity_is_rejected() {
    let invalid_messages = [
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Hello(Hello {
                min_protocol_version: 2,
                max_protocol_version: 1,
                client_name: "client".to_owned(),
                client_version: "1".to_owned(),
                capability_bits: 0,
                max_frame_size: 512,
                max_payload_size: 128,
            }),
        ),
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Authenticate(Authenticate {
                scheme: AuthenticationScheme::Unknown,
                credentials: b"token".to_vec(),
            }),
        ),
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::Authenticated(Authenticated {
                principal_id: 0,
                assigned_entity_id: None,
            }),
        ),
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::UnsubscribeSpace(UnsubscribeSpace { subscription_id: 0 }),
        ),
    ];
    let codec = Codec::default();
    for envelope in invalid_messages {
        assert!(matches!(
            codec.encode(&envelope),
            Err(CodecError::InvalidSemantics { .. })
        ));
    }

    let mut scoped_hello = envelope_for_control(ControlPayload::Hello(Hello {
        min_protocol_version: 1,
        max_protocol_version: 1,
        client_name: "client".to_owned(),
        client_version: "1".to_owned(),
        capability_bits: 0,
        max_frame_size: 512,
        max_payload_size: 128,
    }));
    scoped_hello.namespace_id = 1;
    assert!(matches!(
        codec.encode(&scoped_hello),
        Err(CodecError::InvalidSemantics { .. })
    ));

    let mut missing_channel = Envelope::snapshot(
        DeliveryClass::ReliableOrdered,
        OpaquePayload {
            type_id: 7,
            bytes: vec![],
        },
    );
    missing_channel.namespace_id = 1;
    missing_channel.session_id = 2;
    missing_channel.space_id = 3;
    missing_channel.space_epoch = 4;
    assert!(matches!(
        codec.encode(&missing_channel),
        Err(CodecError::InvalidSemantics { .. })
    ));

    let wrong_delivery = Envelope::control(
        DeliveryClass::BestEffortEvent,
        ControlPayload::Ping(Ping {
            nonce: 1,
            sender_time_micros: 2,
        }),
    );
    assert!(matches!(
        codec.encode(&wrong_delivery),
        Err(CodecError::InvalidSemantics { .. })
    ));
}
