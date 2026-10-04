use woven_protocol::{
    CAPABILITY_CLIENT_LOG, CAPABILITY_POSITIONED_ENTITY_STATE, ClientLog, Codec, CodecError,
    CodecLimits, ControlPayload, DeliveryClass, Envelope, LogLevel, MAX_LOG_MESSAGE_BYTES,
    MessageKind,
};

fn log(level: LogLevel, message: String) -> Envelope {
    let mut envelope = Envelope::control(
        DeliveryClass::ReliableOrdered,
        ControlPayload::ClientLog(ClientLog { level, message }),
    );
    envelope.namespace_id = 11;
    envelope.session_id = 22;
    envelope
}

#[test]
fn stable_values_and_all_levels_roundtrip() {
    assert_eq!(MessageKind::ClientLog.value(), 40);
    assert_eq!(LogLevel::Unknown.value(), 0);
    assert_eq!(LogLevel::Info.value(), 1);
    assert_eq!(LogLevel::Warn.value(), 2);
    assert_eq!(LogLevel::Error.value(), 3);
    assert_eq!(CAPABILITY_CLIENT_LOG, 2);
    assert_eq!(
        CAPABILITY_CLIENT_LOG & CAPABILITY_POSITIONED_ENTITY_STATE,
        0
    );
    assert_eq!(MAX_LOG_MESSAGE_BYTES, 1024);
    let codec = Codec::default();
    for level in [LogLevel::Info, LogLevel::Warn, LogLevel::Error] {
        for message in ["hello".to_owned(), "é".repeat(512), "🧶".repeat(256)] {
            let envelope = log(level, message);
            assert_eq!(
                codec.decode(&codec.encode(&envelope).unwrap()).unwrap(),
                envelope
            );
        }
    }
}

#[test]
fn rejects_unknown_empty_and_oversized_utf8_messages() {
    let codec = Codec::default();
    for envelope in [
        log(LogLevel::Unknown, "hello".to_owned()),
        log(LogLevel::Info, String::new()),
        log(LogLevel::Info, "x".repeat(1025)),
        log(LogLevel::Warn, "é".repeat(513)),
        log(LogLevel::Error, format!("{}x", "🧶".repeat(256))),
    ] {
        assert!(matches!(
            codec.encode(&envelope),
            Err(CodecError::InvalidSemantics { .. })
        ));
    }
}

#[test]
fn rejects_missing_session_extra_scope_and_wrong_delivery() {
    let codec = Codec::default();
    for invalid in 0..8 {
        let mut envelope = log(LogLevel::Info, "hello".to_owned());
        match invalid {
            0 => envelope.namespace_id = 0,
            1 => envelope.session_id = 0,
            2 => envelope.space_id = 1,
            3 => envelope.channel_id = Some(1),
            4 => envelope.entity_id = Some(1),
            5 => envelope.space_epoch = 1,
            6 => envelope.delivery_class = DeliveryClass::ReliableUnordered,
            _ => {
                envelope.routing_position = Some(woven_protocol::RoutingPosition3D {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                });
            }
        }
        assert!(codec.encode(&envelope).is_err());
    }
}

#[test]
fn client_log_golden_is_byte_stable_and_decodes() {
    let envelope = log(LogLevel::Warn, "🧶 session warning".to_owned());
    let golden = include_bytes!("fixtures/client_log_v1.swp");
    let codec = Codec::default();
    assert_eq!(codec.encode(&envelope).unwrap().as_slice(), golden);
    assert_eq!(codec.decode(golden).unwrap(), envelope);
}

#[test]
fn configured_payload_limits_still_apply_in_both_directions() {
    let envelope = log(LogLevel::Info, "é".repeat(8));
    let codec = Codec::new(CodecLimits::new(4096, 15).unwrap()).unwrap();
    assert!(matches!(
        codec.encode(&envelope),
        Err(CodecError::PayloadTooLarge {
            actual: 16,
            maximum: 15
        })
    ));
    let frame = Codec::default().encode(&envelope).unwrap();
    assert!(matches!(
        codec.decode(&frame),
        Err(CodecError::PayloadTooLarge {
            actual: 16,
            maximum: 15
        })
    ));
}
