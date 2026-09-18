//! Cross-language proof that the TypeScript client's encoder is wire-compatible
//! with the Rust `Codec`. Each fixture below is the exact byte sequence produced by
//! `woven-client-ts/src/encode.ts` for the corresponding outbound message.
//!
//! The Rust `Codec` must verify and decode every one of them into the expected
//! semantic envelope. This is the server-side counterpart to the TS-side tests
//! that decode Rust-produced golden fixtures (`tests/fixtures/*.swp`).

use woven_protocol::{
    AuthenticationScheme, Codec, CodecError, Hello, MessagePayload, QueueCancel, QueueClaim,
    QueueHeartbeat, QueueStatusRequest, RequestAdmission, SubscribeSpace,
};

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex nibble"))
        .collect()
}

fn decode_hex(s: &str) -> Result<woven_protocol::Envelope, CodecError> {
    Codec::default().decode(&hex_decode(s))
}

const TS_HELLO: &str = "800000002c00000057564e31000022000e0000000d000c0000000000000000000000000000000000000000000b000400220000001c000000000000010101120014000000000010000c00000008000400120000000000010000000100080000001000000005000000302e322e300000000f000000776f76656e2d636c69656e742d747300";
const TS_AUTHENTICATE: &str = "5c0000002c00000057564e31000022000c0000000b000a0000000000000000000000000000000000000000000900040022000000100000000003010308000c000b000400080000000800000000000002090000006465762d746f6b656e000000";
const TS_JOIN_SESSION: &str = "640000003000000057564e310000000000002200220000002100200014000c00000000000000000000000000000000000b00040022000000240000000000000501000000000000000100000000000000000000000105060008000400060000000400000000000000";
const TS_SUBSCRIBE_SPACE: &str = "7c0000003000000057564e310000000024003a000000390038002c0024001c00000014000000000000000000000013000c0004002400000001000000000000003400000000000007010000000000000001000000000000000100000000000000010000000000000000000000010706000c000400060000000100000000000000";
const TS_RELIABLE_EVENT: &str = "8c0000003000000057564e31000000002400540000005300520044003c0034002c00240000001c0000001400100000000000040024000000010000000000000000000000440000000100000000000000010000000000000001000000000000000100000000000000010000000000000001000000000000000100000000000000000000000000010e0200000068690000";
const TS_ENTITY_STATE: &str = "8c0000003000000057564e31000000002400500000004f004e0044003c0034002c00240000001c00000014001000000000000400240000000200000000000000000000004000000005000000000000000300000000000000010000000000000007000000000000000100000000000000010000000000000001000000000000000000030d050000007374617465000000";
const TS_BEARER_AUTHENTICATE: &str = "600000002c00000057564e31000022000c0000000b000a0000000000000000000000000000000000000000000900040022000000100000000003010308000c000b0004000800000008000000000000010d0000006d616e616765642d746f6b656e000000";
const TS_REQUEST_ADMISSION: &str = "740000003000000057564e31000000000000220026000000250024001c001400000000000000000000000c00000000000b00040022000000280000000000001e0d000000000000000c000000000000000b000000000000000121060008000400060000000400000009000000617474656d70742d31000000";
const TS_QUEUE_STATUS: &str = "6c0000003000000057564e3100000000000022002a000000290028001c001400000000000000000000000c00000000000b000400220000002c000000000000200d000000000000000c000000000000000b0000000000000000000000012306000c000400060000000e00000000000000";
const TS_QUEUE_HEARTBEAT: &str = "6c0000003000000057564e3100000000000022002a000000290028001c001400000000000000000000000c00000000000b000400220000002c000000000000210d000000000000000c000000000000000b0000000000000000000000012406000c000400060000000e00000000000000";
const TS_QUEUE_CLAIM: &str = "6c0000003000000057564e3100000000000022002a000000290028001c001400000000000000000000000c00000000000b000400220000002c000000000000220d000000000000000c000000000000000b0000000000000000000000012506000c000400060000000e00000000000000";
const TS_QUEUE_CANCEL: &str = "6c0000003000000057564e3100000000000022002a000000290028001c001400000000000000000000000c00000000000b000400220000002c000000000000230d000000000000000c000000000000000b0000000000000000000000012606000c000400060000000e00000000000000";

#[test]
fn ts_hello_decodes() {
    let envelope = decode_hex(TS_HELLO).expect("TS Hello must decode");
    let MessagePayload::Control(woven_protocol::ControlPayload::Hello(hello)) = envelope.message
    else {
        panic!("expected Hello, got {:?}", envelope.message);
    };
    assert_eq!(
        hello,
        Hello {
            min_protocol_version: 1,
            max_protocol_version: 1,
            client_name: "woven-client-ts".to_owned(),
            client_version: "0.2.0".to_owned(),
            capability_bits: 0,
            max_frame_size: 65536,
            max_payload_size: 65536,
        }
    );
    assert_eq!(envelope.namespace_id, 0);
}

#[test]
fn ts_authenticate_decodes() {
    let envelope = decode_hex(TS_AUTHENTICATE).expect("TS Authenticate must decode");
    let MessagePayload::Control(woven_protocol::ControlPayload::Authenticate(auth)) =
        envelope.message
    else {
        panic!("expected Authenticate, got {:?}", envelope.message);
    };
    assert_eq!(auth.credentials, b"dev-token");
}

#[test]
fn ts_bearer_authenticate_decodes() {
    let envelope = decode_hex(TS_BEARER_AUTHENTICATE).expect("TS Bearer Authenticate must decode");
    let MessagePayload::Control(woven_protocol::ControlPayload::Authenticate(auth)) =
        envelope.message
    else {
        panic!("expected Authenticate, got {:?}", envelope.message);
    };
    assert_eq!(auth.scheme, AuthenticationScheme::Bearer);
    assert_eq!(auth.credentials, b"managed-token");
}

#[test]
fn ts_managed_admission_requests_decode() {
    let envelope = decode_hex(TS_REQUEST_ADMISSION).expect("TS RequestAdmission must decode");
    assert_eq!(envelope.namespace_id, 11);
    assert_eq!(envelope.session_id, 12);
    assert_eq!(envelope.correlation_id, Some(13));
    assert_eq!(
        envelope.message,
        MessagePayload::Control(woven_protocol::ControlPayload::RequestAdmission(
            RequestAdmission {
                idempotency_key: "attempt-1".to_owned(),
            },
        )),
    );

    let cases = [
        (
            TS_QUEUE_STATUS,
            MessagePayload::Control(woven_protocol::ControlPayload::QueueStatusRequest(
                QueueStatusRequest { ticket_id: 14 },
            )),
        ),
        (
            TS_QUEUE_HEARTBEAT,
            MessagePayload::Control(woven_protocol::ControlPayload::QueueHeartbeat(
                QueueHeartbeat { ticket_id: 14 },
            )),
        ),
        (
            TS_QUEUE_CLAIM,
            MessagePayload::Control(woven_protocol::ControlPayload::QueueClaim(QueueClaim {
                ticket_id: 14,
            })),
        ),
        (
            TS_QUEUE_CANCEL,
            MessagePayload::Control(woven_protocol::ControlPayload::QueueCancel(QueueCancel {
                ticket_id: 14,
            })),
        ),
    ];
    for (fixture, expected) in cases {
        let envelope = decode_hex(fixture).expect("TS queue request must decode");
        assert_eq!(envelope.namespace_id, 11);
        assert_eq!(envelope.session_id, 12);
        assert_eq!(envelope.correlation_id, Some(13));
        assert_eq!(envelope.message, expected);
    }
}

#[test]
fn ts_join_session_decodes() {
    let envelope = decode_hex(TS_JOIN_SESSION).expect("TS JoinSession must decode");
    let MessagePayload::Control(woven_protocol::ControlPayload::JoinSession(_)) = envelope.message
    else {
        panic!("expected JoinSession, got {:?}", envelope.message);
    };
    assert_eq!(envelope.namespace_id, 1);
    assert_eq!(envelope.session_id, 1);
}

#[test]
fn ts_subscribe_space_decodes() {
    let envelope = decode_hex(TS_SUBSCRIBE_SPACE).expect("TS SubscribeSpace must decode");
    let MessagePayload::Control(woven_protocol::ControlPayload::SubscribeSpace(SubscribeSpace)) =
        envelope.message
    else {
        panic!("expected SubscribeSpace, got {:?}", envelope.message);
    };
    assert_eq!(envelope.namespace_id, 1);
    assert_eq!(envelope.session_id, 1);
    assert_eq!(envelope.space_id, 1);
    assert_eq!(envelope.space_epoch, 1);
    assert_eq!(envelope.channel_id, Some(1));
}

#[test]
fn ts_reliable_event_decodes() {
    let envelope = decode_hex(TS_RELIABLE_EVENT).expect("TS ReliableEvent must decode");
    let MessagePayload::ReliableEvent(payload) = envelope.message else {
        panic!("expected ReliableEvent, got {:?}", envelope.message);
    };
    assert_eq!(payload.bytes, b"hi");
    assert_eq!(payload.type_id, 1);
    assert_eq!(envelope.namespace_id, 1);
    assert_eq!(envelope.space_id, 1);
    assert_eq!(envelope.space_epoch, 1);
    assert_eq!(envelope.channel_id, Some(1));
    assert_eq!(envelope.entity_id, Some(1));
    assert_eq!(envelope.sender_sequence, 1);
}

#[test]
fn ts_entity_state_decodes() {
    let envelope = decode_hex(TS_ENTITY_STATE).expect("TS EntityState must decode");
    let MessagePayload::EntityState(payload) = envelope.message else {
        panic!("expected EntityState, got {:?}", envelope.message);
    };
    assert_eq!(payload.bytes, b"state");
    assert_eq!(payload.type_id, 5);
    assert_eq!(envelope.entity_id, Some(7));
    assert_eq!(envelope.sender_sequence, 3);
    assert_eq!(envelope.channel_id, Some(2));
}
