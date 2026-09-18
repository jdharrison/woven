use std::{fs, path::PathBuf};
use woven_protocol::{Codec, ControlPayload, DeliveryClass, Envelope, QueueState, QueueUpdate};

fn main() {
    let mut envelope = Envelope::control(
        DeliveryClass::ReliableOrdered,
        ControlPayload::QueueUpdate(QueueUpdate {
            ticket_id: u64::MAX,
            state: QueueState::Offered,
            position: 0,
            poll_after_ms: 1_000,
            ticket_remaining_ms: 120_000,
            offer_remaining_ms: 30_000,
        }),
    );
    envelope.namespace_id = 1;
    envelope.session_id = 2;
    envelope.correlation_id = Some(3);
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    fs::write(
        directory.join("queue_update_v1.swp"),
        Codec::default().encode(&envelope).expect("valid fixture"),
    )
    .expect("write fixture");
    fs::write(directory.join("queue_update_v1.expected.txt"), "message_kind=QueueUpdate\nnamespace_id=1\nsession_id=2\ncorrelation_id=3\nticket_id=18446744073709551615\nstate=Offered\nposition=0\npoll_after_ms=1000\nticket_remaining_ms=120000\noffer_remaining_ms=30000\n").expect("write expected values");
}
