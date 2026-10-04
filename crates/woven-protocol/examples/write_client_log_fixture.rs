use std::{fs, path::PathBuf};
use woven_protocol::{ClientLog, Codec, ControlPayload, DeliveryClass, Envelope, LogLevel};

fn main() {
    let mut envelope = Envelope::control(
        DeliveryClass::ReliableOrdered,
        ControlPayload::ClientLog(ClientLog {
            level: LogLevel::Warn,
            message: "🧶 session warning".to_owned(),
        }),
    );
    envelope.namespace_id = 11;
    envelope.session_id = 22;
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    fs::write(
        directory.join("client_log_v1.swp"),
        Codec::default().encode(&envelope).expect("valid fixture"),
    )
    .expect("write fixture");
    fs::write(
        directory.join("client_log_v1.expected.txt"),
        "message_kind=ClientLog\nnamespace_id=11\nsession_id=22\nlevel=Warn\nmessage=🧶 session warning\n",
    ).expect("write expected values");
}
