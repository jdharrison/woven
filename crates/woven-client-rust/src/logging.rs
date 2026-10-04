use woven_protocol::{
    CAPABILITY_CLIENT_LOG, ClientLog, CodecError, ControlPayload, DeliveryClass, Envelope,
    LeaveSession, LogLevel, MAX_LOG_MESSAGE_BYTES, MessageKind, MessagePayload, ProtocolErrorCode,
};

use crate::{Client, ClientError};

/// A logger borrowing the client's ordered control stream, with no queue or retries.
/// Successful sends do not acknowledge server receipt or persistence.
pub struct ClientLogger<'a> {
    client: &'a mut Client,
}

impl ClientLogger<'_> {
    /// Send an info-level message to the remembered session.
    pub async fn info(&mut self, message: impl AsRef<str>) -> Result<(), ClientError> {
        self.client.send_log(LogLevel::Info, message.as_ref()).await
    }

    /// Send a warning to the remembered session.
    pub async fn warn(&mut self, message: impl AsRef<str>) -> Result<(), ClientError> {
        self.client.send_log(LogLevel::Warn, message.as_ref()).await
    }

    /// Send an error to the remembered session.
    pub async fn error(&mut self, message: impl AsRef<str>) -> Result<(), ClientError> {
        self.client
            .send_log(LogLevel::Error, message.as_ref())
            .await
    }
}

impl Client {
    /// Borrow a session logger. Join or receive a verified Admitted result first.
    pub fn logger(&mut self) -> ClientLogger<'_> {
        ClientLogger { client: self }
    }

    /// Info-level alias. Completion means sent, not persisted.
    pub async fn log(&mut self, message: impl AsRef<str>) -> Result<(), ClientError> {
        self.send_log(LogLevel::Info, message.as_ref()).await
    }

    async fn send_log(&mut self, level: LogLevel, message: &str) -> Result<(), ClientError> {
        let (namespace_id, session_id) = self.session_scope.ok_or_else(no_session)?;
        if self.negotiated_capability_bits & CAPABILITY_CLIENT_LOG == 0 {
            return Err(ClientError::UnsupportedCapability("ClientLog"));
        }
        if message.is_empty() || message.len() > MAX_LOG_MESSAGE_BYTES {
            return Err(ClientError::Protocol(CodecError::InvalidSemantics {
                message_kind: MessageKind::ClientLog,
                reason: "client log message must contain 1 to 1024 UTF-8 bytes",
            }));
        }
        let mut envelope = Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::ClientLog(ClientLog {
                level,
                message: message.to_owned(),
            }),
        );
        envelope.namespace_id = namespace_id;
        envelope.session_id = session_id;
        self.send_envelope(&envelope).await
    }

    /// Leave the remembered session and disable logging until another join/admission.
    pub async fn leave_session(&mut self, reason: impl AsRef<str>) -> Result<(), ClientError> {
        let (namespace_id, session_id) = self.session_scope.ok_or_else(no_session)?;
        let mut envelope = Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::LeaveSession(LeaveSession {
                reason: reason.as_ref().to_owned(),
            }),
        );
        envelope.namespace_id = namespace_id;
        envelope.session_id = session_id;
        self.send_envelope(&envelope).await?;
        self.session_scope = None;
        Ok(())
    }
}

fn no_session() -> ClientError {
    ClientError::Protocol(CodecError::InvalidSemantics {
        message_kind: MessageKind::ClientLog,
        reason: "no joined or admitted session",
    })
}

pub(crate) fn observe_session_scope(scope: &mut Option<(u64, u64)>, envelope: &Envelope) {
    let matching = *scope == Some((envelope.namespace_id, envelope.session_id));
    match &envelope.message {
        MessagePayload::Control(ControlPayload::LeaveSession(_)) if matching => *scope = None,
        MessagePayload::Control(ControlPayload::ProtocolError(error))
            if (matching || (envelope.namespace_id == 0 && envelope.session_id == 0))
                && (error.related_message_kind == MessageKind::JoinSession
                    || (error.related_message_kind == MessageKind::ClientLog
                        && matches!(
                            error.code,
                            ProtocolErrorCode::AuthenticationRequired
                                | ProtocolErrorCode::Unauthorized
                                | ProtocolErrorCode::InvalidScope
                        ))) =>
        {
            *scope = None;
        }
        _ => {}
    }
}
