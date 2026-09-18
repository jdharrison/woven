use tokio::sync::mpsc;
use woven_core::{
    Command, CommandResult, ConnectionId, IdempotencyKey, JoinDecision, NamespaceId,
    QueueOperation, QueueStatus, QueueTicketId, RejectionReason, SessionId, SessionKey,
};
use woven_protocol::{
    AdmissionRejectionCode, AdmissionResult, AdmissionStatus, Codec, ControlPayload, DeliveryClass,
    Envelope, MessagePayload, ProtocolError, ProtocolErrorCode, QueueState, QueueUpdate,
};

use crate::{TransportError, WorkerHandle, core_error_code, send_envelope};

pub(super) async fn handle(
    worker: &WorkerHandle,
    connection: ConnectionId,
    request: Envelope,
    sender: &mpsc::Sender<Envelope>,
) -> Result<(), ()> {
    // Network adapters already decode through Codec; also defend direct bridge callers
    // before any worker mutation, using the same authoritative semantic validation.
    Codec::default().encode(&request).map_err(|_| ())?;
    let session = SessionKey::new(
        NamespaceId::new(request.namespace_id),
        SessionId::new(request.session_id),
    );
    let mut ticket_id = None;
    let command = match &request.message {
        MessagePayload::Control(ControlPayload::RequestAdmission(value)) => {
            Command::RequestSessionAdmission {
                connection,
                session,
                idempotency_key: IdempotencyKey::new(value.idempotency_key.clone()).ok_or(())?,
            }
        }
        MessagePayload::Control(control) => {
            let (ticket, operation) = match control {
                ControlPayload::QueueStatusRequest(value) => {
                    (value.ticket_id, QueueOperation::Status)
                }
                ControlPayload::QueueHeartbeat(value) => {
                    (value.ticket_id, QueueOperation::Heartbeat)
                }
                ControlPayload::QueueClaim(value) => (value.ticket_id, QueueOperation::Claim),
                ControlPayload::QueueCancel(value) => (value.ticket_id, QueueOperation::Cancel),
                _ => return reject(sender, &request, ProtocolErrorCode::UnsupportedMessage).await,
            };
            ticket_id = Some(ticket);
            Command::SessionQueue {
                connection,
                session,
                ticket: QueueTicketId::new(ticket),
                operation,
            }
        }
        _ => return Err(()),
    };
    let control = match worker.execute(command).await {
        Ok(CommandResult::Admission(decision)) if ticket_id.is_none() => {
            // The worker has already bound membership for Admitted. Never serialize
            // its lease, principal, or resume token or perform a second join here.
            ControlPayload::AdmissionResult(admission_result(decision))
        }
        Ok(CommandResult::Queue(status)) => {
            ControlPayload::QueueUpdate(queue_update(ticket_id.ok_or(())?, status)?)
        }
        Ok(_) => return reject(sender, &request, ProtocolErrorCode::Internal).await,
        Err(error) => {
            let code = match error {
                TransportError::Core(ref error) => core_error_code(error),
                _ => ProtocolErrorCode::Internal,
            };
            return reject(sender, &request, code).await;
        }
    };
    send_envelope(sender, response(&request, control)).await
}

fn response(request: &Envelope, control: ControlPayload) -> Envelope {
    let mut reply = Envelope::control(DeliveryClass::ReliableOrdered, control);
    reply.namespace_id = request.namespace_id;
    reply.session_id = request.session_id;
    reply.correlation_id = request.correlation_id;
    reply
}

async fn reject(
    sender: &mpsc::Sender<Envelope>,
    request: &Envelope,
    code: ProtocolErrorCode,
) -> Result<(), ()> {
    send_envelope(
        sender,
        response(
            request,
            ControlPayload::ProtocolError(ProtocolError {
                code,
                related_message_kind: request.message_kind(),
                message: "admission operation rejected".to_owned(),
            }),
        ),
    )
    .await?;
    Err(())
}

fn admission_result(decision: JoinDecision) -> AdmissionResult {
    let (status, rejection_code, ticket_id) = match decision {
        JoinDecision::Admitted(_) => (
            AdmissionStatus::Admitted,
            AdmissionRejectionCode::None,
            None,
        ),
        JoinDecision::Queued(ticket) => (
            AdmissionStatus::Queued,
            AdmissionRejectionCode::None,
            Some(ticket.id.get()),
        ),
        JoinDecision::Paused => (AdmissionStatus::Paused, AdmissionRejectionCode::None, None),
        JoinDecision::Rejected(reason) => (
            AdmissionStatus::Rejected,
            match reason {
                RejectionReason::ServerPaused => AdmissionRejectionCode::ServerPaused,
                RejectionReason::QueueFull => AdmissionRejectionCode::QueueFull,
                RejectionReason::QueueDisabled => AdmissionRejectionCode::QueueDisabled,
                RejectionReason::AlreadyQueued => AdmissionRejectionCode::AlreadyQueued,
                RejectionReason::InvalidIdempotencyKey => {
                    AdmissionRejectionCode::InvalidIdempotencyKey
                }
            },
            None,
        ),
    };
    AdmissionResult {
        status,
        rejection_code,
        ticket_id,
        poll_after_ms: if matches!(status, AdmissionStatus::Queued | AdmissionStatus::Paused) {
            1_000
        } else {
            0
        },
        // The current worker returns state only, not remaining lifetimes.
        ticket_remaining_ms: 0,
    }
}

fn queue_update(ticket_id: u64, status: QueueStatus) -> Result<QueueUpdate, ()> {
    let (state, position) = match status {
        QueueStatus::Waiting { position } => (
            QueueState::Waiting,
            u32::try_from(position).map_err(|_| ())?,
        ),
        QueueStatus::Offered => (QueueState::Offered, 0),
        QueueStatus::Admitted => (QueueState::Admitted, 0),
        QueueStatus::Cancelled => (QueueState::Cancelled, 0),
        QueueStatus::Expired => (QueueState::Expired, 0),
        QueueStatus::Missing => (QueueState::Missing, 0),
    };
    Ok(QueueUpdate {
        ticket_id,
        state,
        position,
        poll_after_ms: if matches!(state, QueueState::Waiting | QueueState::Offered) {
            1_000
        } else {
            0
        },
        ticket_remaining_ms: 0,
        offer_remaining_ms: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitized_results_encode_for_every_state() {
        let mut request = Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::QueueClaim(woven_protocol::QueueClaim {
                ticket_id: u64::MAX,
            }),
        );
        request.namespace_id = 1;
        request.session_id = 2;
        request.correlation_id = Some(3);
        for status in [
            QueueStatus::Waiting { position: 1 },
            QueueStatus::Offered,
            QueueStatus::Admitted,
            QueueStatus::Cancelled,
            QueueStatus::Expired,
            QueueStatus::Missing,
        ] {
            let value = queue_update(u64::MAX, status).unwrap();
            let reply = response(&request, ControlPayload::QueueUpdate(value));
            let codec = Codec::default();
            assert_eq!(codec.decode(&codec.encode(&reply).unwrap()).unwrap(), reply);
            assert_eq!(reply.correlation_id, Some(3));
            assert_eq!(reply.namespace_id, 1);
            assert_eq!(reply.session_id, 2);
            assert_eq!(value.ticket_remaining_ms, 0);
            assert_eq!(value.offer_remaining_ms, 0);
        }
        for reason in [
            RejectionReason::ServerPaused,
            RejectionReason::QueueFull,
            RejectionReason::QueueDisabled,
            RejectionReason::AlreadyQueued,
            RejectionReason::InvalidIdempotencyKey,
        ] {
            let reply = response(
                &request,
                ControlPayload::AdmissionResult(admission_result(JoinDecision::Rejected(reason))),
            );
            assert!(Codec::default().encode(&reply).is_ok());
        }
    }
}
