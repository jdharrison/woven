use std::{future::Future, time::Duration};

use tokio::time::Instant;
use woven_protocol::{
    AdmissionResult, AdmissionStatus, ControlPayload, DeliveryClass, Envelope, MessageKind,
    MessagePayload, QueueCancel, QueueClaim, QueueHeartbeat, QueueState, QueueStatusRequest,
    QueueUpdate, RequestAdmission,
};

use crate::{Client, ClientError, Transport};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_WAIT: Duration = Duration::from_secs(15 * 60);

/// A semantic outcome is returned as data, never automatically retried.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedAdmissionOutcome {
    Admission(AdmissionResult),
    Queue(QueueUpdate),
}

impl Client {
    /// Request admission and atomic session join. Call before subscriptions/publishing.
    /// The caller supplies a unique nonzero correlation ID for each operation and reuses
    /// the idempotency key only for the same logical request on this connection/scope.
    pub async fn request_admission(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        correlation_id: u64,
        idempotency_key: String,
    ) -> Result<AdmissionResult, ClientError> {
        let reply = self
            .admission_exchange(
                namespace_id,
                session_id,
                correlation_id,
                ControlPayload::RequestAdmission(RequestAdmission { idempotency_key }),
                MessageKind::AdmissionResult,
            )
            .await?;
        match reply {
            ControlPayload::AdmissionResult(value) => Ok(value),
            _ => unreachable!("exchange checks reply kind"),
        }
    }

    /// Observe a ticket without renewing its heartbeat. Foreign tickets return Missing.
    pub async fn queue_status(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        correlation_id: u64,
        ticket_id: u64,
    ) -> Result<QueueUpdate, ClientError> {
        self.queue_exchange(
            namespace_id,
            session_id,
            correlation_id,
            ticket_id,
            ControlPayload::QueueStatusRequest(QueueStatusRequest { ticket_id }),
        )
        .await
    }

    /// Renew the connection-owned ticket heartbeat and observe current status.
    pub async fn queue_heartbeat(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        correlation_id: u64,
        ticket_id: u64,
    ) -> Result<QueueUpdate, ClientError> {
        self.queue_exchange(
            namespace_id,
            session_id,
            correlation_id,
            ticket_id,
            ControlPayload::QueueHeartbeat(QueueHeartbeat { ticket_id }),
        )
        .await
    }

    /// Claim an offer; Admitted means the worker has already joined this connection.
    pub async fn queue_claim(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        correlation_id: u64,
        ticket_id: u64,
    ) -> Result<QueueUpdate, ClientError> {
        self.queue_exchange(
            namespace_id,
            session_id,
            correlation_id,
            ticket_id,
            ControlPayload::QueueClaim(QueueClaim { ticket_id }),
        )
        .await
    }

    /// Cancel a ticket. Admitted means cancellation did not undo an already joined session;
    /// leave/disconnect to release that admission.
    pub async fn queue_cancel(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        correlation_id: u64,
        ticket_id: u64,
    ) -> Result<QueueUpdate, ClientError> {
        self.queue_exchange(
            namespace_id,
            session_id,
            correlation_id,
            ticket_id,
            ControlPayload::QueueCancel(QueueCancel { ticket_id }),
        )
        .await
    }

    async fn queue_exchange(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        correlation_id: u64,
        ticket_id: u64,
        control: ControlPayload,
    ) -> Result<QueueUpdate, ClientError> {
        let reply = self
            .admission_exchange(
                namespace_id,
                session_id,
                correlation_id,
                control,
                MessageKind::QueueUpdate,
            )
            .await?;
        match reply {
            ControlPayload::QueueUpdate(value) if value.ticket_id == ticket_id => Ok(value),
            _ => {
                self.abort_admission();
                Err(ClientError::Transport(
                    "queue reply ticket mismatch".to_owned(),
                ))
            }
        }
    }

    // Exclusive pre-join exchange: never silently discard application traffic. A timeout
    // may interrupt read_exact/write_all mid-frame, so the stream must not be reused.
    async fn admission_exchange(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        correlation_id: u64,
        control: ControlPayload,
        expected: MessageKind,
    ) -> Result<ControlPayload, ClientError> {
        let mut envelope = Envelope::control(DeliveryClass::ReliableOrdered, control);
        envelope.namespace_id = namespace_id;
        envelope.session_id = session_id;
        envelope.correlation_id = Some(correlation_id);
        self.codec
            .encode(&envelope)
            .map_err(ClientError::Protocol)?;
        let operation = async {
            self.send_envelope(&envelope).await?;
            let reply = self.recv().await?;
            if let MessagePayload::Control(ControlPayload::ProtocolError(error)) = &reply.message {
                return Err(ClientError::ServerError(error.clone()));
            }
            if reply.namespace_id != namespace_id
                || reply.session_id != session_id
                || reply.correlation_id != Some(correlation_id)
                || reply.message_kind() != expected
            {
                return Err(ClientError::Transport(
                    "unexpected admission reply scope, correlation, or kind".to_owned(),
                ));
            }
            match reply.message {
                MessagePayload::Control(control) => Ok(control),
                _ => unreachable!("expected kind is a control"),
            }
        };
        let result = tokio::time::timeout(OPERATION_TIMEOUT, operation)
            .await
            .unwrap_or_else(|_| {
                Err(ClientError::Transport(
                    "admission operation timed out; connection closed".to_owned(),
                ))
            });
        if result.is_err() {
            self.abort_admission();
        }
        result
    }

    fn abort_admission(&self) {
        match &self.transport {
            Transport::Quic { connection, .. } => {
                connection.close(quinn::VarInt::from_u32(0), b"admission stopped");
            }
            Transport::WebTransport { connection, .. } => {
                connection.close(wtransport::VarInt::from_u32(0), b"admission stopped");
            }
        }
    }

    /// Wait with heartbeat/claim scheduling and a caller-owned cancellation future.
    /// Consumes the client, returning it only on a semantic outcome. Use a fresh,
    /// authenticated, pre-subscription connection. Deadline must be 1ns..=15 minutes.
    /// Cancellation/timeout closes the connection (including an in-flight admitted
    /// claim), letting the worker release tickets/leases. No transport retries are
    /// attempted: partial stream I/O cannot safely be replayed on this connection.
    /// Dropping this future also drops the owned client/transport.
    pub async fn admit_with_cancellation(
        mut self,
        namespace_id: u64,
        session_id: u64,
        idempotency_key: String,
        timeout: Duration,
        cancellation: impl Future<Output = ()>,
    ) -> Result<(Self, ManagedAdmissionOutcome), ClientError> {
        if timeout.is_zero() || timeout > MAX_WAIT {
            return Err(ClientError::Transport(
                "admission deadline must be positive and at most 15 minutes".to_owned(),
            ));
        }
        let deadline = Instant::now() + timeout;
        let result = tokio::select! {
            biased;
            () = cancellation => Err(ClientError::Transport("admission cancelled; connection closed".to_owned())),
            () = tokio::time::sleep_until(deadline) => Err(ClientError::Transport("admission deadline exceeded; connection closed".to_owned())),
            result = self.wait_admission(namespace_id, session_id, idempotency_key) => result,
        };
        match result {
            Ok(outcome) => Ok((self, outcome)),
            Err(error) => {
                self.abort_admission();
                Err(error)
            }
        }
    }

    async fn wait_admission(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        idempotency_key: String,
    ) -> Result<ManagedAdmissionOutcome, ClientError> {
        let first = self
            .request_admission(namespace_id, session_id, 1, idempotency_key)
            .await?;
        if first.status != AdmissionStatus::Queued {
            return Ok(ManagedAdmissionOutcome::Admission(first));
        }
        let ticket_id = first.ticket_id.expect("codec validates queued ticket");
        let mut correlation = 2;
        let mut poll_after_ms = first.poll_after_ms;
        let mut offered = false;
        loop {
            // Advice is not a timing guarantee. This clamp avoids hot loops and stays
            // well below heartbeat expiry and the four-operations/second ceiling.
            tokio::time::sleep(poll_delay(poll_after_ms)).await;
            let update = if offered {
                self.queue_claim(namespace_id, session_id, correlation, ticket_id)
                    .await?
            } else {
                self.queue_heartbeat(namespace_id, session_id, correlation, ticket_id)
                    .await?
            };
            if !matches!(update.state, QueueState::Waiting | QueueState::Offered) {
                return Ok(ManagedAdmissionOutcome::Queue(update));
            }
            correlation += 1;
            offered = update.state == QueueState::Offered;
            poll_after_ms = update.poll_after_ms;
        }
    }
}

fn poll_delay(advice: u32) -> Duration {
    Duration::from_millis(u64::from(advice.clamp(1_000, 5_000)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advisory_poll_is_bounded() {
        assert_eq!(poll_delay(0), Duration::from_secs(1));
        assert_eq!(poll_delay(2_000), Duration::from_secs(2));
        assert_eq!(poll_delay(u32::MAX), Duration::from_secs(5));
    }
}
