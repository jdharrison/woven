//! The inference coordinator: an adjacent, optional plane that runs one connection per AI
//! identity, accepts bounded inference requests, and routes model tool-call proposals
//! through the deterministic gateway in `woven-inference-tools` (ADR 0009).
//!
//! The coordinator never touches `woven-core` or `woven-protocol` internals: it
//! is just another `WorkerHandle` user, exactly like a transport adapter. Disabling the
//! plane (not calling [`spawn`]) leaves the relay entirely unaffected.

#![forbid(unsafe_code)]

use std::{sync::Arc, time::Duration};

use tokio::sync::{Semaphore, mpsc};
use woven_core::{
    ChannelId, Command, CommandResult, ConnectionId, Credentials, EntityId, NamespaceId, SessionId,
    SessionKey, SpaceEpoch, SpaceId, SpaceKey,
};
use woven_inference_core::{Capability, InferenceEvent, InferenceRequest, Provider};
use woven_inference_tools::{
    ToolCallOutcome, ToolCallRejectionReason, ToolInvocationContext, ToolRegistry,
};
use woven_protocol::{
    ControlPayload, DeliveryClass, Envelope, InferenceAccepted, InferenceCompleted,
    InferenceExpired, InferenceFailed, InferenceProgress, InferenceStreamChunk, MessagePayload,
    PROTOCOL_VERSION, ToolCallAccepted, ToolCallCompleted, ToolCallProposed, ToolCallRejected,
    ToolCallRejectionCode,
};
use woven_transport::{UnroutedControl, WorkerHandle};

/// How the coordinator authenticates and where its identity lives. One value per AI
/// identity; this milestone's development composition registers exactly one.
#[derive(Clone)]
pub struct AiIdentityConfig {
    pub token: String,
    pub namespace: NamespaceId,
    pub session: SessionId,
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    /// Channel the demo state-changing tool publishes its `LatestValue` status updates on.
    pub status_channel: ChannelId,
}

impl AiIdentityConfig {
    fn session_key(&self) -> SessionKey {
        SessionKey {
            namespace: self.namespace,
            session: self.session,
        }
    }

    fn space_key(&self) -> SpaceKey {
        SpaceKey {
            session: self.session_key(),
            space: self.space,
        }
    }
}

pub struct CoordinatorConfig {
    pub worker: WorkerHandle,
    pub identity: AiIdentityConfig,
    pub provider: Arc<dyn Provider>,
    pub tools: Arc<ToolRegistry>,
    /// Bounded concurrent in-flight requests. Additional requests are rejected immediately
    /// with `InferenceFailed` rather than queued unboundedly.
    pub queue_capacity: usize,
}

#[derive(Debug)]
pub enum CoordinatorError {
    SetupFailed,
}

impl std::fmt::Display for CoordinatorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("failed to establish the AI identity's core connection")
    }
}

impl std::error::Error for CoordinatorError {}

const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_DEADLINE: Duration = Duration::from_secs(10);

/// Establish the AI identity's core connection and spawn the coordinator's background
/// tasks. Returns the identity's `ConnectionId`/`EntityId` so the caller (the composition
/// root) can expose them, e.g. for tests that need to address the AI directly.
pub async fn spawn(
    config: CoordinatorConfig,
    inbound: mpsc::Receiver<UnroutedControl>,
) -> Result<(ConnectionId, EntityId), CoordinatorError> {
    let (connection, entity) = establish_identity(&config.worker, &config.identity).await?;
    spawn_drain_poll(config.worker.clone(), connection);
    tokio::spawn(run_loop(config, connection, entity, inbound));
    Ok((connection, entity))
}

async fn establish_identity(
    worker: &WorkerHandle,
    identity: &AiIdentityConfig,
) -> Result<(ConnectionId, EntityId), CoordinatorError> {
    let Ok(CommandResult::Connected(connection)) =
        worker.execute(Command::TransportConnected).await
    else {
        return Err(CoordinatorError::SetupFailed);
    };
    let Ok(CommandResult::Authenticated(_)) = worker
        .execute(Command::Authenticate {
            connection,
            credentials: Credentials::new(identity.token.clone()),
        })
        .await
    else {
        return Err(CoordinatorError::SetupFailed);
    };
    let Ok(CommandResult::Joined) = worker
        .execute(Command::JoinSession {
            connection,
            session: identity.session_key(),
        })
        .await
    else {
        return Err(CoordinatorError::SetupFailed);
    };
    let entity = worker
        .subscribe_and_spawn(connection, identity.space_key(), identity.space_epoch)
        .await
        .map_err(|_| CoordinatorError::SetupFailed)?;
    Ok((connection, entity))
}

/// Regularly drains the AI connection's own outbound queue so ordinary channel traffic it
/// is subscribed to never saturates. This milestone does not yet trigger inference from
/// passively observed events; wiring a specific `channel_id`/`type_id` to do so is a natural
/// follow-up that needs no further protocol or core changes.
fn spawn_drain_poll(worker: WorkerHandle, connection: ConnectionId) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(DRAIN_POLL_INTERVAL);
        loop {
            interval.tick().await;
            if worker
                .execute(Command::DrainOutbound { connection })
                .await
                .is_err()
            {
                return;
            }
        }
    })
}

async fn run_loop(
    config: CoordinatorConfig,
    ai_connection: ConnectionId,
    ai_entity: EntityId,
    mut inbound: mpsc::Receiver<UnroutedControl>,
) {
    let queue = Arc::new(Semaphore::new(config.queue_capacity.max(1)));
    while let Some(unrouted) = inbound.recv().await {
        let UnroutedControl {
            connection: requester,
            envelope,
        } = unrouted;
        // `handle_authenticated` only ever forwards `InferenceRequested`/`InferenceCancelled`
        // here (see `woven-transport`). Cancellation tracking is not implemented in
        // this milestone: a request already dispatched to the provider runs to completion,
        // since the deterministic provider completes synchronously.
        if matches!(
            envelope.message,
            MessagePayload::Control(ControlPayload::InferenceRequested(_))
        ) {
            accept_or_reject(
                &config,
                ai_connection,
                ai_entity,
                requester,
                envelope,
                &queue,
            )
            .await;
        }
    }
}

async fn accept_or_reject(
    config: &CoordinatorConfig,
    ai_connection: ConnectionId,
    ai_entity: EntityId,
    requester: ConnectionId,
    envelope: Envelope,
    queue: &Arc<Semaphore>,
) {
    let MessagePayload::Control(ControlPayload::InferenceRequested(request)) = &envelope.message
    else {
        return;
    };
    let Ok(permit) = Arc::clone(queue).try_acquire_owned() else {
        let full = reply_envelope(
            &envelope,
            ai_entity,
            ControlPayload::InferenceFailed(InferenceFailed {
                reason: "inference provider queue is full".to_owned(),
            }),
        );
        let _ = config.worker.send_to_connection(requester, full).await;
        return;
    };

    let accepted = reply_envelope(
        &envelope,
        ai_entity,
        ControlPayload::InferenceAccepted(InferenceAccepted { queued_position: 0 }),
    );
    let _ = config.worker.send_to_connection(requester, accepted).await;

    let deadline_ms = request.deadline_ms;
    let capability = Capability::new(request.capability.clone());
    let input = request.input.clone();
    let worker = config.worker.clone();
    let provider = Arc::clone(&config.provider);
    let tools = Arc::clone(&config.tools);
    let identity = config.identity.clone();
    tokio::spawn(async move {
        let _permit = permit;
        run_inference(
            worker,
            provider,
            tools,
            identity,
            ai_connection,
            ai_entity,
            requester,
            envelope,
            capability,
            input,
            deadline_ms,
        )
        .await;
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_inference(
    worker: WorkerHandle,
    provider: Arc<dyn Provider>,
    tools: Arc<ToolRegistry>,
    identity: AiIdentityConfig,
    ai_connection: ConnectionId,
    ai_entity: EntityId,
    requester: ConnectionId,
    source_envelope: Envelope,
    capability: Capability,
    input: Vec<u8>,
    deadline_ms: u64,
) {
    let deadline = std::time::Instant::now()
        + if deadline_ms == 0 {
            DEFAULT_DEADLINE
        } else {
            Duration::from_millis(deadline_ms)
        };
    let request = InferenceRequest {
        capability,
        principal: woven_core::PrincipalId::new(1),
        acting_entity: ai_entity,
        deadline,
        cancellation: woven_inference_core::Cancellation::new(),
        context: Vec::new(),
        input,
        streaming: true,
    };
    let outcome = provider.run(request).await;

    for event in outcome.events {
        if std::time::Instant::now() > deadline {
            let expired = reply_envelope(
                &source_envelope,
                ai_entity,
                ControlPayload::InferenceExpired(InferenceExpired {
                    reason: "deadline passed before delivery".to_owned(),
                }),
            );
            let _ = worker.send_to_connection(requester, expired).await;
            return;
        }
        match event {
            InferenceEvent::Progress { percent } => {
                let envelope = reply_envelope(
                    &source_envelope,
                    ai_entity,
                    ControlPayload::InferenceProgress(InferenceProgress { percent }),
                );
                let _ = worker.send_to_connection(requester, envelope).await;
            }
            InferenceEvent::StreamChunk {
                sequence,
                chunk,
                is_final,
            } => {
                let envelope = reply_envelope(
                    &source_envelope,
                    ai_entity,
                    ControlPayload::InferenceStreamChunk(InferenceStreamChunk {
                        sequence,
                        chunk,
                        is_final,
                    }),
                );
                let _ = worker.send_to_connection(requester, envelope).await;
            }
            InferenceEvent::ToolCallProposed(proposal) => {
                handle_tool_call(
                    &worker,
                    &tools,
                    &identity,
                    ai_connection,
                    ai_entity,
                    &source_envelope,
                    proposal,
                )
                .await;
            }
            InferenceEvent::Completed { result } => {
                let envelope = reply_envelope(
                    &source_envelope,
                    ai_entity,
                    ControlPayload::InferenceCompleted(InferenceCompleted { result }),
                );
                let _ = worker.send_to_connection(requester, envelope).await;
            }
            InferenceEvent::Failed { reason } => {
                let envelope = reply_envelope(
                    &source_envelope,
                    ai_entity,
                    ControlPayload::InferenceFailed(InferenceFailed { reason }),
                );
                let _ = worker.send_to_connection(requester, envelope).await;
            }
        }
    }
}

/// Tool-call lifecycle is visible to every subscriber of the AI's space, not just the
/// requester, mirroring how `EntityEntered`/`EntityLeft` are broadcast.
async fn handle_tool_call(
    worker: &WorkerHandle,
    tools: &ToolRegistry,
    identity: &AiIdentityConfig,
    ai_connection: ConnectionId,
    ai_entity: EntityId,
    source_envelope: &Envelope,
    proposal: woven_inference_core::ToolCallProposal,
) {
    let space = identity.space_key();
    let proposed = reply_envelope(
        source_envelope,
        ai_entity,
        ControlPayload::ToolCallProposed(ToolCallProposed {
            tool_id: proposal.tool_id.clone(),
            tool_version: proposal.tool_version,
            arguments: proposal.arguments.clone(),
            expected_revision: proposal.expected_revision,
        }),
    );
    let _ = worker.broadcast_to_space(space, proposed, None).await;

    let context = ToolInvocationContext {
        worker: worker.clone(),
        connection: ai_connection,
        entity: ai_entity,
        space,
        space_epoch: identity.space_epoch,
    };
    match tools.evaluate(&context, &proposal).await {
        ToolCallOutcome::Completed {
            new_revision,
            result,
        } => {
            let accepted = reply_envelope(
                source_envelope,
                ai_entity,
                ControlPayload::ToolCallAccepted(ToolCallAccepted {
                    tool_id: proposal.tool_id.clone(),
                }),
            );
            let _ = worker.broadcast_to_space(space, accepted, None).await;
            let completed = reply_envelope(
                source_envelope,
                ai_entity,
                ControlPayload::ToolCallCompleted(ToolCallCompleted {
                    new_revision,
                    result,
                }),
            );
            let _ = worker.broadcast_to_space(space, completed, None).await;
        }
        ToolCallOutcome::Rejected { code, reason } => {
            let rejected = reply_envelope(
                source_envelope,
                ai_entity,
                ControlPayload::ToolCallRejected(ToolCallRejected {
                    code: map_rejection_code(code),
                    reason,
                }),
            );
            let _ = worker.broadcast_to_space(space, rejected, None).await;
        }
    }
}

const fn map_rejection_code(reason: ToolCallRejectionReason) -> ToolCallRejectionCode {
    match reason {
        ToolCallRejectionReason::UnknownTool => ToolCallRejectionCode::InvalidArguments,
        ToolCallRejectionReason::Stale => ToolCallRejectionCode::Stale,
        ToolCallRejectionReason::PolicyDenied => ToolCallRejectionCode::PolicyDenied,
    }
}

fn delivery_class_for(payload: &ControlPayload) -> DeliveryClass {
    match payload {
        ControlPayload::InferenceProgress(_) | ControlPayload::InferenceStreamChunk(_) => {
            DeliveryClass::BestEffortEvent
        }
        _ => DeliveryClass::ReliableOrdered,
    }
}

fn reply_envelope(source: &Envelope, ai_entity: EntityId, payload: ControlPayload) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: delivery_class_for(&payload),
        namespace_id: source.namespace_id,
        session_id: source.session_id,
        space_id: source.space_id,
        channel_id: None,
        entity_id: Some(ai_entity.get()),
        space_epoch: source.space_epoch,
        server_tick: 0,
        sender_sequence: 0,
        correlation_id: source.correlation_id,
        routing_position: None,
        message: MessagePayload::Control(payload),
    }
}
