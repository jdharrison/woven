//! Transport-independent worker, lifecycle fan-out, and protocol bridge.

#![deny(unsafe_code)]

mod admission;
mod logging;
mod metrics;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use woven_core::{
    Authenticator, CleanupSummary, CoalesceKey, Command, CommandResult, ConnectionId, CoreError,
    DeliveryClass as CoreDelivery, EntityTransition, PersistenceClass, PublishRequest,
    RemovedEntity, SpaceEpoch, SpaceKey, TransportIndependentWorker,
};
use woven_protocol::{
    CAPABILITY_POSITIONED_ENTITY_STATE, ControlPayload, DeliveryClass, EntityEntered,
    EntityLeaveReason, EntityLeft, Envelope, MessageKind, MessagePayload, OpaquePayload,
    PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode, RoutingPosition3D,
};

pub use logging::{LOG_CAPACITY, LogEntry, LogPage, MAX_LOG_PAGE_ENTRIES};
pub use metrics::{LiveCounts, ServerMetrics, ServerMetricsSnapshot};
/// Maximum number of commands pending for the single core owner.
pub const COMMAND_CAPACITY: usize = 256;
/// Maximum protocol frame size advertised by the transport bridge.
pub const MAX_FRAME_BYTES: u32 = 1_048_576;
/// Maximum domain payload size advertised by the transport bridge.
pub const MAX_PAYLOAD_BYTES: u32 = 65_536;
/// How often the worker actively reclaims expired `Stateful` cache entries. TTLs are measured in
/// hours, so this only needs to be coarse-grained enough that idle memory doesn't linger long.
const STATE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Errors from the transport boundary.
#[derive(Debug)]
pub enum TransportError {
    WorkerUnavailable,
    Core(CoreError),
    UnknownConnection,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkerUnavailable => formatter.write_str("core worker is unavailable"),
            Self::Core(error) => write!(formatter, "core error: {error:?}"),
            Self::UnknownConnection => {
                formatter.write_str("connection is not registered for direct delivery")
            }
        }
    }
}

impl std::error::Error for TransportError {}

enum WorkerRequest {
    ClientLog {
        connection: ConnectionId,
        session: woven_core::SessionKey,
        log: woven_protocol::ClientLog,
        reply: oneshot::Sender<Result<(), ProtocolErrorCode>>,
    },
    Logs {
        after: u64,
        limit: usize,
        reply: oneshot::Sender<LogPage>,
    },
    Managed {
        request: woven_core::ManagedRequest,
        reply: oneshot::Sender<Result<woven_core::ManagedOutcome, woven_core::ManagedError>>,
    },
    Command {
        command: Command,
        reply: oneshot::Sender<Result<CommandResult, CoreError>>,
    },
    ChannelPersistence {
        connection: ConnectionId,
        session: woven_core::SessionKey,
        channel: woven_core::ChannelId,
        reply: oneshot::Sender<Result<PersistenceClass, CoreError>>,
    },
    RegisterLifecycle {
        connection: ConnectionId,
        recipient: LifecycleRecipient,
        reply: oneshot::Sender<()>,
    },
    SubscribeAndSpawn {
        connection: ConnectionId,
        space: SpaceKey,
        epoch: SpaceEpoch,
        reply: oneshot::Sender<Result<woven_core::EntityId, CoreError>>,
    },
    ActivateSubscription {
        connection: ConnectionId,
        space: SpaceKey,
        reply: oneshot::Sender<()>,
    },
    BroadcastToSpace {
        space: SpaceKey,
        envelope: Envelope,
        exclude: Option<ConnectionId>,
        reply: oneshot::Sender<()>,
    },
    SendToConnection {
        connection: ConnectionId,
        envelope: Envelope,
        reply: oneshot::Sender<Result<(), TransportError>>,
    },
    LiveCounts {
        reply: oneshot::Sender<LiveCounts>,
    },
}

struct LifecycleRecipient {
    sender: mpsc::Sender<Envelope>,
    shutdown: mpsc::Sender<()>,
}

#[derive(Clone, Copy)]
enum LifecycleAction {
    None,
    Subscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    Unsubscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    LeaveSession {
        connection: ConnectionId,
        session: woven_core::SessionKey,
    },
    Spawn {
        space: SpaceKey,
        epoch: SpaceEpoch,
    },
    RemoveEntity,
    Transition,
    TransportLost {
        connection: ConnectionId,
    },
}

/// A control envelope `handle_authenticated` does not itself implement, handed off to an
/// optional adjacent plane (e.g. the inference coordinator) rather than rejected outright.
/// `woven-transport` has no knowledge of what consumes these.
#[derive(Debug)]
pub struct UnroutedControl {
    pub connection: ConnectionId,
    pub envelope: Envelope,
}

/// Cloneable bounded command client for the single Woven core owner.
#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerRequest>,
    metrics: Arc<ServerMetrics>,
}

impl WorkerHandle {
    /// Capture a client message through the bounded owner; success is not a persistence ACK.
    pub async fn capture_client_log(
        &self,
        connection: ConnectionId,
        session: woven_core::SessionKey,
        log: woven_protocol::ClientLog,
    ) -> Result<(), ProtocolErrorCode> {
        // Bound the mailbox's owned message bytes as well as the retained ring.
        logging::validate_log(&log)?;
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::ClientLog {
                connection,
                session,
                log,
                reply,
            })
            .await
            .map_err(|_| ProtocolErrorCode::Internal)?;
        receive.await.map_err(|_| ProtocolErrorCode::Internal)?
    }

    /// Trusted local read; public feed authentication belongs to the server admin listener.
    pub async fn read_logs(&self, after: u64, limit: usize) -> Result<LogPage, TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Logs {
                after,
                limit,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }

    /// Trusted management operations share the bounded mailbox with native client commands.
    pub async fn manage(
        &self,
        request: woven_core::ManagedRequest,
    ) -> Result<woven_core::ManagedOutcome, woven_core::ManagedError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Managed { request, reply })
            .await
            .map_err(|_| woven_core::ManagedError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| woven_core::ManagedError::WorkerUnavailable)?
    }

    /// Always-on cumulative counters for capacity and cost observability.
    #[must_use]
    pub fn metrics(&self) -> &ServerMetrics {
        &self.metrics
    }

    /// Live connection/session counts read directly from core state, not accumulated.
    pub async fn live_counts(&self) -> Result<LiveCounts, TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::LiveCounts { reply })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }
    /// Submit a command to the single owner and wait for its result.
    pub async fn execute(&self, command: Command) -> Result<CommandResult, TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Command { command, reply })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?
            .map_err(TransportError::Core)
    }

    /// Read the registered policy through the same bounded mailbox as publishes.
    pub async fn channel_persistence(
        &self,
        connection: ConnectionId,
        session: woven_core::SessionKey,
        channel: woven_core::ChannelId,
    ) -> Result<PersistenceClass, TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::ChannelPersistence {
                connection,
                session,
                channel,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?
            .map_err(TransportError::Core)
    }

    pub async fn register_lifecycle(
        &self,
        connection: ConnectionId,
        sender: mpsc::Sender<Envelope>,
        shutdown: mpsc::Sender<()>,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::RegisterLifecycle {
                connection,
                recipient: LifecycleRecipient { sender, shutdown },
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }

    pub async fn discard_and_disconnect(&self, connection: ConnectionId) {
        let _ = self.execute(Command::DrainOutbound { connection }).await;
        let _ = self.execute(Command::TransportLost { connection }).await;
    }

    pub async fn subscribe_and_spawn(
        &self,
        connection: ConnectionId,
        space: SpaceKey,
        epoch: SpaceEpoch,
    ) -> Result<woven_core::EntityId, TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::SubscribeAndSpawn {
                connection,
                space,
                epoch,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?
            .map_err(TransportError::Core)
    }

    pub async fn activate_subscription(
        &self,
        connection: ConnectionId,
        space: SpaceKey,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::ActivateSubscription {
                connection,
                space,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }

    /// Deliver `envelope` directly to every connection currently subscribed to `space`,
    /// bypassing the per-connection outbound queue. Mirrors the delivery path already used
    /// for `EntityEntered`/`EntityLeft`/`SpaceTransition`, generalized for any envelope.
    pub async fn broadcast_to_space(
        &self,
        space: SpaceKey,
        envelope: Envelope,
        exclude: Option<ConnectionId>,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::BroadcastToSpace {
                space,
                envelope,
                exclude,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }

    /// Deliver `envelope` directly to a single registered connection, bypassing the
    /// per-connection outbound queue. The connection must have called `register_lifecycle`.
    pub async fn send_to_connection(
        &self,
        connection: ConnectionId,
        envelope: Envelope,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::SendToConnection {
                connection,
                envelope,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?
    }
}

/// Spawn the bounded, single-owner core command worker.
#[allow(clippy::too_many_lines)]
pub fn spawn_worker<A>(worker: TransportIndependentWorker<A>) -> WorkerHandle
where
    A: Authenticator + Send + 'static,
{
    let metrics = Arc::new(ServerMetrics::default());
    let worker_metrics = Arc::clone(&metrics);
    let (sender, mut receiver) = mpsc::channel::<WorkerRequest>(COMMAND_CAPACITY);
    tokio::spawn(async move {
        let mut worker = worker;
        let mut logs = logging::LogCapture::default();
        let mut recipients: BTreeMap<ConnectionId, LifecycleRecipient> = BTreeMap::new();
        let mut subscriptions = BTreeMap::new();
        let mut admission_interval = tokio::time::interval(Duration::from_millis(100));
        admission_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut sweep_interval = tokio::time::interval(STATE_SWEEP_INTERVAL);
        sweep_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let request = tokio::select! {
                request = receiver.recv() => match request {
                    Some(request) => request,
                    None => break,
                },
                _ = admission_interval.tick() => {
                    worker.core_mut().maintain_admission_at(Instant::now());
                    continue;
                }
                _ = sweep_interval.tick() => {
                    let evicted = worker.core_mut().sweep_expired_state(Instant::now());
                    if evicted > 0 {
                        tracing::info!(
                            target: "woven_relay",
                            evicted_state_entries = evicted,
                            "state_sweep_expired"
                        );
                    }
                    continue;
                }
            };
            let mut reconcile_disconnections = request_can_disconnect(&request);
            match request {
                WorkerRequest::ClientLog {
                    connection,
                    session,
                    log,
                    reply,
                } => {
                    let _ = reply.send(logs.client(
                        worker.core(),
                        connection,
                        session,
                        log,
                        Instant::now(),
                    ));
                }
                WorkerRequest::Logs {
                    after,
                    limit,
                    reply,
                } => {
                    let _ = reply.send(logs.page(after, limit));
                }
                WorkerRequest::Managed { request, reply } => {
                    let result = worker.core_mut().manage_at(request, Instant::now());
                    if let Ok(woven_core::ManagedOutcome::Deleted { connections }) = &result {
                        for connection in connections {
                            logs.observe_connection(
                                worker.core(),
                                *connection,
                                "managed session revoked",
                            );
                            if let Some(recipient) = recipients.remove(connection) {
                                let _ = recipient.shutdown.try_send(());
                            }
                            subscriptions.remove(connection);
                        }
                    }
                    let _ = reply.send(result);
                }
                WorkerRequest::Command { command, reply } => {
                    #[cfg(debug_assertions)]
                    let activity = log_development_activity(&command);
                    let action = lifecycle_action(&command);
                    let context = metrics_context(&command);
                    let log_connection = membership_log_context(&command);
                    let result = match &command {
                        Command::DrainOutbound { connection }
                            if recipients.contains_key(connection) =>
                        {
                            // Core routing starts at subscribe, but transport output starts only
                            // after the accepted/self-entry replies have been queued.
                            let active_spaces = subscriptions.entry(*connection).or_default();
                            worker
                                .core_mut()
                                .drain_outbound_for_spaces(*connection, active_spaces)
                                .map(CommandResult::Outbound)
                        }
                        _ => worker.handle(command),
                    };
                    if let Ok(CommandResult::Published(outcome)) = &result {
                        // Ordinary publishes must not scan the node's membership tracking map.
                        reconcile_disconnections = !outcome.disconnected_slow_consumers.is_empty();
                    }
                    if let Some((connection, reason)) = log_connection {
                        logs.observe_connection(worker.core(), connection, reason);
                    }
                    #[cfg(debug_assertions)]
                    log_development_result(activity, &result);
                    observe_command(&worker_metrics, context, &result);
                    if let Ok(result) = &result {
                        apply_lifecycle_action(
                            &mut worker,
                            &mut recipients,
                            &mut subscriptions,
                            action,
                            result,
                        );
                    }
                    let _ = reply.send(result);
                }
                WorkerRequest::ChannelPersistence {
                    connection,
                    session,
                    channel,
                    reply,
                } => {
                    let _ = reply.send(
                        worker
                            .core()
                            .channel_persistence(connection, session, channel),
                    );
                }
                WorkerRequest::LiveCounts { reply } => {
                    let _ = reply.send(LiveCounts {
                        connections_active: worker.core().connection_count(),
                        sessions_active: worker.core().session_count(),
                    });
                }
                WorkerRequest::RegisterLifecycle {
                    connection,
                    recipient,
                    reply,
                } => {
                    #[cfg(debug_assertions)]
                    tracing::info!(
                        target: "woven_activity",
                        activity = "register_lifecycle",
                        connection_id = connection.get(),
                    );
                    if !worker.core().is_connected(connection) {
                        let _ = recipient.shutdown.try_send(());
                        continue;
                    }
                    recipients.insert(connection, recipient);
                    subscriptions
                        .entry(connection)
                        .or_insert_with(BTreeSet::new);
                    let _ = reply.send(());
                }
                WorkerRequest::SubscribeAndSpawn {
                    connection,
                    space,
                    epoch,
                    reply,
                } => {
                    #[cfg(debug_assertions)]
                    tracing::info!(
                        target: "woven_activity",
                        activity = "subscribe_and_spawn",
                        connection_id = connection.get(),
                        namespace_id = space.session.namespace.get(),
                        session_id = space.session.session.get(),
                        space_id = space.space.get(),
                        space_epoch = epoch.get(),
                    );
                    let result = match worker.core_mut().subscribe(connection, space) {
                        Ok(()) => match worker.core_mut().spawn_entity(connection, space, epoch) {
                            Ok(entity) => {
                                distribute_to_space_excluding(
                                    &mut worker,
                                    &mut recipients,
                                    &mut subscriptions,
                                    space,
                                    &entity_entered_envelope(space, epoch, entity),
                                    Some(connection),
                                );
                                Ok(entity)
                            }
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    };
                    let _ = reply.send(result);
                }
                WorkerRequest::ActivateSubscription {
                    connection,
                    space,
                    reply,
                } => {
                    #[cfg(debug_assertions)]
                    tracing::info!(
                        target: "woven_activity",
                        activity = "activate_subscription",
                        connection_id = connection.get(),
                        namespace_id = space.session.namespace.get(),
                        session_id = space.session.session.get(),
                        space_id = space.space.get(),
                    );
                    subscriptions.entry(connection).or_default().insert(space);
                    let _ = reply.send(());
                }
                WorkerRequest::BroadcastToSpace {
                    space,
                    envelope,
                    exclude,
                    reply,
                } => {
                    #[cfg(debug_assertions)]
                    tracing::info!(
                        target: "woven_activity",
                        activity = "broadcast_to_space",
                        namespace_id = space.session.namespace.get(),
                        session_id = space.session.session.get(),
                        space_id = space.space.get(),
                        message_kind = ?envelope.message.message_kind(),
                        payload_bytes = envelope.payload_bytes().len(),
                    );
                    distribute_to_space_excluding(
                        &mut worker,
                        &mut recipients,
                        &mut subscriptions,
                        space,
                        &envelope,
                        exclude,
                    );
                    let _ = reply.send(());
                }
                WorkerRequest::SendToConnection {
                    connection,
                    envelope,
                    reply,
                } => {
                    #[cfg(debug_assertions)]
                    tracing::info!(
                        target: "woven_activity",
                        activity = "send_to_connection",
                        connection_id = connection.get(),
                        message_kind = ?envelope.message.message_kind(),
                        payload_bytes = envelope.payload_bytes().len(),
                    );
                    let result = deliver_to_connection(
                        &mut worker,
                        &mut recipients,
                        &mut subscriptions,
                        connection,
                        &envelope,
                    );
                    let _ = reply.send(result);
                }
            }
            if reconcile_disconnections {
                for connection in logs.disconnected(worker.core()) {
                    logs.observe_connection(
                        worker.core(),
                        connection,
                        "slow consumer disconnected",
                    );
                    if let Some(recipient) = recipients.remove(&connection) {
                        let _ = recipient.shutdown.try_send(());
                    }
                    subscriptions.remove(&connection);
                }
            }
        }
    });
    WorkerHandle { sender, metrics }
}

fn membership_log_context(command: &Command) -> Option<(ConnectionId, &'static str)> {
    match command {
        Command::JoinSession { connection, .. }
        | Command::JoinSessionWithAdmission { connection, .. }
        | Command::RequestSessionAdmission { connection, .. }
        | Command::SessionQueue { connection, .. }
        | Command::LeaveSession { connection, .. } => Some((*connection, "session left")),
        Command::TransportLost { connection } => Some((*connection, "transport lost")),
        _ => None,
    }
}

fn request_can_disconnect(request: &WorkerRequest) -> bool {
    match request {
        WorkerRequest::Command { command, .. } => {
            !matches!(lifecycle_action(command), LifecycleAction::None)
        }
        WorkerRequest::Managed { request, .. } => {
            matches!(request, woven_core::ManagedRequest::Delete { .. })
        }
        WorkerRequest::SubscribeAndSpawn { .. }
        | WorkerRequest::BroadcastToSpace { .. }
        | WorkerRequest::SendToConnection { .. } => true,
        _ => false,
    }
}

#[cfg(debug_assertions)]
#[allow(clippy::too_many_lines)]
fn log_development_activity(command: &Command) -> &'static str {
    match command {
        Command::TransportConnected => {
            tracing::info!(target: "woven_activity", activity = "transport_connected");
            "transport_connected"
        }
        Command::Authenticate { connection, .. } => {
            tracing::info!(
                target: "woven_activity",
                activity = "authenticate",
                connection_id = connection.get(),
            );
            "authenticate"
        }
        Command::JoinSession {
            connection,
            session,
        }
        | Command::JoinSessionWithAdmission {
            connection,
            session,
            ..
        } => {
            tracing::info!(
                target: "woven_activity",
                activity = "join_session",
                connection_id = connection.get(),
                namespace_id = session.namespace.get(),
                session_id = session.session.get(),
            );
            "join_session"
        }
        Command::RequestSessionAdmission {
            connection,
            session,
            ..
        }
        | Command::SessionQueue {
            connection,
            session,
            ..
        } => {
            tracing::info!(
                target: "woven_activity",
                activity = "request_admission",
                connection_id = connection.get(),
                namespace_id = session.namespace.get(),
                session_id = session.session.get(),
            );
            "request_admission"
        }
        Command::LeaveSession {
            connection,
            session,
        } => {
            tracing::info!(
                target: "woven_activity",
                activity = "leave_session",
                connection_id = connection.get(),
                namespace_id = session.namespace.get(),
                session_id = session.session.get(),
            );
            "leave_session"
        }
        Command::Subscribe { connection, space } | Command::Unsubscribe { connection, space } => {
            let activity = if matches!(command, Command::Subscribe { .. }) {
                "subscribe"
            } else {
                "unsubscribe"
            };
            tracing::info!(
                target: "woven_activity",
                activity,
                connection_id = connection.get(),
                namespace_id = space.session.namespace.get(),
                session_id = space.session.session.get(),
                space_id = space.space.get(),
            );
            activity
        }
        Command::SpawnEntity {
            connection,
            space,
            epoch,
        } => {
            tracing::info!(
                target: "woven_activity",
                activity = "spawn_entity",
                connection_id = connection.get(),
                namespace_id = space.session.namespace.get(),
                session_id = space.session.session.get(),
                space_id = space.space.get(),
                space_epoch = epoch.get(),
            );
            "spawn_entity"
        }
        Command::RemoveEntity {
            connection,
            session,
            entity,
        } => {
            tracing::info!(
                target: "woven_activity",
                activity = "remove_entity",
                connection_id = connection.get(),
                namespace_id = session.namespace.get(),
                session_id = session.session.get(),
                entity_id = entity.get(),
            );
            "remove_entity"
        }
        Command::UpdateEntityPosition {
            connection,
            session,
            entity,
            ..
        } => {
            tracing::info!(
                target: "woven_activity::transform",
                activity = "update_entity_position",
                connection_id = connection.get(),
                namespace_id = session.namespace.get(),
                session_id = session.session.get(),
                entity_id = entity.get(),
            );
            "update_entity_position"
        }
        Command::TransitionEntity(_) => {
            tracing::info!(target: "woven_activity", activity = "transition_entity");
            "transition_entity"
        }
        Command::Publish(request) => {
            if is_transform_publication(request) {
                tracing::info!(
                    target: "woven_activity::transform",
                    activity = "publish_transform",
                    connection_id = request.connection.get(),
                    namespace_id = request.session.namespace.get(),
                    session_id = request.session.session.get(),
                    space_id = request.space.get(),
                    channel_id = request.channel.get(),
                    payload_bytes = request.payload.len(),
                );
                "publish_transform"
            } else {
                tracing::info!(
                    target: "woven_activity",
                    activity = "publish",
                    connection_id = request.connection.get(),
                    namespace_id = request.session.namespace.get(),
                    session_id = request.session.session.get(),
                    space_id = request.space.get(),
                    channel_id = request.channel.get(),
                    payload_bytes = request.payload.len(),
                );
                "publish"
            }
        }
        Command::Snapshot {
            connection,
            session,
        } => {
            tracing::info!(
                target: "woven_activity",
                activity = "snapshot",
                connection_id = connection.get(),
                namespace_id = session.namespace.get(),
                session_id = session.session.get(),
            );
            "snapshot"
        }
        Command::DrainOutbound { connection } => {
            tracing::info!(
                target: "woven_activity",
                activity = "drain_outbound",
                connection_id = connection.get(),
            );
            "drain_outbound"
        }
        Command::TransportLost { connection } => {
            tracing::info!(
                target: "woven_activity",
                activity = "transport_lost",
                connection_id = connection.get(),
            );
            "transport_lost"
        }
    }
}

fn is_transform_publication(request: &PublishRequest) -> bool {
    request.entity.is_some() && matches!(request.delivery, CoreDelivery::LatestValue)
}

/// What (if anything) the always-on metrics counters need to know about a command before
/// it's consumed by `worker.handle`, and, for publishes, after its outcome comes back.
#[derive(Clone, Copy)]
enum MetricsContext {
    None,
    Connected,
    TransportLost {
        connection: ConnectionId,
    },
    Authenticate {
        connection: ConnectionId,
    },
    Join {
        connection: ConnectionId,
        session: woven_core::SessionKey,
    },
    Leave {
        connection: ConnectionId,
        session: woven_core::SessionKey,
    },
    Publish {
        payload_bytes: u64,
        is_transform: bool,
    },
}

fn metrics_context(command: &Command) -> MetricsContext {
    match command {
        Command::TransportConnected => MetricsContext::Connected,
        Command::TransportLost { connection } => MetricsContext::TransportLost {
            connection: *connection,
        },
        Command::Authenticate { connection, .. } => MetricsContext::Authenticate {
            connection: *connection,
        },
        Command::JoinSession {
            connection,
            session,
        }
        | Command::JoinSessionWithAdmission {
            connection,
            session,
            ..
        } => MetricsContext::Join {
            connection: *connection,
            session: *session,
        },
        Command::LeaveSession {
            connection,
            session,
        } => MetricsContext::Leave {
            connection: *connection,
            session: *session,
        },
        Command::Publish(request) => MetricsContext::Publish {
            payload_bytes: request.payload.len() as u64,
            is_transform: is_transform_publication(request),
        },
        _ => MetricsContext::None,
    }
}

/// Records both the always-on counters and a small, curated set of always-on structured
/// relay-lifecycle log events (connect/disconnect/authenticate/join/leave), each tagged with
/// connection/namespace/session identity so a log pipeline (e.g. Cloud Logging) can filter per
/// tenant. Deliberately excludes per-publish events — those are unbounded in volume and already
/// covered by `ServerMetrics`; this is meant to run in production, not just development.
fn observe_command(
    metrics: &ServerMetrics,
    context: MetricsContext,
    result: &Result<CommandResult, CoreError>,
) {
    match context {
        MetricsContext::None => {}
        MetricsContext::Connected => {
            if let Ok(CommandResult::Connected(connection)) = result {
                metrics.record_connected();
                tracing::info!(
                    target: "woven_relay",
                    connection_id = connection.get(),
                    "connection_established"
                );
            }
        }
        MetricsContext::TransportLost { connection } => {
            if result.is_ok() {
                tracing::info!(
                    target: "woven_relay",
                    connection_id = connection.get(),
                    "connection_closed"
                );
            }
        }
        MetricsContext::Authenticate { connection } => {
            if result.is_ok() {
                tracing::info!(
                    target: "woven_relay",
                    connection_id = connection.get(),
                    "connection_authenticated"
                );
            } else {
                metrics.record_authenticate_rejected();
                tracing::warn!(
                    target: "woven_relay",
                    connection_id = connection.get(),
                    "authenticate_rejected"
                );
            }
        }
        MetricsContext::Join {
            connection,
            session,
        } => {
            if result.is_ok() {
                tracing::info!(
                    target: "woven_relay",
                    connection_id = connection.get(),
                    namespace_id = session.namespace.get(),
                    session_id = session.session.get(),
                    "session_joined"
                );
            } else {
                metrics.record_join_rejected();
                tracing::warn!(
                    target: "woven_relay",
                    connection_id = connection.get(),
                    namespace_id = session.namespace.get(),
                    session_id = session.session.get(),
                    "session_join_rejected"
                );
            }
        }
        MetricsContext::Leave {
            connection,
            session,
        } => {
            if result.is_ok() {
                tracing::info!(
                    target: "woven_relay",
                    connection_id = connection.get(),
                    namespace_id = session.namespace.get(),
                    session_id = session.session.get(),
                    "session_left"
                );
            }
        }
        MetricsContext::Publish {
            payload_bytes,
            is_transform,
        } => {
            metrics.record_publish(payload_bytes, is_transform);
            if let Ok(CommandResult::Published(outcome)) = result {
                let dropped = (outcome.queues.dropped_latest
                    + outcome.queues.dropped_best_effort
                    + outcome.queues.critical_capacity_exhausted)
                    as u64;
                let evicted = (outcome.queues.critical_evictions
                    + outcome.queues.evicted_latest
                    + outcome.queues.replaced_latest) as u64;
                metrics.record_publish_outcome(
                    payload_bytes,
                    outcome.recipient_attempts as u64,
                    dropped,
                    evicted,
                );
            }
        }
    }
}

#[cfg(debug_assertions)]
fn log_development_result(activity: &str, result: &Result<CommandResult, CoreError>) {
    macro_rules! log_result {
        ($target:literal) => {
            if let Ok(CommandResult::Outbound(messages)) = result {
                tracing::info!(
                    target: $target,
                    activity,
                    outcome = "accepted",
                    outbound_messages = messages.len(),
                );
            } else if result.is_ok() {
                tracing::info!(target: $target, activity, outcome = "accepted");
            } else {
                tracing::info!(target: $target, activity, outcome = "rejected");
            }
        };
    }

    if matches!(activity, "publish_transform" | "update_entity_position") {
        log_result!("woven_activity::transform");
    } else {
        log_result!("woven_activity");
    }
}

fn lifecycle_action(command: &Command) -> LifecycleAction {
    match command {
        Command::Subscribe { connection, space } => LifecycleAction::Subscribe {
            connection: *connection,
            space: *space,
        },
        Command::Unsubscribe { connection, space } => LifecycleAction::Unsubscribe {
            connection: *connection,
            space: *space,
        },
        Command::LeaveSession {
            connection,
            session,
        } => LifecycleAction::LeaveSession {
            connection: *connection,
            session: *session,
        },
        Command::SpawnEntity { space, epoch, .. } => LifecycleAction::Spawn {
            space: *space,
            epoch: *epoch,
        },
        Command::RemoveEntity { .. } => LifecycleAction::RemoveEntity,
        Command::TransitionEntity(_) => LifecycleAction::Transition,
        Command::TransportLost { connection } => LifecycleAction::TransportLost {
            connection: *connection,
        },
        Command::TransportConnected
        | Command::Authenticate { .. }
        | Command::JoinSession { .. }
        | Command::RequestSessionAdmission { .. }
        | Command::SessionQueue { .. }
        | Command::JoinSessionWithAdmission { .. }
        | Command::UpdateEntityPosition { .. }
        | Command::Publish(_)
        | Command::Snapshot { .. }
        | Command::DrainOutbound { .. } => LifecycleAction::None,
    }
}

fn apply_lifecycle_action<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    action: LifecycleAction,
    result: &CommandResult,
) where
    A: Authenticator,
{
    match (action, result) {
        (LifecycleAction::Subscribe { connection, space }, CommandResult::Subscribed) => {
            subscriptions.entry(connection).or_default().insert(space);
        }
        (
            LifecycleAction::Unsubscribe { connection, space },
            CommandResult::Unsubscribed(summary),
        ) => {
            if let Some(connection_subscriptions) = subscriptions.get_mut(&connection) {
                connection_subscriptions.remove(&space);
            }
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Removed,
            );
        }
        (
            LifecycleAction::LeaveSession {
                connection,
                session,
            },
            CommandResult::Left(summary),
        ) => {
            if let Some(connection_subscriptions) = subscriptions.get_mut(&connection) {
                connection_subscriptions.retain(|space| space.session != session);
            }
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Removed,
            );
        }
        (LifecycleAction::Spawn { space, epoch }, CommandResult::EntitySpawned(entity)) => {
            distribute_to_space(
                worker,
                recipients,
                subscriptions,
                space,
                &entity_entered_envelope(space, epoch, *entity),
            );
        }
        (LifecycleAction::RemoveEntity, CommandResult::EntityRemoved(summary)) => {
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Removed,
            );
        }
        (LifecycleAction::Transition, CommandResult::EntityTransitioned(transition)) => {
            distribute_transition(worker, recipients, subscriptions, *transition);
        }
        (LifecycleAction::TransportLost { connection }, CommandResult::Disconnected(summary)) => {
            recipients.remove(&connection);
            subscriptions.remove(&connection);
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Disconnected,
            );
        }
        _ => {}
    }
}

fn distribute_transition<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    transition: EntityTransition,
) where
    A: Authenticator,
{
    let source = SpaceKey::new(transition.session, transition.source_space);
    distribute_to_space(
        worker,
        recipients,
        subscriptions,
        source,
        &entity_left_envelope(
            source,
            transition.source_epoch,
            transition.entity,
            EntityLeaveReason::Transitioned,
        ),
    );
    let destination = SpaceKey::new(transition.session, transition.destination_space);
    distribute_to_space(
        worker,
        recipients,
        subscriptions,
        destination,
        &entity_entered_envelope(destination, transition.destination_epoch, transition.entity),
    );
}

fn distribute_removed_entities<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    summary: &CleanupSummary,
    reason: EntityLeaveReason,
) where
    A: Authenticator,
{
    for removed in &summary.removed_entities {
        distribute_removed_entity(worker, recipients, subscriptions, *removed, reason);
    }
}

fn distribute_removed_entity<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    removed: RemovedEntity,
    reason: EntityLeaveReason,
) where
    A: Authenticator,
{
    let space = SpaceKey::new(removed.session, removed.space);
    distribute_to_space(
        worker,
        recipients,
        subscriptions,
        space,
        &entity_left_envelope(space, removed.space_epoch, removed.entity, reason),
    );
}

fn distribute_to_space<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    space: SpaceKey,
    envelope: &Envelope,
) where
    A: Authenticator,
{
    distribute_to_space_excluding(worker, recipients, subscriptions, space, envelope, None);
}

fn distribute_to_space_excluding<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    space: SpaceKey,
    envelope: &Envelope,
    excluded: Option<ConnectionId>,
) where
    A: Authenticator,
{
    let targets = subscriptions
        .iter()
        .filter_map(|(connection, connection_subscriptions)| {
            (excluded != Some(*connection) && connection_subscriptions.contains(&space))
                .then_some(*connection)
        })
        .collect::<Vec<_>>();
    let mut disconnected = Vec::new();
    for connection in targets {
        let Some(recipient) = recipients.get(&connection) else {
            continue;
        };
        if recipient.sender.try_send(envelope.clone()).is_err() {
            let _ = recipient.shutdown.try_send(());
            disconnected.push(connection);
        }
    }
    for connection in disconnected {
        cleanup_dead_connection(worker, recipients, subscriptions, connection);
    }
}

/// Deliver `envelope` directly to a single registered connection.
fn deliver_to_connection<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    connection: ConnectionId,
    envelope: &Envelope,
) -> Result<(), TransportError>
where
    A: Authenticator,
{
    let Some(recipient) = recipients.get(&connection) else {
        return Err(TransportError::UnknownConnection);
    };
    if recipient.sender.try_send(envelope.clone()).is_ok() {
        return Ok(());
    }
    let _ = recipient.shutdown.try_send(());
    cleanup_dead_connection(worker, recipients, subscriptions, connection);
    Err(TransportError::UnknownConnection)
}

/// Remove a connection whose write channel has failed and run the same disconnect cleanup
/// and fan-out as an explicit `TransportLost`.
fn cleanup_dead_connection<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    connection: ConnectionId,
) where
    A: Authenticator,
{
    recipients.remove(&connection);
    subscriptions.remove(&connection);
    let _ = worker.handle(Command::DrainOutbound { connection });
    if let Ok(CommandResult::Disconnected(summary)) =
        worker.handle(Command::TransportLost { connection })
    {
        distribute_removed_entities(
            worker,
            recipients,
            subscriptions,
            &summary,
            EntityLeaveReason::Disconnected,
        );
    }
}

#[must_use]
pub fn entity_entered_envelope(
    space: SpaceKey,
    epoch: SpaceEpoch,
    entity: woven_core::EntityId,
) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: DeliveryClass::ReliableOrdered,
        namespace_id: space.session.namespace.get(),
        session_id: space.session.session.get(),
        space_id: space.space.get(),
        channel_id: None,
        entity_id: Some(entity.get()),
        space_epoch: epoch.get(),
        server_tick: 0,
        sender_sequence: 0,
        correlation_id: None,
        routing_position: None,
        message: MessagePayload::Control(ControlPayload::EntityEntered(EntityEntered {
            owner_entity_id: Some(entity.get()),
        })),
    }
}

#[must_use]
pub fn entity_left_envelope(
    space: SpaceKey,
    epoch: SpaceEpoch,
    entity: woven_core::EntityId,
    reason: EntityLeaveReason,
) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: DeliveryClass::ReliableOrdered,
        namespace_id: space.session.namespace.get(),
        session_id: space.session.session.get(),
        space_id: space.space.get(),
        channel_id: None,
        entity_id: Some(entity.get()),
        space_epoch: epoch.get(),
        server_tick: 0,
        sender_sequence: 0,
        correlation_id: None,
        routing_position: None,
        message: MessagePayload::Control(ControlPayload::EntityLeft(EntityLeft { reason })),
    }
}

struct SnapshotPayload(Vec<u8>);

impl std::fmt::Write for SnapshotPayload {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        if value.len() > MAX_PAYLOAD_BYTES as usize - self.0.len() {
            return Err(std::fmt::Error);
        }
        self.0.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

fn bounded_snapshot_payload(
    snapshot: &woven_core::SessionSnapshot,
) -> Result<Vec<u8>, std::fmt::Error> {
    // Debug-formatted snapshots can expand byte vectors substantially; stop at the wire ceiling.
    let mut payload = SnapshotPayload(Vec::new());
    std::fmt::write(&mut payload, format_args!("{snapshot:?}"))?;
    Ok(payload.0)
}

#[allow(clippy::result_unit_err)]
pub async fn handle_authenticated(
    worker: &WorkerHandle,
    connection: ConnectionId,
    envelope: Envelope,
    write_sender: &mpsc::Sender<Envelope>,
    inference_sink: Option<&mpsc::Sender<UnroutedControl>>,
) -> Result<(), ()> {
    handle_authenticated_with_capabilities(
        worker,
        connection,
        envelope,
        write_sender,
        inference_sink,
        0,
    )
    .await
}

#[allow(clippy::too_many_lines, clippy::result_unit_err)]
pub async fn handle_authenticated_with_capabilities(
    worker: &WorkerHandle,
    connection: ConnectionId,
    envelope: Envelope,
    write_sender: &mpsc::Sender<Envelope>,
    inference_sink: Option<&mpsc::Sender<UnroutedControl>>,
    negotiated_capability_bits: u64,
) -> Result<(), ()> {
    use woven_core::{
        ChannelId, NamespaceId, SessionId, SessionKey, SpaceEpoch, SpaceId, SpaceKey,
    };

    if envelope.routing_position.is_some()
        && negotiated_capability_bits & CAPABILITY_POSITIONED_ENTITY_STATE == 0
    {
        send_error(
            write_sender,
            envelope.message_kind(),
            ProtocolErrorCode::UnsupportedMessage,
            "positioned EntityState capability was not negotiated".to_owned(),
        )
        .await;
        return Err(());
    }
    if let Some(sink) = inference_sink
        && matches!(
            envelope.message,
            MessagePayload::Control(
                ControlPayload::InferenceRequested(_) | ControlPayload::InferenceCancelled(_)
            )
        )
    {
        let _ = sink.try_send(UnroutedControl {
            connection,
            envelope,
        });
        return Ok(());
    }
    let session = SessionKey {
        namespace: NamespaceId::new(envelope.namespace_id),
        session: SessionId::new(envelope.session_id),
    };
    if matches!(
        envelope.message_kind(),
        MessageKind::RequestAdmission
            | MessageKind::AdmissionResult
            | MessageKind::QueueStatusRequest
            | MessageKind::QueueHeartbeat
            | MessageKind::QueueClaim
            | MessageKind::QueueCancel
            | MessageKind::QueueUpdate
    ) {
        return admission::handle(worker, connection, envelope, write_sender).await;
    }
    if let MessagePayload::Control(ControlPayload::ClientLog(log)) = &envelope.message {
        let result = if negotiated_capability_bits & woven_protocol::CAPABILITY_CLIENT_LOG == 0 {
            Err(ProtocolErrorCode::UnsupportedMessage)
        } else if envelope.delivery_class != DeliveryClass::ReliableOrdered
            || envelope.space_id != 0
            || envelope.space_epoch != 0
            || envelope.channel_id.is_some()
            || envelope.entity_id.is_some()
            || envelope.routing_position.is_some()
        {
            Err(ProtocolErrorCode::InvalidScope)
        } else if let Err(code) = logging::validate_log(log) {
            Err(code)
        } else {
            worker
                .capture_client_log(connection, session, log.clone())
                .await
        };
        if let Err(code) = result {
            let mut response = Envelope::control(
                DeliveryClass::ReliableOrdered,
                ControlPayload::ProtocolError(ProtocolError {
                    code,
                    related_message_kind: MessageKind::ClientLog,
                    message: "client log rejected".to_owned(),
                }),
            );
            response.correlation_id = envelope.correlation_id;
            return send_envelope(write_sender, response).await;
        }
        // No peer relay, outbound drain, or successful-log acknowledgement.
        return Ok(());
    }
    let space = SpaceKey {
        session,
        space: SpaceId::new(envelope.space_id),
    };
    if matches!(
        envelope.message,
        MessagePayload::Control(ControlPayload::SnapshotRequest(_))
    ) {
        return match worker
            .execute(Command::Snapshot {
                connection,
                session,
            })
            .await
        {
            Ok(CommandResult::Snapshot(snapshot)) => {
                let mut response = envelope;
                response.message = match bounded_snapshot_payload(&snapshot) {
                    Ok(bytes) => MessagePayload::Snapshot(OpaquePayload { type_id: 1, bytes }),
                    Err(_) => {
                        MessagePayload::Control(ControlPayload::ProtocolError(ProtocolError {
                            code: ProtocolErrorCode::PayloadTooLarge,
                            related_message_kind: MessageKind::SnapshotRequest,
                            message: format!(
                                "snapshot payload exceeds limit {MAX_PAYLOAD_BYTES} bytes"
                            ),
                        }))
                    }
                };
                send_envelope(write_sender, response).await
            }
            Ok(_) => Err(()),
            Err(error) => {
                send_error(
                    write_sender,
                    MessageKind::SnapshotRequest,
                    ProtocolErrorCode::Internal,
                    error.to_string(),
                )
                .await;
                Err(())
            }
        };
    }
    if matches!(
        envelope.message,
        MessagePayload::Control(ControlPayload::SubscribeSpace(_))
    ) {
        return match worker
            .subscribe_and_spawn(connection, space, SpaceEpoch::new(envelope.space_epoch))
            .await
        {
            Ok(entity) => {
                let subscription_id = envelope.space_id;
                let mut accepted = envelope.clone();
                accepted.message = MessagePayload::Control(ControlPayload::SubscriptionAccepted(
                    woven_protocol::SubscriptionAccepted {
                        subscription_id,
                        accepted_space_epoch: accepted.space_epoch,
                    },
                ));
                accepted.delivery_class = DeliveryClass::ReliableOrdered;
                send_envelope(write_sender, accepted).await?;
                send_envelope(
                    write_sender,
                    entity_entered_envelope(space, SpaceEpoch::new(envelope.space_epoch), entity),
                )
                .await?;
                worker
                    .activate_subscription(connection, space)
                    .await
                    .map_err(|_| ())?;
                flush_outbound(worker, connection, write_sender).await
            }
            Err(error) => {
                let code = match &error {
                    TransportError::Core(core_error) => core_error_code(core_error),
                    TransportError::WorkerUnavailable | TransportError::UnknownConnection => {
                        ProtocolErrorCode::Internal
                    }
                };
                send_error(
                    write_sender,
                    MessageKind::SubscribeSpace,
                    code,
                    error.to_string(),
                )
                .await;
                Err(())
            }
        };
    }
    let command = match &envelope.message {
        MessagePayload::Control(ControlPayload::JoinSession(_)) => Command::JoinSession {
            connection,
            session,
        },
        MessagePayload::Control(ControlPayload::LeaveSession(_)) => Command::LeaveSession {
            connection,
            session,
        },
        MessagePayload::Control(ControlPayload::UnsubscribeSpace(_)) => {
            Command::Unsubscribe { connection, space }
        }
        MessagePayload::Control(ControlPayload::SpaceTransition(transition)) => {
            Command::TransitionEntity(woven_core::EntityTransitionRequest {
                connection,
                session,
                entity: woven_core::EntityId::new(envelope.entity_id.ok_or(())?),
                source_space: SpaceId::new(transition.from_space_id),
                source_epoch: SpaceEpoch::new(envelope.space_epoch),
                destination_space: SpaceId::new(transition.to_space_id),
                destination_epoch: SpaceEpoch::new(transition.to_space_epoch),
            })
        }
        MessagePayload::ReliableEvent(payload) | MessagePayload::EntityState(payload) => {
            let delivery = core_delivery(envelope.delivery_class).ok_or(())?;
            let entity = envelope.entity_id.map(woven_core::EntityId::new);
            let channel = ChannelId::new(envelope.channel_id.ok_or(())?);
            let persistence = match worker
                .channel_persistence(connection, session, channel)
                .await
            {
                Ok(persistence) => persistence,
                Err(error) => {
                    send_transport_error(write_sender, envelope.message_kind(), &error).await;
                    return Err(());
                }
            };
            Command::Publish(PublishRequest {
                connection,
                session,
                space: SpaceId::new(envelope.space_id),
                space_epoch: SpaceEpoch::new(envelope.space_epoch),
                entity,
                channel,
                sequence: envelope.sender_sequence,
                delivery,
                persistence,
                coalesce_key: if delivery.is_replaceable() {
                    Some(CoalesceKey::new(channel, entity, payload.type_id))
                } else {
                    None
                },
                routing_position: envelope.routing_position.map(|position| {
                    woven_core::EntityPosition::Cartesian3D {
                        x: position.x,
                        y: position.y,
                        z: position.z,
                    }
                }),
                payload: payload.bytes.clone(),
            })
        }
        _ => {
            send_error(
                write_sender,
                envelope.message_kind(),
                ProtocolErrorCode::UnsupportedMessage,
                "message is not implemented by this transport".to_owned(),
            )
            .await;
            return Err(());
        }
    };
    match worker.execute(command).await {
        Ok(_) => {}
        Err(error) => {
            if envelope.delivery_class == DeliveryClass::UnreliableSequenced
                && matches!(error, TransportError::Core(CoreError::StaleSequence { .. }))
            {
                // Datagram reordering is expected; the core has rejected this update atomically.
                tracing::trace!(?connection, ?error, "dropping stale unreliable state");
                return Ok(());
            }
            send_transport_error(write_sender, envelope.message_kind(), &error).await;
            return Err(());
        }
    }
    flush_outbound(worker, connection, write_sender).await
}

#[allow(clippy::result_unit_err)]
pub async fn flush_outbound(
    worker: &WorkerHandle,
    connection: ConnectionId,
    write_sender: &mpsc::Sender<Envelope>,
) -> Result<(), ()> {
    if let Ok(CommandResult::Outbound(messages)) =
        worker.execute(Command::DrainOutbound { connection }).await
    {
        for message in messages {
            send_envelope(write_sender, outbound_envelope(message)).await?;
        }
    }
    Ok(())
}

pub fn outbound_envelope(message: woven_core::OutboundMessage) -> Envelope {
    let delivery = protocol_delivery(message.delivery);
    let opaque = OpaquePayload {
        type_id: message.coalesce_key.map_or(1, |key| key.component),
        bytes: message.payload,
    };
    let payload = if matches!(
        delivery,
        DeliveryClass::LatestValue | DeliveryClass::UnreliableSequenced
    ) {
        MessagePayload::EntityState(opaque)
    } else {
        MessagePayload::ReliableEvent(opaque)
    };
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: delivery,
        namespace_id: message.namespace.get(),
        session_id: message.session.get(),
        space_id: message.space.get(),
        channel_id: Some(message.channel.get()),
        entity_id: message.entity.map(woven_core::EntityId::get),
        space_epoch: message.space_epoch.get(),
        server_tick: 0,
        sender_sequence: message.sequence,
        correlation_id: None,
        routing_position: message.routing_position.map(|position| match position {
            woven_core::EntityPosition::Cartesian3D { x, y, z } => RoutingPosition3D { x, y, z },
            woven_core::EntityPosition::Cartesian2D { .. } => {
                unreachable!("wire routing positions are 3D-only")
            }
        }),
        message: payload,
    }
}

#[allow(clippy::unused_async, clippy::result_unit_err)]
pub async fn send_envelope(sender: &mpsc::Sender<Envelope>, envelope: Envelope) -> Result<(), ()> {
    sender.try_send(envelope).map_err(|_| ())
}

pub async fn send_error(
    sender: &mpsc::Sender<Envelope>,
    related: MessageKind,
    code: ProtocolErrorCode,
    message: String,
) {
    let related = if related == MessageKind::Unknown {
        MessageKind::ProtocolError
    } else {
        related
    };
    let _ = send_envelope(
        sender,
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::ProtocolError(ProtocolError {
                code,
                related_message_kind: related,
                message,
            }),
        ),
    )
    .await;
}

#[must_use]
pub fn core_delivery(delivery: DeliveryClass) -> Option<CoreDelivery> {
    Some(match delivery {
        DeliveryClass::ReliableOrdered => CoreDelivery::ReliableOrdered,
        DeliveryClass::ReliableUnordered => CoreDelivery::ReliableUnordered,
        DeliveryClass::LatestValue => CoreDelivery::LatestValue,
        DeliveryClass::UnreliableSequenced => CoreDelivery::UnreliableSequenced,
        DeliveryClass::BestEffortEvent => CoreDelivery::BestEffortEvent,
        DeliveryClass::Unknown => return None,
    })
}

#[must_use]
pub fn protocol_delivery(delivery: CoreDelivery) -> DeliveryClass {
    match delivery {
        CoreDelivery::ReliableOrdered => DeliveryClass::ReliableOrdered,
        CoreDelivery::ReliableUnordered => DeliveryClass::ReliableUnordered,
        CoreDelivery::LatestValue => DeliveryClass::LatestValue,
        CoreDelivery::UnreliableSequenced => DeliveryClass::UnreliableSequenced,
        CoreDelivery::BestEffortEvent => DeliveryClass::BestEffortEvent,
    }
}

async fn send_transport_error(
    sender: &mpsc::Sender<Envelope>,
    related: MessageKind,
    error: &TransportError,
) {
    let code = match error {
        TransportError::Core(error) => core_error_code(error),
        TransportError::WorkerUnavailable | TransportError::UnknownConnection => {
            ProtocolErrorCode::Internal
        }
    };
    send_error(sender, related, code, error.to_string()).await;
}

#[must_use]
pub fn core_error_code(error: &CoreError) -> ProtocolErrorCode {
    match error {
        CoreError::AuthenticationRequired => ProtocolErrorCode::AuthenticationRequired,
        CoreError::NamespaceReadAccessDenied(_)
        | CoreError::NamespaceWriteAccessDenied(_)
        | CoreError::SessionReadAccessDenied(_)
        | CoreError::SessionWriteAccessDenied(_)
        | CoreError::SpaceReadAccessDenied(_)
        | CoreError::SpaceWriteAccessDenied(_)
        | CoreError::ChannelWriteAccessDenied(_)
        | CoreError::EntityNotOwned(_)
        | CoreError::AuthorityRejected(_)
        | CoreError::AdmissionLeaseRequired(_)
        | CoreError::InvalidAdmissionLease(_) => ProtocolErrorCode::Unauthorized,
        CoreError::SessionNotFound(_) => ProtocolErrorCode::InvalidScope,
        CoreError::SpaceEpochMismatch { .. } => ProtocolErrorCode::StaleEpoch,
        CoreError::StaleSequence { .. } => ProtocolErrorCode::SequenceRejected,
        CoreError::PayloadTooLarge { .. } => ProtocolErrorCode::PayloadTooLarge,
        CoreError::PublishRateLimited { .. } | CoreError::AdmissionRateLimited { .. } => {
            ProtocolErrorCode::RateLimited
        }
        _ => ProtocolErrorCode::Internal,
    }
}
