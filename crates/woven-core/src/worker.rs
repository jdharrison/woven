use std::collections::VecDeque;
use std::time::Instant;

use crate::{
    AdmissionLease, Authenticator, CleanupSummary, ConnectionId, CoreError, Credentials, EntityId,
    EntityPosition, EntityTransition, EntityTransitionRequest, IdempotencyKey, JoinDecision,
    OutboundMessage, PrincipalId, PublishOutcome, PublishRequest, SessionKey, SessionSnapshot,
    SpaceEpoch, SpaceKey, WovenCore,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    TransportConnected,
    Authenticate {
        connection: ConnectionId,
        credentials: Credentials,
    },
    JoinSession {
        connection: ConnectionId,
        session: SessionKey,
    },
    RequestSessionAdmission {
        connection: ConnectionId,
        session: SessionKey,
        idempotency_key: IdempotencyKey,
    },
    SessionQueue {
        connection: ConnectionId,
        session: SessionKey,
        ticket: crate::QueueTicketId,
        operation: crate::QueueOperation,
    },
    JoinSessionWithAdmission {
        connection: ConnectionId,
        session: SessionKey,
        lease: AdmissionLease,
    },
    LeaveSession {
        connection: ConnectionId,
        session: SessionKey,
    },
    Subscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    Unsubscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    SpawnEntity {
        connection: ConnectionId,
        space: SpaceKey,
        epoch: SpaceEpoch,
    },
    RemoveEntity {
        connection: ConnectionId,
        session: SessionKey,
        entity: EntityId,
    },
    UpdateEntityPosition {
        connection: ConnectionId,
        session: SessionKey,
        entity: EntityId,
        position: EntityPosition,
    },
    TransitionEntity(EntityTransitionRequest),
    Publish(PublishRequest),
    Snapshot {
        connection: ConnectionId,
        session: SessionKey,
    },
    DrainOutbound {
        connection: ConnectionId,
    },
    TransportLost {
        connection: ConnectionId,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum CommandResult {
    Connected(ConnectionId),
    Authenticated(PrincipalId),
    Joined,
    Admission(JoinDecision),
    Queue(crate::QueueStatus),
    Left(CleanupSummary),
    Subscribed,
    Unsubscribed(CleanupSummary),
    EntitySpawned(EntityId),
    EntityRemoved(CleanupSummary),
    EntityPositionUpdated,
    EntityTransitioned(EntityTransition),
    Published(PublishOutcome),
    Snapshot(SessionSnapshot),
    Outbound(Vec<OutboundMessage>),
    Disconnected(CleanupSummary),
}

pub struct TransportIndependentWorker<A> {
    core: WovenCore<A>,
}

impl<A: Authenticator> TransportIndependentWorker<A> {
    #[must_use]
    pub const fn new(core: WovenCore<A>) -> Self {
        Self { core }
    }

    #[must_use]
    pub const fn core(&self) -> &WovenCore<A> {
        &self.core
    }

    #[must_use]
    pub const fn core_mut(&mut self) -> &mut WovenCore<A> {
        &mut self.core
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive dispatch arm per worker command"
    )]
    pub fn handle(&mut self, command: Command) -> Result<CommandResult, CoreError> {
        match command {
            Command::TransportConnected => self
                .core
                .transport_connected()
                .map(CommandResult::Connected),
            Command::Authenticate {
                connection,
                credentials,
            } => self
                .core
                .authenticate(connection, &credentials)
                .map(CommandResult::Authenticated),
            Command::JoinSession {
                connection,
                session,
            } => self
                .core
                .join_session(connection, session)
                .map(|()| CommandResult::Joined),
            Command::RequestSessionAdmission {
                connection,
                session,
                idempotency_key,
            } => self
                .core
                .admit_and_join_session_at(connection, session, idempotency_key, Instant::now())
                .map(CommandResult::Admission),
            Command::SessionQueue {
                connection,
                session,
                ticket,
                operation,
            } => self
                .core
                .session_queue_at(connection, session, ticket, operation, Instant::now())
                .map(CommandResult::Queue),
            Command::JoinSessionWithAdmission {
                connection,
                session,
                lease,
            } => self
                .core
                .join_session_with_admission(connection, session, lease)
                .map(|()| CommandResult::Joined),
            Command::LeaveSession {
                connection,
                session,
            } => self
                .core
                .leave_session(connection, session)
                .map(CommandResult::Left),
            Command::Subscribe { connection, space } => self
                .core
                .subscribe(connection, space)
                .map(|()| CommandResult::Subscribed),
            Command::Unsubscribe { connection, space } => self
                .core
                .unsubscribe(connection, space)
                .map(CommandResult::Unsubscribed),
            Command::SpawnEntity {
                connection,
                space,
                epoch,
            } => self
                .core
                .spawn_entity(connection, space, epoch)
                .map(CommandResult::EntitySpawned),
            Command::RemoveEntity {
                connection,
                session,
                entity,
            } => self
                .core
                .remove_entity(connection, session, entity)
                .map(CommandResult::EntityRemoved),
            Command::UpdateEntityPosition {
                connection,
                session,
                entity,
                position,
            } => self
                .core
                .update_entity_position(connection, session, entity, position)
                .map(|()| CommandResult::EntityPositionUpdated),
            Command::TransitionEntity(request) => self
                .core
                .transition_entity(request)
                .map(CommandResult::EntityTransitioned),
            Command::Publish(request) => self.core.publish(request).map(CommandResult::Published),
            Command::Snapshot {
                connection,
                session,
            } => self
                .core
                .snapshot(connection, session)
                .map(CommandResult::Snapshot),
            Command::DrainOutbound { connection } => self
                .core
                .drain_outbound(connection)
                .map(CommandResult::Outbound),
            Command::TransportLost { connection } => self
                .core
                .transport_lost(connection)
                .map(CommandResult::Disconnected),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessError {
    ZeroCapacity,
    Full,
}

pub struct WorkerHarness<A> {
    worker: TransportIndependentWorker<A>,
    capacity: usize,
    commands: VecDeque<Command>,
}

impl<A: Authenticator> WorkerHarness<A> {
    pub fn new(
        worker: TransportIndependentWorker<A>,
        capacity: usize,
    ) -> Result<Self, HarnessError> {
        if capacity == 0 {
            return Err(HarnessError::ZeroCapacity);
        }
        Ok(Self {
            worker,
            capacity,
            commands: VecDeque::with_capacity(capacity),
        })
    }

    pub fn submit(&mut self, command: Command) -> Result<(), HarnessError> {
        if self.commands.len() == self.capacity {
            return Err(HarnessError::Full);
        }
        self.commands.push_back(command);
        Ok(())
    }

    pub fn step(&mut self) -> Option<Result<CommandResult, CoreError>> {
        self.commands
            .pop_front()
            .map(|command| self.worker.handle(command))
    }

    pub fn run_pending(&mut self) -> Vec<Result<CommandResult, CoreError>> {
        let mut outcomes = Vec::with_capacity(self.commands.len());
        while let Some(outcome) = self.step() {
            outcomes.push(outcome);
        }
        outcomes
    }

    #[must_use]
    pub const fn worker(&self) -> &TransportIndependentWorker<A> {
        &self.worker
    }

    #[must_use]
    pub const fn worker_mut(&mut self) -> &mut TransportIndependentWorker<A> {
        &mut self.worker
    }

    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.commands.len()
    }
}
