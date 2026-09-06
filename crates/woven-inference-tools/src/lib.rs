//! Provider-neutral tool registry and deterministic tool-call gateway (ADR 0010).
//!
//! Models never mutate shared state directly. A [`ToolHandler`] evaluates a
//! [`ToolCallProposal`] and, if it approves it, performs the resulting state change itself by
//! calling `woven-core` through the same `WorkerHandle` API a transport uses — subject
//! to the exact same grants and entity-ownership rules as any other client.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use woven_core::{CoalesceKey, Command, ConnectionId, EntityId, SpaceEpoch, SpaceKey};
use woven_inference_core::ToolCallProposal;
use woven_transport::WorkerHandle;

/// Bounded registry capacity; no unbounded collections.
const MAX_REGISTERED_TOOLS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SideEffect {
    ReadOnly,
    StateChanging,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolDefinition {
    pub id: String,
    pub version: u32,
    pub side_effect: SideEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolCallRejectionReason {
    UnknownTool,
    Stale,
    PolicyDenied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ToolCallOutcome {
    Completed {
        new_revision: u64,
        result: Vec<u8>,
    },
    Rejected {
        code: ToolCallRejectionReason,
        reason: String,
    },
}

/// What a [`ToolHandler`] needs to act as the AI identity: a handle to the same bounded core
/// worker every transport uses, and the AI's own registered connection/entity/space.
#[derive(Clone)]
pub struct ToolInvocationContext {
    pub worker: WorkerHandle,
    pub connection: ConnectionId,
    pub entity: EntityId,
    pub space: SpaceKey,
    pub space_epoch: SpaceEpoch,
}

#[async_trait::async_trait]
pub trait ToolHandler: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    async fn invoke(
        &self,
        context: &ToolInvocationContext,
        proposal: &ToolCallProposal,
    ) -> ToolCallOutcome;
}

#[derive(Debug, Eq, PartialEq)]
pub enum ToolRegistryError {
    CapacityReached,
}

/// Bounded id+version -> handler registry.
#[derive(Default)]
pub struct ToolRegistry {
    tools: BTreeMap<(String, u32), std::sync::Arc<dyn ToolHandler>>,
}

impl ToolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        handler: std::sync::Arc<dyn ToolHandler>,
    ) -> Result<(), ToolRegistryError> {
        if self.tools.len() >= MAX_REGISTERED_TOOLS {
            return Err(ToolRegistryError::CapacityReached);
        }
        let definition = handler.definition();
        self.tools
            .insert((definition.id, definition.version), handler);
        Ok(())
    }

    /// Validate and, if approved, execute a model-proposed tool call. Unknown tool
    /// id/version pairs are rejected without ever reaching a handler.
    pub async fn evaluate(
        &self,
        context: &ToolInvocationContext,
        proposal: &ToolCallProposal,
    ) -> ToolCallOutcome {
        let Some(handler) = self
            .tools
            .get(&(proposal.tool_id.clone(), proposal.tool_version))
        else {
            return ToolCallOutcome::Rejected {
                code: ToolCallRejectionReason::UnknownTool,
                reason: format!(
                    "no tool registered for {}@v{}",
                    proposal.tool_id, proposal.tool_version
                ),
            };
        };
        handler.invoke(context, proposal).await
    }
}

/// Builds a `Command::Publish` against the AI's own owned entity as the demo state-changing
/// tool's effect. Kept here (rather than duplicated per handler) since every state-changing
/// tool in this minimal slice follows the same `LatestValue` self-state-update shape.
async fn publish_latest_value(
    context: &ToolInvocationContext,
    channel: woven_core::ChannelId,
    component: u64,
    sequence: u64,
    payload: Vec<u8>,
) -> Result<(), String> {
    let request = woven_core::PublishRequest {
        connection: context.connection,
        session: context.space.session,
        space: context.space.space,
        space_epoch: context.space_epoch,
        entity: Some(context.entity),
        channel,
        sequence,
        delivery: woven_core::DeliveryClass::LatestValue,
        persistence: woven_core::PersistenceClass::Stateful { ttl: None },
        coalesce_key: Some(CoalesceKey::new(channel, Some(context.entity), component)),
        payload,
    };
    context
        .worker
        .execute(Command::Publish(request))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub mod demo {
    //! Two demo tools used by the coordinator's integration tests: a read-only diagnostic
    //! and a state-changing status update gated by `expected_revision`.

    use std::sync::atomic::{AtomicU64, Ordering};

    use woven_core::ChannelId;
    use woven_inference_core::ToolCallProposal;

    use super::{
        SideEffect, ToolCallOutcome, ToolCallRejectionReason, ToolDefinition, ToolHandler,
        ToolInvocationContext, publish_latest_value,
    };

    pub const DIAGNOSTIC_TOOL_ID: &str = "diagnostics.report";
    pub const STATUS_TOOL_ID: &str = "status.set";
    const STATUS_COMPONENT: u64 = 1;

    /// Always-accepted, side-effect-free tool.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct DiagnosticTool;

    #[async_trait::async_trait]
    impl ToolHandler for DiagnosticTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                id: DIAGNOSTIC_TOOL_ID.to_owned(),
                version: 1,
                side_effect: SideEffect::ReadOnly,
            }
        }

        async fn invoke(
            &self,
            _context: &ToolInvocationContext,
            _proposal: &ToolCallProposal,
        ) -> ToolCallOutcome {
            ToolCallOutcome::Completed {
                new_revision: 0,
                result: b"all systems nominal".to_vec(),
            }
        }
    }

    /// Mutates the AI's own `LatestValue` status attribute, gated by `expected_revision` to
    /// demonstrate rejecting a stale state-changing proposal.
    pub struct StatusUpdateTool {
        channel: ChannelId,
        revision: AtomicU64,
    }

    impl StatusUpdateTool {
        #[must_use]
        pub fn new(channel: ChannelId) -> Self {
            Self {
                channel,
                revision: AtomicU64::new(1),
            }
        }
    }

    #[async_trait::async_trait]
    impl ToolHandler for StatusUpdateTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                id: STATUS_TOOL_ID.to_owned(),
                version: 1,
                side_effect: SideEffect::StateChanging,
            }
        }

        async fn invoke(
            &self,
            context: &ToolInvocationContext,
            proposal: &ToolCallProposal,
        ) -> ToolCallOutcome {
            let current = self.revision.load(Ordering::SeqCst);
            if proposal.expected_revision != current {
                return ToolCallOutcome::Rejected {
                    code: ToolCallRejectionReason::Stale,
                    reason: format!(
                        "expected_revision {} does not match current revision {current}",
                        proposal.expected_revision
                    ),
                };
            }
            let next = current + 1;
            match publish_latest_value(
                context,
                self.channel,
                STATUS_COMPONENT,
                next,
                proposal.arguments.clone(),
            )
            .await
            {
                Ok(()) => {
                    self.revision.store(next, Ordering::SeqCst);
                    ToolCallOutcome::Completed {
                        new_revision: next,
                        result: b"status updated".to_vec(),
                    }
                }
                Err(error) => ToolCallOutcome::Rejected {
                    code: ToolCallRejectionReason::PolicyDenied,
                    reason: error,
                },
            }
        }
    }
}
