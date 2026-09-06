use std::time::Duration;

use crate::{
    ChannelId, ConnectionId, EntityId, NamespaceId, PrincipalId, SessionId, SessionKey, SpaceEpoch,
    SpaceId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryClass {
    ReliableOrdered,
    ReliableUnordered,
    LatestValue,
    UnreliableSequenced,
    BestEffortEvent,
}

impl DeliveryClass {
    #[must_use]
    pub const fn is_critical(self) -> bool {
        matches!(self, Self::ReliableOrdered | Self::ReliableUnordered)
    }

    #[must_use]
    pub const fn is_replaceable(self) -> bool {
        matches!(self, Self::LatestValue | Self::UnreliableSequenced)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistenceClass {
    Ephemeral,
    /// Persists in the cache layer while touched. `ttl: None` never expires (survives for the
    /// session's lifetime, matching the original behavior); `Some(duration)` wipes the value
    /// after that long without a write, freeing memory even if nobody ever touches it again.
    Stateful {
        ttl: Option<Duration>,
    },
    Durable,
}

impl PersistenceClass {
    /// Compares by variant only, ignoring `Stateful`'s `ttl`. TTL is a channel-level config
    /// choice (set once at registration), not something a publisher declares per message, so
    /// channel-policy checks must not require an incoming message's `ttl` to match exactly.
    #[must_use]
    pub const fn same_kind(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Ephemeral, Self::Ephemeral)
                | (Self::Stateful { .. }, Self::Stateful { .. })
                | (Self::Durable, Self::Durable)
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CoordinateFrame {
    Logical,
    Cartesian2D { meters_per_unit: f64 },
    Cartesian3D { meters_per_unit: f64 },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EntityPosition {
    Cartesian2D { x: f64, y: f64 },
    Cartesian3D { x: f64, y: f64, z: f64 },
}

impl EntityPosition {
    pub fn validate_for_frame(self, frame: CoordinateFrame) -> Result<(), PositionValidationError> {
        let dimensions_match = matches!(
            (self, frame),
            (
                Self::Cartesian2D { .. },
                CoordinateFrame::Cartesian2D { .. }
            ) | (
                Self::Cartesian3D { .. },
                CoordinateFrame::Cartesian3D { .. }
            )
        );
        if matches!(frame, CoordinateFrame::Logical) {
            return Err(PositionValidationError::LogicalFrame);
        }
        if !dimensions_match {
            return Err(PositionValidationError::DimensionMismatch);
        }
        if self.is_finite() {
            Ok(())
        } else {
            Err(PositionValidationError::NonFiniteCoordinate)
        }
    }

    #[must_use]
    pub const fn is_finite(self) -> bool {
        match self {
            Self::Cartesian2D { x, y } => x.is_finite() && y.is_finite(),
            Self::Cartesian3D { x, y, z } => x.is_finite() && y.is_finite() && z.is_finite(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionValidationError {
    LogicalFrame,
    DimensionMismatch,
    NonFiniteCoordinate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParentAnchor {
    pub parent_space: SpaceId,
    pub anchor_entity: EntityId,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RoutingPolicy {
    BroadcastAll,
    SpatialGrid2D {
        cell_size: f64,
        interest_radius: f64,
        exact_distance: bool,
    },
    SpatialGrid3D {
        cell_size: f64,
        interest_radius: f64,
        exact_distance: bool,
    },
    TopicOnly,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpaceDescriptor {
    pub id: SpaceId,
    pub local_frame: CoordinateFrame,
    pub parent: Option<ParentAnchor>,
    pub epoch: SpaceEpoch,
    pub routing: RoutingPolicy,
}

impl SpaceDescriptor {
    pub fn validate(&self) -> Result<(), SpaceValidationError> {
        if self.id.get() == 0 {
            return Err(SpaceValidationError::ZeroSpaceId);
        }
        if self.epoch.get() == 0 {
            return Err(SpaceValidationError::ZeroEpoch);
        }
        if let Some(parent) = self.parent {
            if parent.parent_space.get() == 0 {
                return Err(SpaceValidationError::ZeroParentSpaceId);
            }
            if parent.anchor_entity.get() == 0 {
                return Err(SpaceValidationError::ZeroAnchorEntityId);
            }
            if parent.parent_space == self.id {
                return Err(SpaceValidationError::SelfParent);
            }
        }

        match self.local_frame {
            CoordinateFrame::Logical => {}
            CoordinateFrame::Cartesian2D { meters_per_unit }
            | CoordinateFrame::Cartesian3D { meters_per_unit } => {
                validate_positive_finite(meters_per_unit)?;
            }
        }

        match self.routing {
            RoutingPolicy::BroadcastAll | RoutingPolicy::TopicOnly => {}
            RoutingPolicy::SpatialGrid2D {
                cell_size,
                interest_radius,
                ..
            } => {
                if !matches!(self.local_frame, CoordinateFrame::Cartesian2D { .. }) {
                    return Err(SpaceValidationError::RoutingDimensionMismatch);
                }
                validate_positive_finite(cell_size)?;
                validate_positive_finite(interest_radius)?;
            }
            RoutingPolicy::SpatialGrid3D {
                cell_size,
                interest_radius,
                ..
            } => {
                if !matches!(self.local_frame, CoordinateFrame::Cartesian3D { .. }) {
                    return Err(SpaceValidationError::RoutingDimensionMismatch);
                }
                validate_positive_finite(cell_size)?;
                validate_positive_finite(interest_radius)?;
            }
        }
        Ok(())
    }
}

fn validate_positive_finite(value: f64) -> Result<(), SpaceValidationError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(SpaceValidationError::InvalidScale)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceValidationError {
    ZeroSpaceId,
    ZeroEpoch,
    ZeroParentSpaceId,
    ZeroAnchorEntityId,
    SelfParent,
    InvalidScale,
    RoutingDimensionMismatch,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoalesceKey {
    pub channel: ChannelId,
    pub entity: Option<EntityId>,
    pub component: u64,
}

impl CoalesceKey {
    #[must_use]
    pub const fn new(channel: ChannelId, entity: Option<EntityId>, component: u64) -> Self {
        Self {
            channel,
            entity,
            component,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScopedCoalesceKey {
    pub namespace: NamespaceId,
    pub session: SessionId,
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    pub application: CoalesceKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundMessage {
    pub namespace: NamespaceId,
    pub session: SessionId,
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    pub entity: Option<EntityId>,
    pub channel: ChannelId,
    pub sequence: u64,
    pub delivery: DeliveryClass,
    pub persistence: PersistenceClass,
    pub coalesce_key: Option<CoalesceKey>,
    pub payload: Vec<u8>,
}

impl OutboundMessage {
    #[must_use]
    pub fn scoped_coalesce_key(&self) -> Option<ScopedCoalesceKey> {
        self.coalesce_key.map(|application| ScopedCoalesceKey {
            namespace: self.namespace,
            session: self.session,
            space: self.space,
            space_epoch: self.space_epoch,
            application,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpaceSnapshot {
    pub descriptor: SpaceDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntitySnapshot {
    pub id: EntityId,
    pub owner_connection: ConnectionId,
    pub owner_principal: PrincipalId,
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateSnapshot {
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    pub entity: Option<EntityId>,
    pub channel: ChannelId,
    pub component: u64,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionSnapshot {
    pub key: SessionKey,
    pub member_count: usize,
    pub subscription_count: usize,
    pub state_bytes: usize,
    pub spaces: Vec<SpaceSnapshot>,
    pub entities: Vec<EntitySnapshot>,
    pub state: Vec<StateSnapshot>,
}
