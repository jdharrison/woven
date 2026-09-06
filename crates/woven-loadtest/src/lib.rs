//! Bounded local load scenarios for Woven routing behavior.

#![deny(unsafe_code)]

use std::{
    fmt,
    time::{Duration, Instant},
};

use woven_core::{
    AccessGrant, AuthenticatedPrincipal, AuthorizationGrants, ChannelDefinition, ChannelId,
    ChannelScope, CoalesceKey, CoordinateFrame, CoreConfig, Credentials, DeliveryClass,
    DevAuthenticator, EntityId, EntityPosition, NamespaceId, PersistenceClass, PrincipalId,
    PublishRateLimit, PublishRequest, RoutingPolicy, SessionId, SessionKey, SpaceDescriptor,
    SpaceEpoch, SpaceId, SpaceKey, WovenCore,
};

const NAMESPACE: NamespaceId = NamespaceId::new(1);
const SESSION_ID: SessionId = SessionId::new(1);
const SPACE_ID: SpaceId = SpaceId::new(1);
const EPOCH: SpaceEpoch = SpaceEpoch::new(1);
const STATE_CHANNEL: ChannelId = ChannelId::new(1);
const PAYLOAD: &[u8] = b"woven-load-state";

/// Routing scenario exercised by the load runner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    BroadcastAll,
    TopicOnly,
    SpatialGrid2D,
    SpatialGrid3D,
}

impl Scenario {
    /// Parse a CLI scenario name.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "broadcast" => Some(Self::BroadcastAll),
            "topic" => Some(Self::TopicOnly),
            "grid2d" => Some(Self::SpatialGrid2D),
            "grid3d" => Some(Self::SpatialGrid3D),
            _ => None,
        }
    }

    /// Stable CLI name for this scenario.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::BroadcastAll => "broadcast",
            Self::TopicOnly => "topic",
            Self::SpatialGrid2D => "grid2d",
            Self::SpatialGrid3D => "grid3d",
        }
    }
}

/// Bounded runner configuration.
#[derive(Clone, Copy, Debug)]
pub struct LoadConfig {
    pub scenario: Scenario,
    pub participants: usize,
    pub rounds: usize,
    pub max_latency_samples: usize,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            scenario: Scenario::BroadcastAll,
            participants: 8,
            rounds: 100,
            max_latency_samples: 8_192,
        }
    }
}

/// Measured local result. CPU and memory are intentionally not guessed by this portable runner.
#[derive(Clone, Debug)]
pub struct Measurement {
    pub scenario: Scenario,
    pub participants: usize,
    pub attempted_publishes: usize,
    pub delivered_messages: usize,
    pub elapsed: Duration,
    pub p50_publish_latency: Option<Duration>,
    pub p95_publish_latency: Option<Duration>,
    pub p99_publish_latency: Option<Duration>,
    pub peak_pending_messages: usize,
    pub latest_replacements: usize,
    pub latest_drops: usize,
    pub best_effort_drops: usize,
    pub logical_cpus: Option<usize>,
    pub operating_system: &'static str,
    pub architecture: &'static str,
}

impl Measurement {
    /// Average attempted publishes per second for the measured interval.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn publishes_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds == 0.0 {
            0.0
        } else {
            self.attempted_publishes as f64 / seconds
        }
    }
}

/// Runner configuration or execution failure.
#[derive(Debug)]
pub enum LoadError {
    InvalidConfiguration(&'static str),
    Core(woven_core::CoreError),
    IdRange,
}

impl fmt::Display for LoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(reason) => {
                write!(formatter, "invalid load configuration: {reason}")
            }
            Self::Core(error) => write!(formatter, "core error: {error:?}"),
            Self::IdRange => formatter.write_str("participant count exceeds supported ID range"),
        }
    }
}

impl std::error::Error for LoadError {}
impl From<woven_core::CoreError> for LoadError {
    fn from(error: woven_core::CoreError) -> Self {
        Self::Core(error)
    }
}

struct Participant {
    connection: woven_core::ConnectionId,
    entity: EntityId,
}

/// Run a bounded local routing scenario and return measured outcomes.
#[allow(clippy::too_many_lines)]
pub fn run(config: LoadConfig) -> Result<Measurement, LoadError> {
    validate_config(config)?;
    let session = SessionKey::new(NAMESPACE, SESSION_ID);
    let mut authenticator = DevAuthenticator::with_capacity(config.participants)
        .map_err(|_| LoadError::InvalidConfiguration("participant count must be non-zero"))?;
    for index in 0..config.participants {
        let principal = PrincipalId::new(u64::try_from(index + 1).map_err(|_| LoadError::IdRange)?);
        authenticator
            .insert(
                token(index),
                AuthenticatedPrincipal::new(principal, grants(session)),
            )
            .map_err(|_| {
                LoadError::InvalidConfiguration("participant count exceeds authenticator capacity")
            })?;
    }
    let mut core = WovenCore::new(
        authenticator,
        CoreConfig {
            max_connections: config.participants,
            publish_rate_limit: PublishRateLimit {
                max_publishes: config.rounds,
                window: Duration::from_secs(60),
            },
            ..CoreConfig::default()
        },
    )?;
    core.register_channel(ChannelDefinition::relay_owned(
        STATE_CHANNEL,
        DeliveryClass::LatestValue,
        PersistenceClass::Stateful { ttl: None },
        PAYLOAD.len(),
    ))?;
    core.provision_session(session)?;
    core.install_space(session, descriptor(config.scenario))?;

    let mut participants = Vec::with_capacity(config.participants);
    for index in 0..config.participants {
        let connection = core.transport_connected()?;
        core.authenticate(connection, &Credentials::new(token(index)))?;
        core.join_session(connection, session)?;
        core.subscribe(connection, SpaceKey::new(session, SPACE_ID))?;
        let entity = core.spawn_entity(connection, SpaceKey::new(session, SPACE_ID), EPOCH)?;
        if let Some(position) = position(config.scenario, index)? {
            core.update_entity_position(connection, session, entity, position)?;
        }
        participants.push(Participant { connection, entity });
    }

    let started = Instant::now();
    let mut latency_samples = Vec::with_capacity(
        config
            .max_latency_samples
            .min(config.participants * config.rounds),
    );
    let mut delivered_messages = 0;
    let mut peak_pending_messages = 0;
    let mut latest_replacements = 0;
    let mut latest_drops = 0;
    let mut best_effort_drops = 0;
    let mut attempted_publishes = 0;
    for round in 1..=config.rounds {
        for participant in &participants {
            let before = Instant::now();
            let outcome = core.publish(PublishRequest {
                connection: participant.connection,
                session,
                space: SPACE_ID,
                space_epoch: EPOCH,
                entity: Some(participant.entity),
                channel: STATE_CHANNEL,
                sequence: u64::try_from(round).map_err(|_| LoadError::IdRange)?,
                delivery: DeliveryClass::LatestValue,
                persistence: PersistenceClass::Stateful { ttl: None },
                coalesce_key: Some(CoalesceKey::new(STATE_CHANNEL, Some(participant.entity), 1)),
                payload: PAYLOAD.to_vec(),
            })?;
            attempted_publishes += 1;
            if latency_samples.len() < config.max_latency_samples {
                latency_samples.push(before.elapsed());
            }
            latest_replacements += outcome.queues.replaced_latest;
            latest_drops += outcome.queues.dropped_latest;
            best_effort_drops += outcome.queues.dropped_best_effort;
        }
        for participant in &participants {
            let outbound = core.drain_outbound(participant.connection)?;
            peak_pending_messages = peak_pending_messages.max(outbound.len());
            delivered_messages += outbound.len();
        }
    }
    let elapsed = started.elapsed();
    latency_samples.sort_unstable();
    Ok(Measurement {
        scenario: config.scenario,
        participants: config.participants,
        attempted_publishes,
        delivered_messages,
        elapsed,
        p50_publish_latency: percentile(&latency_samples, 50),
        p95_publish_latency: percentile(&latency_samples, 95),
        p99_publish_latency: percentile(&latency_samples, 99),
        peak_pending_messages,
        latest_replacements,
        latest_drops,
        best_effort_drops,
        logical_cpus: std::thread::available_parallelism()
            .ok()
            .map(std::num::NonZeroUsize::get),
        operating_system: std::env::consts::OS,
        architecture: std::env::consts::ARCH,
    })
}

fn validate_config(config: LoadConfig) -> Result<(), LoadError> {
    if config.participants == 0 {
        return Err(LoadError::InvalidConfiguration(
            "participants must be non-zero",
        ));
    }
    if config.rounds == 0 {
        return Err(LoadError::InvalidConfiguration("rounds must be non-zero"));
    }
    if config.max_latency_samples == 0 {
        return Err(LoadError::InvalidConfiguration(
            "max latency samples must be non-zero",
        ));
    }
    Ok(())
}

fn grants(session: SessionKey) -> AuthorizationGrants {
    let mut grants = AuthorizationGrants::new();
    grants.grant_namespace(session.namespace, AccessGrant::ReadWrite);
    grants.grant_session(session, AccessGrant::ReadWrite);
    grants.grant_space(SpaceKey::new(session, SPACE_ID), AccessGrant::ReadWrite);
    grants.grant_channel(
        ChannelScope::new(session, STATE_CHANNEL),
        AccessGrant::ReadWrite,
    );
    grants
}

fn descriptor(scenario: Scenario) -> SpaceDescriptor {
    let (local_frame, routing) = match scenario {
        Scenario::BroadcastAll => (CoordinateFrame::Logical, RoutingPolicy::BroadcastAll),
        Scenario::TopicOnly => (CoordinateFrame::Logical, RoutingPolicy::TopicOnly),
        Scenario::SpatialGrid2D => (
            CoordinateFrame::Cartesian2D {
                meters_per_unit: 1.0,
            },
            RoutingPolicy::SpatialGrid2D {
                cell_size: 10.0,
                interest_radius: 15.0,
                exact_distance: true,
            },
        ),
        Scenario::SpatialGrid3D => (
            CoordinateFrame::Cartesian3D {
                meters_per_unit: 1.0,
            },
            RoutingPolicy::SpatialGrid3D {
                cell_size: 10.0,
                interest_radius: 15.0,
                exact_distance: true,
            },
        ),
    };
    SpaceDescriptor {
        id: SPACE_ID,
        local_frame,
        parent: None,
        epoch: EPOCH,
        routing,
    }
}

fn position(scenario: Scenario, index: usize) -> Result<Option<EntityPosition>, LoadError> {
    let index = f64::from(u32::try_from(index).map_err(|_| LoadError::IdRange)?);
    Ok(match scenario {
        Scenario::BroadcastAll | Scenario::TopicOnly => None,
        Scenario::SpatialGrid2D => Some(EntityPosition::Cartesian2D {
            x: index * 3.0,
            y: 0.0,
        }),
        Scenario::SpatialGrid3D => Some(EntityPosition::Cartesian3D {
            x: index * 3.0,
            y: 0.0,
            z: 0.0,
        }),
    })
}

fn percentile(samples: &[Duration], percentile: usize) -> Option<Duration> {
    if samples.is_empty() {
        return None;
    }
    let index = (samples.len() - 1) * percentile / 100;
    samples.get(index).copied()
}

fn token(index: usize) -> String {
    format!("participant-{index}")
}

#[cfg(test)]
mod tests {
    use super::{LoadConfig, Scenario, run};

    #[test]
    fn bounded_runner_exercises_all_routing_policies() {
        for scenario in [
            Scenario::BroadcastAll,
            Scenario::TopicOnly,
            Scenario::SpatialGrid2D,
            Scenario::SpatialGrid3D,
        ] {
            let measurement = run(LoadConfig {
                scenario,
                participants: 4,
                rounds: 3,
                max_latency_samples: 12,
            })
            .expect("scenario runs");
            assert_eq!(measurement.attempted_publishes, 12);
            assert!(measurement.delivered_messages > 0);
            assert!(measurement.p50_publish_latency.is_some());
        }
    }

    #[test]
    fn runner_supports_more_than_development_authenticator_capacity() {
        let measurement = run(LoadConfig {
            scenario: Scenario::SpatialGrid2D,
            participants: 65,
            rounds: 1,
            max_latency_samples: 65,
        })
        .expect("65 participants should run");

        assert_eq!(measurement.attempted_publishes, 65);
    }
}
