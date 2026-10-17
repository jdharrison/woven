//! Privileged local configuration checkpoint. This is not a client/wire protocol or a world save.
use super::{
    Authenticator, BTreeMap, CoordinateFrame, MAX_MANAGED_HISTORY, MAX_MANAGED_SCOPES,
    MAX_MANAGED_SPATIAL_SPACES, ManagedBounds3D, ManagedError, ManagedMutation, ManagedPoint3D,
    RoutingPolicy, Scope, SessionKey, SpaceDescriptor, SpaceEpoch, SpaceId, SpatialBounds3D,
    WovenCore, managed_publish_rate_limit, validate_managed_spatial_descriptor,
};
use serde::{Deserialize, Serialize, de};
use std::{collections::BTreeSet, fmt, marker::PhantomData};

pub const MANAGED_CHECKPOINT_VERSION: u32 = 1;
pub const MANAGED_MIGRATION_BASELINE_VERSION: u32 = 1;
pub const MAX_MANAGED_MIGRATION_ID_BYTES: usize = 128;

/// Explicitly approved lost-history boundary, not proof of exact restoration. The trusted
/// importer must confirm a fresh migration identity, a new serving lineage, and the exact
/// registry high-water mark. Core cannot verify those external facts or recover lost verifiers.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedMigrationBaseline {
    pub version: u32,
    #[serde(deserialize_with = "deserialize_migration_id")]
    pub migration_id: String,
    #[serde(deserialize_with = "deserialize_previous_lineage")]
    pub previous_lineage: String,
    pub closed_scope_through: u64,
    /// Must be explicitly true. This is a limitation, not an archive of retired credentials.
    pub unknown_retired_verifier_history: bool,
}

impl fmt::Debug for ManagedMigrationBaseline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedMigrationBaseline")
            .finish_non_exhaustive()
    }
}

impl ManagedMigrationBaseline {
    fn validate(&self) -> Result<(), ManagedError> {
        if self.version != MANAGED_MIGRATION_BASELINE_VERSION
            || !valid_migration_id(&self.migration_id)
            || !valid_previous_lineage(&self.previous_lineage)
            || self.closed_scope_through == 0
            || !self.unknown_retired_verifier_history
        {
            return Err(ManagedError::InvalidRequest);
        }
        Ok(())
    }
}

/// Trusted persistence boundary containing credential verifiers. Protect it like credentials;
/// never serve it through the public protocol, admin snapshots, logs, or diagnostics.
/// Collection bounds are enforced both during deserialization and again during restoration.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedCheckpoint {
    pub version: u32,
    /// Absent for exact checkpoints. Once established, provenance survives all later mutations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_baseline: Option<ManagedMigrationBaseline>,
    #[serde(deserialize_with = "deserialize_scopes")]
    pub scopes: Vec<ManagedCheckpointScope>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedCheckpointScope {
    pub namespace_id: u64,
    pub session_id: u64,
    pub revision: u64,
    pub last_mutation: ManagedCheckpointMutation,
    pub allocated_ccu: u32,
    pub tick_rate_hz: Option<u32>,
    /// Lowercase hex SHA-256 of the client token, never the token itself or the admin verifier.
    #[serde(deserialize_with = "deserialize_verifier")]
    pub verifier_sha256: String,
    pub retired: bool,
    #[serde(deserialize_with = "deserialize_spaces")]
    pub spatial_spaces: Vec<ManagedCheckpointSpace>,
}

// Do not derive Debug on either verifier-bearing DTO, including indirectly through scopes.
impl fmt::Debug for ManagedCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedCheckpoint")
            .field("version", &self.version)
            .field("scope_count", &self.scopes.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ManagedCheckpointScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedCheckpointScope")
            .field("namespace_id", &self.namespace_id)
            .field("session_id", &self.session_id)
            .field("revision", &self.revision)
            .field("retired", &self.retired)
            .finish_non_exhaustive()
    }
}

/// The original last request, not merely its resolved configuration (PATCH can omit tick rate).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ManagedCheckpointMutation {
    /// Current inventory is known, but the pre-migration last request is not recoverable.
    MigrationBaseline,
    Provision,
    Configuration {
        allocated_ccu: u32,
        tick_rate_hz: Option<u32>,
    },
    Space {
        space_id: u64,
    },
}

/// Exact managed spatial definition; system spaces are reconstructed rather than persisted.
/// Serde stores floats as IEEE-754 `u64` bit patterns, avoiding JSON decoder rounding.
/// `boundsBits` is `[min_x, min_y, min_z, max_x, max_y, max_z]` in the same bit encoding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedCheckpointSpace {
    pub space_id: u64,
    pub epoch: u64,
    #[serde(rename = "metersPerUnitBits", with = "float_bits")]
    pub meters_per_unit: f64,
    #[serde(rename = "cellSizeBits", with = "float_bits")]
    pub cell_size: f64,
    #[serde(rename = "interestRadiusBits", with = "float_bits")]
    pub interest_radius: f64,
    pub exact_distance: bool,
    #[serde(rename = "boundsBits", with = "bounds_bits")]
    pub bounds: ManagedBounds3D,
}

impl ManagedCheckpointSpace {
    fn from_descriptor(descriptor: &SpaceDescriptor) -> Result<Self, ManagedError> {
        validate_managed_spatial_descriptor(descriptor)?;
        let CoordinateFrame::Cartesian3D { meters_per_unit } = descriptor.local_frame else {
            return Err(ManagedError::InvalidRequest);
        };
        let RoutingPolicy::SpatialGrid3D {
            cell_size,
            interest_radius,
            exact_distance,
        } = descriptor.routing
        else {
            return Err(ManagedError::InvalidRequest);
        };
        let bounds = descriptor.bounds.ok_or(ManagedError::InvalidRequest)?;
        Ok(Self {
            space_id: descriptor.id.get(),
            epoch: descriptor.epoch.get(),
            meters_per_unit,
            cell_size,
            interest_radius,
            exact_distance,
            bounds: ManagedBounds3D {
                min: ManagedPoint3D {
                    x: bounds.min_x,
                    y: bounds.min_y,
                    z: bounds.min_z,
                },
                max: ManagedPoint3D {
                    x: bounds.max_x,
                    y: bounds.max_y,
                    z: bounds.max_z,
                },
            },
        })
    }

    fn descriptor(&self) -> SpaceDescriptor {
        SpaceDescriptor {
            id: SpaceId::new(self.space_id),
            epoch: SpaceEpoch::new(self.epoch),
            local_frame: CoordinateFrame::Cartesian3D {
                meters_per_unit: self.meters_per_unit,
            },
            bounds: Some(SpatialBounds3D {
                min_x: self.bounds.min.x,
                min_y: self.bounds.min.y,
                min_z: self.bounds.min.z,
                max_x: self.bounds.max.x,
                max_y: self.bounds.max.y,
                max_z: self.bounds.max.z,
            }),
            parent: None,
            routing: RoutingPolicy::SpatialGrid3D {
                cell_size: self.cell_size,
                interest_radius: self.interest_radius,
                exact_distance: self.exact_distance,
            },
        }
    }
}

impl From<ManagedMutation> for ManagedCheckpointMutation {
    fn from(mutation: ManagedMutation) -> Self {
        match mutation {
            ManagedMutation::MigrationBaseline => Self::MigrationBaseline,
            ManagedMutation::Provision => Self::Provision,
            ManagedMutation::Configuration {
                allocated_ccu,
                tick_rate_hz,
            } => Self::Configuration {
                allocated_ccu,
                tick_rate_hz,
            },
            ManagedMutation::Space(id) => Self::Space { space_id: id.get() },
        }
    }
}

impl ManagedCheckpointScope {
    fn into_scope(self, historical_baseline_scope: bool) -> Result<Scope, ManagedError> {
        if self.spatial_spaces.len() > MAX_MANAGED_SPATIAL_SPACES {
            return Err(ManagedError::SpaceCapacityExhausted);
        }
        managed_publish_rate_limit(self.tick_rate_hz)?;
        let verifier = decode_verifier(&self.verifier_sha256)?;
        let mut spatial_spaces = BTreeMap::new();
        for space in self.spatial_spaces {
            let descriptor = space.descriptor();
            validate_managed_spatial_descriptor(&descriptor)?;
            if spatial_spaces.insert(descriptor.id, descriptor).is_some() {
                return Err(ManagedError::InvalidRequest);
            }
        }
        // An approved baseline inventories current values without asserting a recoverable
        // pre-baseline mutation sequence. Exact (non-baseline) checkpoints retain that check.
        if !historical_baseline_scope && self.revision <= spatial_spaces.len() as u64 {
            return Err(ManagedError::InvalidRequest);
        }
        let last_mutation = match self.last_mutation {
            ManagedCheckpointMutation::MigrationBaseline => {
                if !historical_baseline_scope {
                    return Err(ManagedError::InvalidRequest);
                }
                ManagedMutation::MigrationBaseline
            }
            ManagedCheckpointMutation::Provision => {
                if !spatial_spaces.is_empty() {
                    return Err(ManagedError::InvalidRequest);
                }
                ManagedMutation::Provision
            }
            ManagedCheckpointMutation::Configuration {
                allocated_ccu,
                tick_rate_hz,
            } => {
                managed_publish_rate_limit(tick_rate_hz)?;
                if self.revision < 2
                    || (!historical_baseline_scope
                        && self.revision <= spatial_spaces.len() as u64 + 1)
                    || allocated_ccu != self.allocated_ccu
                    || tick_rate_hz.is_some_and(|hz| Some(hz) != self.tick_rate_hz)
                {
                    return Err(ManagedError::InvalidRequest);
                }
                ManagedMutation::Configuration {
                    allocated_ccu,
                    tick_rate_hz,
                }
            }
            ManagedCheckpointMutation::Space { space_id } => {
                let id = SpaceId::new(space_id);
                if self.revision < 2 || !spatial_spaces.contains_key(&id) {
                    return Err(ManagedError::InvalidRequest);
                }
                ManagedMutation::Space(id)
            }
        };
        Ok(Scope {
            verifier,
            revision: self.revision,
            last_mutation,
            allocated_ccu: self.allocated_ccu,
            tick_rate_hz: self.tick_rate_hz,
            spatial_spaces,
            retired: self.retired,
        })
    }
}

impl<A: Authenticator> WovenCore<A> {
    /// Exports only managed configuration and retired verifier tombstones, never live state.
    /// The caller owns encoding, file permissions, atomic replacement, and durability.
    pub fn export_managed_checkpoint(&self) -> Result<ManagedCheckpoint, ManagedError> {
        let state = self.managed.as_ref().ok_or(ManagedError::InvalidRequest)?;
        let scopes = state
            .scopes
            .iter()
            .map(|(key, scope)| {
                let verifier_sha256 = scope
                    .verifier
                    .iter()
                    .flat_map(|byte| {
                        const HEX: &[u8; 16] = b"0123456789abcdef";
                        [
                            char::from(HEX[(byte >> 4) as usize]),
                            char::from(HEX[(byte & 15) as usize]),
                        ]
                    })
                    .collect();
                Ok(ManagedCheckpointScope {
                    namespace_id: key.namespace.get(),
                    session_id: key.session.get(),
                    revision: scope.revision,
                    last_mutation: scope.last_mutation.into(),
                    allocated_ccu: scope.allocated_ccu,
                    tick_rate_hz: scope.tick_rate_hz,
                    verifier_sha256,
                    retired: scope.retired,
                    spatial_spaces: scope
                        .spatial_spaces
                        .values()
                        .map(ManagedCheckpointSpace::from_descriptor)
                        .collect::<Result<_, _>>()?,
                })
            })
            .collect::<Result<_, ManagedError>>()?;
        Ok(ManagedCheckpoint {
            version: MANAGED_CHECKPOINT_VERSION,
            migration_baseline: state.migration_baseline.clone(),
            scopes,
        })
    }

    /// Restores before starting any listeners or spawning a worker. Requires managed mode,
    /// explicitly registered channels, and a fresh core with no scope/connection history.
    /// Validates the entire checkpoint before publication; failure leaves the core empty.
    /// Occupancy, leases, queues, payloads and rate windows always start empty after a restart.
    pub fn restore_managed_checkpoint(
        &mut self,
        checkpoint: ManagedCheckpoint,
    ) -> Result<(), ManagedError> {
        let state = self.managed.as_ref().ok_or(ManagedError::InvalidRequest)?;
        if checkpoint.version != MANAGED_CHECKPOINT_VERSION
            || state.migration_baseline.is_some()
            || !state.scopes.is_empty()
            || !state.connections.is_empty()
            || !state.operations.is_empty()
            || !self.sessions.is_empty()
            || !self.connections.is_empty()
            || !self.admissions.is_empty()
            || !self.pending_admissions.is_empty()
            || !self.admission_leases.is_empty()
            || !self.admission_tickets.is_empty()
            || self.next_connection_id != 1
            || self.next_entity_id != 1
            || self.journal_outbox_len() != 0
            || self.channels.is_empty()
        {
            return Err(ManagedError::InvalidRequest);
        }
        if checkpoint.scopes.len() > MAX_MANAGED_HISTORY {
            return Err(ManagedError::CapacityExhausted);
        }
        if let Some(baseline) = &checkpoint.migration_baseline {
            baseline.validate()?;
        }
        let mut scopes = BTreeMap::new();
        let mut verifiers = BTreeSet::new();
        let mut active = 0;
        for entry in checkpoint.scopes {
            let key = SessionKey::new(entry.namespace_id.into(), entry.session_id.into());
            self.validate_managed_update(key, entry.revision, entry.allocated_ccu)?;
            let historical_baseline_scope = if let Some(baseline) = &checkpoint.migration_baseline {
                let old_namespace = key.namespace.get() <= baseline.closed_scope_through;
                let old_session = key.session.get() <= baseline.closed_scope_through;
                if old_namespace != old_session {
                    return Err(ManagedError::InvalidRequest);
                }
                old_namespace && old_session
            } else {
                false
            };
            let scope = entry.into_scope(historical_baseline_scope)?;
            if scope.verifier == state.admin_verifier || !verifiers.insert(scope.verifier) {
                return Err(ManagedError::TokenConflict);
            }
            if !scope.retired {
                active += 1;
                if active > MAX_MANAGED_SCOPES || active > self.config.max_sessions {
                    return Err(ManagedError::CapacityExhausted);
                }
            }
            if scopes.insert(key, scope).is_some() {
                return Err(ManagedError::InvalidRequest);
            }
        }

        // Construction uses the same validated provisioning path as a live PUT. Nothing is
        // published to managed authentication until every session and routing index exists.
        let result = (|| {
            for (key, scope) in scopes.iter().filter(|(_, scope)| !scope.retired) {
                self.provision_session(*key)
                    .map_err(|_| ManagedError::CapacityExhausted)?;
                self.configure_managed_session(
                    *key,
                    scope.revision,
                    scope.allocated_ccu,
                    managed_publish_rate_limit(scope.tick_rate_hz)?,
                )
                .map_err(|_| ManagedError::CapacityExhausted)?;
                for descriptor in scope.spatial_spaces.values() {
                    self.install_space(*key, descriptor.clone())
                        .map_err(|_| ManagedError::SpaceCapacityExhausted)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.sessions.clear();
            self.admissions.clear();
            return Err(error);
        }
        let state = self.managed.as_mut().ok_or(ManagedError::InvalidRequest)?;
        state.scopes = scopes;
        state.migration_baseline = checkpoint.migration_baseline;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChannelDefinition, ChannelId, CoreConfig, Credentials, DeliveryClass, DevAuthenticator,
        ManagedRequest, NamespaceId, PersistenceClass, SessionId,
    };
    use std::time::Instant;

    fn core() -> WovenCore<DevAuthenticator> {
        let mut core = WovenCore::new(DevAuthenticator::new(), CoreConfig::default()).unwrap();
        core.enable_managed(&Credentials::new(
            "construction-rollback-test-independent-admin",
        ))
        .unwrap();
        core.register_channel(ChannelDefinition::relay_owned(
            ChannelId::new(1),
            DeliveryClass::ReliableOrdered,
            PersistenceClass::Ephemeral,
            1024,
        ))
        .unwrap();
        core
    }

    #[test]
    fn restore_construction_failure_rolls_back_sessions_admissions_and_verifiers() {
        let mut source = core();
        for (id, token) in [(1, 'a'), (2, 'b')] {
            source
                .manage_at(
                    ManagedRequest::Put {
                        session: SessionKey::new(NamespaceId::new(1), SessionId::new(id)),
                        revision: 1,
                        allocated_ccu: 1,
                        tick_rate_hz: Some(60),
                        credentials: Credentials::new(token.to_string().repeat(64)),
                    },
                    Instant::now(),
                )
                .unwrap();
        }
        for (id, revision) in [(3, 2), (4, 3)] {
            source
                .manage_at(
                    ManagedRequest::PutSpace {
                        session: SessionKey::new(NamespaceId::new(1), SessionId::new(2)),
                        revision,
                        descriptor: SpaceDescriptor {
                            id: SpaceId::new(id),
                            epoch: SpaceEpoch::new(1),
                            local_frame: CoordinateFrame::Cartesian3D {
                                meters_per_unit: 1.0,
                            },
                            bounds: Some(SpatialBounds3D {
                                min_x: -10.0,
                                min_y: -10.0,
                                min_z: -10.0,
                                max_x: 10.0,
                                max_y: 10.0,
                                max_z: 10.0,
                            }),
                            parent: None,
                            routing: RoutingPolicy::SpatialGrid3D {
                                cell_size: 1.0,
                                interest_radius: 2.0,
                                exact_distance: true,
                            },
                        },
                    },
                    Instant::now(),
                )
                .unwrap();
        }
        let checkpoint = source.export_managed_checkpoint().unwrap();
        for (space_limit, error) in [
            (1, ManagedError::CapacityExhausted),
            (3, ManagedError::SpaceCapacityExhausted),
        ] {
            let mut target = core();
            let valid_config = target.config;
            // Public setup deliberately prevents construction-time capacity faults. Lower the
            // private limit only in this unit test to fail system-space setup, then spatial
            // installation after a preceding session and both admission controllers exist.
            target.config.max_spaces_per_session = space_limit;
            assert_eq!(
                target.restore_managed_checkpoint(checkpoint.clone()),
                Err(error)
            );
            assert_eq!(target.session_count(), 0);
            assert_eq!(target.connection_count(), 0);
            assert!(target.admissions.is_empty());
            assert!(target.pending_admissions.is_empty());
            assert!(target.admission_leases.is_empty());
            assert!(target.admission_tickets.is_empty());
            assert!(
                target
                    .export_managed_checkpoint()
                    .unwrap()
                    .scopes
                    .is_empty()
            );
            assert_eq!(target.channels.len(), 1);

            target.config = valid_config;
            target
                .restore_managed_checkpoint(checkpoint.clone())
                .unwrap();
            assert_eq!(target.session_count(), 2);
            assert_eq!(target.admissions.len(), 2);
            assert_eq!(target.export_managed_checkpoint().unwrap(), checkpoint);
            for admission in target.admissions.values() {
                let snapshot = admission.snapshot();
                assert_eq!(snapshot.active_ccu, 0);
                assert_eq!(snapshot.queue_depth, 0);
                assert_eq!(snapshot.offered_slots, 0);
                assert_eq!(snapshot.available_slots, 1);
            }
        }
    }
}

mod float_bits {
    use serde::{Deserialize, Deserializer, Serializer};

    #[allow(
        clippy::trivially_copy_pass_by_ref,
        reason = "Serde's with serializer requires a reference"
    )]
    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.to_bits())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        u64::deserialize(deserializer).map(f64::from_bits)
    }
}

mod bounds_bits {
    use super::{ManagedBounds3D, ManagedPoint3D};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        bounds: &ManagedBounds3D,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        [
            bounds.min.x,
            bounds.min.y,
            bounds.min.z,
            bounds.max.x,
            bounds.max.y,
            bounds.max.z,
        ]
        .map(f64::to_bits)
        .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ManagedBounds3D, D::Error> {
        let [min_x, min_y, min_z, max_x, max_y, max_z] =
            <[u64; 6]>::deserialize(deserializer)?.map(f64::from_bits);
        Ok(ManagedBounds3D {
            min: ManagedPoint3D {
                x: min_x,
                y: min_y,
                z: min_z,
            },
            max: ManagedPoint3D {
                x: max_x,
                y: max_y,
                z: max_z,
            },
        })
    }
}

fn valid_migration_id(value: &str) -> bool {
    (1..=MAX_MANAGED_MIGRATION_ID_BYTES).contains(&value.len())
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_previous_lineage(value: &str) -> bool {
    value.len() == 48
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn deserialize_migration_id<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    deserialize_baseline_text(deserializer, valid_migration_id)
}

fn deserialize_previous_lineage<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    deserialize_baseline_text(deserializer, valid_previous_lineage)
}

fn deserialize_baseline_text<'de, D: de::Deserializer<'de>>(
    deserializer: D,
    validate: fn(&str) -> bool,
) -> Result<String, D::Error> {
    struct BaselineTextVisitor(fn(&str) -> bool);
    impl de::Visitor<'_> for BaselineTextVisitor {
        type Value = String;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bounded valid managed migration metadata")
        }
        fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
            if !(self.0)(value) {
                return Err(E::custom("invalid managed migration metadata"));
            }
            Ok(value.to_owned())
        }
    }
    deserializer.deserialize_str(BaselineTextVisitor(validate))
}

fn decode_verifier(value: &str) -> Result<[u8; 32], ManagedError> {
    if value.len() != 64 {
        return Err(ManagedError::InvalidRequest);
    }
    let mut verifier = [0; 32];
    for (byte, pair) in verifier.iter_mut().zip(value.as_bytes().as_chunks::<2>().0) {
        let nibble = |value| match value {
            b'0'..=b'9' => Ok(value - b'0'),
            b'a'..=b'f' => Ok(value - b'a' + 10),
            _ => Err(ManagedError::InvalidRequest),
        };
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    if verifier == [0; 32] {
        return Err(ManagedError::InvalidRequest);
    }
    Ok(verifier)
}

fn deserialize_verifier<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    struct VerifierVisitor;
    impl de::Visitor<'_> for VerifierVisitor {
        type Value = String;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a nonzero lowercase hex SHA-256 verifier")
        }
        fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
            decode_verifier(value).map_err(|_| E::custom("invalid managed verifier"))?;
            Ok(value.to_owned())
        }
    }
    deserializer.deserialize_str(VerifierVisitor)
}

fn deserialize_scopes<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ManagedCheckpointScope>, D::Error> {
    deserialize_bounded::<D, _, MAX_MANAGED_HISTORY>(deserializer)
}

fn deserialize_spaces<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ManagedCheckpointSpace>, D::Error> {
    deserialize_bounded::<D, _, MAX_MANAGED_SPATIAL_SPACES>(deserializer)
}

fn deserialize_bounded<'de, D, T, const N: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: de::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVisitor<T, const N: usize>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> de::Visitor<'de> for BoundedVisitor<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a managed checkpoint list with at most {N} entries")
        }
        fn visit_seq<S: de::SeqAccess<'de>>(self, mut seq: S) -> Result<Vec<T>, S::Error> {
            if seq.size_hint().is_some_and(|size| size > N) {
                return Err(de::Error::custom("managed checkpoint list exceeds limit"));
            }
            let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(N));
            while let Some(value) = seq.next_element()? {
                if values.len() == N {
                    return Err(de::Error::custom("managed checkpoint list exceeds limit"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(BoundedVisitor::<T, N>(PhantomData))
}
