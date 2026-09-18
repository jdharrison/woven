//! Single-owner managed scope configuration and credential verification.
use super::{
    AccessGrant, AdmissionMetadata, AuthError, AuthenticatedPrincipal, Authenticator, BTreeMap,
    CapacityUpdate, ChannelId, ChannelScope, ConnectionId, CoreError, Credentials, Duration,
    IdempotencyKey, Instant, JoinDecision, PrincipalId, QueuePolicy, RoutingPolicy, SessionKey,
    SpaceDescriptor, SpaceEpoch, SpaceId, SpaceKey, WovenCore, validate_session_key,
};
use crate::{AuthorizationGrants, CoordinateFrame, NodeId};

pub const MAX_MANAGED_SCOPES: usize = 1024;
pub const MAX_MANAGED_HISTORY: usize = 4096;
pub const MAX_MANAGED_CCU: u32 = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedError {
    InvalidRequest,
    ScopeNotFound,
    RevisionConflict,
    ScopeRetired,
    TokenConflict,
    CapacityExhausted,
    WorkerUnavailable,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ManagedRequest {
    Put {
        session: SessionKey,
        revision: u64,
        allocated_ccu: u32,
        credentials: Credentials,
    },
    Get {
        session: SessionKey,
    },
    Patch {
        session: SessionKey,
        revision: u64,
        allocated_ccu: u32,
    },
    Delete {
        session: SessionKey,
        revision: u64,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ManagedOutcome {
    Snapshot {
        created: bool,
        snapshot: ManagedSnapshot,
    },
    Deleted {
        connections: Vec<ConnectionId>,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSnapshot {
    pub namespace_id: String,
    pub session_id: String,
    pub revision: String,
    #[serde(rename = "allocatedCCU")]
    pub allocated_ccu: u32,
    pub admission: ManagedAdmissionSnapshot,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedAdmissionSnapshot {
    #[serde(rename = "effectiveAllocatedCCU")]
    pub effective_allocated_ccu: u32,
    pub pending_target: Option<u32>,
    #[serde(rename = "activeCCU")]
    pub active_ccu: u32,
    pub offered_slots: u32,
    pub queue_depth: usize,
    pub available_slots: u32,
}

fn digest(token: &str) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, token.as_bytes()).as_ref());
    bytes
}

fn client_digest(credentials: &Credentials) -> Result<[u8; 32], ManagedError> {
    let token = credentials.token();
    if token.len() != 64
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ManagedError::InvalidRequest);
    }
    Ok(digest(token))
}

struct Scope {
    verifier: [u8; 32],
    revision: u64,
    allocated_ccu: u32,
    retired: bool,
}

pub(super) struct ManagedState {
    admin_verifier: [u8; 32],
    scopes: BTreeMap<SessionKey, Scope>,
    pub(super) connections: BTreeMap<ConnectionId, SessionKey>,
    pub(super) operations: BTreeMap<ConnectionId, (Instant, u8)>,
}

impl ManagedState {
    pub(super) fn authenticate(
        &mut self,
        connection: ConnectionId,
        credentials: &Credentials,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        let verifier = client_digest(credentials).map_err(|_| AuthError::InvalidCredentials)?;
        let session = self
            .scopes
            .iter()
            .find_map(|(key, scope)| (!scope.retired && scope.verifier == verifier).then_some(*key))
            .ok_or(AuthError::InvalidCredentials)?;
        let mut grants = AuthorizationGrants::new();
        grants.grant_namespace(session.namespace, AccessGrant::ReadWrite);
        grants.grant_session(session, AccessGrant::ReadWrite);
        for id in 1..=2 {
            grants.grant_space(
                SpaceKey::new(session, SpaceId::new(id)),
                AccessGrant::ReadWrite,
            );
        }
        grants.grant_channel(
            ChannelScope::new(session, ChannelId::new(1)),
            AccessGrant::ReadWrite,
        );
        self.connections.insert(connection, session);
        self.operations.insert(connection, (Instant::now(), 0));
        Ok(AuthenticatedPrincipal::new(
            PrincipalId::new(connection.get()),
            grants,
        ))
    }
}

impl<A: Authenticator> WovenCore<A> {
    pub(super) fn check_managed_admission_rate_at(
        &mut self,
        connection: ConnectionId,
        now: Instant,
    ) -> Result<(), CoreError> {
        let Some((start, count)) = self
            .managed
            .as_mut()
            .and_then(|state| state.operations.get_mut(&connection))
        else {
            return Ok(());
        };
        let elapsed = now.saturating_duration_since(*start);
        if elapsed >= Duration::from_secs(1) {
            *start = now;
            *count = 0;
        }
        if *count >= 4 {
            return Err(CoreError::AdmissionRateLimited {
                retry_after: Duration::from_secs(1).saturating_sub(elapsed),
            });
        }
        *count += 1;
        Ok(())
    }
    /// Enables fail-closed managed authentication on an empty core. The fallback authenticator
    /// is never consulted in this mode. Only a digest of the independent admin credential is kept.
    pub fn enable_managed(&mut self, admin: &Credentials) -> Result<(), ManagedError> {
        if self.managed.is_some()
            || !self.connections.is_empty()
            || !self.sessions.is_empty()
            || !(32..=4096).contains(&admin.token().len())
            || !admin.token().bytes().all(|b| b.is_ascii_graphic())
            || self.config.max_spaces_per_session < 2
            || self.config.max_channels < 1
        {
            return Err(ManagedError::InvalidRequest);
        }
        self.managed = Some(ManagedState {
            admin_verifier: digest(admin.token()),
            scopes: BTreeMap::new(),
            connections: BTreeMap::new(),
            operations: BTreeMap::new(),
        });
        Ok(())
    }

    /// All provisioning, admission configuration, verifier publication and teardown happen in
    /// this single synchronous turn. HTTP and data-plane callers share the owning worker.
    pub fn manage_at(
        &mut self,
        request: ManagedRequest,
        now: Instant,
    ) -> Result<ManagedOutcome, ManagedError> {
        match request {
            ManagedRequest::Put {
                session,
                revision,
                allocated_ccu,
                credentials,
            } => self.managed_put(session, revision, allocated_ccu, &credentials, now),
            ManagedRequest::Get { session } => {
                self.validate_managed_update(session, 1, 0)?;
                self.managed_snapshot(session, false, now)
            }
            ManagedRequest::Patch {
                session,
                revision,
                allocated_ccu,
            } => {
                self.validate_managed_update(session, revision, allocated_ccu)?;
                let scope = self
                    .managed
                    .as_ref()
                    .and_then(|state| state.scopes.get(&session))
                    .ok_or(ManagedError::ScopeNotFound)?;
                if scope.retired {
                    return Err(ManagedError::ScopeRetired);
                }
                if revision < scope.revision
                    || (revision == scope.revision && allocated_ccu != scope.allocated_ccu)
                {
                    return Err(ManagedError::RevisionConflict);
                }
                self.apply_session_capacity_at(
                    session,
                    CapacityUpdate {
                        allocated_ccu,
                        revision,
                    },
                    now,
                )
                .map_err(|_| ManagedError::ScopeNotFound)?;
                let scope = self
                    .managed
                    .as_mut()
                    .and_then(|state| state.scopes.get_mut(&session))
                    .ok_or(ManagedError::ScopeNotFound)?;
                scope.revision = revision;
                scope.allocated_ccu = allocated_ccu;
                self.managed_snapshot(session, false, now)
            }
            ManagedRequest::Delete { session, revision } => {
                self.validate_managed_update(session, revision, 0)?;
                let state = self.managed.as_mut().ok_or(ManagedError::InvalidRequest)?;
                let scope = state
                    .scopes
                    .get_mut(&session)
                    .ok_or(ManagedError::ScopeNotFound)?;
                if revision != scope.revision {
                    return Err(ManagedError::RevisionConflict);
                }
                scope.retired = true;
                let connections = state
                    .connections
                    .iter()
                    .filter_map(|(id, key)| (*key == session).then_some(*id))
                    .collect::<Vec<_>>();
                for connection in &connections {
                    let _ = self.transport_lost(*connection);
                }
                self.admissions.remove(&session);
                self.sessions.remove(&session);
                self.pending_admissions
                    .retain(|(_, key), _| *key != session);
                self.admission_leases.retain(|(_, key), _| *key != session);
                self.admission_tickets.retain(|(_, key), _| *key != session);
                Ok(ManagedOutcome::Deleted { connections })
            }
        }
    }

    fn validate_managed_update(
        &self,
        session: SessionKey,
        revision: u64,
        allocated_ccu: u32,
    ) -> Result<(), ManagedError> {
        if self.managed.is_none()
            || validate_session_key(session).is_err()
            || revision == 0
            || allocated_ccu > MAX_MANAGED_CCU
            || u64::from(allocated_ccu) > self.config.max_connections as u64
        {
            return Err(ManagedError::InvalidRequest);
        }
        Ok(())
    }

    fn managed_put(
        &mut self,
        session: SessionKey,
        revision: u64,
        allocated_ccu: u32,
        credentials: &Credentials,
        now: Instant,
    ) -> Result<ManagedOutcome, ManagedError> {
        self.validate_managed_update(session, revision, allocated_ccu)?;
        let verifier = client_digest(credentials)?;
        let state = self.managed.as_ref().ok_or(ManagedError::InvalidRequest)?;
        if let Some(scope) = state.scopes.get(&session) {
            if scope.retired {
                return Err(ManagedError::ScopeRetired);
            }
            if scope.revision != revision
                || scope.allocated_ccu != allocated_ccu
                || scope.verifier != verifier
            {
                return Err(ManagedError::RevisionConflict);
            }
            return self.managed_snapshot(session, false, now);
        }
        if verifier == state.admin_verifier
            || state
                .scopes
                .values()
                .any(|scope| scope.verifier == verifier)
        {
            return Err(ManagedError::TokenConflict);
        }
        // Reserve deletion history at creation so DELETE never fails for lack of a tombstone slot.
        if state.scopes.len() >= MAX_MANAGED_HISTORY || self.sessions.len() >= MAX_MANAGED_SCOPES {
            return Err(ManagedError::CapacityExhausted);
        }
        self.provision_session(session)
            .map_err(|_| ManagedError::CapacityExhausted)?;
        let result = (|| {
            for id in 1..=2 {
                self.install_space(
                    session,
                    SpaceDescriptor {
                        id: SpaceId::new(id),
                        local_frame: CoordinateFrame::Logical,
                        parent: None,
                        epoch: SpaceEpoch::new(1),
                        routing: RoutingPolicy::BroadcastAll,
                    },
                )?;
            }
            self.configure_session_admission(
                session,
                AdmissionMetadata {
                    node_id: NodeId::new(1),
                    session,
                },
                QueuePolicy {
                    reconnect_grace: Duration::ZERO,
                    ..QueuePolicy::default()
                },
                CapacityUpdate {
                    allocated_ccu,
                    revision,
                },
            )
        })();
        if result.is_err() {
            self.sessions.remove(&session);
            self.admissions.remove(&session);
            return Err(ManagedError::CapacityExhausted);
        }
        self.managed
            .as_mut()
            .ok_or(ManagedError::InvalidRequest)?
            .scopes
            .insert(
                session,
                Scope {
                    verifier,
                    revision,
                    allocated_ccu,
                    retired: false,
                },
            );
        self.managed_snapshot(session, true, now)
    }

    fn managed_snapshot(
        &mut self,
        session: SessionKey,
        created: bool,
        now: Instant,
    ) -> Result<ManagedOutcome, ManagedError> {
        let scope = self
            .managed
            .as_ref()
            .and_then(|state| state.scopes.get(&session))
            .filter(|scope| !scope.retired)
            .ok_or(ManagedError::ScopeNotFound)?;
        let controller = self
            .admissions
            .get_mut(&session)
            .ok_or(ManagedError::ScopeNotFound)?;
        controller.maintain_at(now);
        let admission = controller.snapshot();
        Ok(ManagedOutcome::Snapshot {
            created,
            snapshot: ManagedSnapshot {
                namespace_id: session.namespace.to_string(),
                session_id: session.session.to_string(),
                revision: scope.revision.to_string(),
                allocated_ccu: scope.allocated_ccu,
                admission: ManagedAdmissionSnapshot {
                    effective_allocated_ccu: admission.allocated_ccu,
                    pending_target: admission.pending_target,
                    active_ccu: admission.active_ccu,
                    offered_slots: admission.offered_slots,
                    queue_depth: admission.queue_depth,
                    available_slots: admission.available_slots,
                },
            },
        })
    }

    /// Direct admission and membership binding are one worker turn. Existing explicit lease
    /// joins remain idempotent for bridges that still issue the legacy second command.
    pub fn admit_and_join_session_at(
        &mut self,
        connection: ConnectionId,
        session: SessionKey,
        key: IdempotencyKey,
        now: Instant,
    ) -> Result<JoinDecision, CoreError> {
        let decision = self.request_session_admission_at(connection, session, key, now)?;
        if let JoinDecision::Admitted(lease) = decision
            && let Err(error) = self.join_session_with_admission(connection, session, lease)
        {
            self.pending_admissions.remove(&(connection, session));
            if let Some(controller) = self.admissions.get_mut(&session) {
                controller.release_at(lease, crate::ReleaseReason::Intentional, now);
            }
            return Err(error);
        }
        Ok(decision)
    }
}
