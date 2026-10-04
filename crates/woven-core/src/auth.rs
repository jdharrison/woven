use std::collections::BTreeMap;
use std::fmt;

use crate::{ChannelId, NamespaceId, PrincipalId, SessionKey, SpaceKey};

#[derive(Clone, Eq, PartialEq)]
pub struct Credentials {
    token: String,
}

impl Credentials {
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("token", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessGrant {
    Read,
    Write,
    ReadWrite,
}

impl AccessGrant {
    #[must_use]
    pub const fn permits_read(self) -> bool {
        matches!(self, Self::Read | Self::ReadWrite)
    }

    #[must_use]
    pub const fn permits_write(self) -> bool {
        matches!(self, Self::Write | Self::ReadWrite)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChannelScope {
    pub session: SessionKey,
    pub channel: ChannelId,
}

impl ChannelScope {
    #[must_use]
    pub const fn new(session: SessionKey, channel: ChannelId) -> Self {
        Self { session, channel }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuthorizationGrants {
    namespaces: BTreeMap<NamespaceId, AccessGrant>,
    sessions: BTreeMap<SessionKey, AccessGrant>,
    spaces: BTreeMap<SpaceKey, AccessGrant>,
    channels: BTreeMap<ChannelScope, AccessGrant>,
}

impl AuthorizationGrants {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            namespaces: BTreeMap::new(),
            sessions: BTreeMap::new(),
            spaces: BTreeMap::new(),
            channels: BTreeMap::new(),
        }
    }

    pub fn grant_namespace(&mut self, namespace: NamespaceId, access: AccessGrant) {
        self.namespaces.insert(namespace, access);
    }

    pub fn grant_session(&mut self, session: SessionKey, access: AccessGrant) {
        self.sessions.insert(session, access);
    }

    pub fn grant_space(&mut self, space: SpaceKey, access: AccessGrant) {
        self.spaces.insert(space, access);
    }

    pub fn grant_channel(&mut self, channel: ChannelScope, access: AccessGrant) {
        self.channels.insert(channel, access);
    }

    #[must_use]
    pub fn can_read_namespace(&self, namespace: NamespaceId) -> bool {
        self.namespaces
            .get(&namespace)
            .is_some_and(|access| access.permits_read())
    }

    #[must_use]
    pub fn can_write_namespace(&self, namespace: NamespaceId) -> bool {
        self.namespaces
            .get(&namespace)
            .is_some_and(|access| access.permits_write())
    }

    #[must_use]
    pub fn can_read_session(&self, session: SessionKey) -> bool {
        self.sessions
            .get(&session)
            .is_some_and(|access| access.permits_read())
    }

    #[must_use]
    pub fn can_write_session(&self, session: SessionKey) -> bool {
        self.sessions
            .get(&session)
            .is_some_and(|access| access.permits_write())
    }

    #[must_use]
    pub fn can_read_space(&self, space: SpaceKey) -> bool {
        self.spaces
            .get(&space)
            .is_some_and(|access| access.permits_read())
    }

    #[must_use]
    pub fn can_write_space(&self, space: SpaceKey) -> bool {
        self.spaces
            .get(&space)
            .is_some_and(|access| access.permits_write())
    }

    #[must_use]
    pub fn can_read_channel(&self, channel: ChannelScope) -> bool {
        self.channels
            .get(&channel)
            .is_some_and(|access| access.permits_read())
    }

    #[must_use]
    pub fn can_write_channel(&self, channel: ChannelScope) -> bool {
        self.channels
            .get(&channel)
            .is_some_and(|access| access.permits_write())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedPrincipal {
    pub principal_id: PrincipalId,
    grants: AuthorizationGrants,
}

impl AuthenticatedPrincipal {
    #[must_use]
    pub const fn new(principal_id: PrincipalId, grants: AuthorizationGrants) -> Self {
        Self {
            principal_id,
            grants,
        }
    }

    #[must_use]
    pub const fn grants(&self) -> &AuthorizationGrants {
        &self.grants
    }

    pub(crate) const fn grants_mut(&mut self) -> &mut AuthorizationGrants {
        &mut self.grants
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthError {
    InvalidCredentials,
}

pub trait Authenticator {
    fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedPrincipal, AuthError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevAuthenticatorError {
    ZeroCapacity,
    CapacityReached,
}

#[derive(Clone, Debug)]
pub struct DevAuthenticator {
    max_identities: usize,
    identities: BTreeMap<String, AuthenticatedPrincipal>,
}

impl Default for DevAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl DevAuthenticator {
    pub const DEFAULT_MAX_IDENTITIES: usize = 64;

    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_identities: Self::DEFAULT_MAX_IDENTITIES,
            identities: BTreeMap::new(),
        }
    }

    pub fn with_capacity(max_identities: usize) -> Result<Self, DevAuthenticatorError> {
        if max_identities == 0 {
            return Err(DevAuthenticatorError::ZeroCapacity);
        }
        Ok(Self {
            max_identities,
            identities: BTreeMap::new(),
        })
    }

    pub fn insert(
        &mut self,
        token: impl Into<String>,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<AuthenticatedPrincipal>, DevAuthenticatorError> {
        let token = token.into();
        if !self.identities.contains_key(&token) && self.identities.len() == self.max_identities {
            return Err(DevAuthenticatorError::CapacityReached);
        }
        Ok(self.identities.insert(token, principal))
    }
}

impl Authenticator for DevAuthenticator {
    fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedPrincipal, AuthError> {
        self.identities
            .get(credentials.token())
            .cloned()
            .ok_or(AuthError::InvalidCredentials)
    }
}
