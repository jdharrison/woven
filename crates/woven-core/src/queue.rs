use std::collections::{BTreeMap, VecDeque};

use crate::{OutboundMessage, ScopedCoalesceKey};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboundQueueConfig {
    pub total_capacity: usize,
    pub critical_capacity: usize,
    pub latest_capacity: usize,
    pub best_effort_capacity: usize,
}

impl Default for OutboundQueueConfig {
    fn default() -> Self {
        Self {
            total_capacity: 512,
            critical_capacity: 256,
            latest_capacity: 512,
            best_effort_capacity: 128,
        }
    }
}

impl OutboundQueueConfig {
    pub fn validate(self) -> Result<Self, QueueConfigError> {
        if self.total_capacity == 0 {
            return Err(QueueConfigError::ZeroTotalCapacity);
        }
        if self.critical_capacity == 0 {
            return Err(QueueConfigError::ZeroCriticalCapacity);
        }
        if self.critical_capacity > self.total_capacity
            || self.latest_capacity > self.total_capacity
            || self.best_effort_capacity > self.total_capacity
        {
            return Err(QueueConfigError::ClassCapacityExceedsTotal);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueConfigError {
    ZeroTotalCapacity,
    ZeroCriticalCapacity,
    ClassCapacityExceedsTotal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueError {
    MissingCoalesceKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueEviction {
    Latest { key: ScopedCoalesceKey },
    BestEffort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueuePush {
    Queued,
    QueuedCriticalAfterEviction(QueueEviction),
    ReplacedLatest,
    EvictedLatest { key: ScopedCoalesceKey },
    EvictedBestEffortForLatest,
    DroppedLatest,
    DroppedBestEffort,
    CriticalCapacityExhausted,
}

#[derive(Debug)]
pub struct OutboundQueue {
    config: OutboundQueueConfig,
    critical: VecDeque<OutboundMessage>,
    latest_order: VecDeque<ScopedCoalesceKey>,
    latest: BTreeMap<ScopedCoalesceKey, OutboundMessage>,
    best_effort: VecDeque<OutboundMessage>,
    critical_capacity_exhausted: bool,
}

impl OutboundQueue {
    pub fn new(config: OutboundQueueConfig) -> Result<Self, QueueConfigError> {
        let config = config.validate()?;
        Ok(Self {
            config,
            critical: VecDeque::with_capacity(config.critical_capacity),
            latest_order: VecDeque::with_capacity(config.latest_capacity),
            latest: BTreeMap::new(),
            best_effort: VecDeque::with_capacity(config.best_effort_capacity),
            critical_capacity_exhausted: false,
        })
    }

    pub fn push(&mut self, message: OutboundMessage) -> Result<QueuePush, QueueError> {
        if message.delivery.is_critical() {
            return Ok(self.push_critical(message));
        }
        if message.delivery.is_replaceable() {
            return self.push_latest(message);
        }
        Ok(self.push_best_effort(message))
    }

    fn push_critical(&mut self, message: OutboundMessage) -> QueuePush {
        if self.critical.len() == self.config.critical_capacity {
            self.critical_capacity_exhausted = true;
            return QueuePush::CriticalCapacityExhausted;
        }

        let eviction = if self.len() == self.config.total_capacity {
            self.evict_oldest_latest()
                .map(|key| QueueEviction::Latest { key })
                .or_else(|| {
                    self.best_effort
                        .pop_front()
                        .map(|_| QueueEviction::BestEffort)
                })
        } else {
            None
        };
        self.critical.push_back(message);
        eviction.map_or(QueuePush::Queued, QueuePush::QueuedCriticalAfterEviction)
    }

    fn push_latest(&mut self, message: OutboundMessage) -> Result<QueuePush, QueueError> {
        let key = message
            .scoped_coalesce_key()
            .ok_or(QueueError::MissingCoalesceKey)?;
        if let Some(current) = self.latest.get_mut(&key) {
            *current = message;
            self.latest_order.retain(|ordered| *ordered != key);
            self.latest_order.push_back(key);
            return Ok(QueuePush::ReplacedLatest);
        }
        if self.config.latest_capacity == 0 {
            return Ok(QueuePush::DroppedLatest);
        }
        if self.latest.len() == self.config.latest_capacity {
            let evicted = self
                .evict_oldest_latest()
                .expect("latest order and values remain synchronized");
            self.insert_latest(key, message);
            return Ok(QueuePush::EvictedLatest { key: evicted });
        }
        if self.len() == self.config.total_capacity {
            if let Some(evicted) = self.evict_oldest_latest() {
                self.insert_latest(key, message);
                return Ok(QueuePush::EvictedLatest { key: evicted });
            }
            if self.best_effort.pop_front().is_some() {
                self.insert_latest(key, message);
                return Ok(QueuePush::EvictedBestEffortForLatest);
            }
            return Ok(QueuePush::DroppedLatest);
        }
        self.insert_latest(key, message);
        Ok(QueuePush::Queued)
    }

    fn insert_latest(&mut self, key: ScopedCoalesceKey, message: OutboundMessage) {
        self.latest_order.push_back(key);
        self.latest.insert(key, message);
    }

    fn evict_oldest_latest(&mut self) -> Option<ScopedCoalesceKey> {
        let key = self.latest_order.pop_front()?;
        self.latest.remove(&key);
        Some(key)
    }

    fn push_best_effort(&mut self, message: OutboundMessage) -> QueuePush {
        if self.best_effort.len() == self.config.best_effort_capacity
            || self.len() == self.config.total_capacity
        {
            return QueuePush::DroppedBestEffort;
        }
        self.best_effort.push_back(message);
        QueuePush::Queued
    }

    pub fn pop(&mut self) -> Option<OutboundMessage> {
        if let Some(message) = self.critical.pop_front() {
            return Some(message);
        }
        if let Some(key) = self.latest_order.pop_front() {
            return self.latest.remove(&key);
        }
        self.best_effort.pop_front()
    }

    pub fn purge(&mut self, mut should_remove: impl FnMut(&OutboundMessage) -> bool) -> usize {
        let before = self.len();
        self.critical.retain(|message| !should_remove(message));
        self.best_effort.retain(|message| !should_remove(message));
        self.latest.retain(|_, message| !should_remove(message));
        self.latest_order
            .retain(|key| self.latest.contains_key(key));
        before.saturating_sub(self.len())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.critical.len() + self.latest.len() + self.best_effort.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.config.total_capacity
    }

    #[must_use]
    pub const fn is_slow_consumer(&self) -> bool {
        self.critical_capacity_exhausted
    }

    /// Drain eligible messages without moving deferred messages out of the bounded queue.
    pub(crate) fn drain_matching(
        &mut self,
        mut eligible: impl FnMut(&OutboundMessage) -> bool,
    ) -> Vec<OutboundMessage> {
        let mut messages = Vec::with_capacity(self.len());
        for _ in 0..self.critical.len() {
            let message = self
                .critical
                .pop_front()
                .expect("critical length is unchanged by rotation");
            if eligible(&message) {
                messages.push(message);
            } else {
                self.critical.push_back(message);
            }
        }
        for _ in 0..self.latest_order.len() {
            let key = self
                .latest_order
                .pop_front()
                .expect("latest order length is unchanged by rotation");
            let message = self
                .latest
                .get(&key)
                .expect("latest order and values remain synchronized");
            if eligible(message) {
                messages.push(
                    self.latest
                        .remove(&key)
                        .expect("eligible latest value exists"),
                );
            } else {
                self.latest_order.push_back(key);
            }
        }
        for _ in 0..self.best_effort.len() {
            let message = self
                .best_effort
                .pop_front()
                .expect("best-effort length is unchanged by rotation");
            if eligible(&message) {
                messages.push(message);
            } else {
                self.best_effort.push_back(message);
            }
        }
        messages
    }

    pub fn drain(&mut self) -> Vec<OutboundMessage> {
        let mut messages = Vec::with_capacity(self.len());
        while let Some(message) = self.pop() {
            messages.push(message);
        }
        messages
    }
}
