use std::collections::VecDeque;
use std::future::{Future, Ready, ready};

use crate::OutboundMessage;

#[derive(Clone, Debug, PartialEq)]
pub struct JournalRecord {
    pub message: OutboundMessage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalError {
    pub message: String,
}

pub trait JournalSink {
    type AppendFuture<'a>: Future<Output = Result<(), JournalError>> + Send + 'a
    where
        Self: 'a;

    fn append(&self, record: JournalRecord) -> Self::AppendFuture<'_>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopJournalSink;

impl JournalSink for NoopJournalSink {
    type AppendFuture<'a> = Ready<Result<(), JournalError>>;

    fn append(&self, _record: JournalRecord) -> Self::AppendFuture<'_> {
        ready(Ok(()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalOutboxError {
    Full,
}

#[derive(Debug)]
pub struct JournalOutbox {
    capacity: usize,
    records: VecDeque<JournalRecord>,
}

impl JournalOutbox {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            records: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, record: JournalRecord) -> Result<(), JournalOutboxError> {
        if self.records.len() == self.capacity {
            return Err(JournalOutboxError::Full);
        }
        self.records.push_back(record);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<JournalRecord> {
        self.records.pop_front()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    #[must_use]
    pub fn remaining_capacity(&self) -> usize {
        self.capacity.saturating_sub(self.records.len())
    }
}
