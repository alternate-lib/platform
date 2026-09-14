use std::{collections::BTreeMap, future::Future, time::Duration};

use jiff::Timestamp;

#[cfg(test)]
mod fake;
#[cfg(feature = "postgres")]
pub mod postgres;

pub trait QueueProducer {
    type MessageId;
    type Error: std::error::Error;

    fn send(
        &self,
        message: QueueMessage,
    ) -> impl Future<Output = Result<Self::MessageId, Self::Error>> + Send;
}

pub trait QueueConsumer {
    type Receipt;
    type Error: std::error::Error;

    fn receive(
        &self,
    ) -> impl Future<Output = Result<Option<QueueDelivery<Self::Receipt>>, Self::Error>> + Send;

    fn ack(&self, receipt: Self::Receipt) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    pub id: Option<String>,
    pub payload: Vec<u8>,
    pub attributes: BTreeMap<String, String>,
}

impl QueueMessage {
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id: None,
            payload: payload.into(),
            attributes: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct QueueDelivery<R> {
    pub id: String,
    pub payload: Vec<u8>,
    pub attempts: usize,
    pub attributes: BTreeMap<String, String>,
    pub receipt: R,
}

pub trait QueueLeaseReclaim: QueueConsumer {
    fn reclaim(&self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait QueueLeaseRenewal: QueueConsumer {
    fn renew(
        &self,
        receipt: &Self::Receipt,
        visibility: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait QueueRejection: QueueConsumer {
    fn reject(
        &self,
        receipt: Self::Receipt,
        action: RejectAction,
    ) -> impl Future<Output = Result<RejectOutcome, Self::Error>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectAction {
    Retry {
        after: Option<Duration>,
        max_attempts: Option<usize>,
    },
    DeadLetter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejectOutcome {
    pub attempts: usize,
    pub exhausted: bool,
}

pub trait QueueScheduled: QueueProducer {
    fn schedule(
        &self,
        message: QueueMessage,
        run_at: Timestamp,
    ) -> impl Future<Output = Result<Self::MessageId, Self::Error>> + Send;
}

pub trait QueueScheduledPromotion: QueueConsumer {
    fn promote(&self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    Ready,
    Inflight,
    Scheduled,
    Dead,
}

#[derive(Debug, Clone)]
pub struct MessageStatus {
    pub id: String,
    pub state: MessageState,
    pub attempts: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("message ID is already in use")]
    DuplicateMessageId,

    #[error("delivery receipt is stale or no longer owns the lease")]
    StaleReceipt,
}
