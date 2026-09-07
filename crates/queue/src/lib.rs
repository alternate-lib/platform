use std::{collections::BTreeMap, error::Error, future::Future, time::Duration};

#[cfg(test)]
mod fake;

pub trait QueueProducer: Send + Sync {
    type MessageId: Send + 'static;
    type Error: Error + Send + Sync + 'static;

    fn send(
        &self,
        message: QueueMessage,
    ) -> impl Future<Output = Result<Self::MessageId, Self::Error>> + Send;
}

pub trait QueueConsumer: Send + Sync {
    type Receipt: Send + Sync + 'static;
    type Error: Error + Send + Sync + 'static;

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

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("message ID is already in use")]
    DuplicateMessageId,

    #[error("delivery receipt is stale or no longer owns the lease")]
    StaleReceipt,
}
