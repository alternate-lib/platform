use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use crate::{QueueConsumer, QueueDelivery, QueueError, QueueMessage, QueueProducer};

#[derive(Debug, Clone)]
pub struct FakeQueue {
    state: Arc<Mutex<FakeQueueState>>,
    config: FakeQueueConfig,
    clock: FakeClock,
}

impl FakeQueue {
    pub fn new(config: FakeQueueConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeQueueState::default())),
            config,
            clock: FakeClock::default(),
        }
    }

    pub fn clock(&self) -> &FakeClock {
        &self.clock
    }

    fn id_in_use(state: &FakeQueueState, id: &str) -> bool {
        state.ready.iter().any(|message| message.id == id)
            || state.scheduled.values().any(|message| message.id == id)
            || state.inflight.values().any(|lease| lease.enqueued.id == id)
            || state.dead.contains_key(id)
    }

    fn generated_id(state: &mut FakeQueueState) -> String {
        loop {
            let id = format!("msg-{:012}", state.next_id);
            state.next_id += 1;
            if !Self::id_in_use(state, &id) {
                return id;
            }
        }
    }
}

#[derive(Debug, Default)]
struct FakeQueueState {
    next_id: u64,
    next_token: u64,
    ready: VecDeque<Enqueued>,
    scheduled: BTreeMap<(i64, String), Enqueued>,
    inflight: HashMap<u64, Lease>,
    dead: HashMap<String, Enqueued>,
    attempts: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
struct Enqueued {
    id: String,
    payload: Vec<u8>,
    attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct Lease {
    enqueued: Enqueued,
    expires_at_micros: i64,
}

impl QueueProducer for FakeQueue {
    type MessageId = String;
    type Error = QueueError;

    async fn send(&self, message: QueueMessage) -> Result<Self::MessageId, Self::Error> {
        let mut state = self.state.lock().expect("queue state lock");

        let id = match message.id {
            Some(id) if Self::id_in_use(&state, &id) => return Err(QueueError::DuplicateMessageId),
            Some(id) => id,
            None => Self::generated_id(&mut state),
        };

        state.ready.push_back(Enqueued {
            id: id.clone(),
            payload: message.payload,
            attributes: message.attributes,
        });

        Ok(id)
    }
}

impl QueueConsumer for FakeQueue {
    type Receipt = FakeReceipt;
    type Error = QueueError;

    async fn receive(&self) -> Result<Option<QueueDelivery<Self::Receipt>>, Self::Error> {
        let mut state = self.state.lock().expect("queue state lock");

        let Some(enqueued) = state.ready.pop_front() else {
            return Ok(None);
        };

        let token = state.next_token;
        state.next_token += 1;

        let attempts = state.attempts.get(&enqueued.id).copied().unwrap_or(0);

        state.inflight.insert(
            token,
            Lease {
                enqueued: enqueued.clone(),
                expires_at_micros: self.clock().now_micros()
                    + duration_micros(self.config.visibility),
            },
        );

        Ok(Some(QueueDelivery {
            id: enqueued.id,
            payload: enqueued.payload,
            attempts,
            attributes: enqueued.attributes,
            receipt: FakeReceipt { token },
        }))
    }

    async fn ack(&self, receipt: Self::Receipt) -> Result<(), Self::Error> {
        let mut state = self.state.lock().expect("queue state lock");

        state
            .inflight
            .remove(&receipt.token)
            .ok_or(QueueError::StaleReceipt)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FakeReceipt {
    token: u64,
}

#[derive(Debug, Clone)]
pub struct FakeQueueConfig {
    pub visibility: Duration,
}

impl Default for FakeQueueConfig {
    fn default() -> Self {
        Self {
            visibility: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeClock(Arc<AtomicI64>);

impl FakeClock {
    pub fn now_micros(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }

    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(duration_micros(by), Ordering::SeqCst);
    }
}

fn duration_micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).expect("duration exceeds fake clock range")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn message(payload: &str) -> QueueMessage {
        QueueMessage::new(payload.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn preserves_caller_supplied_id() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        let mut msg = message("payload");
        msg.id = Some("app-job-42".to_owned());

        let id = queue.send(msg).await.unwrap();
        assert_eq!(id, "app-job-42");

        let delivery = queue.receive().await.unwrap().unwrap();
        assert_eq!(delivery.id, "app-job-42");
    }

    #[tokio::test]
    async fn generates_id_when_absent() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        let id = queue.send(message("payload")).await.unwrap();
        assert_ne!(id, "");

        let delivery = queue.receive().await.unwrap().unwrap();
        assert_eq!(delivery.id, id);
    }

    #[tokio::test]
    async fn fresh_delivery_reports_zero_attempts() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();
        let delivery = queue.receive().await.unwrap().unwrap();

        assert_eq!(delivery.attempts, 0);
    }

    #[tokio::test]
    async fn ack_success() {
        let queue = FakeQueue::new(FakeQueueConfig::default());
        queue.send(message("payload")).await.unwrap();

        let delivery = queue.receive().await.unwrap().unwrap();
        queue.ack(delivery.receipt).await.unwrap();

        assert!(queue.receive().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn duplicate_message_id_is_rejected_until_ack() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        let mut msg = message("payload");
        msg.id = Some("same-id".to_owned());
        queue.send(msg.clone()).await.unwrap();

        assert!(matches!(
            queue.send(msg.clone()).await,
            Err(QueueError::DuplicateMessageId)
        ));

        let delivery = queue.receive().await.unwrap().unwrap();
        queue.ack(delivery.receipt).await.unwrap();

        assert_eq!(queue.send(msg).await.unwrap(), "same-id");
    }

    #[test]
    fn fake_clock_is_monotonic_and_controllable() {
        let clock = FakeClock::default();
        assert_eq!(clock.now_micros(), 0);

        clock.advance(Duration::from_secs(2));
        clock.advance(Duration::from_millis(500));
        assert_eq!(clock.now_micros(), 2_500_000);
    }
}
