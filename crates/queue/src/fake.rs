use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use jiff::Timestamp;

use crate::{
    QueueConsumer, QueueDelivery, QueueError, QueueLeaseReclaim, QueueLeaseRenewal, QueueMessage,
    QueueProducer, QueueRejection, QueueScheduled, QueueScheduledPromotion, RejectAction,
    RejectOutcome,
};

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

impl QueueLeaseReclaim for FakeQueue {
    async fn reclaim(&self) -> Result<usize, Self::Error> {
        let now = self.clock.now_micros();
        let mut state = self.state.lock().expect("queue state lock");

        let expired: Vec<u64> = state
            .inflight
            .iter()
            .filter(|(_, lease)| lease.expires_at_micros <= now)
            .map(|(token, _)| *token)
            .collect();

        let mut moved = 0;
        for token in expired {
            if let Some(lease) = state.inflight.remove(&token) {
                state.ready.push_back(lease.enqueued);
                moved += 1;
            }
        }

        Ok(moved)
    }
}

impl QueueLeaseRenewal for FakeQueue {
    async fn renew(
        &self,
        receipt: &Self::Receipt,
        visibility: Duration,
    ) -> Result<(), Self::Error> {
        let mut state = self.state.lock().expect("queue state lock");

        let lease = state
            .inflight
            .get_mut(&receipt.token)
            .ok_or(QueueError::StaleReceipt)?;

        lease.expires_at_micros = self.clock.now_micros() + duration_micros(visibility);

        Ok(())
    }
}

impl QueueRejection for FakeQueue {
    async fn reject(
        &self,
        receipt: Self::Receipt,
        action: RejectAction,
    ) -> Result<RejectOutcome, Self::Error> {
        let mut state = self.state.lock().expect("queue state lock");

        let lease = state
            .inflight
            .remove(&receipt.token)
            .ok_or(QueueError::StaleReceipt)?;

        match action {
            RejectAction::Retry {
                after,
                max_attempts,
            } => {
                let attempts = state.attempts.entry(lease.enqueued.id.clone()).or_insert(0);
                *attempts += 1;
                let attempts = *attempts;

                let exhausted = max_attempts.is_some_and(|max| attempts > max);

                if exhausted {
                    state.dead.insert(lease.enqueued.id.clone(), lease.enqueued);
                } else if let Some(after) = after {
                    let run_at = self.clock.now_micros() + duration_micros(after);

                    state
                        .scheduled
                        .insert((run_at, lease.enqueued.id.clone()), lease.enqueued);
                } else {
                    state.ready.push_back(lease.enqueued);
                }

                Ok(RejectOutcome {
                    attempts,
                    exhausted,
                })
            }
            RejectAction::DeadLetter => {
                let attempts = state.attempts.get(&lease.enqueued.id).copied().unwrap_or(0);
                state.dead.insert(lease.enqueued.id.clone(), lease.enqueued);

                Ok(RejectOutcome {
                    attempts,
                    exhausted: false,
                })
            }
        }
    }
}

impl QueueScheduled for FakeQueue {
    async fn schedule(
        &self,
        message: QueueMessage,
        run_at: Timestamp,
    ) -> Result<Self::MessageId, Self::Error> {
        let mut state = self.state.lock().expect("queue state lock");

        let id = match message.id.clone() {
            Some(id) if Self::id_in_use(&state, &id) => return Err(QueueError::DuplicateMessageId),
            Some(id) => id,
            None => Self::generated_id(&mut state),
        };

        state.scheduled.insert(
            (run_at.as_microsecond(), id.clone()),
            Enqueued {
                id: id.clone(),
                payload: message.payload,
                attributes: message.attributes,
            },
        );

        Ok(id)
    }
}

impl QueueScheduledPromotion for FakeQueue {
    async fn promote(&self) -> Result<usize, Self::Error> {
        let mut state = self.state.lock().expect("queue state lock");

        let now = self.clock.now_micros();
        let due = state
            .scheduled
            .keys()
            .filter(|(run_at, _)| *run_at <= now)
            .cloned()
            .collect::<Vec<_>>();

        let mut promoted = 0;
        for key in due {
            if let Some(enqueued) = state.scheduled.remove(&key) {
                state.ready.push_back(enqueued);
                promoted += 1;
            }
        }

        Ok(promoted)
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

    pub fn now(&self) -> Timestamp {
        Timestamp::from_microsecond(self.now_micros())
            .expect("fake clock stays within Timestamp range")
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

    #[tokio::test]
    async fn stale_ack_returns_error_and_leaves_newer_lease_intact() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();
        let first = queue.receive().await.unwrap().unwrap();
        queue.clock().advance(Duration::from_secs(31));

        let reclaimed = queue.reclaim().await.unwrap();
        assert_eq!(reclaimed, 1);

        let second = queue.receive().await.unwrap().unwrap();
        assert_eq!(second.id, first.id);

        assert!(queue.ack(first.receipt).await.is_err());

        queue.ack(second.receipt).await.unwrap();

        assert!(queue.receive().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lease_renewal_extends_visibility() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        let delivery = queue.receive().await.unwrap().unwrap();
        queue.clock().advance(Duration::from_secs(20));

        queue
            .renew(&delivery.receipt, Duration::from_secs(60))
            .await
            .unwrap();
        queue.clock().advance(Duration::from_secs(20));

        let reclaimed = queue.reclaim().await.unwrap();
        assert_eq!(reclaimed, 0, "renewed lease must not be reclaimed early");

        queue.ack(delivery.receipt).await.unwrap();
    }

    #[tokio::test]
    async fn reclaim_does_not_increment_attempts() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        let first = queue.receive().await.unwrap().unwrap();
        assert_eq!(first.attempts, 0);

        queue.clock().advance(Duration::from_secs(31));
        queue.reclaim().await.unwrap();

        let second = queue.receive().await.unwrap().unwrap();
        assert_eq!(second.id, first.id);
        assert_eq!(second.attempts, 0, "reclamation is not a failed execution");
    }

    #[tokio::test]
    async fn reclaim_of_empty_queue_returns_zero() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        let reclaimed = queue.reclaim().await.unwrap();
        assert_eq!(reclaimed, 0);
    }

    #[tokio::test]
    async fn ack_after_reclaim_is_stale() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        let delivery = queue.receive().await.unwrap().unwrap();
        queue.clock().advance(Duration::from_secs(31));

        queue.reclaim().await.unwrap();

        let result = queue.ack(delivery.receipt).await;
        assert!(matches!(result, Err(QueueError::StaleReceipt)));
    }

    #[tokio::test]
    async fn stale_reject_does_not_increment_attempts() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        let first = queue.receive().await.unwrap().unwrap();
        queue.clock().advance(Duration::from_secs(31));

        queue.reclaim().await.unwrap();

        let second = queue.receive().await.unwrap().unwrap();

        let outcome = queue
            .reject(
                first.receipt,
                RejectAction::Retry {
                    after: None,
                    max_attempts: Some(3),
                },
            )
            .await;
        assert!(outcome.is_err());

        assert_eq!(second.attempts, 0);

        let outcome = queue
            .reject(
                second.receipt,
                RejectAction::Retry {
                    after: None,
                    max_attempts: Some(3),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.attempts, 1);
        assert!(!outcome.exhausted);
    }

    #[tokio::test]
    async fn retry_returns_message_to_ready() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();
        let delivery = queue.receive().await.unwrap().unwrap();

        let outcome = queue
            .reject(
                delivery.receipt,
                RejectAction::Retry {
                    after: None,
                    max_attempts: Some(3),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.attempts, 1);
        assert!(!outcome.exhausted);

        let redelivered = queue.receive().await.unwrap().unwrap();
        assert_eq!(redelivered.id, delivery.id);
        assert_eq!(redelivered.attempts, 1);
    }

    #[tokio::test]
    async fn retry_exhaustion_is_atomic_and_dead_letters() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        for expected in [1, 2] {
            let delivery = queue.receive().await.unwrap().unwrap();

            let outcome = queue
                .reject(
                    delivery.receipt,
                    RejectAction::Retry {
                        after: None,
                        max_attempts: Some(2),
                    },
                )
                .await
                .unwrap();
            assert_eq!(outcome.attempts, expected);
            assert!(!outcome.exhausted);
        }

        let delivery = queue.receive().await.unwrap().unwrap();

        let outcome = queue
            .reject(
                delivery.receipt,
                RejectAction::Retry {
                    after: None,
                    max_attempts: Some(2),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.attempts, 3);
        assert!(outcome.exhausted);

        assert!(queue.receive().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn without_retry_limit_never_exhausts() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        for expected in 1..=5 {
            let delivery = queue.receive().await.unwrap().unwrap();

            let outcome = queue
                .reject(
                    delivery.receipt,
                    RejectAction::Retry {
                        after: None,
                        max_attempts: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(outcome.attempts, expected);
            assert!(!outcome.exhausted);
        }
    }

    #[tokio::test]
    async fn dead_letter_terminates_without_extra_attempt() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        let delivery = queue.receive().await.unwrap().unwrap();

        let outcome = queue
            .reject(delivery.receipt, RejectAction::DeadLetter)
            .await
            .unwrap();
        assert_eq!(outcome.attempts, 0);
        assert!(!outcome.exhausted);

        assert!(queue.receive().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delayed_retry_is_invisible_until_due() {
        let queue = FakeQueue::new(FakeQueueConfig::default());

        queue.send(message("payload")).await.unwrap();

        let delivery = queue.receive().await.unwrap().unwrap();

        queue
            .reject(
                delivery.receipt,
                RejectAction::Retry {
                    after: Some(Duration::from_secs(60)),
                    max_attempts: Some(3),
                },
            )
            .await
            .unwrap();
        assert!(queue.receive().await.unwrap().is_none());

        queue.clock().advance(Duration::from_secs(61));

        assert_eq!(queue.promote().await.unwrap(), 1);

        let redelivered = queue.receive().await.unwrap().unwrap();
        assert_eq!(redelivered.attempts, 1);
    }

    #[tokio::test]
    async fn delayed_queue_schedule_is_microsecond_precise() {
        let queue = FakeQueue::new(FakeQueueConfig::default());
        let base = queue.clock().now();

        queue
            .schedule(message("payload"), base + Duration::from_micros(1_500_750))
            .await
            .unwrap();
        assert!(queue.receive().await.unwrap().is_none());

        queue.clock().advance(Duration::from_micros(1_500_750));

        assert_eq!(queue.promote().await.unwrap(), 1);

        let delivery = queue.receive().await.unwrap().unwrap();
        assert_eq!(delivery.id, "msg-000000000000");
    }
}
