//! Bounded, noncanonical state projections from the preferred sequencer.
//!
//! A cursor is an exclusive resume position: a snapshot at N contains every change through N,
//! and a subscription from N starts at N + 1. There is no ledger fallback. Consumers must
//! reconcile from a fresh snapshot after any error, including when reconciliation itself
//! outlives the replay window. A source restart or discontinuous state replacement changes
//! the epoch. Frames do not affect ordinary transaction/event numbering.
//!
//! Transport adapters must periodically publish [`TransientFeed::status`] as an authoritative
//! watermark, even while no frames are emitted, and clients must compare it to their last
//! applied cursor. This detects a lost final frame during quiet periods. A subscription alone
//! cannot detect losses introduced by a downstream transport.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use sov_modules_api::rest::ApiState;
use sov_modules_api::{ApiStateAccessor, SequencerTransientUpdate, Spec, TxHash};
use tokio::sync::watch;
use uuid::Uuid;

/// Exclusive position in one process's transient history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransientCursor {
    /// Unique source incarnation, changed on restart or invalidation.
    pub epoch: Uuid,
    /// Last completely applied frame; zero is the beginning of an epoch.
    pub sequence: u64,
}

/// One atomic set of complete replacement images and deletions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransientFrame {
    /// Position after applying all updates in this frame.
    pub cursor: TransientCursor,
    /// Accepted transaction, or `None` for committed batch-boundary changes.
    pub tx_hash: Option<TxHash>,
    /// Apply every update before advancing the cursor.
    pub updates: Vec<SequencerTransientUpdate>,
}

impl TransientFrame {
    fn retained_bytes(&self) -> usize {
        self.updates.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.updates
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SequencerTransientUpdate>()),
            ),
            |size, update| {
                size.saturating_add(update.key.capacity())
                    .saturating_add(update.value.as_ref().map_or(0, Vec::capacity))
            },
        )
    }
}

/// Why replay continuity was invalidated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransientResetReason {
    /// A new process has no previous in-memory history.
    Startup,
    /// Recovery, replay, or a checkpoint replacement without proven continuity.
    StateReplaced,
    /// A runtime could not project a committed write.
    ProjectionFailed,
    /// One complete frame exceeds the configured replay bounds.
    OversizedFrame,
    /// Sequence numbers must never wrap within an epoch.
    CursorExhausted,
}

/// A feed error always requires a fresh coherent snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransientFeedError {
    /// The requested source incarnation is no longer current.
    #[error("transient source reset: {reason:?}")]
    Reset {
        /// Current source position.
        current: TransientCursor,
        /// Reason for the latest epoch change.
        reason: TransientResetReason,
    },
    /// At least one required frame has been evicted.
    #[error("transient cursor is older than retained history")]
    TooOld {
        /// Earliest exclusive cursor whose complete suffix remains available.
        oldest: TransientCursor,
    },
    /// A cursor cannot refer to data the source has not produced.
    #[error("transient cursor is ahead of the source")]
    Future {
        /// Current source position.
        current: TransientCursor,
    },
}

/// Current replay coverage and cumulative invalidation counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransientFeedStatus {
    /// Position of the latest complete frame.
    pub current: TransientCursor,
    /// Earliest exclusive cursor that can still resume without a gap.
    pub oldest: TransientCursor,
    /// Number of frames retained.
    pub retained_frames: usize,
    /// Allocation capacities of retained frames, excluding bounded ring/allocator metadata.
    pub retained_bytes: usize,
    /// Epoch invalidations since this source was created.
    pub resets: u64,
    /// Rejected stale resumes or lagged live reads.
    pub too_old_requests: u64,
}

struct FeedState {
    current: TransientCursor,
    reset_reason: TransientResetReason,
    frames: VecDeque<Arc<TransientFrame>>,
    bytes: usize,
    max_frames: usize,
    max_bytes: usize,
    resets: u64,
    too_old_requests: u64,
}

impl FeedState {
    fn validate(&mut self, cursor: TransientCursor) -> Result<(), TransientFeedError> {
        if cursor.epoch != self.current.epoch {
            return Err(TransientFeedError::Reset {
                current: self.current,
                reason: self.reset_reason,
            });
        }
        if cursor.sequence > self.current.sequence {
            return Err(TransientFeedError::Future {
                current: self.current,
            });
        }
        let oldest = self.frames.front().map_or(self.current.sequence, |frame| {
            frame.cursor.sequence.saturating_sub(1)
        });
        if cursor.sequence < oldest {
            self.too_old_requests = self.too_old_requests.saturating_add(1);
            return Err(TransientFeedError::TooOld {
                oldest: TransientCursor {
                    sequence: oldest,
                    ..self.current
                },
            });
        }
        Ok(())
    }

    fn reset(&mut self, reason: TransientResetReason) {
        self.current = TransientCursor {
            epoch: Uuid::now_v7(),
            sequence: 0,
        };
        self.frames.clear();
        self.bytes = 0;
        self.reset_reason = reason;
        self.resets = self.resets.saturating_add(1);
        tracing::warn!(?reason, epoch = %self.current.epoch, "Transient feed requires reconciliation");
    }

    fn publish(&mut self, tx_hash: Option<TxHash>, updates: Vec<SequencerTransientUpdate>) {
        if updates.is_empty() {
            return;
        }
        let Some(sequence) = self.current.sequence.checked_add(1) else {
            self.reset(TransientResetReason::CursorExhausted);
            return;
        };
        let frame = Arc::new(TransientFrame {
            cursor: TransientCursor {
                sequence,
                ..self.current
            },
            tx_hash,
            updates,
        });
        let bytes = frame.retained_bytes();
        if self.max_frames == 0 || bytes > self.max_bytes {
            self.reset(TransientResetReason::OversizedFrame);
            return;
        }
        while self.frames.len() >= self.max_frames
            || self.bytes > self.max_bytes.saturating_sub(bytes)
        {
            let removed = self.frames.pop_front().expect("nonempty bounded replay");
            self.bytes = self.bytes.saturating_sub(removed.retained_bytes());
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.current = frame.cursor;
        self.frames.push_back(frame);
    }
}

/// Shared bounded replay and publication barrier.
///
/// Limits cover retained frame allocation capacities and count; ring/allocator metadata is
/// additionally bounded by the frame count. Subscribers hold at most the frames their caller
/// is currently consuming. They have no payload queues and can never extend replay retention.
#[derive(Clone)]
pub struct TransientFeed {
    state: Arc<Mutex<FeedState>>,
    changed: watch::Sender<()>,
}

impl TransientFeed {
    /// Create a new source incarnation with explicit retention limits.
    pub fn new(max_frames: usize, max_bytes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(FeedState {
                current: TransientCursor {
                    epoch: Uuid::now_v7(),
                    sequence: 0,
                },
                reset_reason: TransientResetReason::Startup,
                frames: VecDeque::new(),
                bytes: 0,
                max_frames,
                max_bytes,
                resets: 0,
                too_old_requests: 0,
            })),
            changed: watch::channel(()).0,
        }
    }

    fn lock(&self) -> MutexGuard<'_, FeedState> {
        self.state
            .lock()
            .expect("transient publication lock poisoned")
    }

    /// Inspect replay coverage. Retention duration depends on update volume and image sizes.
    pub fn status(&self) -> TransientFeedStatus {
        let guard = self.lock();
        TransientFeedStatus {
            current: guard.current,
            oldest: TransientCursor {
                sequence: guard
                    .frames
                    .front()
                    .map_or(guard.current.sequence, |frame| {
                        frame.cursor.sequence.saturating_sub(1)
                    }),
                ..guard.current
            },
            retained_frames: guard.frames.len(),
            retained_bytes: guard.bytes,
            resets: guard.resets,
            too_old_requests: guard.too_old_requests,
        }
    }

    /// Pin current state and its exclusive cursor atomically.
    ///
    /// Pass the API state from the same sequencer as this feed. The accessor pins its
    /// checkpoint/read transaction; all scanning must happen after this method returns.
    /// After scanning, subscribe from the returned cursor and reconcile again on any gap.
    pub fn snapshot<S: Spec>(
        &self,
        api_state: &ApiState<S>,
    ) -> anyhow::Result<(TransientCursor, ApiStateAccessor<S>)> {
        let guard = self.lock();
        Ok((guard.current, api_state.build_api_state_accessor(None)?))
    }

    /// Attach a replay-to-live subscription atomically, strictly after `cursor`.
    pub fn subscribe(
        &self,
        cursor: TransientCursor,
    ) -> Result<TransientSubscription, TransientFeedError> {
        let mut guard = self.lock();
        guard.validate(cursor)?;
        Ok(TransientSubscription {
            feed: self.clone(),
            cursor,
            changed: self.changed.subscribe(),
        })
    }

    pub(crate) fn apply(
        &self,
        tx_hash: Option<TxHash>,
        updates: anyhow::Result<Vec<SequencerTransientUpdate>>,
        apply_state: impl FnOnce(),
    ) {
        let mut guard = self.lock();
        apply_state();
        match updates {
            Ok(updates) => guard.publish(tx_hash, updates),
            Err(error) => {
                tracing::error!(%error, "Failed to project accepted state into transient feed");
                guard.reset(TransientResetReason::ProjectionFailed);
            }
        }
        self.changed.send_replace(());
    }

    pub(crate) fn replace(&self, continuous: bool, apply_state: impl FnOnce()) {
        let mut guard = self.lock();
        apply_state();
        if !continuous {
            guard.reset(TransientResetReason::StateReplaced);
        }
        self.changed.send_replace(());
    }
}

/// One replay-to-live reader with constant memory overhead.
pub struct TransientSubscription {
    feed: TransientFeed,
    cursor: TransientCursor,
    changed: watch::Receiver<()>,
}

impl TransientSubscription {
    /// Wait for the next complete frame. Terminate and reconcile after any error.
    pub async fn next(&mut self) -> Result<Arc<TransientFrame>, TransientFeedError> {
        loop {
            {
                let mut guard = self.feed.lock();
                guard.validate(self.cursor)?;
                let index = guard.frames.front().and_then(|first| {
                    usize::try_from(
                        self.cursor
                            .sequence
                            .saturating_sub(first.cursor.sequence.saturating_sub(1)),
                    )
                    .ok()
                });
                if let Some(frame) = index.and_then(|index| guard.frames.get(index)).cloned() {
                    self.cursor = frame.cursor;
                    return Ok(frame);
                }
            }
            // The subscription owns a sender through `feed`, so this cannot close.
            self.changed
                .changed()
                .await
                .expect("transient feed sender is owned");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image(value: u8) -> Vec<SequencerTransientUpdate> {
        vec![SequencerTransientUpdate {
            key: vec![1],
            value: Some(vec![value]),
        }]
    }

    fn cursor(feed: &TransientFeed) -> TransientCursor {
        feed.lock().current
    }

    #[tokio::test]
    async fn replay_live_cut_is_exclusive_and_complete() {
        let feed = TransientFeed::new(3, 4096);
        feed.apply(None, Ok(image(1)), || {});
        let cut = cursor(&feed);
        feed.apply(None, Ok(image(2)), || {});
        let mut stream = feed.subscribe(cut).unwrap();
        feed.apply(None, Ok(image(3)), || {});
        assert_eq!(stream.next().await.unwrap().updates, image(2));
        assert_eq!(stream.next().await.unwrap().updates, image(3));
    }

    #[tokio::test]
    async fn count_eviction_fails_replay_and_slow_live_reader() {
        let feed = TransientFeed::new(1, 4096);
        let cut = cursor(&feed);
        let mut slow = feed.subscribe(cut).unwrap();
        feed.apply(None, Ok(image(1)), || {});
        feed.apply(None, Ok(image(2)), || {});
        assert!(matches!(
            feed.subscribe(cut),
            Err(TransientFeedError::TooOld { .. })
        ));
        assert!(matches!(
            slow.next().await,
            Err(TransientFeedError::TooOld { .. })
        ));
        let oldest = TransientCursor { sequence: 1, ..cut };
        assert_eq!(
            feed.subscribe(oldest)
                .unwrap()
                .next()
                .await
                .unwrap()
                .updates,
            image(2)
        );
    }

    #[test]
    fn byte_limit_counts_capacities_and_resets_on_oversized_frame() {
        let feed = TransientFeed::new(10, 512);
        for value in 0..20 {
            feed.apply(None, Ok(image(value)), || {});
            assert!(feed.lock().bytes <= 512);
        }
        let cut = cursor(&feed);
        let mut large = image(1);
        large[0].key.reserve(1024);
        feed.apply(None, Ok(large), || {});
        assert!(matches!(
            feed.subscribe(cut),
            Err(TransientFeedError::Reset {
                reason: TransientResetReason::OversizedFrame,
                ..
            })
        ));
        assert_eq!(feed.lock().bytes, 0);
    }

    #[tokio::test]
    async fn projection_error_applies_state_then_invalidates_without_partial_frame() {
        let feed = TransientFeed::new(3, 4096);
        let old = cursor(&feed);
        let mut stream = feed.subscribe(old).unwrap();
        let mut applied = false;
        feed.apply(None, Err(anyhow::anyhow!("bad image")), || applied = true);
        assert!(applied);
        assert!(matches!(
            stream.next().await,
            Err(TransientFeedError::Reset {
                reason: TransientResetReason::ProjectionFailed,
                ..
            })
        ));
        assert!(feed.lock().frames.is_empty());
    }

    #[test]
    fn continuous_rebase_preserves_cursor_but_reset_and_restart_do_not() {
        let feed = TransientFeed::new(3, 4096);
        feed.apply(None, Ok(image(1)), || {});
        let old = cursor(&feed);
        feed.replace(true, || {});
        assert_eq!(cursor(&feed), old);
        feed.replace(false, || {});
        assert!(matches!(
            feed.subscribe(old),
            Err(TransientFeedError::Reset { .. })
        ));
        assert_ne!(
            cursor(&feed).epoch,
            cursor(&TransientFeed::new(3, 4096)).epoch
        );
    }

    #[test]
    fn future_and_exhausted_cursors_never_silently_skip() {
        let feed = TransientFeed::new(3, 4096);
        let old = cursor(&feed);
        assert!(matches!(
            feed.subscribe(TransientCursor { sequence: 1, ..old }),
            Err(TransientFeedError::Future { .. })
        ));
        feed.lock().current.sequence = u64::MAX;
        feed.apply(None, Ok(image(1)), || {});
        assert_ne!(cursor(&feed).epoch, old.epoch);
    }
    // The generic MockKernel treats every height as historical. This kernel models
    // the preferred sequencer's current, not-yet-finalized height instead.
    struct LiveKernel;

    impl sov_modules_api::capabilities::KernelWithSlotMapping<sov_test_utils::TestSpec> for LiveKernel {
        fn visible_slot_number_at(
            &self,
            slot: sov_rollup_interface::common::SlotNumber,
            _: &mut ApiStateAccessor<sov_test_utils::TestSpec>,
        ) -> Option<sov_modules_api::VisibleSlotNumber> {
            Some(sov_modules_api::VisibleSlotNumber::new_dangerous(
                slot.get(),
            ))
        }
        fn rollup_height_to_visible_slot_number(
            &self,
            height: sov_rollup_interface::common::RollupHeight,
            _: &mut ApiStateAccessor<sov_test_utils::TestSpec>,
        ) -> Option<sov_modules_api::VisibleSlotNumber> {
            Some(sov_modules_api::VisibleSlotNumber::new_dangerous(
                height.get(),
            ))
        }
        fn true_slot_number_to_rollup_height(
            &self,
            slot: sov_rollup_interface::common::SlotNumber,
            _: &mut ApiStateAccessor<sov_test_utils::TestSpec>,
        ) -> Option<sov_rollup_interface::common::RollupHeight> {
            Some(sov_rollup_interface::common::RollupHeight::new(slot.get()))
        }
        fn current_rollup_height(
            &self,
            _: &mut ApiStateAccessor<sov_test_utils::TestSpec>,
        ) -> sov_rollup_interface::common::RollupHeight {
            sov_rollup_interface::common::RollupHeight::new(1)
        }
        fn true_slot_number_at_historical_height(
            &self,
            _: sov_rollup_interface::common::RollupHeight,
            _: &mut ApiStateAccessor<sov_test_utils::TestSpec>,
        ) -> Option<sov_rollup_interface::common::SlotNumber> {
            None
        }
        fn base_fee_per_gas_at(
            &self,
            _: sov_rollup_interface::common::RollupHeight,
            state: &mut ApiStateAccessor<sov_test_utils::TestSpec>,
        ) -> Option<<<sov_test_utils::TestSpec as Spec>::Gas as sov_modules_api::Gas>::Price>
        {
            Some(sov_modules_api::GetGasPrice::gas_price(state))
        }
        fn get_latest_rollup_height(
            &self,
            _: &<sov_test_utils::TestSpec as Spec>::Storage,
        ) -> sov_rollup_interface::common::RollupHeight {
            sov_rollup_interface::common::RollupHeight::new(1)
        }
        fn get_true_slot_number_for_height_unbound(
            &self,
            _: sov_rollup_interface::common::RollupHeight,
            _: &<sov_test_utils::TestSpec as Spec>::Storage,
        ) -> Option<sov_rollup_interface::common::SlotNumber> {
            None
        }
    }

    #[tokio::test]
    async fn snapshot_is_pinned_while_later_writes_and_checkpoint_replacement_proceed() {
        use sov_modules_api::capabilities::mocks::MockKernel;
        use sov_modules_api::{
            ConcurrentStateCheckpoint, StateCheckpoint, StateReader, TxChangeSet,
        };
        use sov_state::codec::BcsCodec;
        use sov_state::{Namespace, SlotKey, SlotValue, SlotValueFromCodec, User};
        use sov_test_utils::storage::SimpleStorageManager;
        use sov_test_utils::TestSpec;

        let storage_manager = SimpleStorageManager::new();
        let storage = storage_manager.create_storage();
        let kernel = Arc::new(MockKernel::<TestSpec>::new(1, 1));
        let checkpoint = Arc::new(ConcurrentStateCheckpoint::from_state_checkpoint(
            StateCheckpoint::new(storage.clone(), kernel.as_ref()),
        ));
        let (sender, receiver) = watch::channel(checkpoint.clone());
        let api_state = ApiState::build(
            Arc::new(()),
            receiver,
            Arc::new(LiveKernel),
            None,
            sov_shutdown::PrimaryShutdownController::new(),
        );
        let feed = TransientFeed::new(3, 4096);
        let key = SlotKey::test_key(1);
        let value = |n: u8| SlotValue::new(&n, &BcsCodec {});
        let changes = |n| TxChangeSet {
            writes: vec![((key.clone(), Namespace::User), Some(value(n)))],
            reads: None,
        };
        feed.apply(None, Ok(image(1)), || {
            checkpoint.apply_tx_changes(changes(1));
        });
        let (cut, mut pinned) = feed.snapshot(&api_state).unwrap();
        feed.apply(None, Ok(image(2)), || {
            checkpoint.apply_tx_changes(changes(2));
        });
        let mut stream = feed.subscribe(cut).unwrap();
        assert_eq!(
            StateReader::<User>::get(&mut pinned, &key).unwrap(),
            Some(value(1))
        );
        assert_eq!(stream.next().await.unwrap().updates, image(2));
        let replacement = Arc::new(ConcurrentStateCheckpoint::from_state_checkpoint(
            StateCheckpoint::new(storage, kernel.as_ref()),
        ));
        replacement.apply_tx_changes(changes(2));
        feed.replace(true, || {
            sender.send_replace(replacement);
        });
        assert_eq!(
            feed.snapshot(&api_state).unwrap().0.sequence,
            cut.sequence + 1
        );
        assert_eq!(
            StateReader::<User>::get(&mut pinned, &key).unwrap(),
            Some(value(1))
        );
    }
}
