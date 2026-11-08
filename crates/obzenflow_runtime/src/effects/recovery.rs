// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::backpressure::BackpressureReader;
use crate::stages::common::heartbeat::RetryWaitGuard;
use crate::stages::common::supervision::backpressure_drain::CONTROL_RESPONSIVENESS_CAP;
use obzenflow_core::event::CausalFrontier;
use obzenflow_core::MiddlewareExecutionScope;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use tokio::time::Instant;

/// The holds an effect call renews on its stage's input edges while it waits
/// to retry, so backpressure sees a deliberate wait as a live reader.
#[derive(Clone, Default)]
pub struct InputHolds {
    readers: Arc<[BackpressureReader]>,
    heartbeat: Option<Arc<HeartbeatState>>,
    calls_in_flight: Arc<AtomicUsize>,
}

impl InputHolds {
    /// Holds on `readers`, one per input edge. Each wait also reports the
    /// stage's heartbeat as waiting to retry.
    pub fn new(
        readers: impl IntoIterator<Item = BackpressureReader>,
        heartbeat: Option<Arc<HeartbeatState>>,
    ) -> Self {
        Self {
            readers: readers.into_iter().collect(),
            heartbeat,
            calls_in_flight: Arc::default(),
        }
    }

    /// Wait for `delay`, renewing every hold at the control cadence while no
    /// physical call of the stage is in flight. Renewals stop when the wait
    /// ends or is dropped.
    pub async fn wait_to_retry(&self, delay: Duration) {
        let _waiting = self.heartbeat.clone().map(RetryWaitGuard::new);
        let deadline = Instant::now().checked_add(delay);
        let mut renewal = Instant::now();
        loop {
            self.renew();
            renewal = match renewal.checked_add(CONTROL_RESPONSIVENESS_CAP) {
                Some(next) => next,
                None => return std::future::pending().await,
            };
            match deadline {
                Some(deadline) if deadline <= renewal => {
                    tokio::time::sleep_until(deadline).await;
                    return;
                }
                _ => tokio::time::sleep_until(renewal).await,
            }
        }
    }

    fn renew(&self) {
        if self.calls_in_flight.load(Ordering::Acquire) > 0 {
            return;
        }
        for reader in self.readers.iter() {
            reader.renew_hold();
        }
    }

    pub(crate) fn physical_call(&self) -> PhysicalCallInFlight {
        self.calls_in_flight.fetch_add(1, Ordering::AcqRel);
        PhysicalCallInFlight {
            calls_in_flight: self.calls_in_flight.clone(),
        }
    }
}

pub(crate) struct PhysicalCallInFlight {
    calls_in_flight: Arc<AtomicUsize>,
}

impl Drop for PhysicalCallInFlight {
    fn drop(&mut self) {
        self.calls_in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Commits the recovery rows a boundary has written before it waits, each
/// batch as one atomic group under the call's cursor, numbered by its wait.
#[derive(Clone)]
pub struct EffectProgress {
    sink: Arc<ProgressSink>,
}

enum ProgressSink {
    Detached {
        batches: Mutex<Vec<Vec<ChainEvent>>>,
    },
    Journal {
        journal: Box<ProgressJournal>,
        failure: Mutex<Option<EffectError>>,
    },
}

impl EffectProgress {
    pub(crate) fn detached() -> Self {
        Self {
            sink: Arc::new(ProgressSink::Detached {
                batches: Mutex::new(Vec::new()),
            }),
        }
    }

    pub(crate) fn journal(journal: ProgressJournal) -> Self {
        Self {
            sink: Arc::new(ProgressSink::Journal {
                journal: Box::new(journal),
                failure: Mutex::new(None),
            }),
        }
    }

    /// Commit `rows` as one atomic group before a wait. A batch holding a
    /// `Data` row is refused, and a refused or failed commit ends the call.
    pub async fn commit(&self, rows: Vec<ChainEvent>) -> Result<(), EffectError> {
        if rows.is_empty() {
            return Ok(());
        }
        match &*self.sink {
            ProgressSink::Detached { batches } => {
                if rows.iter().any(ChainEvent::consumes_data_credit) {
                    return Err(EffectError::EffectProvenanceMismatch(
                        "recovery progress contains Data".to_string(),
                    ));
                }
                batches
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(rows);
                Ok(())
            }
            ProgressSink::Journal { journal, failure } => {
                let committed = journal.commit(rows).await;
                if let Err(error) = &committed {
                    failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| error.clone());
                }
                committed
            }
        }
    }

    pub(crate) fn failure(&self) -> Option<EffectError> {
        match &*self.sink {
            ProgressSink::Detached { .. } => None,
            ProgressSink::Journal { failure, .. } => failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    /// The batches an operation built without a journal has committed.
    #[cfg(any(test, feature = "test-support"))]
    pub fn committed(&self) -> Vec<Vec<ChainEvent>> {
        match &*self.sink {
            ProgressSink::Detached { batches } => batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            ProgressSink::Journal { .. } => Vec::new(),
        }
    }
}

pub(crate) struct ProgressJournal {
    pub(crate) cursor: EffectCursor,
    pub(crate) committed_waits: AtomicU32,
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    pub(crate) flow_context: Option<FlowContext>,
    pub(crate) instrumentation: Option<Arc<StageInstrumentation>>,
    pub(crate) heartbeat_state: Option<Arc<HeartbeatState>>,
    pub(crate) backpressure_writer: BackpressureWriter,
    pub(crate) parent: crate::messaging::DeliveredRecord<ChainPayload>,
    pub(crate) causal: CausalFrontier,
    pub(crate) coordinator: Arc<EffectCursorCoordinator>,
}

impl ProgressJournal {
    async fn commit(&self, rows: Vec<ChainEvent>) -> Result<(), EffectError> {
        if rows.iter().any(ChainEvent::consumes_data_credit) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "recovery progress for cursor {:?} contains Data",
                self.cursor
            )));
        }
        let wait = self
            .committed_waits
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .ok_or_else(|| EffectError::Execution("recovery wait ordinal overflow".to_string()))?;
        let entries = rows
            .into_iter()
            .map(|mut event| {
                event.replay_context = None;
                AtomicCommitEntry {
                    event,
                    options: CommitOptions::default(),
                    intent: StageAppendIntent::FrameworkObservability,
                }
            })
            .collect();
        let group_id = effect_recovery_progress_group_id(&self.cursor, wait);
        let committer = OutputCommitter {
            data_journal: &self.data_journal,
            flow_context: self.flow_context.as_ref(),
            instrumentation: self.instrumentation.as_ref(),
            heartbeat_state: self.heartbeat_state.as_ref(),
            output_contract: None,
            backpressure_writer: Some(&self.backpressure_writer),
            observer_scope: MiddlewareExecutionScope::LiveEffectBoundary,
        };
        crate::supervised_base::publication::with_snapshot(
            self.causal.clone(),
            committer.commit_atomic_group(group_id.as_str(), entries, Some(&self.parent)),
        )
        .await
        .map(drop)
        .map_err(|error| {
            self.coordinator.poison(self.cursor.clone());
            EffectError::Journal(error.to_string())
        })
    }
}
