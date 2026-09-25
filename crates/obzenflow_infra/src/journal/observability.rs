// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::{JournalEvent, JournalRecord};
use obzenflow_core::journal::{JournalCapture, JournalError, ObservabilityPolicy};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

#[derive(Default)]
struct State {
    policy: ObservabilityPolicy,
    next: Option<Instant>,
    reserved: bool,
}

/// One small clock gate per journal. No task, timer, queue, or accumulated credit.
#[derive(Clone, Default)]
pub(super) struct JournalObservability(Arc<Mutex<State>>);

impl JournalObservability {
    pub(super) fn configure(&self, policy: ObservabilityPolicy) -> Result<(), JournalError> {
        if let ObservabilityPolicy::Periodic { interval } = policy {
            if interval.is_zero() || Instant::now().checked_add(interval).is_none() {
                return Err(JournalError::Implementation {
                    message: "Observability interval is outside the timer range".into(),
                    source: "invalid observability interval".into(),
                });
            }
        }
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.policy != policy {
            if state.reserved {
                return Err(JournalError::Implementation {
                    message: "Cannot change observability policy during an append".into(),
                    source: "observability append in progress".into(),
                });
            }
            state.policy = policy;
            state.next = None;
        }
        Ok(())
    }

    pub(super) fn prepare<T: JournalEvent>(
        &self,
        events: Vec<T>,
        mut capture: JournalCapture<T>,
    ) -> (Vec<T>, Reservation) {
        let mut reservation = Reservation(None);
        if matches!(capture, JournalCapture::Historical) {
            return (events, reservation);
        }
        let mut selected = false;
        let events = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let (mut envelope, payload) = event.into_parts();
                if envelope.observability.is_none() && matches!(capture, JournalCapture::Live(None))
                {
                    return T::from_parts(envelope, payload);
                }
                let admitted = {
                    let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
                    match state.policy {
                        ObservabilityPolicy::EveryRecord => true,
                        ObservabilityPolicy::Periodic { .. }
                            if !selected
                                && !state.reserved
                                && state.next.is_none_or(|next| Instant::now() >= next) =>
                        {
                            selected = true;
                            state.reserved = true;
                            reservation.0 = Some(self.clone());
                            true
                        }
                        _ => false,
                    }
                };
                if !admitted {
                    envelope.observability = None;
                }
                let event = T::from_parts(envelope, payload);
                if admitted {
                    return capture.prepare(index, event);
                }
                event
            })
            .collect();
        (events, reservation)
    }
}

pub(super) struct Reservation(Option<JournalObservability>);

impl Reservation {
    pub(super) fn finish<T: JournalEvent>(
        self,
        result: &Result<Vec<JournalRecord<T::Payload>>, JournalError>,
    ) {
        let Some(gate) = self.0 else { return };
        if matches!(result, Err(JournalError::CommitIndeterminate { .. })) {
            return; // Unknown commitment must not release the allowance.
        }
        let mut state = gate.0.lock().unwrap_or_else(|error| error.into_inner());
        state.reserved = false;
        if result.as_ref().is_ok_and(|records| {
            records
                .iter()
                .any(|record| record.envelope.observability.is_some())
        }) {
            if let ObservabilityPolicy::Periodic { interval } = state.policy {
                state.next = Instant::now().checked_add(interval);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::journal::{DiskJournal, MemoryJournal};
    use obzenflow_core::event::observability::{
        CaptureReason, CaptureScope, CaptureSeq, CaptureStamp, ObservabilityContext,
        RuntimeObservability,
    };
    use obzenflow_core::event::provenance::{ExecutionAccounting, RuntimeProvenance};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{ChainEvent, FlowId, Journal, JournalOwner, StageId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    pub(crate) fn event(stage: StageId, seq: u64) -> ChainEvent {
        let mut packet = ObservabilityContext::new(CaptureStamp {
            capture_scope: CaptureScope {
                flow_id: FlowId::new(),
                resume_generation: Default::default(),
            },
            observer: stage.into(),
            capture_seq: CaptureSeq(seq),
            capture_reason: CaptureReason::Record,
            observed_at_ms: 123,
        });
        packet.runtime = Some(RuntimeObservability {
            in_flight: Some(seq as u32),
            ..Default::default()
        });
        ChainEventFactory::data_event(stage.into(), "sparse.fact", serde_json::json!({"seq": seq}))
            .with_runtime_provenance(RuntimeProvenance {
                accounting: ExecutionAccounting {
                    events_processed_total: seq,
                    ..Default::default()
                },
            })
            .with_observability_context(packet)
    }

    async fn conformance(
        journal: Arc<dyn Journal<ChainEvent>>,
        independent: Arc<dyn Journal<ChainEvent>>,
        stage: StageId,
    ) {
        let policy = ObservabilityPolicy::Periodic {
            interval: Duration::from_millis(250),
        };
        // Default is dense, even without advancing time.
        for seq in 1..=3 {
            assert!(journal
                .append(event(stage, seq), Default::default())
                .await
                .unwrap()
                .envelope
                .observability
                .is_some());
        }
        // Preparation may replace the packet while the authored fact and its
        // accounting pass unchanged through both providers' ordinary append.
        let authored = event(stage, 0);
        let prepared = journal
            .append(
                authored.clone(),
                obzenflow_core::journal::AppendOptions::default().with_capture(
                    JournalCapture::Live(Some(Box::new(|_, event: &ChainEvent| {
                        let mut packet = event.envelope.observability.clone().unwrap();
                        packet.runtime.as_mut().unwrap().in_flight = Some(99);
                        Some(packet)
                    }))),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&prepared.payload).unwrap(),
            serde_json::to_value(&authored.payload).unwrap(),
        );
        assert_eq!(
            serde_json::to_value(&prepared.envelope.provenance.event).unwrap(),
            serde_json::to_value(&authored.envelope.provenance.event).unwrap(),
        );
        assert_eq!(
            prepared
                .envelope
                .observability
                .unwrap()
                .runtime
                .unwrap()
                .in_flight,
            Some(99),
        );
        journal
            .configure(obzenflow_core::journal::JournalConfig {
                observability: policy,
            })
            .unwrap();
        independent
            .configure(obzenflow_core::journal::JournalConfig {
                observability: policy,
            })
            .unwrap();
        let original = event(stage, 4);
        let selected = journal
            .append(original.clone(), Default::default())
            .await
            .unwrap();
        assert!(selected.envelope.observability.is_some());
        let denied = journal
            .clone()
            .append(original.clone(), Default::default())
            .await
            .unwrap();
        assert!(
            denied.envelope.observability.is_none(),
            "shared handle cannot bypass the journal gate"
        );
        assert_eq!(
            serde_json::to_value(&selected.payload).unwrap(),
            serde_json::to_value(&denied.payload).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&selected.envelope.provenance.event).unwrap(),
            serde_json::to_value(&denied.envelope.provenance.event).unwrap()
        );
        assert!(independent
            .append(original.clone(), Default::default())
            .await
            .unwrap()
            .envelope
            .observability
            .is_some());

        let captures = Arc::new(AtomicUsize::new(0));
        let capture = |count: Arc<AtomicUsize>| {
            JournalCapture::Live(Some(Box::new(move |_, event: &ChainEvent| {
                count.fetch_add(1, Ordering::SeqCst);
                event.envelope.observability.clone()
            })))
        };
        journal
            .append(
                original.clone(),
                obzenflow_core::journal::AppendOptions::default()
                    .with_capture(capture(captures.clone())),
            )
            .await
            .unwrap();
        assert_eq!(captures.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_millis(249)).await;
        assert!(journal
            .append(original.clone(), Default::default())
            .await
            .unwrap()
            .envelope
            .observability
            .is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        journal
            .append(
                original.clone(),
                obzenflow_core::journal::AppendOptions::default()
                    .with_capture(capture(captures.clone())),
            )
            .await
            .unwrap();
        assert_eq!(captures.load(Ordering::SeqCst), 1);

        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(250)).await;
            let records = journal
                .append_group(
                    "atomic",
                    vec![original.clone(); 3],
                    obzenflow_core::journal::AppendOptions::default()
                        .with_capture(capture(captures.clone())),
                )
                .await
                .unwrap();
            assert_eq!(records.len(), 3);
            assert_eq!(
                records
                    .iter()
                    .filter(|record| record.envelope.observability.is_some())
                    .count(),
                1
            );
        }
        assert_eq!(captures.load(Ordering::SeqCst), 5);
        tokio::time::advance(Duration::from_secs(60)).await;
        // Idle time gives one allowance, not a burst of accumulated credit.
        assert!(journal
            .append(original.clone(), Default::default())
            .await
            .unwrap()
            .envelope
            .observability
            .is_some());
        assert!(journal
            .append(original.clone(), Default::default())
            .await
            .unwrap()
            .envelope
            .observability
            .is_none());
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(journal
                .append(original.clone(), Default::default())
                .await
                .unwrap()
                .envelope
                .observability
                .is_some());
        }
        let historical = journal
            .append(
                original.clone(),
                obzenflow_core::journal::AppendOptions::default()
                    .with_capture(JournalCapture::Historical),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(historical.envelope.observability).unwrap(),
            serde_json::to_value(&original.envelope.observability).unwrap()
        );
        assert!(journal
            .append(original.clone(), Default::default())
            .await
            .unwrap()
            .envelope
            .observability
            .is_none());
        tokio::time::advance(Duration::from_millis(250)).await;
        assert!(journal
            .append_group("", vec![original.clone()], Default::default())
            .await
            .is_err());
        assert!(
            journal
                .append(original, Default::default())
                .await
                .unwrap()
                .envelope
                .observability
                .is_some(),
            "known non-commit releases the allowance"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sparse_memory_journals_share_only_their_own_allowance() {
        let stage = StageId::new();
        conformance(
            Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage))),
            Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage))),
            stage,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn sparse_disk_journals_share_only_their_own_allowance() {
        let directory = tempfile::tempdir().unwrap();
        let stage = StageId::new();
        conformance(
            Arc::new(
                DiskJournal::with_owner(
                    directory.path().join("data.log"),
                    JournalOwner::stage(stage),
                )
                .unwrap(),
            ),
            Arc::new(
                DiskJournal::with_owner(
                    directory.path().join("errors.log"),
                    JournalOwner::stage(stage),
                )
                .unwrap(),
            ),
            stage,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn sparse_reservation_cools_down_from_commit_and_keeps_indeterminate_allowance() {
        let gate = JournalObservability::default();
        gate.configure(ObservabilityPolicy::Periodic {
            interval: Duration::from_millis(250),
        })
        .unwrap();
        let stage = StageId::new();
        let (events, reservation) = gate.prepare(vec![event(stage, 1)], JournalCapture::default());
        tokio::time::advance(Duration::from_secs(10)).await;
        let (denied, _) = gate.prepare(vec![event(stage, 2)], JournalCapture::default());
        assert!(denied[0].envelope.observability.is_none());
        let journal = MemoryJournal::with_owner(JournalOwner::stage(stage));
        let result = journal
            .append_group("commit", events, Default::default())
            .await;
        reservation.finish::<ChainEvent>(&result);
        let (denied, _) = gate.prepare(vec![event(stage, 3)], JournalCapture::default());
        assert!(denied[0].envelope.observability.is_none());
        tokio::time::advance(Duration::from_millis(250)).await;
        let (_, reservation) = gate.prepare(vec![event(stage, 4)], JournalCapture::default());
        reservation.finish::<ChainEvent>(&Err(JournalError::CommitIndeterminate {
            source: "uncertain commit".into(),
        }));
        tokio::time::advance(Duration::from_secs(100)).await;
        let (denied, _) = gate.prepare(vec![event(stage, 5)], JournalCapture::default());
        assert!(denied[0].envelope.observability.is_none());
    }

    #[tokio::test]
    async fn optional_capture_panic_keeps_the_fact_append_successful() {
        let stage = StageId::new();
        let journal = MemoryJournal::with_owner(JournalOwner::stage(stage));
        let original = event(stage, 1);
        let record = journal
            .append(
                original.clone(),
                obzenflow_core::journal::AppendOptions::default().with_capture(
                    JournalCapture::Live(Some(Box::new(|_, _| panic!("diagnostic failure")))),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(record.authored()).unwrap(),
            serde_json::to_value(original).unwrap()
        );
    }
}
