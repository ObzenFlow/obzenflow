// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::journal::{AppendOptions, JournalError, JournalReader, JournalStorage};
use obzenflow_core::{ChainEvent, EventId, JournalOwner, SystemId};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

#[derive(Default)]
struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

struct RecordedSpan {
    name: &'static str,
    parent: Option<u64>,
    fields: Fields,
}

#[derive(Default)]
struct Recording {
    spans: Vec<RecordedSpan>,
    entered: Vec<u64>,
}

struct Capture(Arc<Mutex<Recording>>);

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_span() && metadata.target() == "obzenflow::performance"
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut recording = self.0.lock().unwrap();
        let parent = attributes.parent().map(Id::into_u64).or_else(|| {
            attributes
                .is_contextual()
                .then(|| recording.entered.last().copied())
                .flatten()
        });
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        recording.spans.push(RecordedSpan {
            name: attributes.metadata().name(),
            parent,
            fields,
        });
        Id::from_u64(recording.spans.len() as u64)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        self.0.lock().unwrap().entered.push(span.into_u64());
    }

    fn exit(&self, span: &Id) {
        assert_eq!(self.0.lock().unwrap().entered.pop(), Some(span.into_u64()));
    }
}

struct GatedJournal {
    id: JournalId,
    started: tokio::sync::Notify,
    active: Arc<AtomicUsize>,
}

struct Reading(Arc<AtomicUsize>);

impl Drop for Reading {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl JournalStorage<ChainEvent> for GatedJournal {
    fn storage_id(&self) -> &JournalId {
        &self.id
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        None
    }

    async fn storage_append(
        &self,
        _: ChainEvent,
        _: AppendOptions<ChainEvent>,
    ) -> Result<JournalRecord<ChainPayload>, JournalError> {
        unreachable!("tail refresh must not append")
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        unreachable!("tail refresh must not scan")
    }

    async fn storage_read_event(
        &self,
        _: &EventId,
    ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        unreachable!("tail refresh uses its dedicated lookup")
    }

    async fn storage_reader_from(
        &self,
        _: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        unreachable!("tail refresh must not open a reader")
    }

    async fn storage_read_last_n(
        &self,
        _: usize,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        unreachable!("tail refresh must not search history")
    }

    async fn storage_read_metrics_tail(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        async {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _reading = Reading(self.active.clone());
            self.started.notify_one();
            std::future::pending().await
        }
        .instrument(
            tracing::debug_span!(target: "obzenflow::performance", "test_metrics_tail_read"),
        )
        .await
    }
}

#[tokio::test]
async fn tail_readers_keep_independent_span_identity_and_cancel_with_their_owner() {
    for enabled in [false, true] {
        for drop_owner in [false, true] {
            let recording = Arc::new(Mutex::new(Recording::default()));
            let dispatch = if enabled {
                tracing::Dispatch::new(Capture(recording.clone()))
            } else {
                tracing::Dispatch::none()
            };
            let writer = WriterId::from(SystemId::new());
            let journals = [(); 3].map(|()| {
                Arc::new(GatedJournal {
                    id: JournalId::new(),
                    started: Default::default(),
                    active: Arc::default(),
                })
            });
            let mut readers = tracing::dispatcher::with_default(&dispatch, || {
                tracing::debug_span!(target: "obzenflow::performance", "startup_inline_action")
                    .in_scope(|| {
                        let mut readers = TailReaders::default();
                        for (journal, kind) in journals.iter().zip([
                            Some(MetricsJournalKind::Data),
                            Some(MetricsJournalKind::Error),
                            None,
                        ]) {
                            readers.spawn(journal.clone(), Arc::default(), writer, kind, |_, _| {});
                        }
                        readers
                    })
            });
            // The caller's scoped dispatcher has ended before any reader runs.
            tokio::time::timeout(Duration::from_secs(2), async {
                for journal in &journals {
                    journal.started.notified().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(readers.len(), 3);
            assert!(journals
                .iter()
                .all(|j| j.active.load(Ordering::SeqCst) == 1));
            assert!(recording.lock().unwrap().entered.is_empty());
            if drop_owner {
                drop(readers);
            } else {
                readers.stop().await;
                assert_eq!(readers.len(), 0);
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                while journals
                    .iter()
                    .any(|j| j.active.load(Ordering::SeqCst) != 0)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let recording = recording.lock().unwrap();
            assert!(recording.entered.is_empty());
            if !enabled {
                assert!(recording.spans.is_empty());
                continue;
            }
            let children: Vec<_> = recording
                .spans
                .iter()
                .filter(|span| span.name == "test_metrics_tail_read")
                .collect();
            assert_eq!(children.len(), 3);
            let mut kinds = Vec::new();
            for child in children {
                let root = &recording.spans[child.parent.unwrap() as usize - 1];
                assert_eq!(root.name, "metrics_tail_reader");
                assert_eq!(root.parent, None, "reader must not inherit startup state");
                assert_eq!(root.fields.0["supervisor"], METRICS_NAME);
                assert_eq!(root.fields.0["supervisor_kind"], "MetricsAggregator");
                assert_eq!(root.fields.0["writer_id"], writer.to_string());
                assert_eq!(root.fields.0["supervision_mode"], "self_supervised");
                assert!(!root.fields.0.contains_key("state"));
                kinds.push(root.fields.0["journal_kind"].as_str());
            }
            kinds.sort_unstable();
            assert_eq!(kinds, ["data", "error", "system"]);
        }
    }
}
