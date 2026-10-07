// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Instrument, Metadata, Subscriber};

struct ObservedSpan {
    name: &'static str,
    parent: Option<u64>,
    enters: usize,
    exits: usize,
}

#[derive(Default)]
struct ObservedSpans {
    spans: Vec<ObservedSpan>,
    entered: Vec<u64>,
}

struct CaptureSpans(Arc<Mutex<ObservedSpans>>);

impl Subscriber for CaptureSpans {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "obzenflow::performance"
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut observed = self.0.lock().unwrap();
        let parent = attributes.parent().map(Id::into_u64).or_else(|| {
            attributes
                .is_contextual()
                .then(|| observed.entered.last().copied())
                .flatten()
        });
        observed.spans.push(ObservedSpan {
            name: attributes.metadata().name(),
            parent,
            enters: 0,
            exits: 0,
        });
        Id::from_u64(observed.spans.len() as u64)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        let mut observed = self.0.lock().unwrap();
        observed.spans[span.into_u64() as usize - 1].enters += 1;
        observed.entered.push(span.into_u64());
    }

    fn exit(&self, span: &Id) {
        let mut observed = self.0.lock().unwrap();
        assert_eq!(observed.entered.pop(), Some(span.into_u64()));
        observed.spans[span.into_u64() as usize - 1].exits += 1;
    }
}

struct GatedReader {
    release: tokio::sync::oneshot::Receiver<()>,
    event: Option<JournalRecord<ChainPayload>>,
}

#[async_trait]
impl obzenflow_core::journal::JournalStorageReader<ChainEvent> for GatedReader {
    async fn storage_next(&mut self) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        if self.event.is_some() {
            (&mut self.release)
                .await
                .expect("reader gate remains owned");
        }
        Ok(self.event.take())
    }

    fn storage_position(&self) -> u64 {
        u64::from(self.event.is_none())
    }

    fn storage_is_at_end(&self) -> bool {
        self.event.is_none()
    }
}

#[tokio::test]
async fn subscription_read_spans_preserve_parent_and_exit_while_pending() {
    for selection in [
        ReaderSelectionPolicy::AvailabilityRoundRobin,
        ReaderSelectionPolicy::CanonicalMerge,
    ] {
        let upstream = StageId::new();
        let journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(upstream)));
        let event = ChainEventFactory::data_event(
            upstream.into(),
            "test.payload",
            std::num::NonZeroU32::MIN,
            json!({"value": 1}),
        );
        let record = journal.append(event, Default::default()).await.unwrap();
        let expected_id = *record.id();
        let mut subscription = UpstreamSubscription::new_with_names(
            "instrumented_consumer",
            &[(upstream, "upstream".into(), journal)],
        )
        .await
        .unwrap()
        .with_reader_selection(selection);
        let (release, gate) = tokio::sync::oneshot::channel();
        subscription.readers[0].reader = Box::new(GatedReader {
            release: gate,
            event: Some(record),
        });
        let observed = Arc::new(Mutex::new(ObservedSpans::default()));
        let dispatch = tracing::Dispatch::new(CaptureSpans(observed.clone()));
        let parent = tracing::dispatcher::with_default(
            &dispatch,
            || tracing::debug_span!(target: "obzenflow::performance", "test_supervisor"),
        );
        let mut operation = Box::pin(
            subscription
                .poll_next_with_state("Running", None)
                .instrument(parent)
                .with_subscriber(dispatch),
        );

        assert!(futures::poll!(&mut operation).is_pending());
        {
            let captured = observed.lock().unwrap();
            assert!(captured.entered.is_empty(), "Pending must exit every span");
            assert_eq!(
                captured
                    .spans
                    .iter()
                    .filter(|span| span.name == "subscription_journal_next")
                    .count(),
                1,
                "one suspended read owns one span"
            );
        }
        release.send(()).unwrap();
        let PollResult::Event(delivered) = operation.as_mut().await else {
            panic!("resumed subscription must deliver its gated input");
        };
        assert_eq!(*delivered.id(), expected_id);
        drop(operation);

        let captured = observed.lock().unwrap();
        assert!(captured.entered.is_empty());
        assert!(captured.spans.iter().all(|span| span.enters == span.exits));
        let (read_index, read) = captured
            .spans
            .iter()
            .enumerate()
            .find(|(_, span)| span.name == "subscription_journal_next")
            .unwrap();
        assert!(read.enters >= 2, "the same read span resumes after Pending");
        let mut path = Vec::new();
        let mut cursor = Some(read_index as u64 + 1);
        while let Some(id) = cursor {
            let span = &captured.spans[id as usize - 1];
            path.push(span.name);
            cursor = span.parent;
        }
        path.reverse();
        let mut expected = vec!["test_supervisor", "subscription_poll"];
        match selection {
            ReaderSelectionPolicy::AvailabilityRoundRobin => {
                expected.push("subscription_round_robin");
            }
            ReaderSelectionPolicy::CanonicalMerge => expected.extend([
                "subscription_canonical_merge",
                "subscription_merge_candidate",
                "subscription_refill_head",
            ]),
        }
        expected.extend(["subscription_read_step", "subscription_journal_next"]);
        assert_eq!(path, expected);
    }
}
