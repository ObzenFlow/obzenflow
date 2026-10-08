// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
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
async fn pending_subscription_read_resumes_with_the_same_record() {
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
            "consumer",
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
        let mut operation = Box::pin(subscription.poll_next_with_state("Running", None));

        assert!(futures::poll!(&mut operation).is_pending());
        release.send(()).unwrap();
        let PollResult::Event(delivered) = operation.as_mut().await else {
            panic!("resumed subscription must deliver its gated input");
        };
        assert_eq!(*delivered.id(), expected_id);
        drop(operation);
    }
}
