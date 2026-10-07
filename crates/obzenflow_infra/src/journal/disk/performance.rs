// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Preserve diagnostic ancestry across the existing blocking-pool boundary.

#[tracing::instrument(
    target = "obzenflow::performance",
    level = "debug",
    name = "disk_journal_blocking_dispatch",
    skip_all
)]
pub(super) async fn blocking<R, F>(work: F) -> Result<R, tokio::task::JoinError>
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    if !tracing::span_enabled!(target: "obzenflow::performance", tracing::Level::DEBUG) {
        return tokio::task::spawn_blocking(work).await;
    }
    let parent = tracing::Span::current();
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    tokio::task::spawn_blocking(move || {
        tracing::dispatcher::with_default(&dispatch, || {
            // Created on the worker so this span excludes pool queue time.
            // Its parent includes queueing, work, and resuming the async caller.
            tracing::debug_span!(
                target: "obzenflow::performance",
                parent: &parent,
                "disk_journal_blocking_work"
            )
            .in_scope(work)
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::super::DiskJournal;
    use obzenflow_core::event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::journal::Journal;
    use obzenflow_core::{JournalOwner, StageId, WriterId};
    use std::sync::{Arc, Mutex};
    use tracing::{instrument::WithSubscriber, Instrument, Subscriber};
    use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan, Layer};

    struct Paths(Arc<Mutex<Vec<Vec<&'static str>>>>);

    impl<S: Subscriber + for<'lookup> LookupSpan<'lookup>> Layer<S> for Paths {
        fn on_new_span(
            &self,
            _: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            context: Context<'_, S>,
        ) {
            let path = context
                .span(id)
                .unwrap()
                .scope()
                .from_root()
                .map(|span| span.metadata().name())
                .collect();
            self.0.lock().unwrap().push(path);
        }
    }

    #[tokio::test]
    async fn disk_read_write_and_codec_keep_ancestry_across_tasks() {
        let paths = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry()
            .with(Paths(paths.clone()))
            .with(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.is_span()
                    && metadata.target() == "obzenflow::performance"
                    && *metadata.level() <= tracing::Level::DEBUG
            }));
        async {
            let parent = tracing::debug_span!(target: "obzenflow::performance", "journal_test");
            async {
                let directory = tempfile::tempdir().unwrap();
                let stage = StageId::new();
                let journal = DiskJournal::<ChainEvent>::with_owner(
                    directory.path().join("journal.log"),
                    JournalOwner::stage(stage),
                )
                .unwrap();
                let event = ChainEventFactory::data_event(
                    WriterId::from(stage),
                    "test.performance",
                    std::num::NonZeroU32::MIN,
                    serde_json::json!({"value": 42}),
                );
                let written = journal.append(event, Default::default()).await.unwrap();
                let mut reader = journal.reader_from(0).await.unwrap();
                let read = reader.next().await.unwrap().unwrap();
                assert_eq!(read.id(), written.id());
            }
            .instrument(parent)
            .await;
        }
        .with_subscriber(subscriber)
        .await;

        let paths = paths.lock().unwrap();
        for expected in [
            vec![
                "journal_test",
                "disk_journal_append",
                "disk_journal_append_record",
                "disk_journal_codec_encode",
            ],
            vec![
                "journal_test",
                "disk_journal_append",
                "disk_journal_append_record",
                "disk_journal_blocking_dispatch",
                "disk_journal_blocking_work",
                "disk_journal_file_write",
            ],
            vec![
                "journal_test",
                "disk_journal_append",
                "disk_journal_append_record",
                "disk_journal_blocking_dispatch",
                "disk_journal_blocking_work",
                "disk_journal_file_flush",
            ],
            vec![
                "journal_test",
                "disk_journal_read_next",
                "disk_journal_buffered_read_async",
            ],
            vec![
                "journal_test",
                "disk_journal_read_next",
                "disk_journal_blocking_dispatch",
                "disk_journal_blocking_work",
                "disk_journal_frame_classify",
                "disk_journal_codec_decode",
            ],
        ] {
            assert!(
                paths.contains(&expected),
                "missing performance path {expected:?}; observed {paths:?}"
            );
        }
        for child in [
            "disk_journal_codec_encode_provenance",
            "disk_journal_codec_encode_observability",
            "disk_journal_codec_encode_payload_json",
            "disk_journal_codec_encode_frame",
        ] {
            let expected = vec![
                "journal_test",
                "disk_journal_append",
                "disk_journal_append_record",
                "disk_journal_codec_encode",
                child,
            ];
            assert!(
                paths.contains(&expected),
                "missing encoder child {expected:?}"
            );
        }
        for child in [
            "disk_journal_codec_decode_routing",
            "disk_journal_codec_decode_provenance",
            "disk_journal_codec_decode_observability",
            "disk_journal_codec_decode_payload_json",
            "disk_journal_codec_decode_typed_payload",
            "disk_journal_codec_decode_record",
        ] {
            let expected = vec![
                "journal_test",
                "disk_journal_read_next",
                "disk_journal_blocking_dispatch",
                "disk_journal_blocking_work",
                "disk_journal_frame_classify",
                "disk_journal_codec_decode",
                child,
            ];
            assert!(
                paths.contains(&expected),
                "missing decoder child {expected:?}"
            );
        }
        assert!(paths.iter().any(|path| path.ends_with(&[
            "disk_journal_definition_publish",
            "disk_journal_definition_metadata",
        ])));
        assert!(paths
            .iter()
            .any(|path| path.last() == Some(&"disk_journal_archive_canonicalize")));
    }
}
