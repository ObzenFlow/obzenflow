// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A validator on either side of a join records each row it rejects in its
//! own error journal, so the join receives only valid rows and joins them all.

use crate::replay_testkit;
use obzenflow_core::event::{ChainPayload, JournalRecord};

use obzenflow::stages::sources::ValuesSource;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    JoinReferenceView, TypedFiniteSourceHandler, TypedJoinHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn reference_rows() -> impl TypedFiniteSourceHandler<Output = RefItem> {
    ValuesSource::new(
        (1..=4)
            .map(|index| RefItem {
                key: format!("r{index}"),
                label: format!("label-{index}"),
            })
            .collect::<Vec<_>>(),
    )
}

fn stream_rows() -> impl TypedFiniteSourceHandler<Output = StreamItem> {
    ValuesSource::new(
        (1..=5_u64)
            .map(|index| StreamItem {
                key: format!("r{}", ((index - 1) % 3) + 1),
                value: index,
            })
            .collect::<Vec<_>>(),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefItem {
    key: String,
    label: String,
}

impl TypedPayload for RefItem {
    const EVENT_TYPE: &'static str = "join_err.ref";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamItem {
    key: String,
    value: u64,
}

impl TypedPayload for StreamItem {
    const EVENT_TYPE: &'static str = "join_err.stream";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JoinedItem {
    key: String,
    label: String,
    value: u64,
}

impl TypedPayload for JoinedItem {
    const EVENT_TYPE: &'static str = "join_err.joined";
}

/// Validation stage: rejects value 3 with a Domain error, re-emits everything
/// else.
#[derive(Clone, Debug)]
struct Validator;

impl Validator {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for Validator {
    type Input = StreamItem;
    type Output = StreamItem;

    fn process(&self, item: StreamItem) -> Result<StreamItem, HandlerError> {
        if item.value == 3 {
            return Err(HandlerError::Domain(format!(
                "rejected stream value {}",
                item.value
            )));
        }
        Ok(item)
    }
}

#[derive(Clone, Debug)]
struct RefValidator;

impl RefValidator {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for RefValidator {
    type Input = RefItem;
    type Output = RefItem;

    fn process(&self, item: RefItem) -> Result<RefItem, HandlerError> {
        if item.key == "r4" {
            return Err(HandlerError::Domain(
                "rejected reference value 4".to_string(),
            ));
        }
        Ok(item)
    }
}

type DropSink = obzenflow::stages::sinks::DiscardSink<JoinedItem>;

#[derive(Clone, Debug)]
struct GuardedInnerJoin {
    typed_handler_calls: Arc<AtomicUsize>,
}

impl TypedJoinHandler for GuardedInnerJoin {
    type State = ();
    type ReferenceKey = String;
    type Reference = RefItem;
    type Stream = StreamItem;
    type Output = JoinedItem;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, reference: &Self::Reference) -> Result<String, HandlerError> {
        self.typed_handler_calls.fetch_add(1, Ordering::SeqCst);
        Ok(reference.key.clone())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, String, RefItem>,
        stream: StreamItem,
    ) -> Result<Vec<JoinedItem>, HandlerError> {
        self.typed_handler_calls.fetch_add(1, Ordering::SeqCst);
        let Some(reference) = references.select(&stream.key) else {
            return Ok(Vec::new());
        };
        Ok(vec![JoinedItem {
            key: stream.key,
            label: reference.label,
            value: stream.value,
        }])
    }
}

fn build_flow(journal_base: PathBuf, typed_handler_calls: Arc<AtomicUsize>) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let reference_source = reference_rows();
        let reference_validator = RefValidator::new();
        let stream_source = stream_rows();
        let stream_validator = Validator::new();
        let joined = GuardedInnerJoin {
            typed_handler_calls,
        };
        let collector = DropSink::new();

        Ok(flow! {
            name: "join_error_passthrough_identity",
            journals: disk_journals(journal_base),

            stages: {
                ref_src = source!(RefItem => reference_source);
                ref_validator = transform!(RefItem -> RefItem => reference_validator);
                stream_src = source!(StreamItem => stream_source);
                validator = transform!(StreamItem -> StreamItem => stream_validator);
                joined = join!(catalog ref_validator: RefItem, StreamItem -> JoinedItem => joined);
                collector = sink!(JoinedItem => collector);
            },

            topology: {
                ref_src |> ref_validator;
                stream_src |> validator;
                (ref_validator, validator) |> joined;
                joined |> collector;
            }
        })
    })
}

fn is_error_row(envelope: &JournalRecord<ChainPayload>) -> bool {
    matches!(
        envelope.envelope.provenance.event.processing.status,
        ProcessingStatus::Error { .. }
    )
}

#[tokio::test]
async fn a_join_never_receives_a_rejected_row_and_joins_the_rest() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let typed_handler_calls = Arc::new(AtomicUsize::new(0));

    replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            typed_handler_calls.clone(),
        ))
        .await
        .expect("flow with a rejected row on each join side must complete");

    assert_eq!(
        typed_handler_calls.load(Ordering::SeqCst),
        7,
        "only three valid reference rows and four valid stream rows may reach the typed handler"
    );

    let run_dir = replay_testkit::latest_run_dir(&journal_base);
    for (stage, rejected, kind_detail) in [
        ("validator", "stream_src", "rejected stream value 3"),
        ("ref_validator", "ref_src", "rejected reference value 4"),
    ] {
        let data = replay_testkit::read_stage_envelopes_appended(&run_dir, stage).await;
        assert!(
            !data.iter().any(is_error_row),
            "{stage}'s data journal holds no error-marked row"
        );
        let failures = replay_testkit::processing_failures(&run_dir, stage).await;
        let [(record, failure)] = failures.as_slice() else {
            panic!("{stage} records exactly one rejection: {failures:?}");
        };
        assert_eq!(
            record.envelope.provenance.event.writer_id,
            replay_testkit::stage_writer(&run_dir, stage),
            "the validator is the record's author"
        );
        assert_eq!(failure.kind, ErrorKind::Domain);
        assert_eq!(failure.detail, kind_detail);
        let source_rows = replay_testkit::read_stage_envelopes_appended(&run_dir, rejected).await;
        let input = source_rows
            .iter()
            .find(|row| {
                row.envelope.provenance.event.id
                    == record.envelope.provenance.event.causality.parent_ids[0]
            })
            .expect("the record's parent is the rejected input");
        assert_eq!(failure.input.payload, input.payload());
    }

    let join_rows = replay_testkit::read_stage_envelopes_appended(&run_dir, "joined").await;
    assert!(!join_rows.iter().any(is_error_row));
    let mut joined_values: Vec<u64> = join_rows
        .iter()
        .filter_map(|envelope| JoinedItem::from_event(&envelope.authored()))
        .map(|item| item.value)
        .collect();
    joined_values.sort_unstable();
    assert_eq!(
        joined_values,
        vec![1, 2, 4, 5],
        "every valid stream row joins; the rejected row does not"
    );
    assert!(
        replay_testkit::read_stage_error_envelopes(&run_dir, "joined")
            .await
            .is_empty(),
        "the join rejects nothing it never received"
    );
}

#[derive(Clone, Debug)]
struct RejectingJoin;

impl TypedJoinHandler for RejectingJoin {
    type State = ();
    type ReferenceKey = String;
    type Reference = RefItem;
    type Stream = StreamItem;
    type Output = JoinedItem;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, reference: &Self::Reference) -> Result<String, HandlerError> {
        Ok(reference.key.clone())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, String, RefItem>,
        stream: StreamItem,
    ) -> Result<Vec<JoinedItem>, HandlerError> {
        if stream.value == 2 {
            return Err(HandlerError::Domain("stream value 2 has no match".into()));
        }
        let Some(reference) = references.select(&stream.key) else {
            return Ok(Vec::new());
        };
        Ok(vec![JoinedItem {
            key: stream.key,
            label: reference.label,
            value: stream.value,
        }])
    }
}

#[tokio::test]
async fn a_join_records_the_stream_row_its_handler_rejects() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let journals = journal_base.clone();
    replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let reference_source = reference_rows();
            let stream_source = stream_rows();
            let collector = DropSink::new();
            Ok(flow! {
                name: "join_rejects_a_stream_row",
                journals: disk_journals(journals.clone()),

                stages: {
                    ref_src = source!(RefItem => reference_source);
                    stream_src = source!(StreamItem => stream_source);
                    joined = join!(catalog ref_src: RefItem, StreamItem -> JoinedItem => RejectingJoin);
                    collector = sink!(JoinedItem => collector);
                },

                topology: {
                    (ref_src, stream_src) |> joined;
                    joined |> collector;
                }
            })
        }))
        .await
        .expect("a join that rejects one row completes");

    let run_dir = replay_testkit::latest_run_dir(&journal_base);
    let join_rows = replay_testkit::read_stage_envelopes_appended(&run_dir, "joined").await;
    assert!(!join_rows.iter().any(is_error_row));
    let mut joined_values: Vec<u64> = join_rows
        .iter()
        .filter_map(|envelope| JoinedItem::from_event(&envelope.authored()))
        .map(|item| item.value)
        .collect();
    joined_values.sort_unstable();
    assert_eq!(joined_values, vec![1, 3, 4, 5]);

    let failures = replay_testkit::processing_failures(&run_dir, "joined").await;
    let [(record, failure)] = failures.as_slice() else {
        panic!("the join records its one rejection: {failures:?}");
    };
    assert_eq!(
        record.envelope.provenance.event.writer_id,
        replay_testkit::stage_writer(&run_dir, "joined")
    );
    assert_eq!(failure.kind, ErrorKind::Domain);
    assert_eq!(failure.detail, "stream value 2 has no match");
    assert_eq!(failure.input.event_type.0, StreamItem::EVENT_TYPE);
    let stream_rows = replay_testkit::read_stage_envelopes_appended(&run_dir, "stream_src").await;
    let input = stream_rows
        .iter()
        .find(|row| StreamItem::from_event(&row.authored()).is_some_and(|item| item.value == 2))
        .expect("the rejected stream row");
    assert_eq!(
        record.envelope.provenance.event.causality.parent_ids[0],
        input.envelope.provenance.event.id
    );
}
