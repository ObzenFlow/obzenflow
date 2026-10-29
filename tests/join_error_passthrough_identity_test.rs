// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A validator on either side of a join records each row it rejects in its
//! own error journal, so the join receives only valid rows and joins them all.

use obzenflow_core::event::{ChainPayload, JournalRecord};
mod replay_testkit;

use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler,
    TypedJoinHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

/// Reference source: four catalog rows. The reference validator below rejects
/// the final row.
#[derive(Clone, Debug)]
struct RefSource {
    next_index: usize,
}

impl RefSource {
    fn new() -> Self {
        Self { next_index: 0 }
    }
}

impl TypedFiniteSourceHandler for RefSource {
    type Output = RefItem;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next_index >= 4 {
            return Ok(None);
        }
        self.next_index += 1;
        Ok(Some(vec![RefItem {
            key: format!("r{}", self.next_index),
            label: format!("label-{}", self.next_index),
        }]))
    }
}

/// Stream source: five rows, keys cycling r1..r3, values 1..=5.
#[derive(Clone, Debug)]
struct StreamSource {
    next_index: usize,
}

impl StreamSource {
    fn new() -> Self {
        Self { next_index: 0 }
    }
}

impl TypedFiniteSourceHandler for StreamSource {
    type Output = StreamItem;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next_index >= 5 {
            return Ok(None);
        }
        self.next_index += 1;
        let key = format!("r{}", ((self.next_index - 1) % 3) + 1);
        Ok(Some(vec![StreamItem {
            key,
            value: self.next_index as u64,
        }]))
    }
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

#[derive(Clone, Debug)]
struct DropSink;

#[async_trait]
impl InlineSink for DropSink {
    type Input = JoinedItem;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
        )
        .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _event: JoinedItem) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}

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
        let reference_source = RefSource::new();
        let reference_validator = RefValidator::new();
        let stream_source = StreamSource::new();
        let stream_validator = Validator::new();
        let joined = GuardedInnerJoin {
            typed_handler_calls,
        };
        let collector = DropSink;

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

    FlowApplication::builder()
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
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let reference_source = RefSource::new();
            let stream_source = StreamSource::new();
            let collector = DropSink;
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
