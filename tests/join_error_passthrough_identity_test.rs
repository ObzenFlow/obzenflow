// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095d adjacency: error-passthrough through a join with foreign
//! authorship on the forwarded row.
//!
//! A business error (Validation/Domain) stays in the main pipeline:
//! `mark_as_error` preserves the input event's `writer_id`, so the error row
//! in the validator's journal names the original author, not the validator.
//! The join's stream side then consumes a data row whose `writer_id` is not
//! its direct upstream. Edge identity for acks, pending-ack routing, and
//! heartbeat attribution must come from the reader slot
//! (`last_delivered_upstream_stage()`), never from the event author; the
//! subscription-level contract is pinned in
//! `delivered_upstream_identity_ignores_event_writer`. This test pins the
//! join arm end to end: the error row traverses the join's passthrough path
//! and every valid row still joins, asserted from the event-sourced journals.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, JoinHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
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

/// Reference source: four catalog rows. The reference validator below marks
/// the final row as an in-band business error.
#[derive(Clone, Debug)]
struct RefSource {
    next_index: usize,
    writer_id: WriterId,
}

impl RefSource {
    fn new() -> Self {
        Self {
            next_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for RefSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_index >= 4 {
            return Ok(None);
        }
        self.next_index += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            RefItem::EVENT_TYPE,
            json!(RefItem {
                key: format!("r{}", self.next_index),
                label: format!("label-{}", self.next_index),
            }),
        )]))
    }
}

/// Stream source: five rows, keys cycling r1..r3, values 1..=5.
#[derive(Clone, Debug)]
struct StreamSource {
    next_index: usize,
    writer_id: WriterId,
}

impl StreamSource {
    fn new() -> Self {
        Self {
            next_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for StreamSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_index >= 5 {
            return Ok(None);
        }
        self.next_index += 1;
        let key = format!("r{}", ((self.next_index - 1) % 3) + 1);
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            StreamItem::EVENT_TYPE,
            json!(StreamItem {
                key,
                value: self.next_index as u64,
            }),
        )]))
    }
}

/// Validation stage: rejects value 3 with a Domain error (in-band routing per
/// FLOWIP error classification), re-emits everything else.
#[derive(Clone, Debug)]
struct Validator {
    writer_id: WriterId,
}

impl Validator {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for Validator {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let Some(item) = StreamItem::from_event(&event) else {
            return Ok(Vec::new());
        };
        if item.value == 3 {
            return Err(HandlerError::Domain(format!(
                "rejected stream value {}",
                item.value
            )));
        }
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            StreamItem::EVENT_TYPE,
            json!(item),
            obzenflow_core::config::LineagePolicy::default(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct RefValidator {
    writer_id: WriterId,
}

impl RefValidator {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for RefValidator {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let Some(item) = RefItem::from_event(&event) else {
            return Ok(Vec::new());
        };
        if item.key == "r4" {
            return Err(HandlerError::Domain(
                "rejected reference value 4".to_string(),
            ));
        }
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            RefItem::EVENT_TYPE,
            json!(item),
            obzenflow_core::config::LineagePolicy::default(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DropSink;

#[async_trait]
impl SinkHandler for DropSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> Result<obzenflow_core::event::payloads::delivery_payload::DeliveryPayload, HandlerError>
    {
        Ok(
            obzenflow_core::event::payloads::delivery_payload::DeliveryPayload::success(
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        )
    }

    // Deterministic local drop: re-delivery under either archive verb is safe.
    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

#[derive(Clone, Debug, Default)]
struct GuardedJoinState {
    references: HashMap<String, RefItem>,
}

#[derive(Clone, Debug)]
struct GuardedInnerJoin {
    unexpected_error_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl JoinHandler for GuardedInnerJoin {
    type State = GuardedJoinState;

    fn initial_state(&self) -> Self::State {
        GuardedJoinState::default()
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            self.unexpected_error_calls.fetch_add(1, Ordering::SeqCst);
            return Err(HandlerError::Other(
                "join handler received a pre-error row".to_string(),
            ));
        }

        if let Some(reference) = RefItem::from_event(&event) {
            state.references.insert(reference.key.clone(), reference);
            return Ok(Vec::new());
        }

        let Some(stream) = StreamItem::from_event(&event) else {
            return Ok(Vec::new());
        };
        let Some(reference) = state.references.get(&stream.key) else {
            return Ok(Vec::new());
        };
        let joined = JoinedItem {
            key: stream.key,
            label: reference.label.clone(),
            value: stream.value,
        };
        Ok(vec![ChainEventFactory::derived_data_event(
            writer_id,
            &event,
            JoinedItem::EVENT_TYPE,
            json!(joined),
            obzenflow_core::config::LineagePolicy::default(),
        )])
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

fn build_flow(journal_base: PathBuf, unexpected_error_calls: Arc<AtomicUsize>) -> FlowDefinition {
    flow! {
        name: "join_error_passthrough_identity",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            ref_src = source!(RefItem => RefSource::new());
            ref_validator = transform!(RefItem -> RefItem => RefValidator::new());
            stream_src = source!(StreamItem => StreamSource::new());
            validator = transform!(StreamItem -> StreamItem => Validator::new());
            joined = join!(catalog ref_validator: RefItem, StreamItem -> JoinedItem => GuardedInnerJoin {
                unexpected_error_calls,
            });
            collector = sink!(JoinedItem => DropSink);
        },

        topology: {
            ref_src |> ref_validator;
            stream_src |> validator;
            validator |> joined;
            joined |> collector;
        }
    }
}

fn is_error_row(envelope: &obzenflow_core::event::EventEnvelope<ChainEvent>) -> bool {
    matches!(
        envelope.event.processing_info.status,
        ProcessingStatus::Error { .. }
    )
}

#[tokio::test]
async fn join_forwards_error_row_with_foreign_author_and_joins_the_rest() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let unexpected_error_calls = Arc::new(AtomicUsize::new(0));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            unexpected_error_calls.clone(),
        ))
        .await
        .expect("flow with an in-band business error must complete");

    assert_eq!(
        unexpected_error_calls.load(Ordering::SeqCst),
        0,
        "pre-error rows from both join sides must bypass the join handler"
    );

    let run_dir = replay_testkit::latest_run_dir(&journal_base);

    // Premise: the validator's journal carries the error row with the
    // ORIGINAL author's writer_id (mark_as_error preserves authorship), so
    // the join's stream side really consumes a foreign-writer data row.
    let stream_rows = replay_testkit::read_stage_envelopes_appended(&run_dir, "stream_src").await;
    let source_writer = stream_rows
        .iter()
        .find(|envelope| envelope.event.is_data())
        .map(|envelope| envelope.event.writer_id)
        .expect("stream source journal must contain data rows");

    let validator_rows = replay_testkit::read_stage_envelopes_appended(&run_dir, "validator").await;
    let error_rows: Vec<_> = validator_rows
        .iter()
        .filter(|envelope| envelope.event.is_data() && is_error_row(envelope))
        .collect();
    assert_eq!(
        error_rows.len(),
        1,
        "exactly one stream row is rejected by validation"
    );
    assert_eq!(
        error_rows[0].event.writer_id, source_writer,
        "premise: the error row preserves the original author's writer_id"
    );
    let valid_writer = validator_rows
        .iter()
        .find(|envelope| envelope.event.is_data() && !is_error_row(envelope))
        .map(|envelope| envelope.event.writer_id)
        .expect("validator journal must contain re-emitted valid rows");
    assert_ne!(
        error_rows[0].event.writer_id, valid_writer,
        "premise: the forwarded error row's author differs from the validator's own writer"
    );

    // The join consumed the foreign-writer row through the error-passthrough
    // arm and every valid row still joined: four joined outputs (values
    // 1, 2, 4, 5), asserted from the join's journal.
    let join_rows = replay_testkit::read_stage_envelopes_appended(&run_dir, "joined").await;
    let mut joined_values: Vec<u64> = join_rows
        .iter()
        .filter(|envelope| !is_error_row(envelope))
        .filter_map(|envelope| JoinedItem::from_event(&envelope.event))
        .map(|item| item.value)
        .collect();
    joined_values.sort_unstable();
    assert_eq!(
        joined_values,
        vec![1, 2, 4, 5],
        "every valid stream row joins; the rejected row does not"
    );

    // The error row itself is forwarded through the join (business errors
    // stay in the main pipeline as the durable record).
    let forwarded_stream_errors: Vec<_> = join_rows
        .iter()
        .filter(|envelope| envelope.event.is_data() && is_error_row(envelope))
        .filter(|envelope| StreamItem::from_event(&envelope.event).is_some())
        .collect();
    assert_eq!(
        forwarded_stream_errors.len(),
        1,
        "the join forwards the error-marked row downstream"
    );
    assert_eq!(
        StreamItem::from_event(&forwarded_stream_errors[0].event)
            .expect("forwarded error row keeps its payload")
            .value,
        3,
        "the forwarded error row is the rejected stream item"
    );

    let forwarded_reference_errors: Vec<_> = join_rows
        .iter()
        .filter(|envelope| envelope.event.is_data() && is_error_row(envelope))
        .filter(|envelope| RefItem::from_event(&envelope.event).is_some())
        .collect();
    assert_eq!(
        forwarded_reference_errors.len(),
        1,
        "the join forwards a pre-error reference row without hydrating it"
    );
}
