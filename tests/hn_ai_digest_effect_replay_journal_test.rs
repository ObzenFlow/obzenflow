// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit;

#[path = "../examples/hn_ai_digest_demo/config.rs"]
mod config;
#[path = "../examples/hn_ai_digest_demo/decoder.rs"]
mod decoder;
#[path = "../examples/hn_ai_digest_demo/domain.rs"]
mod domain;
#[path = "test_support/exported_jsonl.rs"]
mod exported_jsonl;
#[path = "support/fake_ai.rs"]
pub mod fake_ai;
#[path = "../examples/hn_ai_digest_demo/flow.rs"]
mod hn_demo_flow;
#[path = "../examples/hn_ai_digest_demo/mock_server.rs"]
mod mock_server;
#[cfg(feature = "e2e")]
#[path = "support/postgres.rs"]
mod postgres_support;
#[path = "../examples/hn_ai_digest_demo/util.rs"]
mod util;

use fake_ai::{
    build_context_with, chat_model, enforced_backpressure, last_user_message, FakeChatModels,
};
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec, Chunk, Partials,
};
use obzenflow_adapters::middleware::{circuit_breaker, CircuitBreaker};
use obzenflow_core::ai::{
    AiClientError, AiMapReduceChunkFailed, AiMapReduceGroupingEnded, AiMapReduceMapInput,
    AiMapReduceReduceFailed, AiMapReduceRoleFailure, AiMapReduceTaggedPartial,
    AiMapReduceWindowFailed, AiProviderFailureKind, AiUnavailableCause, ChatTarget,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::observability::ObservationRecord;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, CircuitBreakerHealthClassification, ExecutionPayload, RecoveryFact,
};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{
    ChainPayload, EffectAttemptStarted, EffectFailureDetail, EffectOutcomePayload,
    EffectRecoveryAbandoned,
};
use obzenflow_core::{id::StageId, EventId, TypedPayload, WriterId};
use obzenflow_dsl::{ai, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::{FlowApplication, FlowApplicationBuilder};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::effects::{SinkRedeliverySafety, EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::source::{
    FiniteSourceConnector, PositionedFiniteReader, Seek, SeekableSource, SourceInputIdentity,
    SourcePosition, SourceReaderInitContext, SourceRecord,
};
use obzenflow_runtime::testing::BackpressureAckGate;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const CHAT_COMPLETION_EFFECT_TYPE: &str = "obzenflow.ai.chat_completion";
const FIXTURE_REPLY: &str = "recorded [^1]";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DigestItem {
    value: u64,
}

impl TypedPayload for DigestItem {
    const EVENT_TYPE: &'static str = "flowip_128g.digest_item";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DigestPartial {
    value: u64,
}

impl TypedPayload for DigestPartial {
    const EVENT_TYPE: &'static str = "flowip_128g.digest_partial";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DigestOut {
    total: u64,
}

impl TypedPayload for DigestOut {
    const EVENT_TYPE: &'static str = "flowip_128g.digest_out";
}

#[derive(Clone, Default)]
struct MapRole {
    fail_prepare: bool,
    fail_interpret: bool,
    convert_unavailable: bool,
    instructions: Option<&'static str>,
    prepare_calls: Option<Arc<AtomicUsize>>,
}

impl AiRole<Chunk<DigestItem>> for MapRole {
    type Effect = ChatCompletion;
    type Out = DigestPartial;

    fn prepare(&self, chunk: &Chunk<DigestItem>) -> Result<ChatRequestSpec, HandlerError> {
        if let Some(calls) = &self.prepare_calls {
            calls.fetch_add(1, Ordering::SeqCst);
        }
        if self.fail_prepare {
            return Err(HandlerError::Validation(
                "fixture preparation failure".to_string(),
            ));
        }
        Ok(ChatRequestSpec::new(
            self.instructions.unwrap_or("Digest."),
            format!("{} items", chunk.len()),
        ))
    }

    fn interpret(
        &self,
        chunk: Chunk<DigestItem>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DigestPartial, HandlerError> {
        if self.convert_unavailable && outcome.unavailable().is_some() {
            return Ok(DigestPartial { value: 0 });
        }
        outcome.replied()?;
        if self.fail_interpret {
            return Err(HandlerError::Domain(
                "fixture interpretation failure".to_string(),
            ));
        }
        Ok(DigestPartial {
            value: chunk.iter().map(|item| item.value).sum(),
        })
    }
}

#[derive(Clone, Copy)]
struct ReduceRole;

impl AiRole<Partials<DigestItem, DigestPartial>> for ReduceRole {
    type Effect = ChatCompletion;
    type Out = DigestOut;

    fn prepare(
        &self,
        input: &Partials<DigestItem, DigestPartial>,
    ) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new(
            "Digest.",
            format!("{} partials", input.partials().len()),
        ))
    }

    fn interpret(
        &self,
        input: Partials<DigestItem, DigestPartial>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DigestOut, HandlerError> {
        outcome.replied()?;
        Ok(DigestOut {
            total: input.partials().iter().map(|partial| partial.value).sum(),
        })
    }
}

fn recording() -> FakeChatModels {
    FakeChatModels::replying(|_| FIXTURE_REPLY.to_string())
}

fn is_reduce_request(request: &obzenflow_core::ai::ChatRequest) -> bool {
    last_user_message(request).ends_with("partials")
}

fn breaker() -> CircuitBreaker {
    circuit_breaker().consecutive_failures(5)
}

#[derive(Clone, Debug)]
struct ItemSource {
    next: u64,
    last: u64,
}

impl ItemSource {
    fn one_to(last: u64) -> Self {
        Self { next: 1, last }
    }
}

impl TypedFiniteSourceHandler for ItemSource {
    type Output = DigestItem;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > self.last {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![DigestItem { value }]))
    }
}

#[derive(Clone, Debug)]
struct CollectOut {
    outputs: Arc<Mutex<Vec<DigestOut>>>,
}

#[async_trait::async_trait]
impl InlineSink for CollectOut {
    type Input = DigestOut;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("FLOWIP-128g fixture".to_string()))
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, output: DigestOut) -> Result<(), SinkWriteFailure> {
        self.outputs
            .lock()
            .expect("output collector lock")
            .push(output);
        Ok(())
    }
}

fn run_config(journal_base: &Path, ai: &str) -> PathBuf {
    let config = journal_base.with_extension("toml");
    std::fs::write(
        &config,
        format!(
            "[runtime.backpressure.flow]\nmode = \"enforce\"\nwindow = 3\nstall_timeout_ms = 5000\n\n{ai}"
        ),
    )
    .expect("write the run configuration");
    config
}

fn enforced_app(journal_base: &Path, models: &FakeChatModels) -> FlowApplicationBuilder {
    FlowApplication::builder()
        .with_config_file(run_config(
            journal_base,
            "[ai.models]\nmodel = \"deterministic\"\n",
        ))
        .with_ai_models(models.providers())
}

fn build_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    items: u64,
    map_role: MapRole,
) -> FlowDefinition {
    build_flow_with(journal_base, outputs, items, map_role, breaker())
}

fn build_flow_with(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    items: u64,
    map_role: MapRole,
    ai_breaker: CircuitBreaker,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let items = ItemSource::one_to(items);
        let digest = ai::map(map_role).chunks(1).reduce(ReduceRole).on_eof();
        let collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_effect_replay_journal",
            journals: disk_journals(journal_base),
            stages: {
                items = source!(DigestItem => items);
                digest = ai!(DigestItem -> DigestOut with ai_breaker => digest);
                collected = sink!(DigestOut => collected);
            },

            topology: {
                items |> digest;
                digest |> collected;
            }
        })
    })
}

fn build_recovery_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    ai_breaker: CircuitBreaker,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let recovery_items = ItemSource::one_to(1);
        let digest = ai::map(MapRole::default())
            .chunks(1)
            .reduce(ReduceRole)
            .on_eof();
        let recovery_collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_recovery_composition",
            journals: disk_journals(journal_base),
            stages: {
                recovery_items = source!(DigestItem => recovery_items);
                recovery_digest = ai!(DigestItem -> DigestOut with ai_breaker => digest);
                recovery_collected = sink!(DigestOut => recovery_collected);
            },

            topology: {
                recovery_items |> recovery_digest;
                recovery_digest |> recovery_collected;
            }
        })
    })
}

fn build_credit_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    prepare_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let credit_items = ItemSource::one_to(2);
        let digest = ai::map(MapRole {
            prepare_calls: Some(prepare_calls),
            ..MapRole::default()
        })
        .chunks(1)
        .reduce(ReduceRole)
        .on_eof();
        let ai_breaker = breaker();
        let credit_collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_credit_composition",
            journals: disk_journals(journal_base),
            stages: {
                credit_items = source!(DigestItem => credit_items);
                credit_digest = ai!(DigestItem -> DigestOut with ai_breaker => digest);
                credit_collected = sink!(DigestOut => credit_collected);
            },

            topology: {
                credit_items |> credit_digest;
                credit_digest |> credit_collected;
            }
        })
    })
}

fn build_interruption_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let interrupt_items = ItemSource::one_to(5);
        let digest = ai::map(MapRole::default())
            .chunks(1)
            .reduce(ReduceRole)
            .on_eof();
        let ai_breaker = breaker();
        let interrupt_collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_chunk_interruption",
            journals: disk_journals(journal_base),
            stages: {
                interrupt_items = source!(DigestItem => interrupt_items);
                interrupt_digest = ai!(DigestItem -> DigestOut with ai_breaker => digest);
                interrupt_collected = sink!(DigestOut => interrupt_collected);
            },

            topology: {
                interrupt_items |> interrupt_digest;
                interrupt_digest |> interrupt_collected;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut entries = std::fs::read_dir(base.join("flows"))
        .expect("flow archive directory")
        .map(|entry| entry.expect("flow archive entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    entries.sort();
    entries.pop().expect("completed flow archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json"))
            .expect("run manifest is readable"),
    )
    .expect("run manifest is valid JSON")
}

fn stage_writer(run_dir: &Path, stage_key: &str) -> WriterId {
    let manifest = archive_manifest(run_dir);
    let stage_id = manifest["stages"][stage_key]["stage_id"]
        .as_str()
        .unwrap_or_else(|| panic!("run manifest names stage ID for {stage_key}"))
        .parse::<StageId>()
        .unwrap_or_else(|error| panic!("stage ID for {stage_key} parses: {error}"));
    WriterId::from(stage_id)
}

async fn stage_envelopes(run_dir: &Path, stage_key: &str) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = archive_manifest(run_dir);
    let relative = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .expect("stage data journal path");
    replay_testkit::read_journal_envelopes::<ChainEvent>(&run_dir.join(relative)).await
}

fn map_inputs(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<AiMapReduceMapInput<DigestItem>> {
    envelopes
        .iter()
        .filter_map(|envelope| AiMapReduceMapInput::<DigestItem>::from_event(&envelope.authored()))
        .collect()
}

fn grouping_ends(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<AiMapReduceGroupingEnded> {
    envelopes
        .iter()
        .filter_map(|envelope| AiMapReduceGroupingEnded::from_event(&envelope.authored()))
        .collect()
}

fn assert_grouping_authorship(
    run_dir: &Path,
    item_events: &[JournalRecord<ChainPayload>],
    chunk_events: &[JournalRecord<ChainPayload>],
    expected_groups: usize,
) {
    let chunk_writer = stage_writer(run_dir, "digest__chunk");
    let items = item_events
        .iter()
        .filter(|envelope| DigestItem::matches_event_type(&envelope.event_type()))
        .map(|envelope| {
            (
                envelope.envelope.provenance.event.id,
                envelope.causal_coordinate(),
            )
        })
        .collect::<HashMap<_, _>>();
    assert!(items
        .keys()
        .all(|_| item_events[0].envelope.provenance.event.writer_id != chunk_writer));

    let groups = chunk_events
        .iter()
        .filter(|envelope| {
            AiMapReduceMapInput::<DigestItem>::matches_event_type(&envelope.event_type())
        })
        .collect::<Vec<_>>();
    assert_eq!(groups.len(), expected_groups);
    assert_eq!(
        grouping_ends(chunk_events).len(),
        1,
        "one grouping-ended fact closes the stage's only window"
    );
    for envelope in groups {
        assert_eq!(
            envelope.envelope.provenance.event.writer_id, chunk_writer,
            "group facts are authored by the grouping member"
        );
        let parent = envelope
            .envelope
            .provenance
            .event
            .causality
            .parent_ids
            .first()
            .expect("a group fact retains its items as parents");
        let item_coordinate = items
            .get(parent)
            .expect("a group's parent is one of the stage's items");
        assert!(
            envelope
                .envelope
                .provenance
                .journal
                .vector_clock
                .get(&envelope.causal_coordinate())
                > 0,
            "group facts advance the grouping member's clock component"
        );
        assert!(
            envelope
                .envelope
                .provenance
                .journal
                .vector_clock
                .get(item_coordinate)
                > 0,
            "group facts merge the item writer's causal component"
        );
    }
}

#[derive(Debug, PartialEq)]
struct StableDeliveryReceipt {
    parent_event_type: String,
    parent_payload: serde_json::Value,
    result: serde_json::Value,
    destination: String,
    method: serde_json::Value,
    items: Option<u64>,
}

fn stable_delivery_receipts(
    parent_events: &[JournalRecord<ChainPayload>],
    sink_events: &[JournalRecord<ChainPayload>],
) -> Vec<StableDeliveryReceipt> {
    let parents = parent_events
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            payload if payload.consumes_data_credit() => Some((
                envelope.envelope.provenance.event.id,
                (envelope.event_type().to_string(), envelope.payload()),
            )),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    sink_events
        .iter()
        .filter_map(|envelope| {
            let ChainPayload::Delivery(delivery) = &envelope.payload else {
                return None;
            };
            let parent_id = envelope
                .envelope
                .provenance
                .event
                .causality
                .parent_ids
                .first()
                .expect("a sink delivery receipt retains its causal parent");
            let (parent_event_type, parent_payload) = parents.get(parent_id).unwrap_or_else(|| {
                panic!("sink delivery parent {parent_id} is journalled upstream")
            });

            Some(StableDeliveryReceipt {
                parent_event_type: parent_event_type.clone(),
                parent_payload: parent_payload.clone(),
                result: serde_json::to_value(&delivery.result)
                    .expect("delivery result serialises for stable comparison"),
                destination: delivery.destination.clone(),
                method: serde_json::to_value(&delivery.delivery_method)
                    .expect("delivery method serialises for stable comparison"),
                items: delivery.items_delivered,
            })
        })
        .collect()
}

fn replay_args(archive: &Path) -> Vec<OsString> {
    vec![
        OsString::from("obzenflow"),
        OsString::from("--replay-from"),
        archive.as_os_str().to_os_string(),
    ]
}

fn resume_args(archive: &Path, allow_incomplete: bool) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("obzenflow"),
        OsString::from("--resume-from"),
        archive.as_os_str().to_os_string(),
    ];
    if allow_incomplete {
        args.push(OsString::from("--allow-incomplete-archive"));
    }
    args
}

async fn assert_archive_contract_rejected_before_connect(
    journal_base: &Path,
    archive: &Path,
    expected_detail: &str,
) {
    let models = FakeChatModels::forbidden();
    let result = enforced_app(journal_base, &models)
        .with_cli_args(replay_args(archive))
        .run_async(build_flow(
            journal_base.to_path_buf(),
            Arc::new(Mutex::new(Vec::new())),
            5,
            MapRole::default(),
        ))
        .await;

    let error = result.expect_err("an incompatible ai! archive must fail closed");
    assert!(
        error.to_string().contains(expected_detail),
        "archive rejection should name '{expected_detail}': {error}"
    );
    assert_eq!(models.connect_count(), 0);
    assert_eq!(models.call_count(), 0);
}

fn effect_evidence_ids(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<EventId> {
    let mut ids = envelopes
        .iter()
        .filter(|envelope| {
            EffectAttemptStarted::matches_event_type(&envelope.event_type())
                || chat_completion_reply(&envelope.authored()).is_some()
        })
        .map(|envelope| envelope.envelope.provenance.event.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn chat_completion_reply(event: &ChainEvent) -> Option<serde_json::Value> {
    let ChainPayload::Execution(ExecutionPayload::EffectRecord(record)) = &event.payload else {
        return None;
    };
    if record.descriptor.effect_type.as_str() != CHAT_COMPLETION_EFFECT_TYPE {
        return None;
    }
    match &record.outcome {
        EffectOutcomePayload::Succeeded { output } => Some(output.clone()),
        EffectOutcomePayload::SucceededFact { .. } | EffectOutcomePayload::Failed { .. } => None,
    }
}

fn attempt_starts(envelopes: &[JournalRecord<ChainPayload>]) -> usize {
    envelopes
        .iter()
        .filter(|envelope| EffectAttemptStarted::matches_event_type(&envelope.event_type()))
        .count()
}

fn assert_atomic_completion_groups(envelopes: &[JournalRecord<ChainPayload>], expected: usize) {
    let completions = envelopes
        .iter()
        .filter(|envelope| chat_completion_reply(&envelope.authored()).is_some())
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), expected);
    for completion in completions {
        assert!(
            completion
                .envelope
                .provenance
                .journal
                .journal_group_id
                .as_deref()
                .is_some_and(|group| group.starts_with("effect-outcome:v1:")),
            "completion must be committed under its cursor-derived outcome group"
        );
        let member = completion
            .envelope
            .provenance
            .journal
            .journal_group_member
            .expect("atomic outcome carries physical-frame membership");
        assert!(member.size > 0);
        assert!(member.index < member.size);
    }
}

fn assert_completion_contract(
    envelopes: &[JournalRecord<ChainPayload>],
    expected: usize,
    expected_label: &str,
    expected_provider: &str,
    expected_model: &str,
) {
    let completions = envelopes
        .iter()
        .filter_map(|envelope| {
            chat_completion_reply(&envelope.authored()).map(|reply| (envelope, reply))
        })
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), expected);
    for (envelope, completion) in completions {
        let provenance = envelope
            .envelope
            .provenance
            .event
            .effect_provenance
            .as_ref()
            .expect("completion carries effect provenance");
        assert_eq!(provenance.descriptor.label.as_str(), expected_label);
        assert_eq!(
            provenance.descriptor.schema_version, 3,
            "recorded-reply storage is the ChatCompletion v3 schema"
        );
        let observability = &completion["observability"];
        assert_eq!(observability["provider"], expected_provider);
        assert_eq!(observability["model"], expected_model);
        assert_eq!(
            observability["hashes"]["version"],
            obzenflow_core::ai::LLM_HASH_VERSION_SHA256_V1
        );
        assert!(!observability["hashes"]["prompt_hash"]
            .as_str()
            .unwrap_or_default()
            .is_empty());
        assert!(!observability["hashes"]["params_hash"]
            .as_str()
            .unwrap_or_default()
            .is_empty());
        assert!(!observability["estimated_input_tokens"].is_null());
        assert_eq!(
            observability["estimated_input_resolution"]["model"],
            expected_model
        );
        assert_eq!(
            observability.get("usage"),
            completion["response"].get("usage"),
            "the durable observation retains the exact returned usage, including None"
        );
    }
    assert!(
        envelopes.iter().all(|envelope| {
            envelope
                .envelope
                .observability
                .as_ref()
                .is_none_or(|observability| {
                    !observability
                        .records
                        .iter()
                        .any(|record| matches!(record, ObservationRecord::Llm { .. }))
                })
        }),
        "LLM usage stays in framework reply evidence, never in optional LLM attachments"
    );
}

fn circuit_breaker_event_count(
    envelopes: &[JournalRecord<ChainPayload>],
    predicate: impl Fn(&CircuitBreakerFact) -> bool,
) -> usize {
    envelopes
        .iter()
        .filter(|envelope| {
            matches!(
                &envelope.payload,
                ChainPayload::Execution(ExecutionPayload::CircuitBreaker(event)) if predicate(event)
            )
        })
        .count()
}

fn chunk_failures(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<AiMapReduceChunkFailed> {
    envelopes
        .iter()
        .filter_map(|envelope| AiMapReduceChunkFailed::from_event(&envelope.authored()))
        .collect()
}

fn window_failures(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<AiMapReduceWindowFailed> {
    envelopes
        .iter()
        .filter_map(|envelope| AiMapReduceWindowFailed::from_event(&envelope.authored()))
        .collect()
}

fn reduce_failures(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<AiMapReduceReduceFailed> {
    envelopes
        .iter()
        .filter_map(|envelope| AiMapReduceReduceFailed::from_event(&envelope.authored()))
        .collect()
}

fn breaker_transitions(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<(WriterId, String)> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                fact @ (CircuitBreakerFact::Opened { .. }
                | CircuitBreakerFact::HalfOpen { .. }
                | CircuitBreakerFact::Closed { .. }
                | CircuitBreakerFact::StateChanged { .. }),
            )) => Some((
                envelope.envelope.provenance.event.writer_id,
                fact.unit().as_str().to_owned(),
            )),
            _ => None,
        })
        .collect()
}

fn outage_models(fail_reduce: bool) -> FakeChatModels {
    FakeChatModels::with_reply(move |request| {
        if is_reduce_request(request) == fail_reduce {
            Err(AiClientError::Timeout {
                message: "endpoint down".to_string(),
            })
        } else {
            Ok(fake_ai::response(FIXTURE_REPLY))
        }
    })
}

fn tripping_breaker() -> CircuitBreaker {
    circuit_breaker()
        .consecutive_failures(1)
        .open_for(Duration::from_secs(60))
}

async fn wait_for_counter(counter: &AtomicUsize, minimum: usize) {
    while counter.load(Ordering::SeqCst) < minimum {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[test]
fn hn_witness_uses_materialization_and_the_ai_stage() {
    let _ = config::DEFAULT_HN_MAX_STORIES;
    let _ = config::DEFAULT_HN_SOURCE_RATE_LIMIT;
    let _ = config::HnSettings::into_inputs;
    let _ = hn_demo_flow::settings_flow_definition;
    let _ = hn_demo_flow::run_example;
    let _ = hn_demo_flow::run_demo_blocking;
    let _ = hn_demo_flow::build_presentation;

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let flow_source = std::fs::read_to_string(root.join("examples/hn_ai_digest_demo/flow.rs"))
        .expect("HN witness flow source is readable");
    let app_config_source =
        std::fs::read_to_string(root.join("examples/hn_ai_digest_demo/config.rs"))
            .expect("HN application config source is readable");
    let checked_config =
        std::fs::read_to_string(root.join("examples/hn_ai_digest_demo/obzenflow.toml"))
            .expect("HN witness config is readable");

    for required in [
        "FlowDefinition::materialize_with(move |_runtime_config, settings: HnSettings| {",
        ".into_inputs()",
        "Ok(build_flow_definition(inputs, HnFlowOptions::default()))",
        "FlowDefinition::materialize(move |_| {",
        "let HnRunInputs {",
        "let roles = HnDigest {",
        "let digest = ai::map(roles.clone())",
        ".chunks(max_stories_per_group)",
        ".reduce(roles)",
        ".on_eof();",
        "circuit_breaker().consecutive_failures(HN_AI_BREAKER_FAILURES)",
        "digest = ai!(FormattedStory -> HnDigestSummary with ai_breaker => digest);",
        "impl AiRole<Chunk<FormattedStory>> for HnDigest {",
        "impl AiRole<Partials<FormattedStory, HnDigestGroupSummary>> for HnDigest {",
        "let hn_source = HttpPullSource::new(decoder, http_source_config);",
        "let console_sink = sinks::ConsoleSink::new(format_digest_summary_for_console);",
        "let postgres_sink = HnDigestPostgresConfig::from_env()",
        ".and_then(build_digest_postgres_sink)",
        "HnDigestSummary => handler_set!(",
        "handler_set!(console_sink, postgres_sink)",
        ".insert_into(config.schema, HN_DIGEST_TABLE, HN_DIGEST_INSERT)?",
        ".batch_size(1)?",
        ".redelivery_safety(SinkRedeliverySafety::DuplicateSensitive)",
        "digest_summary = sink!(",
    ] {
        assert!(
            flow_source.contains(required),
            "HN witness must retain the locked source clause: {required}"
        );
    }
    for forbidden in [
        "ModelConfig",
        "RigChatClient",
        "LazyRigChatClient",
        "ChatEffectBinding",
        "chat_binding_override",
        "ai_map_reduce!",
        "ai_resilience",
        "uses at_least_once",
        "via chat",
        "HnTopStories",
        "batch = stateful!",
        "budget_per_group",
        "resolve_hn_group_budget",
        "config.ai.",
        "HN_AI_PROVIDER",
        "HN_AI_MODEL",
        "std::env",
        "env_var(",
        "read_to_string(",
        "\n            bindings:",
        "effect_ports",
        "install_into",
        "DemoConfig",
        "EffectPortResolver",
        "EffectRegistrationBuilder",
        "LogicalEffectBindingName",
        "ResolvedEffectPort",
        "CHAT_CLIENT",
        "bind_deferred_with_metadata",
        "chat_registration",
        "HnSinkSelectionProbe",
        "let digest_summary = sink!(",
    ] {
        assert!(
            !flow_source.contains(forbidden),
            "HN witness may not regain the retired/eager source spelling: {forbidden}"
        );
    }
    assert!(checked_config.contains("[runtime.backpressure.flow]"));
    assert!(checked_config.contains("mode = \"enforce\""));
    assert!(checked_config.contains("window = 3"));
    assert!(checked_config.contains("[ai.models]"));
    assert!(checked_config.contains("endpoint = \"ollama\""));
    assert!(checked_config.contains("model = \"llama3.1:8b\""));
    assert!(!checked_config.contains("provider ="));
    assert!(checked_config.contains("[sinks]"));
    assert!(checked_config.contains("handler = \"console_sink\""));
    assert!(checked_config.contains("[app]"));
    assert!(checked_config.contains("source = \"mock\""));
    for retired in [
        "\"HN_LIVE\"",
        "\"HN_MAX_STORIES\"",
        "\"HN_AI_INTERESTS\"",
        "env_bool_or",
        "group_budget_tokens",
        "TokenCount",
    ] {
        assert!(
            !app_config_source.contains(retired),
            "HN run inputs may not regain a retired input: {retired}"
        );
    }
}

#[tokio::test]
async fn map_failure_branches_preserve_their_distinct_durable_contracts() {
    const GROUPS: usize = 5;

    let prepare_temp = tempfile::tempdir().expect("temporary preparation journal root");
    let prepare_models = recording();
    let prepare_outputs = Arc::new(Mutex::new(Vec::new()));
    let prepare_base = prepare_temp.path().join("journals");
    enforced_app(&prepare_base, &prepare_models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            prepare_base.clone(),
            prepare_outputs.clone(),
            5,
            MapRole {
                fail_prepare: true,
                ..MapRole::default()
            },
        ))
        .await
        .expect("preparation failures fail their window and the run completes");
    let prepare_archive = latest_run_dir(&prepare_base);
    let prepare_map = stage_envelopes(&prepare_archive, "digest__map").await;
    let prepare_failures = chunk_failures(&prepare_map);
    assert_eq!(prepare_failures.len(), GROUPS);
    assert!(prepare_failures.iter().all(|failure| matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Role {
            kind: ErrorKind::Validation,
            ..
        }
    )));
    assert_eq!(prepare_models.connect_count(), 0);
    assert_eq!(prepare_models.call_count(), 0);
    assert_eq!(attempt_starts(&prepare_map), 0);
    let prepare_collect = stage_envelopes(&prepare_archive, "digest__collect").await;
    let [window_failed] = window_failures(&prepare_collect)
        .try_into()
        .unwrap_or_else(|failures: Vec<_>| panic!("one window fails, found {}", failures.len()));
    assert_eq!(window_failed.failures.len(), GROUPS);
    assert_eq!(
        attempt_starts(&stage_envelopes(&prepare_archive, "digest__reduce").await),
        0,
        "a failed window never reduces"
    );
    assert!(prepare_outputs.lock().unwrap().is_empty());

    let interpretation_temp = tempfile::tempdir().expect("temporary interpretation journal root");
    let interpretation_models = recording();
    let interpretation_outputs = Arc::new(Mutex::new(Vec::new()));
    let interpretation_base = interpretation_temp.path().join("journals");
    enforced_app(&interpretation_base, &interpretation_models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            interpretation_base.clone(),
            interpretation_outputs.clone(),
            5,
            MapRole {
                fail_interpret: true,
                ..MapRole::default()
            },
        ))
        .await
        .expect("interpretation failures retain their successful completions");
    let interpretation_map =
        stage_envelopes(&latest_run_dir(&interpretation_base), "digest__map").await;
    let interpretation_failures = chunk_failures(&interpretation_map);
    assert_eq!(interpretation_failures.len(), GROUPS);
    assert!(interpretation_failures.iter().all(|failure| matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Role {
            kind: ErrorKind::Domain,
            ..
        }
    )));
    assert_eq!(interpretation_models.call_count(), GROUPS);
    assert_completion_contract(
        &interpretation_map,
        GROUPS,
        "ai.map",
        "ollama",
        "deterministic",
    );
    assert!(interpretation_outputs.lock().unwrap().is_empty());

    let provider_temp = tempfile::tempdir().expect("temporary provider-failure journal root");
    let provider_models = FakeChatModels::with_reply(|_| {
        Err(AiClientError::InvalidRequest {
            message: "fixture rejected request".to_string(),
        })
    });
    let provider_outputs = Arc::new(Mutex::new(Vec::new()));
    let provider_base = provider_temp.path().join("journals");
    enforced_app(&provider_base, &provider_models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            provider_base.clone(),
            provider_outputs.clone(),
            5,
            MapRole::default(),
        ))
        .await
        .expect("an unavailable model reaches the role, which fails its group");
    let provider_map = stage_envelopes(&latest_run_dir(&provider_base), "digest__map").await;
    let provider_failures = chunk_failures(&provider_map);
    assert_eq!(provider_failures.len(), GROUPS);
    assert!(provider_failures.iter().all(|failure| matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Unavailable { unavailable }
            if unavailable.cause == AiUnavailableCause::Provider {
                failure: AiProviderFailureKind::InvalidRequest,
            }
    )));
    assert_eq!(provider_models.call_count(), GROUPS);
    assert_eq!(attempt_starts(&provider_map), GROUPS);
    assert_eq!(
        provider_map
            .iter()
            .filter(|envelope| envelope.event_type() == "effect.execution_failed")
            .count(),
        GROUPS
    );
    assert!(provider_map
        .iter()
        .all(|envelope| chat_completion_reply(&envelope.authored()).is_none()));
    assert!(provider_outputs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_empty_input_never_reduces_live_or_on_replay() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live = recording();
    let live_outputs = Arc::new(Mutex::new(Vec::new()));

    enforced_app(&journal_base, &live)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_outputs.clone(),
            0,
            MapRole::default(),
        ))
        .await
        .expect("an empty input completes");

    assert_eq!(live.connect_count(), 0);
    assert_eq!(live.call_count(), 0);
    assert!(live_outputs.lock().unwrap().is_empty());

    for archive in [latest_run_dir(&journal_base)] {
        let chunk = stage_envelopes(&archive, "digest__chunk").await;
        assert!(map_inputs(&chunk).is_empty());
        let ended = grouping_ends(&chunk);
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].group_count, 0);
        assert_eq!(
            attempt_starts(&stage_envelopes(&archive, "digest__map").await)
                + attempt_starts(&stage_envelopes(&archive, "digest__reduce").await),
            0
        );
    }

    let replay = FakeChatModels::forbidden();
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let live_archive = latest_run_dir(&journal_base);
    enforced_app(&journal_base, &replay)
        .with_cli_args(replay_args(&live_archive))
        .run_async(build_flow(
            journal_base.clone(),
            replay_outputs.clone(),
            0,
            MapRole::default(),
        ))
        .await
        .expect("strict replay of an empty input completes");
    assert!(replay_outputs.lock().unwrap().is_empty());
    assert_eq!(replay.connect_count(), 0);
}

#[tokio::test]
async fn live_history_replays_without_connecting_or_invoking_chat() {
    let proof = tempfile::Builder::new()
        .prefix("flowip-145a-hn-replay-")
        .tempdir_in("target")
        .expect("temporary journal root")
        .keep();
    let journal_base = proof.join("journals");
    let live = recording();
    let live_outputs = Arc::new(Mutex::new(Vec::new()));

    enforced_app(&journal_base, &live)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_outputs.clone(),
            5,
            MapRole::default(),
        ))
        .await
        .expect("live map-reduce run succeeds");

    assert_eq!(*live_outputs.lock().unwrap(), vec![DigestOut { total: 15 }]);

    let live_archive = latest_run_dir(&journal_base);
    let live_items = stage_envelopes(&live_archive, "items").await;
    let live_chunk = stage_envelopes(&live_archive, "digest__chunk").await;
    let live_map = stage_envelopes(&live_archive, "digest__map").await;
    let live_reduce = stage_envelopes(&live_archive, "digest__reduce").await;
    let groups = map_inputs(&live_chunk).len();
    assert_eq!(groups, 5);
    assert_grouping_authorship(&live_archive, &live_items, &live_chunk, groups);
    assert_eq!(live.call_count(), groups + 1, "N map calls plus one reduce");
    assert_eq!(attempt_starts(&live_map), groups);
    assert_eq!(attempt_starts(&live_reduce), 1);
    assert_atomic_completion_groups(&live_map, groups);
    assert_atomic_completion_groups(&live_reduce, 1);
    assert_completion_contract(&live_map, groups, "ai.map", "ollama", "deterministic");
    assert_completion_contract(&live_reduce, 1, "ai.reduce", "ollama", "deterministic");
    let live_map_ids = effect_evidence_ids(&live_map);
    let live_reduce_ids = effect_evidence_ids(&live_reduce);

    let manifest_path = live_archive.join("run_manifest.json");
    let original_manifest =
        std::fs::read_to_string(&manifest_path).expect("live run manifest is readable");
    let parsed_manifest: serde_json::Value =
        serde_json::from_str(&original_manifest).expect("live run manifest is valid JSON");
    for member in ["digest__map", "digest__reduce"] {
        let spec = &parsed_manifest["stages"][member]["ai_models"][0];
        assert_eq!(spec["effect_type"], CHAT_COMPLETION_EFFECT_TYPE);
        assert_eq!(spec["endpoint"], "ollama");
        assert_eq!(spec["model"], "deterministic");
    }
    let mut incompatible_archives = Vec::new();

    let mut missing_spec_capability = parsed_manifest.clone();
    missing_spec_capability["capabilities"]
        .as_object_mut()
        .expect("capability map")
        .remove("ai_model_specs");
    incompatible_archives.push((
        "missing model-spec capability",
        missing_spec_capability,
        "lacks run-manifest capability 'ai_model_specs: 1'",
    ));

    let mut missing_history_capability = parsed_manifest.clone();
    missing_history_capability["capabilities"]
        .as_object_mut()
        .expect("capability map")
        .remove("effect_attempt_history");
    incompatible_archives.push((
        "missing attempt-history capability",
        missing_history_capability,
        "effect_attempt_history",
    ));

    let mut missing_admission_capability = parsed_manifest.clone();
    missing_admission_capability["capabilities"]
        .as_object_mut()
        .expect("capability map")
        .remove("bounded_direct_fact_admission");
    incompatible_archives.push((
        "missing bounded-admission capability",
        missing_admission_capability,
        "bounded_direct_fact_admission",
    ));

    let mut wrong_admission_capability = parsed_manifest.clone();
    wrong_admission_capability["capabilities"]["bounded_direct_fact_admission"] = json!(2);
    incompatible_archives.push((
        "wrong bounded-admission capability version",
        wrong_admission_capability,
        "bounded_direct_fact_admission",
    ));

    let mut missing_plan_row = parsed_manifest.clone();
    missing_plan_row["bounded_direct_fact_admission"]
        .as_array_mut()
        .expect("bounded admission rows")
        .pop()
        .expect("the witness has generated map and reduce rows");
    incompatible_archives.push((
        "missing generated plan row",
        missing_plan_row,
        "bounded direct-fact admission metadata",
    ));

    let mut duplicate_plan_row = parsed_manifest.clone();
    let duplicate = duplicate_plan_row["bounded_direct_fact_admission"]
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .expect("the witness has a generated admission row");
    duplicate_plan_row["bounded_direct_fact_admission"]
        .as_array_mut()
        .expect("bounded admission rows")
        .push(duplicate);
    incompatible_archives.push((
        "duplicate generated plan row",
        duplicate_plan_row,
        "bounded direct-fact admission metadata",
    ));

    let mut wrong_plan_input = parsed_manifest.clone();
    wrong_plan_input["bounded_direct_fact_admission"][0]["input_event_type"] =
        json!("flowip_128g.wrong_input:v1");
    incompatible_archives.push((
        "wrong generated plan input type",
        wrong_plan_input,
        "bounded direct-fact admission metadata",
    ));

    let mut wrong_plan_bound = parsed_manifest.clone();
    wrong_plan_bound["bounded_direct_fact_admission"][0]["max_live_data_rows"] = json!(4);
    incompatible_archives.push((
        "wrong generated plan bound",
        wrong_plan_bound,
        "bounded direct-fact admission metadata",
    ));

    for (case, incompatible_manifest, expected_detail) in incompatible_archives {
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&incompatible_manifest)
                .expect("incompatible manifest serialises"),
        )
        .unwrap_or_else(|error| panic!("{case}: temporary manifest write failed: {error}"));
        assert_archive_contract_rejected_before_connect(
            &journal_base,
            &live_archive,
            expected_detail,
        )
        .await;
    }
    std::fs::write(&manifest_path, original_manifest)
        .expect("compatible live manifest is restored");

    assert!(exported_jsonl::omit_observations(&live_archive, |_| false) > 0);

    let divergent = FakeChatModels::forbidden();
    let divergent_result = enforced_app(&journal_base, &divergent)
        .with_cli_args(replay_args(&live_archive))
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            5,
            MapRole {
                fail_prepare: true,
                ..MapRole::default()
            },
        ))
        .await;
    assert!(
        divergent_result.is_err(),
        "a new preparation failure cannot replace archived effect history"
    );
    assert_eq!(divergent.connect_count() + divergent.call_count(), 0);

    let replay = FakeChatModels::forbidden();
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    enforced_app(&journal_base, &replay)
        .with_cli_args(replay_args(&live_archive))
        .run_async(build_flow(
            journal_base.clone(),
            replay_outputs.clone(),
            5,
            MapRole::default(),
        ))
        .await
        .expect("strict replay rematerialises the generated effect history");

    assert_eq!(replay.connect_count(), 0);
    assert_eq!(replay.call_count(), 0);
    let replay_archive = latest_run_dir(&journal_base);
    let replay_items = stage_envelopes(&replay_archive, "items").await;
    let replay_chunk = stage_envelopes(&replay_archive, "digest__chunk").await;
    let replay_map = stage_envelopes(&replay_archive, "digest__map").await;
    let replay_reduce = stage_envelopes(&replay_archive, "digest__reduce").await;
    assert_grouping_authorship(&replay_archive, &replay_items, &replay_chunk, groups);
    assert_eq!(
        *replay_outputs.lock().unwrap(),
        vec![DigestOut { total: 15 }]
    );

    assert_eq!(effect_evidence_ids(&replay_map), live_map_ids);
    assert_eq!(effect_evidence_ids(&replay_reduce), live_reduce_ids);
    assert_atomic_completion_groups(&replay_map, groups);
    assert_atomic_completion_groups(&replay_reduce, 1);

    let verification = verify_run_dirs(
        &live_archive,
        &replay_archive,
        &VerifyOptions {
            write_report: false,
            ..VerifyOptions::default()
        },
    )
    .expect("run-directory verification executes");
    let verification_details = match &verification {
        VerifyOutcome::Completed { report, .. } => {
            serde_json::to_string_pretty(report).expect("verification report serialises")
        }
        VerifyOutcome::Refused(reason) => format!("verification refused: {reason}"),
    };
    assert_eq!(
        verification.exit_code(),
        0,
        "{}\n{verification_details}",
        obzenflow_infra::verify::render_verdict(&verification),
    );
    std::fs::write(proof.join("verification.json"), verification_details)
        .expect("retain the omission replay verdict");
    println!("HN omission/replay proof: {}", proof.display());
}

#[tokio::test]
async fn recovery_abandonment_fails_the_window_without_start_two() {
    let temp = tempfile::tempdir().expect("temporary recovery-composition journal root");
    let journal_base = temp.path().join("journals");
    let entered = Arc::new(Notify::new());
    let hanging = FakeChatModels::hanging(entered.clone());

    let live_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let hanging = hanging.clone();
        async move {
            enforced_app(&journal_base, &hanging)
                .with_cli_args(["obzenflow"])
                .run_async(build_recovery_flow(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                    breaker(),
                ))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("the live map call reaches the hanging client after Start(1)");
    assert_eq!(hanging.call_count(), 1);
    live_task.abort();
    let _ = live_task.await;

    let in_doubt_archive = latest_run_dir(&journal_base);
    let in_doubt_map = stage_envelopes(&in_doubt_archive, "recovery_digest__map").await;
    assert_eq!(attempt_starts(&in_doubt_map), 1);
    assert!(in_doubt_map.iter().all(|envelope| {
        !matches!(
            &envelope.payload,
            ChainPayload::Execution(ExecutionPayload::EffectRecord(_))
        ) && chat_completion_reply(&envelope.authored()).is_none()
    }));

    let resume = recording();
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    enforced_app(&journal_base, &resume)
        .with_cli_args(resume_args(&in_doubt_archive, true))
        .run_async(build_recovery_flow(
            journal_base.clone(),
            resume_outputs.clone(),
            breaker().rejecting_recovery_for_test(),
        ))
        .await
        .expect("recovery rejection reaches the role and fails its window");

    assert_eq!(
        resume.call_count(),
        0,
        "the rejecting recovery boundary prevents a second physical chat call"
    );
    assert!(resume_outputs.lock().unwrap().is_empty());

    let abandonment_archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&abandonment_archive, "recovery_digest__map").await;
    let starts = map
        .iter()
        .filter_map(|envelope| EffectAttemptStarted::from_event(&envelope.authored()))
        .collect::<Vec<_>>();
    assert_eq!(
        starts
            .iter()
            .map(|started| started.attempt.get())
            .collect::<Vec<_>>(),
        vec![1]
    );
    let abandonments = map
        .iter()
        .filter_map(|envelope| EffectRecoveryAbandoned::from_event(&envelope.authored()))
        .collect::<Vec<_>>();
    assert_eq!(abandonments.len(), 1);
    assert_eq!(abandonments[0].highest_started_attempt.get(), 1);
    let failures = chunk_failures(&map);
    assert_eq!(failures.len(), 1);
    assert!(matches!(
        &failures[0].cause,
        AiMapReduceRoleFailure::Unavailable { unavailable }
            if matches!(
                unavailable.cause,
                AiUnavailableCause::RecoveryAbandoned {
                    last_started_attempt: 1,
                    ..
                }
            )
    ));
    assert!(!map
        .iter()
        .any(|envelope| chat_completion_reply(&envelope.authored()).is_some()));

    let collect = stage_envelopes(&abandonment_archive, "recovery_digest__collect").await;
    assert_eq!(window_failures(&collect).len(), 1);
    let reduce = stage_envelopes(&abandonment_archive, "recovery_digest__reduce").await;
    assert!(!reduce.iter().any(|envelope| {
        EffectAttemptStarted::matches_event_type(&envelope.event_type())
            || chat_completion_reply(&envelope.authored()).is_some()
    }));

    let strict = FakeChatModels::forbidden();
    enforced_app(&journal_base, &strict)
        .with_cli_args(replay_args(&abandonment_archive))
        .run_async(build_recovery_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            breaker(),
        ))
        .await
        .expect("strict replay rematerialises abandonment without a dependency");
    let strict_archive = latest_run_dir(&journal_base);

    let refused = FakeChatModels::forbidden();
    let refusal = enforced_app(&journal_base, &refused)
        .with_cli_args(resume_args(&abandonment_archive, false))
        .run_async(build_recovery_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            breaker().rejecting_recovery_for_test(),
        ))
        .await
        .expect_err("a completed finite run has no work left to resume");
    assert!(
        refusal
            .to_string()
            .contains("there is no work left to continue"),
        "{refusal}"
    );
    assert_eq!(refused.connect_count() + refused.call_count(), 0);
    assert_eq!(latest_run_dir(&journal_base), strict_archive);

    let replay_map = stage_envelopes(&strict_archive, "recovery_digest__map").await;
    assert_eq!(effect_evidence_ids(&replay_map), effect_evidence_ids(&map));
    let verification = verify_run_dirs(
        &abandonment_archive,
        &strict_archive,
        &VerifyOptions {
            write_report: false,
            ..VerifyOptions::default()
        },
    )
    .expect("recovery-composition run verification executes");
    assert_eq!(
        verification.exit_code(),
        0,
        "{}",
        obzenflow_infra::verify::render_verdict(&verification)
    );
}

#[tokio::test]
async fn map_waits_for_all_three_real_edge_credits_before_second_role_call() {
    let temp = tempfile::tempdir().expect("temporary credit-composition journal root");
    let journal_base = temp.path().join("journals");
    let gate = BackpressureAckGate::install("credit_digest__map", "credit_digest__collect", 0)
        .expect("the generated map-to-collector edge freezes before its first acknowledgement");
    let models = recording();
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let models = models.clone();
        let prepare_calls = prepare_calls.clone();
        let outputs = outputs.clone();
        async move {
            enforced_app(&journal_base, &models)
                .with_cli_args(["obzenflow"])
                .run_async(build_credit_flow(journal_base, outputs, prepare_calls))
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(10), gate.wait_for_withheld(3))
        .await
        .expect("the first generated continuation fills the three-credit edge");
    assert_eq!(models.call_count(), 1);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);

    let active_archive = latest_run_dir(&journal_base);
    let active_map = stage_envelopes(&active_archive, "credit_digest__map").await;
    assert_eq!(attempt_starts(&active_map), 1);

    gate.release(1).expect("first credit returns");
    assert!(
        tokio::time::timeout(
            Duration::from_millis(150),
            wait_for_counter(&models.calls, 2)
        )
        .await
        .is_err(),
        "one returned credit cannot admit the second three-row continuation"
    );
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);

    gate.release(1).expect("second credit returns");
    assert!(
        tokio::time::timeout(
            Duration::from_millis(150),
            wait_for_counter(&models.calls, 2)
        )
        .await
        .is_err(),
        "two returned credits still cannot admit the second continuation"
    );
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);

    gate.release(1).expect("third credit returns");
    tokio::time::timeout(Duration::from_secs(5), wait_for_counter(&models.calls, 2))
        .await
        .expect("all three returned credits admit the second map continuation");
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 2);

    gate.open();
    tokio::time::timeout(Duration::from_secs(10), run_task)
        .await
        .expect("the unfrozen flow terminates")
        .expect("flow task joins")
        .expect("credit-composition flow succeeds");
    assert_eq!(models.call_count(), 3);
    assert_eq!(*outputs.lock().unwrap(), vec![DigestOut { total: 3 }]);

    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "credit_digest__map").await;
    assert_eq!(attempt_starts(&map), 2);
    assert_eq!(
        map.iter()
            .filter(|envelope| chat_completion_reply(&envelope.authored()).is_some())
            .count(),
        2
    );
}

#[tokio::test]
async fn resume_completes_groups_interrupted_before_grouping_ended() {
    let temp = tempfile::tempdir().expect("temporary chunk-interruption journal root");
    let journal_base = temp.path().join("journals");
    let gate = BackpressureAckGate::install("interrupt_digest__chunk", "interrupt_digest__map", 0)
        .expect("the generated chunk-to-map edge freezes before its first acknowledgement");
    let live = recording();

    let live_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let live = live.clone();
        async move {
            enforced_app(&journal_base, &live)
                .with_cli_args(["obzenflow"])
                .run_async(build_interruption_flow(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                ))
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(10), gate.wait_for_withheld(3))
        .await
        .expect("the first three groups commit while their acknowledgements are withheld");
    live_task.abort();
    let _ = live_task.await;

    let interrupted = latest_run_dir(&journal_base);
    let interrupted_chunk = stage_envelopes(&interrupted, "interrupt_digest__chunk").await;
    assert_eq!(
        grouping_ends(&interrupted_chunk).len(),
        0,
        "grouping ends only at a natural end of input, after every group"
    );
    assert_eq!(
        map_inputs(&interrupted_chunk).len(),
        3,
        "the fixture interrupts after the first credit window"
    );

    gate.open();

    let resumed_outputs = Arc::new(Mutex::new(Vec::new()));
    enforced_app(&journal_base, &recording())
        .with_cli_args(resume_args(&interrupted, true))
        .run_async(build_interruption_flow(
            journal_base.clone(),
            resumed_outputs.clone(),
        ))
        .await
        .expect("resume completes the interrupted groups");

    assert_eq!(
        *resumed_outputs.lock().unwrap(),
        vec![DigestOut { total: 15 }]
    );
    let resumed = latest_run_dir(&journal_base);
    let resumed_chunk = stage_envelopes(&resumed, "interrupt_digest__chunk").await;
    assert_eq!(
        map_inputs(&resumed_chunk)
            .iter()
            .map(|group| group.group.get())
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4],
        "resume authors every group in input order"
    );
    let ended = grouping_ends(&resumed_chunk);
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].group_count, 5);
}

#[tokio::test]
async fn resume_reproduces_a_map_that_passed_grouping_ended_through() {
    let temp = tempfile::tempdir().expect("temporary pass-through journal root");
    let journal_base = temp.path().join("journals");
    let reduce_entered = Arc::new(Notify::new());
    let live =
        FakeChatModels::hanging_when(reduce_entered.clone(), is_reduce_request, FIXTURE_REPLY);

    let live_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let live = live.clone();
        async move {
            enforced_app(&journal_base, &live)
                .with_cli_args(["obzenflow"])
                .run_async(build_interruption_flow(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                ))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), reduce_entered.notified())
        .await
        .expect("the reduce call starts after the map passed grouping-ended through");
    live_task.abort();
    let _ = live_task.await;
    assert_eq!(
        live.call_count(),
        6,
        "five map calls and the hanging reduce"
    );

    let interrupted = latest_run_dir(&journal_base);
    let interrupted_map = stage_envelopes(&interrupted, "interrupt_digest__map").await;
    assert_eq!(
        grouping_ends(&interrupted_map).len(),
        1,
        "the interrupted map journal holds the grouping-ended fact it passed through"
    );

    let resumed_outputs = Arc::new(Mutex::new(Vec::new()));
    let resumed = recording();
    enforced_app(&journal_base, &resumed)
        .with_cli_args(resume_args(&interrupted, false))
        .run_async(build_interruption_flow(
            journal_base.clone(),
            resumed_outputs.clone(),
        ))
        .await
        .expect("resume reproduces the map's archived actions past grouping-ended");

    assert_eq!(
        *resumed_outputs.lock().unwrap(),
        vec![DigestOut { total: 15 }]
    );
    assert_eq!(
        resumed.call_count(),
        1,
        "only the interrupted reduce call runs live"
    );
}

#[cfg(feature = "e2e")]
#[tokio::test(flavor = "multi_thread")]
async fn postgres_output_inserts_one_deterministic_hn_digest_with_stable_receipt() {
    let pool = postgres_support::pool().await;
    let schema = postgres_support::required_env("OBZENFLOW_POSTGRES_SCHEMA");
    sqlx::query(&format!(
        "TRUNCATE TABLE \"{schema}\".hn_digest_summaries RESTART IDENTITY"
    ))
    .execute(&pool)
    .await
    .expect("reset the isolated HN digest destination");

    let temp = tempfile::tempdir().expect("temporary PostgreSQL HN-flow journal root");
    let journal_base = temp.path().join("journals");
    let server = mock_server::spawn_mock_hn_server().expect("deterministic HN server starts");
    let inputs = config::HnRunInputs {
        max_stories: 5,
        poll_timeout_secs: 10,
        source_rate_limit: 1_000.0,
        max_stories_per_group: 5,
        interests: Some("runtime protocols".to_string()),
        mode_label: "mock".to_string(),
        base_url: server.base_url(),
    };
    let config_path = witness_config(temp.path(), |_| {});
    let models = recording();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--config"),
            config_path.as_os_str().to_os_string(),
        ])
        .with_ai_models(models.providers())
        .run_async(hn_demo_flow::build_flow_definition(
            inputs,
            hn_demo_flow::HnFlowOptions {
                journal_base: journal_base.clone(),
            },
        ))
        .await
        .expect("the shared HN flow delivers its digest to PostgreSQL");

    assert!(server.request_count() > 0);
    assert_eq!(models.call_count(), 2);

    let row_count = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COUNT(*) FROM \"{schema}\".hn_digest_summaries"
    ))
    .fetch_one(&pool)
    .await
    .expect("count the HN digest destination rows");
    assert_eq!(row_count, 1);

    let row = sqlx::query_as::<
        _,
        (
            i64,
            bool,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Vec<String>,
            String,
        ),
    >(&format!(
        "SELECT digest_id, created_at IS NOT NULL, source_mode, source_identity, \
         ai_provider, ai_model, token_estimator, interests, story_ids, output_markdown \
         FROM \"{schema}\".hn_digest_summaries"
    ))
    .fetch_one(&pool)
    .await
    .expect("inspect the HN digest destination");
    assert_eq!(row.0, 1);
    assert!(row.1);
    assert_eq!(row.2, "mock");
    assert_eq!(row.3, "mock://hacker-news/");
    assert_eq!(row.4, "ollama");
    assert_eq!(row.5, "llama3.1:8b");
    assert_eq!(row.6, "heuristic");
    assert_eq!(row.7.as_deref(), Some("runtime protocols"));
    assert_eq!(row.8, ["100", "101", "102", "103", "104"]);
    assert_eq!(
        row.9,
        "recorded [^1]\n\n### Links\n\n\
         [^1]: [Rust: tooling update #100](<https://example.com/hn/100>) · \
         [HN discussion](<https://news.ycombinator.com/item?id=100>)"
    );

    let archive = latest_run_dir(&journal_base);
    let reduce = stage_envelopes(&archive, "digest__reduce").await;
    let sink = stage_envelopes(&archive, "digest_summary").await;
    let receipts = stable_delivery_receipts(&reduce, &sink);
    let [receipt] = receipts.as_slice() else {
        panic!(
            "the PostgreSQL HN treatment must emit one receipt, found {}",
            receipts.len()
        );
    };
    assert_eq!(
        receipt.destination,
        format!("postgres.{schema}.hn_digest_summaries")
    );
    assert_eq!(
        receipt.method,
        serde_json::to_value(DeliveryMethod::DatabaseInsert {
            table: format!("{schema}.hn_digest_summaries"),
        })
        .expect("database-insert receipt method serialises")
    );
    assert_eq!(receipt.items, Some(1));
}

fn shipped_config(file: &str) -> toml::Table {
    let shipped = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples/hn_ai_digest_demo")
        .join(file);
    toml::from_str(&std::fs::read_to_string(shipped).unwrap()).unwrap()
}

fn witness_config(dir: &Path, edit: impl FnOnce(&mut toml::Table)) -> PathBuf {
    shipped_witness_config("obzenflow.toml", dir, edit)
}

fn shipped_witness_config(file: &str, dir: &Path, edit: impl FnOnce(&mut toml::Table)) -> PathBuf {
    let mut config = shipped_config(file);
    assert!(config.remove("app").is_some(), "the demo ships [app]");
    edit(&mut config);
    let path = dir.join(format!("witness-{file}"));
    std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    path
}

#[test]
fn the_live_and_replay_configurations_differ_from_the_default_as_documented() {
    let default = shipped_config("obzenflow.toml");
    let mut live = shipped_config("obzenflow.live.toml");
    let app = live
        .get_mut("app")
        .and_then(toml::Value::as_table_mut)
        .expect("[app] table");
    assert_eq!(
        app.insert("source".to_owned(), default["app"]["source"].clone()),
        Some(toml::Value::String("live".to_owned()))
    );
    assert_eq!(live, default, "the live file differs only in app.source");

    let mut live = shipped_config("obzenflow.live.toml");
    assert!(live.remove("ai").is_some(), "the live file configures AI");
    assert_eq!(
        shipped_config("obzenflow.replay.toml"),
        live,
        "the replay file is the live file without [ai]"
    );
}

#[tokio::test]
async fn hn_settings_convert_to_the_run_inputs_the_flow_reads() {
    let settings: config::HnSettings = toml::from_str(
        "max_stories = 5\npoll_timeout_secs = 10\ninterests = \"runtime protocols\"\n\n\
         [digest]\ngroup_max_stories = 4\n",
    )
    .unwrap();
    let inputs = settings.into_inputs().expect("a mock source starts");
    assert_eq!(inputs.max_stories, 5);
    assert_eq!(inputs.poll_timeout_secs, 10);
    assert_eq!(inputs.interests.as_deref(), Some("runtime protocols"));
    assert_eq!(inputs.max_stories_per_group, 4);
    assert_eq!(inputs.mode_label, "mock");
    assert_eq!(inputs.base_url.host_str(), Some("127.0.0.1"));
    assert_eq!(
        inputs.source_rate_limit,
        config::DEFAULT_HN_SOURCE_RATE_LIMIT
    );

    let live: config::HnSettings = toml::from_str("source = \"live\"").unwrap();
    let live = live.into_inputs().unwrap();
    assert_eq!(live.mode_label, "live");
    assert_eq!(
        live.base_url.as_str(),
        "https://hacker-news.firebaseio.com/"
    );
    assert_eq!(live.max_stories, config::DEFAULT_HN_MAX_STORIES);
    assert_eq!(live.max_stories_per_group, 10);
    assert!(
        toml::from_str::<config::HnSettings>("[digest]\ngroup_budget_tokens = 10000").is_err(),
        "the token budget left with the chunking clause"
    );
}

#[tokio::test]
async fn hn_group_size_zero_fails_the_build_naming_the_stage() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let server = mock_server::spawn_mock_hn_server().expect("deterministic HN server starts");
    let inputs = config::HnRunInputs {
        max_stories: 5,
        poll_timeout_secs: 10,
        source_rate_limit: 1_000.0,
        max_stories_per_group: 0,
        interests: None,
        mode_label: "mock".to_string(),
        base_url: server.base_url(),
    };
    let config_path = witness_config(temp.path(), |_| {});
    let models = recording();
    let error = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--config"),
            config_path.as_os_str().to_os_string(),
        ])
        .with_ai_models(models.providers())
        .run_async(hn_demo_flow::build_flow_definition(
            inputs,
            hn_demo_flow::HnFlowOptions { journal_base },
        ))
        .await
        .expect_err("a group of zero stories can never hold an item");
    assert!(
        error
            .to_string()
            .contains("stage `digest`: `.chunks(0)` can never hold an item; use 1 or more"),
        "{error}"
    );
    assert_eq!(models.connect_count() + models.call_count(), 0);
}

#[tokio::test]
async fn checked_gate_executes_the_shared_production_hn_flow_live_and_replay() {
    let temp = tempfile::tempdir().expect("temporary real HN-flow journal root");
    let journal_base = temp.path().join("journals");
    let server = mock_server::spawn_mock_hn_server().expect("deterministic HN server starts");
    let demo_inputs = config::HnRunInputs {
        max_stories: 5,
        poll_timeout_secs: 10,
        source_rate_limit: 1_000.0,
        max_stories_per_group: 5,
        interests: Some("runtime protocols".to_string()),
        mode_label: "mock".to_string(),
        base_url: server.base_url(),
    };
    let periodic_observability = |config: &mut toml::Table| {
        config
            .get_mut("runtime")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert(
                "observability".into(),
                toml::toml! { mode = "periodic"
                interval_ms = 250 }
                .into(),
            );
    };
    let config_path = witness_config(temp.path(), periodic_observability);
    let replay_config_path =
        shipped_witness_config("obzenflow.replay.toml", temp.path(), periodic_observability);
    let live = recording();

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--config"),
            config_path.as_os_str().to_os_string(),
        ])
        .with_ai_models(live.providers())
        .run_async(hn_demo_flow::build_flow_definition(
            demo_inputs.clone(),
            hn_demo_flow::HnFlowOptions {
                journal_base: journal_base.clone(),
            },
        ))
        .await
        .expect("the shared production HN flow runs live");
    let live_source_requests = server.request_count();
    assert!(
        live_source_requests > 0,
        "the live shared flow must poll the prepared HN fixture"
    );

    let live_archive = latest_run_dir(&journal_base);
    let live_chunk = stage_envelopes(&live_archive, "digest__chunk").await;
    let live_map = stage_envelopes(&live_archive, "digest__map").await;
    let live_reduce = stage_envelopes(&live_archive, "digest__reduce").await;
    let live_digest_summary = stage_envelopes(&live_archive, "digest_summary").await;
    let live_delivery_receipts = stable_delivery_receipts(&live_reduce, &live_digest_summary);
    assert!(
        !live_delivery_receipts.is_empty(),
        "the production digest summary must journal at least one stable delivery receipt"
    );
    let map_calls = live_chunk
        .iter()
        .filter(|envelope| {
            AiMapReduceMapInput::<serde_json::Value>::matches_event_type(&envelope.event_type())
        })
        .count();
    assert!(map_calls > 0);
    assert_eq!(
        live.connect_count(),
        1,
        "map and reduce share the stage's binding"
    );
    assert_eq!(live.call_count(), map_calls + 1);
    assert_completion_contract(&live_map, map_calls, "ai.map", "ollama", "llama3.1:8b");
    assert_completion_contract(&live_reduce, 1, "ai.reduce", "ollama", "llama3.1:8b");
    let manifest = archive_manifest(&live_archive);
    assert_eq!(
        manifest["stages"]["digest__map"]["ai_models"][0]["model"],
        "llama3.1:8b"
    );
    let live_map_ids = effect_evidence_ids(&live_map);
    let live_reduce_ids = effect_evidence_ids(&live_reduce);

    let storage = obzenflow_infra::testing::journal::audit_archive(&live_archive)
        .expect("the current-schema production HN archive is fully decodable");
    assert!(storage.provenance_bytes < storage.logical_provenance_bytes);
    assert!(storage.observability_bytes < storage.logical_observability_bytes);
    assert!(storage.effect_record_envelope.count > 0);
    assert!(storage.composite_record_envelope.count > 0);

    // Replay prepares a fresh host fixture with a different physical endpoint.
    // The live guard can be retired; neither endpoint is durable flow identity.
    drop(server);
    let replay_server =
        mock_server::spawn_mock_hn_server().expect("strict replay HN server starts");
    let replay_inputs = config::HnRunInputs {
        base_url: replay_server.base_url(),
        ..demo_inputs
    };
    let replay = FakeChatModels::forbidden();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--config"),
            replay_config_path.as_os_str().to_os_string(),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .with_ai_models(replay.providers())
        .run_async(hn_demo_flow::build_flow_definition(
            replay_inputs,
            hn_demo_flow::HnFlowOptions {
                journal_base: journal_base.clone(),
            },
        ))
        .await
        .expect("strict replay rematerialises the shared production HN flow");

    assert_eq!(replay.connect_count(), 0);
    assert_eq!(replay.call_count(), 0);
    assert_eq!(
        replay_server.request_count(),
        0,
        "strict replay may start a fresh host fixture but must not poll it"
    );
    let replay_archive = latest_run_dir(&journal_base);
    // Archived source attachments keep their original selection and stamps;
    // replay speed cannot recapture or resample them.
    let source_packets = |rows: Vec<JournalRecord<ChainPayload>>| {
        rows.into_iter()
            .filter(|row| matches!(row.payload, ChainPayload::Fact(_)))
            .map(|row| serde_json::to_value(&row.envelope.observability).unwrap())
            .collect::<Vec<_>>()
    };
    let live_packets = source_packets(stage_envelopes(&live_archive, "hn_stories").await);
    let replay_packets = source_packets(stage_envelopes(&replay_archive, "hn_stories").await);
    assert!(live_packets.iter().any(|packet| !packet.is_null()));
    assert!(live_packets.iter().any(serde_json::Value::is_null));
    assert_eq!(replay_packets, live_packets);
    let replay_map = stage_envelopes(&replay_archive, "digest__map").await;
    let replay_reduce = stage_envelopes(&replay_archive, "digest__reduce").await;
    let replay_digest_summary = stage_envelopes(&replay_archive, "digest_summary").await;
    let replay_delivery_receipts = stable_delivery_receipts(&replay_reduce, &replay_digest_summary);
    assert_eq!(effect_evidence_ids(&replay_map), live_map_ids);
    assert_eq!(effect_evidence_ids(&replay_reduce), live_reduce_ids);
    assert_eq!(replay_delivery_receipts, live_delivery_receipts);

    let verification = verify_run_dirs(
        &live_archive,
        &replay_archive,
        &VerifyOptions {
            write_report: false,
            ..VerifyOptions::default()
        },
    )
    .expect("shared production run-directory verification executes");
    let verification_details = match &verification {
        VerifyOutcome::Completed { report, .. } => {
            for stage_key in ["hn_stories", "formatter"] {
                let stage = report
                    .stages
                    .get(stage_key)
                    .unwrap_or_else(|| panic!("verification reports stage {stage_key}"));
                assert_eq!(stage.status, "matched", "stage {stage_key} must match");
                assert!(
                    stage.positional_rows_baseline > 0,
                    "stage {stage_key} must have positive baseline rows"
                );
                assert_eq!(
                    stage.positional_rows_candidate, stage.positional_rows_baseline,
                    "stage {stage_key} must have equal positive replay rows"
                );
            }
            serde_json::to_string_pretty(report).expect("verification report serialises")
        }
        VerifyOutcome::Refused(reason) => format!("verification refused: {reason}"),
    };
    assert_eq!(
        verification.exit_code(),
        0,
        "{}\n{verification_details}",
        obzenflow_infra::verify::render_verdict(&verification),
    );
}

#[tokio::test]
async fn descriptor_bound_rejects_one_and_two_credit_windows_before_connect() {
    for window in [1_u64, 2] {
        let temp = tempfile::tempdir().expect("temporary journal root");
        let models = recording();
        let result = build_flow(
            temp.path().join("journals"),
            Arc::new(Mutex::new(Vec::new())),
            5,
            MapRole::default(),
        )
        .build(fake_ai::build_context_with(
            &models,
            std::iter::once(fake_ai::chat_model("deterministic"))
                .chain(fake_ai::enforced_backpressure(window)),
        ))
        .await;

        let error = match result {
            Ok(_) => {
                panic!("a {window}-credit window cannot admit the three-row generated continuation")
            }
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains("requires 3 live physical Data credits")
                && message.contains(&format!("resolved enforced window {window}")),
            "build error must expose the descriptor/window mismatch: {message}"
        );
        assert_eq!(models.connect_count(), 0);
        assert_eq!(models.call_count(), 0);
    }
}

#[tokio::test]
async fn one_attempt_ordinal_does_not_claim_downstream_retry_cardinality() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let downstream_attempts = Arc::new(AtomicUsize::new(0));
    let models = FakeChatModels::with_reply({
        let downstream_attempts = downstream_attempts.clone();
        move |_| {
            downstream_attempts.fetch_add(2, Ordering::SeqCst);
            Ok(fake_ai::response("returned after an internal retry"))
        }
    });

    enforced_app(&journal_base, &models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            5,
            MapRole::default(),
        ))
        .await
        .expect("the internally retrying fake should complete");

    let archive = latest_run_dir(&journal_base);
    let chunks = stage_envelopes(&archive, "digest__chunk").await;
    let map = stage_envelopes(&archive, "digest__map").await;
    let reduce = stage_envelopes(&archive, "digest__reduce").await;
    let groups = map_inputs(&chunks).len();
    let port_invocations = groups + 1;

    assert_eq!(models.call_count(), port_invocations);
    assert_eq!(
        downstream_attempts.load(Ordering::SeqCst),
        port_invocations * 2,
        "two fake downstream attempts remain opaque inside each chat invocation"
    );

    let generated = [&map, &reduce];
    let starts = generated
        .iter()
        .map(|stage| attempt_starts(stage))
        .sum::<usize>();
    let completions = generated
        .iter()
        .flat_map(|stage| stage.iter())
        .filter(|envelope| chat_completion_reply(&envelope.authored()).is_some())
        .count();
    let settlements = generated
        .iter()
        .map(|stage| {
            circuit_breaker_event_count(stage, |event| {
                matches!(event, CircuitBreakerFact::AttemptSettled { .. })
            })
        })
        .sum::<usize>();
    let recoveries = generated
        .iter()
        .map(|stage| {
            stage
                .iter()
                .filter(|envelope| {
                    matches!(
                        &envelope.payload,
                        ChainPayload::Execution(ExecutionPayload::Recovery(
                            RecoveryFact::RecoveryCompleted { .. }
                        ))
                    )
                })
                .count()
        })
        .sum::<usize>();
    let direct_data_types = generated
        .iter()
        .flat_map(|stage| stage.iter())
        .filter_map(|envelope| match &envelope.payload {
            payload if payload.consumes_data_credit() => Some(envelope.event_type().to_string()),
            _ => None,
        })
        .fold(
            BTreeMap::<String, usize>::new(),
            |mut counts, event_type| {
                *counts.entry(event_type).or_default() += 1;
                counts
            },
        );

    assert_eq!(starts, port_invocations);
    assert_eq!(completions, port_invocations);
    assert_eq!(settlements, port_invocations);
    assert_eq!(recoveries, port_invocations);
    assert_eq!(
        direct_data_types,
        BTreeMap::from([
            (AiMapReduceGroupingEnded::event_type_name(), 1_usize),
            (
                AiMapReduceTaggedPartial::<DigestItem, DigestPartial>::event_type_name(),
                groups,
            ),
            (
                EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE.to_string(),
                port_invocations,
            ),
            (DigestOut::event_type_name(), 1),
            (EffectAttemptStarted::event_type_name(), port_invocations),
        ]),
        "the passed-through grouping end plus three rows per call are the complete Data set; \
         internal downstream retries allocate no ordinal, resilience settlement, or durable row"
    );
}

#[tokio::test]
async fn resolved_client_target_mismatch_is_fatal_before_start_or_chat() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models =
        FakeChatModels::with_client_target(ChatTarget::new("ollama", "wrong-client-model"));
    let result = enforced_app(&journal_base, &models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            5,
            MapRole::default(),
        ))
        .await;

    assert!(
        result.is_err(),
        "a connected client for another target must terminate the stage"
    );
    assert_eq!(
        models.connect_count(),
        1,
        "port authority and its metadata snapshot come from one connect"
    );
    assert_eq!(
        models.call_count(),
        0,
        "target validation must precede ChatClient::chat"
    );

    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "digest__map").await;
    assert!(
        !map.iter().any(|envelope| {
            EffectAttemptStarted::matches_event_type(&envelope.event_type())
                || chat_completion_reply(&envelope.authored()).is_some()
                || matches!(
                    &envelope.payload,
                    ChainPayload::Execution(ExecutionPayload::EffectRecord(_))
                )
        }),
        "client target validation must precede the attempt boundary"
    );
    assert!(
        !map.iter().any(|envelope| matches!(
            &envelope.payload,
            ChainPayload::Execution(
                ExecutionPayload::CircuitBreaker(_)
                    | ExecutionPayload::Recovery(_)
                    | ExecutionPayload::RateLimiter(_)
                    | ExecutionPayload::Backpressure(_)
            )
        )),
        "client target validation must precede policy observation"
    );
    assert!(
        chunk_failures(&map).is_empty(),
        "configuration fatals are not group failures"
    );
}

#[tokio::test]
async fn strict_replay_keeps_the_recorded_endpoint_when_configuration_names_another() {
    let temp = tempfile::tempdir().expect("temporary endpoint journal root");
    let journal_base = temp.path().join("journals");
    let endpoint = |url: &str| {
        format!(
            "[ai.endpoints.fixture]\nprovider = \"openai_compatible\"\nbase_url = \"{url}\"\n\n\
             [ai.models]\nendpoint = \"fixture\"\nmodel = \"deterministic\"\n"
        )
    };

    let live = recording();
    FlowApplication::builder()
        .with_config_file(run_config(
            &journal_base,
            &endpoint("http://fixture-a.invalid/v1"),
        ))
        .with_ai_models(live.providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            5,
            MapRole::default(),
        ))
        .await
        .expect("the endpoint-a run succeeds");
    assert_eq!(live.call_count(), 6);
    let live_archive = latest_run_dir(&journal_base);

    let replay = FakeChatModels::forbidden();
    let outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_config_file(run_config(
            &journal_base,
            &endpoint("http://fixture-b.invalid/v1"),
        ))
        .with_ai_models(replay.providers())
        .with_cli_args(replay_args(&live_archive))
        .run_async(build_flow(
            journal_base.clone(),
            outputs.clone(),
            5,
            MapRole::default(),
        ))
        .await
        .expect("strict replay binds the recorded endpoint and reads no endpoint table");
    assert_eq!(*outputs.lock().unwrap(), vec![DigestOut { total: 15 }]);
    assert_eq!(replay.connect_count() + replay.call_count(), 0);
}

#[tokio::test]
async fn post_start_target_invariant_commits_a_failed_attempt_terminal() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = FakeChatModels::with_reply(|request| {
        Err(AiClientError::target_mismatch(
            ChatTarget::new("ollama", "mutated-after-start"),
            request.target(),
        ))
    });
    let result = enforced_app(&journal_base, &models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            5,
            MapRole::default(),
        ))
        .await;

    assert!(
        result.is_err(),
        "a post-Start target invariant must terminate the stage"
    );
    assert_eq!(models.connect_count(), 1);
    assert_eq!(models.call_count(), 1);

    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "digest__map").await;
    assert_eq!(
        attempt_starts(&map),
        1,
        "one physical chat invocation has one Start"
    );

    let failed = map
        .iter()
        .find(|envelope| envelope.event_type() == "effect.execution_failed")
        .expect("post-Start invariant commits a generic failed outcome");
    let ChainPayload::Execution(ExecutionPayload::EffectRecord(record)) = &failed.payload else {
        panic!("effect failure is a typed execution record");
    };
    let EffectOutcomePayload::Failed {
        detail:
            Some(EffectFailureDetail::PortBindingInvariantViolation {
                port,
                expected,
                observed,
            }),
        ..
    } = &record.outcome
    else {
        panic!("the target mismatch retains its typed physical failure shape")
    };
    assert_eq!(port, "chat");
    assert_eq!(expected, "<not disclosed>");
    assert_eq!(observed, "<not disclosed>");
    assert_eq!(
        failed
            .envelope
            .provenance
            .event
            .effect_provenance
            .as_ref()
            .and_then(|provenance| provenance.attempt),
        Some(obzenflow_core::event::EffectAttemptOrdinal::new(1))
    );
    assert!(failed
        .envelope
        .provenance
        .journal
        .journal_group_id
        .as_deref()
        .is_some_and(|group| group.starts_with("effect-outcome:v1:")));
    assert_eq!(
        failed
            .envelope
            .provenance
            .journal
            .journal_group_member
            .expect("failed terminal has atomic membership")
            .index,
        0
    );

    let mut saw_ignored_settlement = false;
    let mut saw_completed_recovery = false;
    for envelope in &map {
        match &envelope.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                CircuitBreakerFact::AttemptSettled {
                    attempt,
                    health_classification,
                    ..
                },
            )) => {
                assert_eq!(*attempt, 1);
                assert!(matches!(
                    health_classification,
                    CircuitBreakerHealthClassification::Ignored
                ));
                assert_eq!(
                    envelope.envelope.provenance.journal.journal_group_id,
                    failed.envelope.provenance.journal.journal_group_id,
                    "attempt settlement belongs to the same atomic terminal"
                );
                saw_ignored_settlement = true;
            }
            ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::RecoveryCompleted { total_attempts, .. },
            )) => {
                assert_eq!(*total_attempts, 1);
                assert_eq!(
                    envelope.envelope.provenance.journal.journal_group_id,
                    failed.envelope.provenance.journal.journal_group_id,
                    "recovery completion belongs to the same atomic terminal"
                );
                saw_completed_recovery = true;
            }
            _ => {}
        }
    }
    assert!(saw_ignored_settlement);
    assert!(saw_completed_recovery);
}

fn parity(item: &DigestItem) -> u64 {
    item.value % 2
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Offset(u64);

impl SourcePosition for Offset {}

#[derive(Clone, Copy, Debug)]
struct ContinuableItems {
    last: u64,
    holds_after: Option<u64>,
}

impl ContinuableItems {
    fn one_to(last: u64) -> Self {
        Self {
            last,
            holds_after: None,
        }
    }
}

struct ContinuableItemsReader {
    next: u64,
    items: ContinuableItems,
    identity: SourceInputIdentity,
}

impl SeekableSource for ContinuableItemsReader {
    type Position = Offset;

    fn input_identity(&self) -> &SourceInputIdentity {
        &self.identity
    }

    fn beginning(&self) -> Offset {
        Offset(0)
    }

    fn seek(&mut self, position: Offset) -> Result<(), SourceError> {
        self.next = position.0;
        Ok(())
    }
}

impl PositionedFiniteReader for ContinuableItemsReader {
    type Output = DigestItem;

    fn next(&mut self) -> Result<Option<Vec<SourceRecord<DigestItem, Offset>>>, SourceError> {
        let end = self.items.holds_after.unwrap_or(self.items.last);
        if self.next >= end {
            return Ok(self.items.holds_after.map(|_| Vec::new()));
        }
        let batch = (self.next + 1..=end)
            .map(|value| SourceRecord {
                value: DigestItem { value },
                after: Offset(value),
            })
            .collect();
        self.next = end;
        Ok(Some(batch))
    }
}

impl FiniteSourceConnector for ContinuableItems {
    type Output = DigestItem;
    type Reader = ContinuableItemsReader;
    type Continuation = Seek<Offset>;

    fn open(&self, _: SourceReaderInitContext) -> Result<ContinuableItemsReader, SourceError> {
        Ok(ContinuableItemsReader {
            next: 0,
            items: *self,
            identity: SourceInputIdentity::new("continuable_items", &json!({ "last": self.last }))
                .expect("the identity encodes"),
        })
    }
}

fn build_keyed_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    keyed_items: ContinuableItems,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let by_parity = ai::map(MapRole::default())
            .key_by(parity)
            .chunks(2)
            .reduce(ReduceRole)
            .on_eof();
        let ai_breaker = breaker();
        let keyed_collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_keyed",
            journals: disk_journals(journal_base),
            stages: {
                keyed_items = source!(DigestItem => keyed_items);
                by_parity = ai!(DigestItem -> DigestOut with ai_breaker => by_parity);
                keyed_collected = sink!(DigestOut => keyed_collected);
            },

            topology: {
                keyed_items |> by_parity;
                by_parity |> keyed_collected;
            }
        })
    })
}

fn open_key_limit(journal_base: &Path, limit: u64) -> PathBuf {
    run_config(
        journal_base,
        &format!(
            "[ai.models]\nmodel = \"deterministic\"\n\n[runtime.ai.stages.by_parity]\nmax_open_keys = {limit}\n"
        ),
    )
}

const SECOND_OPEN_KEY: &str =
    "stage `by_parity` failed: key 0 would be its 2nd open key, over the limit of 1";

async fn exceed_the_open_key_limit(journal_base: &Path) -> PathBuf {
    let live = recording();
    let error = FlowApplication::builder()
        .with_config_file(open_key_limit(journal_base, 1))
        .with_ai_models(live.providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_keyed_flow(
            journal_base.to_path_buf(),
            Arc::new(Mutex::new(Vec::new())),
            ContinuableItems::one_to(5),
        ))
        .await
        .expect_err("a second open key exceeds a limit of one");
    assert!(error.to_string().contains(SECOND_OPEN_KEY), "{error}");
    latest_run_dir(journal_base)
}

#[tokio::test]
async fn strict_replay_stops_at_the_key_that_exceeded_the_recorded_limit() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let archive = exceed_the_open_key_limit(&journal_base).await;

    let replay = FakeChatModels::forbidden();
    let mut args = replay_args(&archive);
    args.push(OsString::from("--allow-incomplete-archive"));
    let error = FlowApplication::builder()
        .with_config_file(open_key_limit(&journal_base, 10))
        .with_ai_models(replay.providers())
        .with_cli_args(args)
        .run_async(build_keyed_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            ContinuableItems::one_to(5),
        ))
        .await
        .expect_err("strict replay reads the recorded limit, not the configured one");
    assert!(error.to_string().contains(SECOND_OPEN_KEY), "{error}");
    assert_eq!(replay.connect_count() + replay.call_count(), 0);
}

#[tokio::test]
async fn resume_after_raising_the_open_key_limit_completes() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let archive = exceed_the_open_key_limit(&journal_base).await;

    let outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_config_file(open_key_limit(&journal_base, 10))
        .with_ai_models(recording().providers())
        .with_cli_args(resume_args(&archive, true))
        .run_async(build_keyed_flow(
            journal_base.clone(),
            outputs.clone(),
            ContinuableItems::one_to(5),
        ))
        .await
        .expect("resume continues under the raised limit");
    let mut totals = outputs
        .lock()
        .unwrap()
        .iter()
        .map(|out| out.total)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(totals, [6, 9], "each key reduces its own items");
}

#[tokio::test]
async fn an_open_key_limit_for_a_stage_without_a_key_fails_the_build() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = recording();
    let error = FlowApplication::builder()
        .with_config_file(run_config(
            &journal_base,
            "[ai.models]\nmodel = \"deterministic\"\n\n[runtime.ai.stages.digest]\nmax_open_keys = 5\n",
        ))
        .with_ai_models(models.providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base,
            Arc::new(Mutex::new(Vec::new())),
            1,
            MapRole::default(),
        ))
        .await
        .expect_err("an unkeyed stage has no open keys to limit");
    assert!(
        error.to_string().contains(
            "config: [runtime.ai.stages.digest] sets max_open_keys, but `digest` has no key"
        ),
        "{error}"
    );
    assert_eq!(models.connect_count() + models.call_count(), 0);
}

fn keyed_groups(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<(String, u64, Vec<u64>)> {
    let mut groups = map_inputs(envelopes)
        .into_iter()
        .map(|input| {
            (
                input.key.map(|key| key.to_string()).unwrap_or_default(),
                input.group.get(),
                input.items.iter().map(|item| item.value).collect(),
            )
        })
        .collect::<Vec<_>>();
    groups.sort();
    groups
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_stop_finalizes_no_open_key_and_resume_completes_each_window() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let config = |base: &Path| run_config(base, "[ai.models]\nmodel = \"deterministic\"\n");

    let control_base = temp.path().join("control");
    FlowApplication::builder()
        .with_config_file(config(&control_base))
        .with_ai_models(recording().providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_keyed_flow(
            control_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            ContinuableItems::one_to(8),
        ))
        .await
        .expect("the uninterrupted run completes");
    let control =
        keyed_groups(&stage_envelopes(&latest_run_dir(&control_base), "by_parity__chunk").await);

    let journal_base = temp.path().join("journals");
    let live = recording();
    let handle = build_keyed_flow(
        journal_base.clone(),
        Arc::new(Mutex::new(Vec::new())),
        ContinuableItems {
            last: 8,
            holds_after: Some(6),
        },
    )
    .build(build_context_with(
        &live,
        std::iter::once(chat_model("deterministic")).chain(enforced_backpressure(3)),
    ))
    .await
    .expect("the keyed flow builds");
    handle.start().await.expect("the keyed flow starts");
    tokio::time::timeout(Duration::from_secs(10), wait_for_counter(&live.calls, 2))
        .await
        .expect("each key maps its first group");
    handle
        .stop_graceful(Duration::from_secs(5))
        .await
        .expect("the operator stops the flow");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("the stopped flow terminates")
        .expect("an operator stop ends the run");
    drop(handle);

    let stopped = latest_run_dir(&journal_base);
    let stopped_chunk = stage_envelopes(&stopped, "by_parity__chunk").await;
    assert!(
        grouping_ends(&stopped_chunk).is_empty(),
        "a stop is not the end of input"
    );
    let stopped_groups = keyed_groups(&stopped_chunk);
    assert_eq!(stopped_groups.len(), 2, "no open group is flushed");
    assert!(stopped_groups.iter().all(|group| control.contains(group)));
    assert_eq!(live.call_count(), 2, "no remainder group and no reduce");

    let outputs = Arc::new(Mutex::new(Vec::new()));
    let resumed = recording();
    FlowApplication::builder()
        .with_config_file(config(&journal_base))
        .with_ai_models(resumed.providers())
        .with_cli_args(resume_args(&stopped, true))
        .run_async(build_keyed_flow(
            journal_base.clone(),
            outputs.clone(),
            ContinuableItems::one_to(8),
        ))
        .await
        .expect("resume completes each key's window");

    let mut totals = outputs
        .lock()
        .unwrap()
        .iter()
        .map(|out| out.total)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(totals, [16, 20]);
    assert_eq!(
        resumed.call_count(),
        4,
        "each key maps its rebuilt group and reduces once"
    );
    let resumed_chunk = stage_envelopes(&latest_run_dir(&journal_base), "by_parity__chunk").await;
    assert_eq!(keyed_groups(&resumed_chunk), control);
}

#[tokio::test]
async fn a_map_only_provider_failure_reaches_the_error_journal() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = FakeChatModels::with_reply(|_| {
        Err(AiClientError::Timeout {
            message: "endpoint down".to_string(),
        })
    });
    FlowApplication::builder()
        .with_config_file(run_config(
            &journal_base,
            "[ai.models]\nmodel = \"deterministic\"\n",
        ))
        .with_ai_models(models.providers())
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize({
            let journal_base = journal_base.clone();
            move |_| {
                let items = ItemSource::one_to(2);
                let tag = ai::map(MapRole::default()).chunks(1);
                let tagged = obzenflow::stages::sinks::DiscardSink::<DigestPartial>::new();

                Ok(flow! {
                    name: "hn_ai_digest_map_only_outage",
                    journals: disk_journals(journal_base),
                    stages: {
                        items = source!(DigestItem => items);
                        tag = ai!(DigestItem -> DigestPartial => tag);
                        tagged = sink!(DigestPartial => tagged);
                    },

                    topology: {
                        items |> tag;
                        tag |> tagged;
                    }
                })
            }
        }))
        .await
        .expect("a failed group is an error row, not a stage failure");

    let archive = latest_run_dir(&journal_base);
    let relative = archive_manifest(&archive)["stages"]["tag__map"]["error_journal_file"]
        .as_str()
        .expect("the map member's error journal")
        .to_string();
    let errors = replay_testkit::read_journal_envelopes::<ChainEvent>(&archive.join(relative))
        .await
        .into_iter()
        .filter_map(|envelope| match &envelope.authored().processing.status {
            ProcessingStatus::Error { message, kind } => Some((kind.clone(), message.clone())),
            ProcessingStatus::Success => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 2, "each group's failure is one error row");
    for (kind, message) in errors {
        assert_eq!(kind, Some(ErrorKind::Remote));
        assert!(message.contains("endpoint down"), "{message}");
    }
}

#[derive(Clone, Copy)]
struct OneRole {
    reduce_version: &'static str,
}

impl AiRole<Chunk<DigestItem>> for OneRole {
    type Effect = ChatCompletion;
    type Out = DigestPartial;

    fn prepare(&self, chunk: &Chunk<DigestItem>) -> Result<ChatRequestSpec, HandlerError> {
        MapRole::default().prepare(chunk)
    }

    fn interpret(
        &self,
        chunk: Chunk<DigestItem>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DigestPartial, HandlerError> {
        MapRole::default().interpret(chunk, outcome)
    }
}

impl AiRole<Partials<DigestItem, DigestPartial>> for OneRole {
    type Effect = ChatCompletion;
    type Out = DigestOut;

    fn stage_logic_version(&self) -> &str {
        self.reduce_version
    }

    fn prepare(
        &self,
        input: &Partials<DigestItem, DigestPartial>,
    ) -> Result<ChatRequestSpec, HandlerError> {
        ReduceRole.prepare(input)
    }

    fn interpret(
        &self,
        input: Partials<DigestItem, DigestPartial>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DigestOut, HandlerError> {
        ReduceRole.interpret(input, outcome)
    }
}

fn build_one_role_flow(journal_base: PathBuf, reduce_version: &'static str) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let items = ItemSource::one_to(3);
        let role = OneRole { reduce_version };
        let digest = ai::map(role).chunks(1).reduce(role).on_eof();
        let collected = CollectOut {
            outputs: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(flow! {
            name: "hn_ai_digest_one_role",
            journals: disk_journals(journal_base),
            stages: {
                items = source!(DigestItem => items);
                digest = ai!(DigestItem -> DigestOut => digest);
                collected = sink!(DigestOut => collected);
            },

            topology: {
                items |> digest;
                digest |> collected;
            }
        })
    })
}

#[tokio::test]
async fn bumping_only_the_reduce_version_refuses_replay_at_the_reduce_members_first_call() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let config = run_config(&journal_base, "[ai.models]\nmodel = \"deterministic\"\n");

    FlowApplication::builder()
        .with_config_file(config.clone())
        .with_ai_models(recording().providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_one_role_flow(journal_base.clone(), "reduce-v1"))
        .await
        .expect("one role type serves map and reduce");
    let archive = latest_run_dir(&journal_base);
    let manifest = archive_manifest(&archive);
    assert_eq!(
        manifest["stages"]["digest__map"]["stage_logic_version"],
        "1"
    );
    assert_eq!(
        manifest["stages"]["digest__reduce"]["stage_logic_version"],
        "reduce-v1"
    );
    for member in ["digest__map", "digest__reduce"] {
        assert!(
            !stage_envelopes(&archive, member)
                .await
                .iter()
                .any(|envelope| matches!(
                    &envelope.payload,
                    ChainPayload::Execution(
                        ExecutionPayload::CircuitBreaker(_) | ExecutionPayload::RateLimiter(_)
                    )
                )),
            "{member}: a row without `with` has no policy"
        );
    }

    let replay = FakeChatModels::forbidden();
    let error = FlowApplication::builder()
        .with_config_file(config)
        .with_ai_models(replay.providers())
        .with_cli_args(replay_args(&archive))
        .run_async(build_one_role_flow(journal_base.clone(), "reduce-v2"))
        .await
        .expect_err("the reduce member's recorded calls belong to another version")
        .to_string();
    assert!(
        error.contains("effect digest__reduce#")
            && error.contains("the stage logic version differs from the recording; model matches"),
        "{error}"
    );
    assert_eq!(replay.connect_count() + replay.call_count(), 0);
    let replayed = stage_envelopes(&latest_run_dir(&journal_base), "digest__map").await;
    assert_eq!(
        replayed
            .iter()
            .filter(|envelope| {
                AiMapReduceTaggedPartial::<DigestItem, DigestPartial>::from_event(
                    &envelope.authored(),
                )
                .is_some()
            })
            .count(),
        3,
        "the map member's recorded calls still match"
    );
}

#[tokio::test]
async fn a_middleware_table_for_an_ai_stage_without_middleware_fails_the_build() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = recording();
    let error = FlowApplication::builder()
        .with_config_file(run_config(
            &journal_base,
            "[ai.models]\nmodel = \"deterministic\"\n\n\
             [middleware.stages.digest.circuit_breaker]\nconsecutive_failures = 1\n",
        ))
        .with_ai_models(models.providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_one_role_flow(journal_base, "reduce-v1"))
        .await
        .expect_err("`digest` lists no breaker to configure");
    assert!(
        error.to_string().contains(
            "config: [middleware.stages.digest] sets middleware.circuit_breaker.consecutive_failures, but `digest` lists no middleware in `with` that reads it"
        ),
        "{error}"
    );
    assert_eq!(models.connect_count() + models.call_count(), 0);
}

#[tokio::test]
async fn the_authored_stage_table_sets_the_model_for_every_member() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = recording();
    FlowApplication::builder()
        .with_config_file(run_config(
            &journal_base,
            "[ai.models.stages.digest]\nmodel = \"digest-model\"\n",
        ))
        .with_ai_models(models.providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            2,
            MapRole::default(),
        ))
        .await
        .expect("the stage table configures both model calls");

    let calls = models.recorded.lock().expect("recorded calls").clone();
    assert_eq!(
        calls
            .iter()
            .map(|call| (is_reduce_request(&call.request), call.model.as_str()))
            .collect::<Vec<_>>(),
        [
            (false, "digest-model"),
            (false, "digest-model"),
            (true, "digest-model")
        ]
    );
    let manifest = archive_manifest(&latest_run_dir(&journal_base));
    for member in ["digest__map", "digest__reduce"] {
        assert_eq!(
            manifest["stages"][member]["ai_models"][0]["model"], "digest-model",
            "{member}"
        );
    }
}

#[tokio::test]
async fn strict_replay_names_the_request_when_a_prompt_changed() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live = recording();
    enforced_app(&journal_base, &live)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            1,
            MapRole::default(),
        ))
        .await
        .expect("the live run records its map call");
    let archive = latest_run_dir(&journal_base);

    let replay = FakeChatModels::forbidden();
    let error = enforced_app(&journal_base, &replay)
        .with_cli_args(replay_args(&archive))
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            1,
            MapRole {
                instructions: Some("Digest every story."),
                ..MapRole::default()
            },
        ))
        .await
        .expect_err("a changed prompt cannot replay its recording");
    assert!(
        error.to_string().contains(
            "effect digest__map#1/0: the request differs from the recording (prompt or parameters changed); model and stage logic version match"
        ),
        "{error}"
    );
    assert_eq!(replay.connect_count() + replay.call_count(), 0);
}

async fn interrupt_during_first_map_call(journal_base: &Path, ai: &str) -> PathBuf {
    let entered = Arc::new(Notify::new());
    let hanging = FakeChatModels::hanging(entered.clone());
    let live_task = tokio::spawn({
        let config = run_config(journal_base, ai);
        let journal_base = journal_base.to_path_buf();
        let hanging = hanging.clone();
        async move {
            FlowApplication::builder()
                .with_config_file(config)
                .with_ai_models(hanging.providers())
                .with_cli_args(["obzenflow"])
                .run_async(build_interruption_flow(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                ))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("the first map call starts before the interruption");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let source = stage_envelopes(&latest_run_dir(journal_base), "interrupt_items").await;
            if source.iter().any(|envelope| {
                matches!(
                    envelope.payload,
                    ChainPayload::FlowControl(FlowControlPayload::Eof { .. })
                )
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the source journals the end of its input before the interruption");
    live_task.abort();
    assert!(live_task
        .await
        .expect_err("the interrupted run never finishes")
        .is_cancelled());
    assert_eq!(hanging.call_count(), 1);
    latest_run_dir(journal_base)
}

#[tokio::test]
async fn resume_keeps_the_recorded_model_for_an_in_doubt_call_and_configures_new_ones() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let model = |name: &str| format!("[ai.models]\nmodel = \"{name}\"\n");
    let interrupted = interrupt_during_first_map_call(&journal_base, &model("model-a")).await;

    let resumed = recording();
    let outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_config_file(run_config(&journal_base, &model("model-b")))
        .with_ai_models(resumed.providers())
        .with_cli_args(resume_args(&interrupted, true))
        .run_async(build_interruption_flow(
            journal_base.clone(),
            outputs.clone(),
        ))
        .await
        .expect("resume completes on the changed model");
    assert_eq!(*outputs.lock().unwrap(), vec![DigestOut { total: 15 }]);

    let served = resumed
        .recorded
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.request.target().model)
        .collect::<Vec<_>>();
    assert_eq!(
        served,
        ["model-a", "model-b", "model-b", "model-b", "model-b", "model-b"],
        "the in-doubt call keeps its recorded model, and every new call uses the configured one"
    );
    let resumed_archive = latest_run_dir(&journal_base);
    let manifest = archive_manifest(&resumed_archive);
    let recorded_models = manifest["stages"]["interrupt_digest__map"]["ai_models"]
        .as_array()
        .expect("the resumed run records its models")
        .iter()
        .map(|spec| spec["model"].as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();
    assert_eq!(recorded_models, ["model-b", "model-a"]);

    let replay = FakeChatModels::forbidden();
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_config_file(run_config(&journal_base, ""))
        .with_ai_models(replay.providers())
        .with_cli_args(replay_args(&resumed_archive))
        .run_async(build_interruption_flow(
            journal_base.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("strict replay of the resumed lineage reads both recorded models");
    assert_eq!(replay.connect_count() + replay.call_count(), 0);
    assert_eq!(
        *replay_outputs.lock().unwrap(),
        vec![DigestOut { total: 15 }]
    );
}

#[tokio::test]
async fn resume_fails_an_in_doubt_call_whose_recorded_endpoint_is_gone() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let endpoint = |name: &str| {
        format!(
            "[ai.endpoints.{name}]\nprovider = \"openai_compatible\"\nbase_url = \"http://{name}.invalid/v1\"\n\n\
             [ai.models]\nendpoint = \"{name}\"\nmodel = \"deterministic\"\n"
        )
    };
    let interrupted = interrupt_during_first_map_call(&journal_base, &endpoint("east")).await;

    let resumed = recording();
    FlowApplication::builder()
        .with_config_file(run_config(&journal_base, &endpoint("west")))
        .with_ai_models(resumed.providers())
        .with_cli_args(resume_args(&interrupted, true))
        .run_async(build_interruption_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ))
        .await
        .expect("an unreachable recorded endpoint fails only its in-doubt call");

    assert_eq!(
        resumed.call_count(),
        4,
        "the four new groups run on the configured endpoint; the failed group stops its window"
    );
    assert!(resumed
        .recorded
        .lock()
        .unwrap()
        .iter()
        .all(|call| call.endpoint == "west"));
    let map = stage_envelopes(&latest_run_dir(&journal_base), "interrupt_digest__map").await;
    let [failure] = chunk_failures(&map)
        .try_into()
        .unwrap_or_else(|failures: Vec<_>| panic!("one group fails, found {}", failures.len()));
    assert!(matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Unavailable { unavailable }
            if unavailable.cause == AiUnavailableCause::Provider {
                failure: AiProviderFailureKind::Remote,
            } && unavailable.reason.contains("endpoint `east`")
    ));
}

#[tokio::test]
async fn a_map_outage_opens_the_shared_breaker_and_fails_the_reduce_call_fast() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = outage_models(false);
    enforced_app(&journal_base, &models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_with(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            1,
            MapRole {
                convert_unavailable: true,
                ..MapRole::default()
            },
            tripping_breaker(),
        ))
        .await
        .expect("a converted map outage lets its window reduce");

    assert_eq!(
        models.call_count(),
        1,
        "the reduce call never reaches the endpoint the map call found down"
    );
    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "digest__map").await;
    let reduce = stage_envelopes(&archive, "digest__reduce").await;
    assert_eq!(
        breaker_transitions(&map),
        [(stage_writer(&archive, "digest__map"), "digest".to_string())]
    );
    assert!(breaker_transitions(&reduce).is_empty());
    assert_eq!(attempt_starts(&reduce), 0);
    let [failure] = reduce_failures(&reduce)
        .try_into()
        .unwrap_or_else(|failures: Vec<_>| {
            panic!("one window fails to reduce, found {}", failures.len())
        });
    assert!(matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Unavailable { unavailable }
            if matches!(
                &unavailable.cause,
                AiUnavailableCause::BoundaryRejected { source, code }
                    if source == "circuit_breaker" && code == "circuit_open"
            )
    ));
}

#[tokio::test]
async fn a_reduce_failure_opens_the_breaker_in_the_reduce_journal() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = outage_models(true);
    enforced_app(&journal_base, &models)
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_with(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            2,
            MapRole::default(),
            tripping_breaker(),
        ))
        .await
        .expect("a failed reduce fails only its window");

    assert_eq!(models.call_count(), 3);
    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "digest__map").await;
    let reduce = stage_envelopes(&archive, "digest__reduce").await;
    assert!(breaker_transitions(&map).is_empty());
    assert_eq!(
        breaker_transitions(&reduce),
        [(
            stage_writer(&archive, "digest__reduce"),
            "digest".to_string()
        )],
        "the member whose call opened the breaker writes the transition and names the stage"
    );
    assert_eq!(reduce_failures(&reduce).len(), 1);
}

#[tokio::test]
async fn the_authored_stage_configures_the_shared_breaker_and_a_member_table_fails() {
    let config = |dir: &Path, table: &str| {
        run_config(
            dir,
            &format!(
                "[ai.models]\nmodel = \"deterministic\"\n\n[{table}.circuit_breaker]\nconsecutive_failures = 1\n"
            ),
        )
    };

    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = outage_models(false);
    FlowApplication::builder()
        .with_config_file(config(&journal_base, "middleware.stages.digest"))
        .with_ai_models(models.providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            1,
            MapRole {
                convert_unavailable: true,
                ..MapRole::default()
            },
        ))
        .await
        .expect("the authored stage's table configures the stage");
    assert_eq!(
        models.call_count(),
        1,
        "one map failure opens the breaker configured at `digest`, so the reduce is rejected"
    );

    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let models = outage_models(false);
    let error = FlowApplication::builder()
        .with_config_file(config(&journal_base, "middleware.stages.digest__map"))
        .with_ai_models(models.providers())
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base,
            Arc::new(Mutex::new(Vec::new())),
            1,
            MapRole::default(),
        ))
        .await
        .expect_err("a generated member is not a configuration point");
    assert!(
        error.to_string().contains(
            "config: [middleware.stages.digest__map] names a generated member of `digest`; configure `digest`"
        ),
        "{error}"
    );
    assert_eq!(models.connect_count() + models.call_count(), 0);
}
