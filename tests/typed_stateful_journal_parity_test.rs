// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134e journal oracle for plain typed stateful handlers and accumulators.

use obzenflow::typed::stateful as typed_stateful;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventId, StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, StatefulEmission, TypedStatefulHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Input {
    value: u64,
    key: String,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "flowip_134e.input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FoldSnapshot {
    running_total: u64,
    contributions: u64,
}

impl TypedPayload for FoldSnapshot {
    const EVENT_TYPE: &'static str = "flowip_134e.fold_snapshot";
}

#[derive(Clone, Debug, Default)]
struct FoldState {
    running_total: u64,
    contributions: u64,
}

#[derive(Clone, Debug)]
struct WindowedFold;

impl TypedStatefulHandler for WindowedFold {
    type State = FoldState;
    type Input = Input;
    type Output = FoldSnapshot;

    fn initial_state(&self) -> Self::State {
        FoldState::default()
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        state.running_total = state.running_total.saturating_add(input.value);
        state.contributions = state.contributions.saturating_add(1);
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state.contributions >= 2
    }

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::ResetEpoch {
            next_state: FoldState {
                running_total: state.running_total,
                contributions: 0,
            },
            outputs: (state.contributions > 0)
                .then_some(FoldSnapshot {
                    running_total: state.running_total,
                    contributions: state.contributions,
                })
                .into_iter()
                .collect(),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct GroupTotal {
    total: u64,
    count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct GroupSnapshot {
    key: String,
    total: u64,
    count: u64,
}

impl TypedPayload for GroupSnapshot {
    const EVENT_TYPE: &'static str = "flowip_134e.group_snapshot";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CurrentRanking {
    keys: Vec<String>,
    scores: Vec<u64>,
}

impl TypedPayload for CurrentRanking {
    const EVENT_TYPE: &'static str = "flowip_134e.current_ranking";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AggregateRanking {
    keys: Vec<String>,
    total_scores: Vec<u64>,
    counts: Vec<u64>,
}

impl TypedPayload for AggregateRanking {
    const EVENT_TYPE: &'static str = "flowip_134e.aggregate_ranking";
}

#[derive(Clone, Debug)]
struct ValuesSource {
    values: Vec<Input>,
    next: usize,
    writer_id: WriterId,
}

impl ValuesSource {
    fn new() -> Self {
        Self {
            values: vec![
                Input {
                    value: 1,
                    key: "a".into(),
                },
                Input {
                    value: 2,
                    key: "b".into(),
                },
                Input {
                    value: 3,
                    key: "a".into(),
                },
                Input {
                    value: 4,
                    key: "b".into(),
                },
                Input {
                    value: 5,
                    key: "a".into(),
                },
                Input {
                    value: 6,
                    key: "b".into(),
                },
            ],
            next: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ValuesSource {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let Some(value) = self.values.get(self.next).cloned() else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(vec![value.to_event(self.writer_id)]))
    }
}

#[derive(Clone, Debug)]
struct RejectThree;

impl TypedTransformHandler for RejectThree {
    type Input = Input;
    type Output = Input;

    fn process(&self, input: Self::Input) -> Result<Self::Output, HandlerError> {
        if input.value == 3 {
            Err(HandlerError::Domain("three is rejected".to_string()))
        } else {
            Ok(input)
        }
    }
}

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let input_handler = ValuesSource::new();
        let validate_handler = RejectThree;
        let fold_handler = WindowedFold;
        let grouped = typed_stateful::group_by(
            |input: &Input| input.key.clone(),
            |state: &mut GroupTotal, input: &Input| {
                state.total = state.total.saturating_add(input.value);
                state.count = state.count.saturating_add(1);
            },
            |key: &String, state: &GroupTotal| GroupSnapshot {
                key: key.clone(),
                total: state.total,
                count: state.count,
            },
        )
        .emit_on_eof();
        let current_ranking = typed_stateful::top_n(
            2,
            |input: &Input| input.key.clone(),
            |input: &Input| input.value as f64,
            |snapshot: typed_stateful::TopNSnapshot<String, Input>| CurrentRanking {
                keys: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect(),
                scores: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.score as u64)
                    .collect(),
            },
        )
        .emit_on_eof();
        let aggregate_ranking = typed_stateful::top_n_by(
            2,
            |input: &Input| input.key.clone(),
            |input: &Input| input.value as f64,
            |snapshot: typed_stateful::TopNBySnapshot<String, Input>| AggregateRanking {
                keys: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect(),
                total_scores: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.total_score as u64)
                    .collect(),
                counts: snapshot.top_n.iter().map(|entry| entry.count).collect(),
            },
        )
        .emit_on_eof();
        let fold_sink = SinkTyped::new(|_snapshot: FoldSnapshot| async move {}).idempotent();
        let group_sink = SinkTyped::new(|_snapshot: GroupSnapshot| async move {}).idempotent();
        let current_ranking_sink =
            SinkTyped::new(|_snapshot: CurrentRanking| async move {}).idempotent();
        let aggregate_ranking_sink =
            SinkTyped::new(|_snapshot: AggregateRanking| async move {}).idempotent();

        Ok(flow! {
            name: "typed_stateful_journal_parity",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(Input => input_handler);
                validate = transform!(Input -> Input => validate_handler);
                fold = stateful!(Input -> FoldSnapshot => fold_handler);
                grouped = stateful!(Input -> GroupSnapshot => grouped);
                current_ranking = stateful!(Input -> CurrentRanking => current_ranking);
                aggregate_ranking = stateful!(Input -> AggregateRanking => aggregate_ranking);
                fold_sink = sink!(FoldSnapshot => fold_sink);
                group_sink = sink!(GroupSnapshot => group_sink);
                current_ranking_sink = sink!(CurrentRanking => current_ranking_sink);
                aggregate_ranking_sink = sink!(AggregateRanking => aggregate_ranking_sink);
            },

            topology: {
                inputs |> validate;
                validate |> fold;
                validate |> grouped;
                validate |> current_ranking;
                validate |> aggregate_ranking;
                fold |> fold_sink;
                grouped |> group_sink;
                current_ranking |> current_ranking_sink;
                aggregate_ranking |> aggregate_ranking_sink;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(base.join("flows"))
        .expect("flows directory exists")
        .map(|entry| entry.expect("flow directory entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().expect("flow produced a replay archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("manifest is readable"),
    )
    .expect("manifest parses")
}

fn stage_writer(run_dir: &Path, stage_name: &str) -> WriterId {
    let manifest = archive_manifest(run_dir);
    let stage_id = manifest["stages"][stage_name]["stage_id"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names stage ID for {stage_name}"))
        .parse::<StageId>()
        .unwrap_or_else(|error| panic!("stage ID for {stage_name} parses: {error}"));
    WriterId::from(stage_id)
}

async fn read_stage_journal(
    run_dir: &Path,
    stage_name: &str,
    manifest_field: &str,
) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name][manifest_field]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names {manifest_field} for stage {stage_name}"));
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .expect("stage journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("stage journal reads")
}

async fn read_stage(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    read_stage_journal(run_dir, stage_name, "data_journal_file").await
}

async fn read_stage_errors(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    read_stage_journal(run_dir, stage_name, "error_journal_file").await
}

fn facts<T: TypedPayload>(events: &[EventEnvelope<ChainEvent>]) -> Vec<T> {
    events
        .iter()
        .filter_map(|envelope| T::from_event(&envelope.event))
        .collect()
}

fn delivery_count(events: &[EventEnvelope<ChainEvent>]) -> usize {
    events
        .iter()
        .filter(|envelope| matches!(envelope.event.content, ChainEventContent::Delivery(_)))
        .count()
}

fn assert_canonical_fact_and_eof<T: TypedPayload>(
    events: &[EventEnvelope<ChainEvent>],
    expected_rows: usize,
) {
    let canonical = T::versioned_event_type();
    let rows = events
        .iter()
        .filter(|envelope| T::event_type_matches(&envelope.event.event_type()))
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), expected_rows);
    assert!(rows
        .iter()
        .all(|envelope| envelope.event.event_type() == canonical));

    let eof_keys = events
        .iter()
        .rev()
        .find_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                writer_seq_by_event_type,
                ..
            }) => Some(writer_seq_by_event_type),
            _ => None,
        })
        .expect("stateful journal contains EOF evidence");
    let matching = eof_keys
        .iter()
        .filter(|(key, _)| T::event_type_matches(key.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "one canonical EOF bucket: {eof_keys:?}");
    assert_eq!(matching[0].0.as_str(), canonical);
    assert_eq!(matching[0].1 .0, expected_rows as u64);
    assert!(eof_keys.keys().all(|key| key.as_str() != T::EVENT_TYPE));
}

fn parent_values(output: &ChainEvent, inputs_by_id: &HashMap<EventId, Input>) -> Vec<u64> {
    output
        .causality
        .parent_ids
        .iter()
        .map(|parent| {
            inputs_by_id
                .get(parent)
                .unwrap_or_else(|| panic!("parent {parent} is a successful validate fact"))
                .value
        })
        .collect()
}

struct ProjectionJournals<'a> {
    validate: &'a [EventEnvelope<ChainEvent>],
    validate_errors: &'a [EventEnvelope<ChainEvent>],
    fold: &'a [EventEnvelope<ChainEvent>],
    grouped: &'a [EventEnvelope<ChainEvent>],
    current_ranking: &'a [EventEnvelope<ChainEvent>],
    aggregate_ranking: &'a [EventEnvelope<ChainEvent>],
    fold_sink: &'a [EventEnvelope<ChainEvent>],
    group_sink: &'a [EventEnvelope<ChainEvent>],
    current_ranking_sink: &'a [EventEnvelope<ChainEvent>],
    aggregate_ranking_sink: &'a [EventEnvelope<ChainEvent>],
}

fn assert_stateful_projection(run_dir: &Path, journals: ProjectionJournals<'_>) {
    let ProjectionJournals {
        validate,
        validate_errors,
        fold,
        grouped,
        current_ranking,
        aggregate_ranking,
        fold_sink,
        group_sink,
        current_ranking_sink,
        aggregate_ranking_sink,
    } = journals;
    assert!(
        validate_errors.is_empty(),
        "ordinary in-band transform errors remain on the data path"
    );
    let pre_errors = validate
        .iter()
        .filter(|envelope| {
            matches!(
                envelope.event.processing_info.status,
                ProcessingStatus::Error { .. }
            )
        })
        .filter_map(|envelope| Input::from_event(&envelope.event))
        .collect::<Vec<_>>();
    assert_eq!(
        pre_errors,
        vec![Input {
            value: 3,
            key: "a".into()
        }]
    );

    let successful_inputs = validate
        .iter()
        .filter_map(|envelope| {
            matches!(
                envelope.event.processing_info.status,
                ProcessingStatus::Success
            )
            .then(|| Input::from_event(&envelope.event))
            .flatten()
            .map(|input| (envelope.event.id, input))
        })
        .collect::<HashMap<_, _>>();
    assert_eq!(
        successful_inputs
            .values()
            .map(|input| input.value)
            .collect::<HashSet<_>>(),
        HashSet::from([1, 2, 4, 5, 6])
    );

    assert!(fold.iter().all(|envelope| {
        !Input::event_type_matches(&envelope.event.event_type())
            && !matches!(
                envelope.event.processing_info.status,
                ProcessingStatus::Error { .. }
            )
    }));
    assert!(grouped.iter().all(|envelope| {
        !Input::event_type_matches(&envelope.event.event_type())
            && !matches!(
                envelope.event.processing_info.status,
                ProcessingStatus::Error { .. }
            )
    }));

    assert_eq!(
        facts::<FoldSnapshot>(fold),
        vec![
            FoldSnapshot {
                running_total: 3,
                contributions: 2
            },
            FoldSnapshot {
                running_total: 12,
                contributions: 2
            },
            FoldSnapshot {
                running_total: 18,
                contributions: 1
            },
        ]
    );
    let fold_rows = fold
        .iter()
        .filter(|envelope| FoldSnapshot::from_event(&envelope.event).is_some())
        .collect::<Vec<_>>();
    assert_eq!(
        fold_rows
            .iter()
            .map(|envelope| parent_values(&envelope.event, &successful_inputs))
            .collect::<Vec<_>>(),
        vec![vec![1, 2], vec![4, 5], vec![6]],
        "reset epochs partition the whole-batch frontier by invoked boundary"
    );
    assert!(fold_rows
        .iter()
        .all(|envelope| envelope.event.writer_id == stage_writer(run_dir, "fold")));

    let grouped_rows = grouped
        .iter()
        .filter_map(|envelope| {
            GroupSnapshot::from_event(&envelope.event).map(|snapshot| {
                (
                    snapshot.key,
                    snapshot.total,
                    snapshot.count,
                    &envelope.event,
                )
            })
        })
        .collect::<Vec<_>>();
    let mut grouped_payloads = grouped_rows
        .iter()
        .map(|(key, total, count, _)| (key.clone(), (*total, *count)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(grouped_payloads.remove("a"), Some((6, 2)));
    assert_eq!(grouped_payloads.remove("b"), Some((12, 3)));
    assert!(grouped_payloads.is_empty());
    for (key, _, _, event) in grouped_rows {
        assert_eq!(event.writer_id, stage_writer(run_dir, "grouped"));
        let expected = if key == "a" {
            vec![1, 5]
        } else {
            vec![2, 4, 6]
        };
        assert_eq!(
            parent_values(event, &successful_inputs),
            expected,
            "each group owns only its exact contribution frontier"
        );
    }

    assert_eq!(
        facts::<CurrentRanking>(current_ranking),
        vec![CurrentRanking {
            keys: vec!["b".into(), "a".into()],
            scores: vec![6, 5],
        }],
        "TopN ranks each key's latest value by replacement"
    );
    assert_eq!(
        facts::<AggregateRanking>(aggregate_ranking),
        vec![AggregateRanking {
            keys: vec!["b".into(), "a".into()],
            total_scores: vec![12, 6],
            counts: vec![3, 2],
        }],
        "TopNBy ranks cumulative per-key aggregates"
    );
    for (stage_name, rows) in [
        ("current_ranking", current_ranking),
        ("aggregate_ranking", aggregate_ranking),
    ] {
        let row = rows
            .iter()
            .find(|envelope| matches!(envelope.event.content, ChainEventContent::Data { .. }))
            .unwrap_or_else(|| panic!("{stage_name} authored one data row"));
        assert_eq!(row.event.writer_id, stage_writer(run_dir, stage_name));
        assert_eq!(
            parent_values(&row.event, &successful_inputs),
            vec![1, 2, 4, 5, 6],
            "{stage_name} retains the complete whole-batch contribution frontier"
        );
    }

    assert_eq!(delivery_count(fold_sink), 3);
    assert_eq!(delivery_count(group_sink), 2);
    assert_eq!(delivery_count(current_ranking_sink), 1);
    assert_eq!(delivery_count(aggregate_ranking_sink), 1);
    assert_canonical_fact_and_eof::<FoldSnapshot>(fold, 3);
    assert_canonical_fact_and_eof::<GroupSnapshot>(grouped, 2);
    assert_canonical_fact_and_eof::<CurrentRanking>(current_ranking, 1);
    assert_canonical_fact_and_eof::<AggregateRanking>(aggregate_ranking, 1);
}

async fn run(journal_base: &Path, replay_from: Option<&Path>) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(journal_base.to_path_buf()))
        .await
        .expect("typed stateful parity flow completes");
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_stateful_boundaries_have_live_replay_journal_parity() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");

    run(&journal_base, None).await;
    let live = latest_run_dir(&journal_base);
    let live_validate = read_stage(&live, "validate").await;
    let live_validate_errors = read_stage_errors(&live, "validate").await;
    let live_fold = read_stage(&live, "fold").await;
    let live_grouped = read_stage(&live, "grouped").await;
    let live_current_ranking = read_stage(&live, "current_ranking").await;
    let live_aggregate_ranking = read_stage(&live, "aggregate_ranking").await;
    let live_fold_sink = read_stage(&live, "fold_sink").await;
    let live_group_sink = read_stage(&live, "group_sink").await;
    let live_current_ranking_sink = read_stage(&live, "current_ranking_sink").await;
    let live_aggregate_ranking_sink = read_stage(&live, "aggregate_ranking_sink").await;
    assert_stateful_projection(
        &live,
        ProjectionJournals {
            validate: &live_validate,
            validate_errors: &live_validate_errors,
            fold: &live_fold,
            grouped: &live_grouped,
            current_ranking: &live_current_ranking,
            aggregate_ranking: &live_aggregate_ranking,
            fold_sink: &live_fold_sink,
            group_sink: &live_group_sink,
            current_ranking_sink: &live_current_ranking_sink,
            aggregate_ranking_sink: &live_aggregate_ranking_sink,
        },
    );

    run(&journal_base, Some(&live)).await;
    let replay = latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    let replay_validate = read_stage(&replay, "validate").await;
    let replay_validate_errors = read_stage_errors(&replay, "validate").await;
    let replay_fold = read_stage(&replay, "fold").await;
    let replay_grouped = read_stage(&replay, "grouped").await;
    let replay_current_ranking = read_stage(&replay, "current_ranking").await;
    let replay_aggregate_ranking = read_stage(&replay, "aggregate_ranking").await;
    let replay_fold_sink = read_stage(&replay, "fold_sink").await;
    let replay_group_sink = read_stage(&replay, "group_sink").await;
    let replay_current_ranking_sink = read_stage(&replay, "current_ranking_sink").await;
    let replay_aggregate_ranking_sink = read_stage(&replay, "aggregate_ranking_sink").await;
    assert_stateful_projection(
        &replay,
        ProjectionJournals {
            validate: &replay_validate,
            validate_errors: &replay_validate_errors,
            fold: &replay_fold,
            grouped: &replay_grouped,
            current_ranking: &replay_current_ranking,
            aggregate_ranking: &replay_aggregate_ranking,
            fold_sink: &replay_fold_sink,
            group_sink: &replay_group_sink,
            current_ranking_sink: &replay_current_ranking_sink,
            aggregate_ranking_sink: &replay_aggregate_ranking_sink,
        },
    );

    assert_eq!(
        facts::<FoldSnapshot>(&replay_fold),
        facts::<FoldSnapshot>(&live_fold)
    );
    let mut live_groups = facts::<GroupSnapshot>(&live_grouped);
    let mut replay_groups = facts::<GroupSnapshot>(&replay_grouped);
    live_groups.sort_by(|left, right| left.key.cmp(&right.key));
    replay_groups.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(replay_groups, live_groups);
    assert_eq!(
        facts::<CurrentRanking>(&replay_current_ranking),
        facts::<CurrentRanking>(&live_current_ranking)
    );
    assert_eq!(
        facts::<AggregateRanking>(&replay_aggregate_ranking),
        facts::<AggregateRanking>(&live_aggregate_ranking)
    );
}
