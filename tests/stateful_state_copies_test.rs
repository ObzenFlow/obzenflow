// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit;

use async_trait::async_trait;
use obzenflow::stages::stateful::{
    Accumulator, EmitAlways, EveryN, OnEOF, StatefulWithEmission, TimeWindow,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_stateful, flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, StatefulEmission, TypedFiniteSourceHandler, TypedStatefulHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const VALUES: [u64; 5] = [1, 2, 3, 4, 5];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Reading {
    value: u64,
}

impl TypedPayload for Reading {
    const EVENT_TYPE: &'static str = "stateful_state_copies.reading";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Total {
    sum: u64,
    count: u64,
}

impl TypedPayload for Total {
    const EVENT_TYPE: &'static str = "stateful_state_copies.total";
}

fn total(sum: u64, count: u64) -> Total {
    Total { sum, count }
}

#[derive(Debug)]
struct Counted {
    sum: u64,
    count: u64,
    copies: Arc<AtomicUsize>,
}

impl Counted {
    fn new(copies: &Arc<AtomicUsize>) -> Self {
        Self {
            sum: 0,
            count: 0,
            copies: Arc::clone(copies),
        }
    }

    fn add(&mut self, value: u64) {
        self.sum += value;
        self.count += 1;
    }

    fn total(&self) -> Option<Total> {
        (self.count > 0).then_some(total(self.sum, self.count))
    }
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.copies.fetch_add(1, Ordering::SeqCst);
        Self {
            sum: self.sum,
            count: self.count,
            copies: Arc::clone(&self.copies),
        }
    }
}

#[derive(Clone, Default)]
struct StateCopies {
    hand_written: Arc<AtomicUsize>,
    on_eof: Arc<AtomicUsize>,
    every_n: Arc<AtomicUsize>,
    emit_always: Arc<AtomicUsize>,
    time_window: Arc<AtomicUsize>,
    ledger: Arc<AtomicUsize>,
}

impl StateCopies {
    fn by_stage(&self) -> BTreeMap<&'static str, usize> {
        [
            ("hand_written", &self.hand_written),
            ("on_eof", &self.on_eof),
            ("every_n", &self.every_n),
            ("emit_always", &self.emit_always),
            ("time_window", &self.time_window),
            ("ledger", &self.ledger),
        ]
        .into_iter()
        .map(|(stage, copies)| (stage, copies.load(Ordering::SeqCst)))
        .collect()
    }
}

#[derive(Clone, Debug, Default)]
struct Readings {
    next: usize,
}

impl TypedFiniteSourceHandler for Readings {
    type Output = Reading;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let Some(value) = VALUES.get(self.next).copied() else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(vec![Reading { value }]))
    }
}

#[derive(Clone, Debug)]
struct Pairs {
    copies: Arc<AtomicUsize>,
}

impl TypedStatefulHandler for Pairs {
    type State = Counted;
    type Input = Reading;
    type Output = Total;

    fn initial_state(&self) -> Self::State {
        Counted::new(&self.copies)
    }

    fn accumulate(&self, state: &mut Self::State, reading: Reading) {
        state.add(reading.value);
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state.count >= 2
    }

    fn emit(&self, state: &Self::State) -> Result<StatefulEmission<Self::Output>, HandlerError> {
        Ok(StatefulEmission::ResetEpoch {
            outputs: state.total().into_iter().collect(),
        })
    }

    fn post_emit(&self, state: &mut Self::State) {
        state.sum = 0;
        state.count = 0;
    }
}

#[derive(Clone, Debug)]
struct Sum {
    copies: Arc<AtomicUsize>,
}

impl Accumulator for Sum {
    type State = Counted;
    type Input = Reading;
    type Output = Total;

    fn initial_state(&self) -> Self::State {
        Counted::new(&self.copies)
    }

    fn accumulate(&self, state: &mut Self::State, reading: Reading) {
        state.add(reading.value);
    }

    fn outputs(&self, state: &Self::State) -> Vec<Self::Output> {
        state.total().into_iter().collect()
    }
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct Ledger {
    copies: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulStatefulHandler for Ledger {
    type State = Counted;
    type Input = Reading;
    type Output = Total;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    fn initial_state(&self) -> Self::State {
        Counted::new(&self.copies)
    }

    async fn decide(
        &mut self,
        state: &Self::State,
        reading: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.emit(total(state.sum + reading.value, state.count + 1))
            .await?;
        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        state.sum = fact.sum;
        state.count = fact.count;
        Ok(())
    }
}

fn build_flow(journal_base: PathBuf, copies: StateCopies) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let readings = Readings::default();
        let hand_written = Pairs {
            copies: copies.hand_written.clone(),
        };
        let on_eof = StatefulWithEmission::new(
            Sum {
                copies: copies.on_eof.clone(),
            },
            OnEOF,
        );
        let every_n = StatefulWithEmission::new(
            Sum {
                copies: copies.every_n.clone(),
            },
            EveryN::new(2),
        );
        let emit_always = StatefulWithEmission::new(
            Sum {
                copies: copies.emit_always.clone(),
            },
            EmitAlways,
        );
        let time_window = StatefulWithEmission::new(
            Sum {
                copies: copies.time_window.clone(),
            },
            TimeWindow::new(Duration::ZERO),
        );
        let ledger = Ledger {
            copies: copies.ledger.clone(),
        };
        let hand_written_sink = replay_testkit::Discard::<Total>::default();
        let on_eof_sink = replay_testkit::Discard::<Total>::default();
        let every_n_sink = replay_testkit::Discard::<Total>::default();
        let emit_always_sink = replay_testkit::Discard::<Total>::default();
        let time_window_sink = replay_testkit::Discard::<Total>::default();
        let ledger_sink = replay_testkit::Discard::<Total>::default();

        Ok(flow! {
            name: "stateful_state_copies",
            journals: disk_journals(journal_base),

            stages: {
                readings = source!(Reading => readings);
                hand_written = stateful!(Reading -> Total => hand_written);
                on_eof = stateful!(Reading -> Total => on_eof);
                every_n = stateful!(Reading -> Total => every_n);
                emit_always = stateful!(Reading -> Total => emit_always);
                time_window = stateful!(Reading -> Total => time_window);
                ledger = effectful_stateful!(Reading -> Total => ledger);
                hand_written_sink = sink!(Total => hand_written_sink);
                on_eof_sink = sink!(Total => on_eof_sink);
                every_n_sink = sink!(Total => every_n_sink);
                emit_always_sink = sink!(Total => emit_always_sink);
                time_window_sink = sink!(Total => time_window_sink);
                ledger_sink = sink!(Total => ledger_sink);
            },

            topology: {
                readings |> hand_written;
                readings |> on_eof;
                readings |> every_n;
                readings |> emit_always;
                readings |> time_window;
                readings |> ledger;
                hand_written |> hand_written_sink;
                on_eof |> on_eof_sink;
                every_n |> every_n_sink;
                emit_always |> emit_always_sink;
                time_window |> time_window_sink;
                ledger |> ledger_sink;
            }
        })
    })
}

async fn run(journal_base: &Path, copies: &StateCopies, replay_from: Option<&Path>) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(journal_base.to_path_buf(), copies.clone()))
        .await
        .expect("stateful state copy flow completes");
}

async fn totals(run_dir: &Path, stage_key: &str) -> Vec<Total> {
    replay_testkit::read_stage_envelopes_appended(run_dir, stage_key)
        .await
        .iter()
        .filter_map(|record| Total::from_event(&record.authored()))
        .collect()
}

async fn assert_every_boundary_emitted(run_dir: &Path) {
    assert_eq!(
        totals(run_dir, "hand_written").await,
        vec![total(3, 2), total(7, 2), total(5, 1)]
    );
    assert_eq!(totals(run_dir, "on_eof").await, vec![total(15, 5)]);
    assert_eq!(
        totals(run_dir, "every_n").await,
        vec![total(3, 2), total(10, 4), total(15, 5)]
    );
    assert_eq!(
        totals(run_dir, "emit_always").await,
        vec![
            total(1, 1),
            total(3, 2),
            total(6, 3),
            total(10, 4),
            total(15, 5),
            total(15, 5),
        ]
    );
    assert_eq!(
        totals(run_dir, "time_window").await,
        vec![
            total(1, 1),
            total(2, 1),
            total(3, 1),
            total(4, 1),
            total(5, 1),
        ]
    );
    assert_eq!(
        totals(run_dir, "ledger").await,
        vec![
            total(1, 1),
            total(3, 2),
            total(6, 3),
            total(10, 4),
            total(15, 5),
        ]
    );
}

fn assert_no_state_copies(copies: &StateCopies) {
    let by_stage = copies.by_stage();
    assert!(
        by_stage.values().all(|copies| *copies == 0),
        "state copies by stage: {by_stage:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stateful_stages_emit_drain_and_fold_without_copying_state() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let copies = StateCopies::default();

    run(&journal_base, &copies, None).await;
    let live = replay_testkit::latest_run_dir(&journal_base);
    assert_every_boundary_emitted(&live).await;
    assert_no_state_copies(&copies);

    run(&journal_base, &copies, Some(&live)).await;
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    assert_every_boundary_emitted(&replay).await;
    assert_no_state_copies(&copies);
}
