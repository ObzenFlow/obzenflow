// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Product-catalogue regression for first-class stateful accumulators.

use obzenflow::schema::TypedPayload;
use obzenflow::stages::stateful::{
    self, Accumulator, Conflate, EmissionStrategy, GroupBy, Reduce, StatefulEmission, TopN, TopNBy,
    TopNBySnapshot, TopNSnapshot, TypedStatefulHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Score {
    key: String,
    score: f64,
}

impl TypedPayload for Score {
    const EVENT_TYPE: &'static str = "stateful.catalog.score";
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Count(u64);

impl TypedPayload for Count {
    const EVENT_TYPE: &'static str = "stateful.catalog.count";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Ranking {
    keys: Vec<String>,
    scores: Vec<f64>,
}

impl TypedPayload for Ranking {
    const EVENT_TYPE: &'static str = "stateful.catalog.ranking";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct KeyCount {
    key: String,
    count: u64,
}

impl TypedPayload for KeyCount {
    const EVENT_TYPE: &'static str = "stateful.catalog.key_count";
}

fn assert_accumulator<A: Accumulator>(_accumulator: &A) {}

fn assert_stateful_handler<H>(_handler: H)
where
    H: TypedStatefulHandler<Input = Score, Output = Ranking>,
{
}

#[test]
fn every_accumulator_is_directly_constructible_as_a_public_strategy() {
    let reduce = Reduce::new(Count::default(), |count: &mut Count, _score: &Score| {
        count.0 += 1;
    });
    let conflate = Conflate::new(|score: &Score| score.key.clone());
    let group_by = GroupBy::new(
        |score: &Score| score.key.clone(),
        |count: &mut u64, _score: &Score| *count += 1,
        |key: &String, count: &u64| KeyCount {
            key: key.clone(),
            count: *count,
        },
    );
    let top_n = TopN::new(
        3,
        |score: &Score| score.key.clone(),
        |score: &Score| score.score,
        |snapshot: TopNSnapshot<String, Score>| Ranking {
            keys: snapshot
                .top_n
                .iter()
                .map(|entry| entry.key.clone())
                .collect(),
            scores: snapshot.top_n.iter().map(|entry| entry.score).collect(),
        },
    );
    let top_n_by = TopNBy::new(
        3,
        |score: &Score| score.key.clone(),
        |score: &Score| score.score,
        |snapshot: TopNBySnapshot<String, Score>| Ranking {
            keys: snapshot
                .top_n
                .iter()
                .map(|entry| entry.key.clone())
                .collect(),
            scores: snapshot
                .top_n
                .iter()
                .map(|entry| entry.total_score)
                .collect(),
        },
    );

    assert_accumulator(&reduce);
    assert_accumulator(&conflate);
    assert_accumulator(&group_by);
    assert_accumulator(&top_n);
    assert_accumulator(&top_n_by);

    assert_stateful_handler(top_n.emit_on_eof());
    assert_stateful_handler(top_n_by.emit_on_eof());
}

#[test]
fn stateful_helpers_construct_the_first_class_accumulators() {
    let reduce = stateful::reduce(Count::default(), |count: &mut Count, _score: &Score| {
        count.0 += 1;
    });
    let conflate = stateful::conflate(|score: &Score| score.key.clone());
    let group_by = stateful::group_by(
        |score: &Score| score.key.clone(),
        |count: &mut u64, _score: &Score| *count += 1,
        |key: &String, count: &u64| KeyCount {
            key: key.clone(),
            count: *count,
        },
    );
    let top_n = stateful::top_n(
        3,
        |score: &Score| score.key.clone(),
        |score: &Score| score.score,
        |snapshot: TopNSnapshot<String, Score>| Ranking {
            keys: snapshot
                .top_n
                .iter()
                .map(|entry| entry.key.clone())
                .collect(),
            scores: snapshot.top_n.iter().map(|entry| entry.score).collect(),
        },
    );
    let top_n_by = stateful::top_n_by(
        3,
        |score: &Score| score.key.clone(),
        |score: &Score| score.score,
        |snapshot: TopNBySnapshot<String, Score>| Ranking {
            keys: snapshot
                .top_n
                .iter()
                .map(|entry| entry.key.clone())
                .collect(),
            scores: snapshot
                .top_n
                .iter()
                .map(|entry| entry.total_score)
                .collect(),
        },
    );

    assert_accumulator(&reduce);
    assert_accumulator(&conflate);
    assert_accumulator(&group_by);
    assert_accumulator(&top_n);
    assert_accumulator(&top_n_by);
}

#[test]
fn top_n_replacement_and_top_n_by_aggregation_are_distinct_contracts() {
    let top_n = TopN::new(
        2,
        |score: &Score| score.key.clone(),
        |score: &Score| score.score,
        |snapshot: TopNSnapshot<String, Score>| Ranking {
            keys: snapshot
                .top_n
                .iter()
                .map(|entry| entry.key.clone())
                .collect(),
            scores: snapshot.top_n.iter().map(|entry| entry.score).collect(),
        },
    );
    let top_n_by = TopNBy::new(
        2,
        |score: &Score| score.key.clone(),
        |score: &Score| score.score,
        |snapshot: TopNBySnapshot<String, Score>| Ranking {
            keys: snapshot
                .top_n
                .iter()
                .map(|entry| entry.key.clone())
                .collect(),
            scores: snapshot
                .top_n
                .iter()
                .map(|entry| entry.total_score)
                .collect(),
        },
    );
    let mut replacement_state = Accumulator::initial_state(&top_n);
    let mut aggregate_state = Accumulator::initial_state(&top_n_by);

    for score in [
        Score {
            key: "a".into(),
            score: 10.0,
        },
        Score {
            key: "b".into(),
            score: 8.0,
        },
        Score {
            key: "a".into(),
            score: 4.0,
        },
    ] {
        Accumulator::accumulate(&top_n, &mut replacement_state, score.clone());
        Accumulator::accumulate(&top_n_by, &mut aggregate_state, score);
    }

    assert_eq!(
        Accumulator::outputs(&top_n, &replacement_state),
        vec![Ranking {
            keys: vec!["b".into(), "a".into()],
            scores: vec![8.0, 4.0],
        }]
    );
    assert_eq!(
        Accumulator::outputs(&top_n_by, &aggregate_state),
        vec![Ranking {
            keys: vec!["a".into(), "b".into()],
            scores: vec![14.0, 8.0],
        }]
    );
}

fn emit_and_advance<H: TypedStatefulHandler>(handler: &H, state: &mut H::State) -> Vec<H::Output> {
    let (next_state, outputs) = match handler.emit(state).expect("emit typed values") {
        StatefulEmission::RetainEpoch {
            next_state,
            outputs,
        }
        | StatefulEmission::ResetEpoch {
            next_state,
            outputs,
        } => (next_state, outputs),
    };
    *state = next_state;
    outputs
}

#[test]
fn conflate_emit_always_retains_the_latest_value_for_every_key() {
    let handler = stateful::conflate(|score: &Score| score.key.clone()).emit_always();
    let mut state = handler.initial_state();
    let mut snapshots = Vec::new();
    for (key, score) in [("a", 10.0), ("b", 8.0), ("a", 4.0)] {
        handler.accumulate(
            &mut state,
            Score {
                key: key.into(),
                score,
            },
        );
        assert!(handler.should_emit(&state));
        snapshots.push(
            emit_and_advance(&handler, &mut state)
                .into_iter()
                .map(|score| (score.key, score.score))
                .collect::<Vec<_>>(),
        );
        assert!(
            !handler.should_emit(&state),
            "emission consumes the cadence"
        );
    }
    assert_eq!(
        snapshots,
        vec![
            vec![("a".into(), 10.0)],
            vec![("a".into(), 10.0), ("b".into(), 8.0)],
            vec![("a".into(), 4.0), ("b".into(), 8.0)],
        ]
    );
    assert_eq!(
        handler.drain(&state).expect("drain the retained view"),
        vec![
            Score {
                key: "a".into(),
                score: 4.0
            },
            Score {
                key: "b".into(),
                score: 8.0
            },
        ]
    );
}

#[derive(Clone, Debug)]
struct EveryTwoScores;

impl EmissionStrategy for EveryTwoScores {
    fn should_emit(&self, events_seen: u64, _elapsed: Option<std::time::Duration>) -> bool {
        events_seen >= 2
    }
}

#[test]
fn custom_emission_resets_its_cadence_and_retains_the_fold() {
    let handler = stateful::reduce(Count::default(), |count: &mut Count, _: &Score| {
        count.0 += 1;
    })
    .with_emission(EveryTwoScores);
    let mut state = handler.initial_state();
    let mut outputs = Vec::new();
    for index in 1..=5 {
        handler.accumulate(
            &mut state,
            Score {
                key: "a".into(),
                score: 1.0,
            },
        );
        assert_eq!(handler.should_emit(&state), index % 2 == 0);
        if handler.should_emit(&state) {
            outputs.extend(emit_and_advance(&handler, &mut state));
        }
    }
    assert_eq!(outputs, vec![Count(2), Count(4)]);
    assert_eq!(
        handler.drain(&state).expect("flush the partial period"),
        vec![Count(5)]
    );
}

#[derive(Clone, Debug)]
struct CountScores;

impl Accumulator for CountScores {
    type State = u64;
    type Input = Score;
    type Output = Count;

    fn initial_state(&self) -> u64 {
        0
    }

    fn accumulate(&self, total: &mut u64, _: Score) {
        *total += 1;
    }

    fn outputs(&self, total: &u64) -> Vec<Count> {
        vec![Count(*total)]
    }
}

#[test]
fn custom_accumulator_composes_with_the_public_emission_wrapper() {
    let handler = stateful::StatefulWithEmission::new(CountScores, stateful::OnEOF);
    let mut state = handler.initial_state();
    for key in ["a", "b", "a"] {
        handler.accumulate(
            &mut state,
            Score {
                key: key.into(),
                score: 1.0,
            },
        );
        assert!(!handler.should_emit(&state), "OnEOF waits for drain");
    }
    assert_eq!(
        handler
            .drain(&state)
            .expect("project the custom accumulator"),
        vec![Count(3)]
    );
}
