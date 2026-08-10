// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Product-catalogue regression for first-class stateful accumulators.

use obzenflow::typed::stateful as typed_stateful;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::stateful::strategies::accumulators::{
    Accumulator, Conflate, GroupBy, Reduce, TopN, TopNBy, TopNBySnapshot, TopNSnapshot,
};
use obzenflow_runtime::stages::TypedStatefulHandler;
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
fn typed_stateful_helpers_construct_the_first_class_accumulators() {
    let reduce = typed_stateful::reduce(Count::default(), |count: &mut Count, _score: &Score| {
        count.0 += 1;
    });
    let conflate = typed_stateful::conflate(|score: &Score| score.key.clone());
    let group_by = typed_stateful::group_by(
        |score: &Score| score.key.clone(),
        |count: &mut u64, _score: &Score| *count += 1,
        |key: &String, count: &u64| KeyCount {
            key: key.clone(),
            count: *count,
        },
    );
    let top_n = typed_stateful::top_n(
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
    let top_n_by = typed_stateful::top_n_by(
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
