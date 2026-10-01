// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::event::ChainEventFactory;

fn root(run: FlowId) -> JournalClock {
    JournalClock::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new()),
        EventId::new(),
        None,
        &CausalFrontier::default(),
    )
    .unwrap()
    .0
}
fn input(commitment: &JournalClock) -> CausalFrontier {
    CausalFrontier {
        clock: commitment.clock.clone(),
    }
}

#[test]
fn frontier_merge_is_associative_commutative_and_idempotent_including_ties() {
    let run = FlowId::new();
    let a = root(run);
    let b = JournalClock::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new()),
        EventId::new(),
        None,
        &input(&a),
    )
    .unwrap()
    .0;
    let c = JournalClock::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new()),
        EventId::new(),
        None,
        &input(&a),
    )
    .unwrap()
    .0;
    let inputs = [input(&a), input(&b), input(&c)];
    let mut expected = CausalFrontier::default();
    for input in &inputs {
        expected.merge(input).unwrap();
    }
    assert_eq!(
        expected.clock.clocks,
        [
            (a.reference.coordinate(), 1),
            (b.reference.coordinate(), 1),
            (c.reference.coordinate(), 1),
        ]
        .into_iter()
        .collect()
    );
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let mut pair = inputs[order[1]].clone();
        pair.merge(&inputs[order[2]]).unwrap();
        let mut actual = inputs[order[0]].clone();
        actual.merge(&pair).unwrap();
        actual.merge(&actual.clone()).unwrap();
        assert_eq!(actual, expected);
    }
    assert!(CausalOrderingService::are_concurrent(&b.clock, &c.clock));
}

#[test]
fn append_is_exactly_the_maximum_of_previous_and_inputs_then_local_increment() {
    let run = FlowId::new();
    let a = root(run);
    let b = root(run);
    let prior = JournalClock::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new()),
        EventId::new(),
        None,
        &input(&a),
    )
    .unwrap()
    .0;
    let (next, predecessor) = JournalClock::prepare(
        run,
        prior.reference.coordinate(),
        EventId::new(),
        Some(&prior),
        &input(&b),
    )
    .unwrap();
    let mut expected = VectorClock {
        clocks: [
            (a.reference.coordinate(), 1),
            (b.reference.coordinate(), 1),
            (prior.reference.coordinate(), 2),
        ]
        .into_iter()
        .collect(),
    };
    assert_eq!(next.clock, expected);
    assert_eq!(predecessor, Some(prior.reference));
    let (later, _) = JournalClock::prepare(
        run,
        prior.reference.coordinate(),
        EventId::new(),
        Some(&next),
        &CausalFrontier::default(),
    )
    .unwrap();
    expected.clocks.insert(prior.reference.coordinate(), 3);
    assert_eq!(
        later.clock, expected,
        "an append without new inputs retains earlier knowledge"
    );
}

#[test]
fn destination_claims_and_counter_exhaustion_fail_before_preparation() {
    let run = FlowId::new();
    let mut prior = root(run);
    let coordinate = prior.reference.coordinate();
    assert_eq!(
        JournalClock::prepare(run, coordinate, EventId::new(), None, &input(&prior)).unwrap_err(),
        CausalError::FutureDestination
    );
    prior.reference.sequence = u64::MAX;
    prior.clock.clocks.insert(coordinate, u64::MAX);
    assert_eq!(
        JournalClock::prepare(
            run,
            coordinate,
            EventId::new(),
            Some(&prior),
            &CausalFrontier::default()
        )
        .unwrap_err(),
        CausalError::SequenceExhausted
    );
}

#[test]
fn admission_rejects_legacy_keys_duplicate_coordinates_and_invalid_predecessors() {
    use serde_json::json;
    let record = JournalRecord::new(
        JournalWriterId::new(),
        ChainEventFactory::data_event(
            crate::StageId::new().into(),
            "test",
            std::num::NonZeroU32::MIN,
            json!({}),
        ),
    );
    let clock = serde_json::to_value(&record.envelope.provenance.journal.vector_clock).unwrap();
    let entry = clock["entries"][0].clone();
    assert!(serde_json::from_value::<VectorClock>(json!({"clocks":{"author":1}})).is_err());
    assert!(serde_json::from_value::<VectorClock>(json!({"entries":[entry,entry]})).is_err());
    let mut zero = entry;
    zero["sequence"] = json!(0);
    assert!(serde_json::from_value::<VectorClock>(json!({"entries":[zero]})).is_err());
    let reference = record.commitment();
    for predecessor in [
        reference,
        JournalCommitRef {
            sequence: 0,
            ..reference
        },
    ] {
        let mut invalid = record.clone();
        invalid.envelope.provenance.journal.previous = Some(predecessor);
        assert!(JournalClock::from_record(&invalid).is_err());
    }
    let mut invalid = record;
    invalid
        .envelope
        .provenance
        .journal
        .vector_clock
        .clocks
        .insert(reference.coordinate(), 2);
    invalid.envelope.provenance.journal.previous = Some(JournalCommitRef {
        run_id: FlowId::new(),
        ..reference
    });
    assert!(JournalClock::from_record(&invalid).is_err());
}

#[test]
fn clock_width_depends_on_journals_incorporated_not_history_length() {
    for participants in [1, 8, 32] {
        let run = FlowId::new();
        let mut frontier = CausalFrontier::default();
        for _ in 0..participants {
            frontier.merge(&input(&root(run))).unwrap();
        }
        let coordinate = CausalCoordinate::new(JournalWriterId::new());
        let mut previous = None;
        for sequence in 1..=512 {
            let (commitment, predecessor) = JournalClock::prepare(
                run,
                coordinate,
                EventId::new(),
                previous.as_ref(),
                &frontier,
            )
            .unwrap();
            assert_eq!(commitment.clock.clocks.len(), participants + 1);
            assert_eq!(commitment.clock.get(&coordinate), sequence);
            assert_eq!(predecessor, previous.as_ref().map(|p| p.reference));
            frontier.merge(&input(&commitment)).unwrap();
            previous = Some(commitment);
        }
    }
}
