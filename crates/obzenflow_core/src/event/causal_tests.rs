// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::event::ChainEventFactory;
use crate::journal::causal::{CausalProof, CausalProofCache};

fn root(run: FlowId) -> CausalCommit {
    CausalCommit::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new(), crate::StageId::new().into()),
        EventId::new(),
        None,
        &CausalFrontier::default(),
    )
    .unwrap()
    .0
}

#[test]
fn frontier_merge_is_associative_commutative_and_idempotent_including_ties() {
    let run = FlowId::new();
    let a = root(run);
    let (b, _) = CausalCommit::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new(), crate::StageId::new().into()),
        EventId::new(),
        None,
        &a.frontier(),
    )
    .unwrap();
    let (c, _) = CausalCommit::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new(), crate::StageId::new().into()),
        EventId::new(),
        None,
        &a.frontier(),
    )
    .unwrap();
    let inputs = [a.frontier(), b.frontier(), c.frontier()];
    let mut expected = CausalFrontier::default();
    for input in &inputs {
        expected.merge(input).unwrap();
    }
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
        assert!(actual.references().len() <= actual.clock().clocks.len());
    }
    assert!(CausalOrderingService::are_concurrent(&b.clock, &c.clock));
}

#[test]
fn exact_proof_rejects_invented_components_and_conflicting_full_references() {
    let run = FlowId::new();
    let a = root(run);
    let b = root(run);
    let mut input = a.frontier();
    input.merge(&b.frontier()).unwrap();
    let (mut child, witnesses) = CausalCommit::prepare(
        run,
        CausalCoordinate::new(JournalWriterId::new(), crate::StageId::new().into()),
        EventId::new(),
        None,
        &input,
    )
    .unwrap();
    let mut cache = CausalProofCache::new(10, 100);
    cache.admit(a.clone()).unwrap();
    assert!(matches!(
        cache.verify(&child, &witnesses),
        CausalProof::Unresolved { .. }
    ));
    cache.admit(b).unwrap();
    assert!(matches!(
        cache.verify(&child, &witnesses),
        CausalProof::Valid { .. }
    ));
    child
        .clock
        .clocks
        .insert(root(run).reference.coordinate(), 999);
    assert!(matches!(
        cache.verify(&child, &witnesses),
        CausalProof::Invalid { .. }
    ));
    let mut impostor = a.clone();
    impostor.reference.event_id = EventId::new();
    assert_eq!(
        cache.admit(impostor).unwrap_err(),
        CausalError::ConflictingCommitment
    );
    let mut wrong_run = a.clone();
    wrong_run.reference.run_id = FlowId::new();
    assert_eq!(
        cache.admit(wrong_run).unwrap_err(),
        CausalError::ConflictingCommitment
    );
    let mut bounded = CausalProofCache::new(0, 0);
    bounded.admit(a).unwrap();
    assert!(matches!(
        bounded.verify(&child, &witnesses),
        CausalProof::Unresolved {
            budget_exhausted: true,
            ..
        }
    ));
}

#[test]
fn destination_claims_and_counter_exhaustion_fail_before_preparation() {
    let run = FlowId::new();
    let mut prior = root(run);
    let coordinate = prior.reference.coordinate();
    assert_eq!(
        CausalCommit::prepare(run, coordinate, EventId::new(), None, &prior.frontier())
            .unwrap_err(),
        CausalError::FutureDestination
    );
    prior.reference.sequence = u64::MAX;
    prior.clock.clocks.insert(coordinate, u64::MAX);
    assert_eq!(
        CausalCommit::prepare(
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
fn admission_rejects_legacy_keys_duplicate_coordinates_and_malformed_references() {
    use serde_json::json;

    let record = JournalRecord::new(
        JournalWriterId::new(),
        ChainEventFactory::data_event(crate::StageId::new().into(), "test", json!({})),
    );
    let clock = serde_json::to_value(&record.envelope.provenance.journal.vector_clock).unwrap();
    let entry = clock["entries"][0].clone();
    assert!(serde_json::from_value::<VectorClock>(json!({"clocks": {"author": 1}})).is_err());
    assert!(serde_json::from_value::<VectorClock>(json!({"entries": [entry, entry]})).is_err());
    let mut zero = entry;
    zero["sequence"] = json!(0);
    assert!(serde_json::from_value::<VectorClock>(json!({"entries": [zero]})).is_err());

    let reference = CausalCommit::from_record(&record).unwrap().reference;
    for malformed in [
        reference,
        CommittedCausalRef {
            sequence: 0,
            ..reference
        },
    ] {
        let mut invalid = record.clone();
        invalid.envelope.provenance.journal.causal.witnesses = vec![malformed];
        assert!(CausalFrontier::from_record(&invalid).is_err());
    }
    let mut invalid = record;
    invalid
        .envelope
        .provenance
        .journal
        .vector_clock
        .clocks
        .insert(reference.coordinate(), 2);
    invalid.envelope.provenance.journal.causal.previous = Some(CommittedCausalRef {
        run_id: FlowId::new(),
        ..reference
    });
    assert!(CausalFrontier::from_record(&invalid).is_err());
}

#[test]
fn witnesses_stay_linear_in_coordinates_independent_of_history_length() {
    for participants in [1, 8, 32] {
        let run = FlowId::new();
        let mut frontier = CausalFrontier::default();
        for _ in 0..participants {
            frontier.merge(&root(run).frontier()).unwrap();
        }
        let coordinate =
            CausalCoordinate::new(JournalWriterId::new(), crate::StageId::new().into());
        let mut previous = None;
        let mut total_bytes = 0;
        for _ in 0..512 {
            let (commitment, witnesses) = CausalCommit::prepare(
                run,
                coordinate,
                EventId::new(),
                previous.as_ref(),
                &frontier,
            )
            .unwrap();
            assert!(witnesses.witnesses.len() <= participants + 1);
            let bytes = serde_json::to_vec(&witnesses).unwrap().len()
                + serde_json::to_vec(&commitment.clock).unwrap().len();
            total_bytes += bytes;
            assert!(bytes < (participants + 2) * 600);
            frontier.merge(&commitment.frontier()).unwrap();
            previous = Some(commitment);
        }
        assert!(total_bytes < 512 * (participants + 2) * 600);
    }
}
