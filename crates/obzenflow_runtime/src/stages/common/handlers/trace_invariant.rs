// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095l: the metamorphic commutativity trial behind `#[trace_invariant]`.
//!
//! A stage may declare its fold order-invariant (a barrier) only with a proof.
//! The proof is this trial: for each sample input word, the handler's observable
//! output must be identical under every permutation of that word. The proof is
//! bound to the declaration by the `#[trace_invariant]` attribute, which emits a
//! test calling [`assert_trace_invariant`] and is the only minter of the witness
//! a `TraceInvariant` declaration carries.

use crate::stages::common::handlers::stateful::StatefulHandler;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::{ChainEvent, StageId, WriterId};

/// The run-id-free observable form of one emitted event: its type and payload.
/// Writer and event ids are dropped, so only order-bearing content survives.
pub type ProjectedOutput = (String, serde_json::Value);

/// Marshal typed inputs into a `ChainEvent` word, the way a stage edge would. The
/// writer id is a fixed placeholder the trial projects away.
pub fn word_from<T: TypedPayload>(inputs: impl IntoIterator<Item = T>) -> Vec<ChainEvent> {
    let writer = WriterId::from(StageId::new());
    inputs
        .into_iter()
        .map(|input| input.to_event(writer))
        .collect()
}

/// Observe a stateful handler's output for one input word: fold the word in, then
/// project `create_events` to a run-id-free `(event_type, payload)` sequence.
pub fn observe_output<H: StatefulHandler>(
    make: &impl Fn() -> H,
    word: &[ChainEvent],
) -> Vec<ProjectedOutput> {
    let mut handler = make();
    let mut state = handler.initial_state();
    for event in word {
        handler.accumulate(&mut state, event.clone());
    }
    match handler.create_events(&state) {
        Ok(events) => events
            .iter()
            .map(|event| (event.event_type(), event.payload()))
            .collect(),
        Err(err) => vec![(format!("<handler-error: {err:?}>"), serde_json::Value::Null)],
    }
}

/// FLOWIP-095l: assert a stateful handler's observable output is invariant under
/// input permutation. For each sample word, every permutation must produce the
/// same output as the original order. Sound and conservative: it proves
/// output-order-invariance over the sampled words, which is exactly what a
/// `#[trace_invariant]` barrier claims.
///
/// Permutations are exhaustive for words up to length 6 and a deterministic
/// sample (reversal, every rotation, adjacent swaps) beyond that, so a long word
/// still exercises non-trivial reorderings without factorial blow-up.
pub fn assert_trace_invariant<H: StatefulHandler>(make: impl Fn() -> H, words: &[Vec<ChainEvent>]) {
    for (index, word) in words.iter().enumerate() {
        let baseline = observe_output(&make, word);
        for permutation in permutations(word) {
            let observed = observe_output(&make, &permutation);
            assert!(
                observed == baseline,
                "FLOWIP-095l #[trace_invariant] trial failed on sample word {index}: the \
                 handler's output depends on input order. A reordering produced {observed:?} \
                 but the original order produced {baseline:?}. This handler is not \
                 order-invariant; declare it OrderSensitive instead of #[trace_invariant]."
            );
        }
    }
}

/// Permutations of `word`: exhaustive (Heap's algorithm) up to length 6, else a
/// deterministic sample that still reorders meaningfully.
fn permutations(word: &[ChainEvent]) -> Vec<Vec<ChainEvent>> {
    if word.len() <= 1 {
        return vec![word.to_vec()];
    }
    if word.len() <= 6 {
        let mut out = Vec::new();
        let mut scratch = word.to_vec();
        let n = scratch.len();
        heap_permute(n, &mut scratch, &mut out);
        return out;
    }
    let mut out = vec![word.to_vec()];
    let mut reversed = word.to_vec();
    reversed.reverse();
    out.push(reversed);
    for shift in 1..word.len() {
        let mut rotated = word.to_vec();
        rotated.rotate_left(shift);
        out.push(rotated);
    }
    for i in 0..word.len() - 1 {
        let mut swapped = word.to_vec();
        swapped.swap(i, i + 1);
        out.push(swapped);
    }
    out
}

fn heap_permute(k: usize, scratch: &mut Vec<ChainEvent>, out: &mut Vec<Vec<ChainEvent>>) {
    if k == 1 {
        out.push(scratch.clone());
        return;
    }
    for i in 0..k {
        heap_permute(k - 1, scratch, out);
        if k.is_multiple_of(2) {
            scratch.swap(i, k - 1);
        } else {
            scratch.swap(0, k - 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handler_error::HandlerError;
    use crate::stages::common::handlers::InputOrderSemantics;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Num {
        value: u64,
    }
    impl TypedPayload for Num {
        const EVENT_TYPE: &'static str = "test.trace_invariant.num";
    }

    #[derive(Serialize, Deserialize)]
    struct SumOut {
        total: u64,
    }
    impl TypedPayload for SumOut {
        const EVENT_TYPE: &'static str = "test.trace_invariant.sum";
    }

    #[derive(Serialize, Deserialize)]
    struct ListOut {
        values: Vec<u64>,
    }
    impl TypedPayload for ListOut {
        const EVENT_TYPE: &'static str = "test.trace_invariant.list";
    }

    fn emit<T: TypedPayload>(payload: T) -> ChainEvent {
        payload.to_event(WriterId::from(StageId::new()))
    }

    /// Commutative: state is a running sum, output is the total.
    #[derive(Default)]
    struct Sum;
    #[async_trait]
    impl StatefulHandler for Sum {
        type State = u64;
        fn accumulate(&mut self, state: &mut u64, event: ChainEvent) {
            *state += event.payload()["value"].as_u64().unwrap_or(0);
        }
        fn initial_state(&self) -> u64 {
            0
        }
        fn create_events(&self, state: &u64) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![emit(SumOut { total: *state })])
        }
    }

    /// Order-sensitive: state is the arrival-ordered list.
    #[derive(Default)]
    struct Listy;
    #[async_trait]
    impl StatefulHandler for Listy {
        type State = Vec<u64>;
        fn accumulate(&mut self, state: &mut Vec<u64>, event: ChainEvent) {
            state.push(event.payload()["value"].as_u64().unwrap_or(0));
        }
        fn initial_state(&self) -> Vec<u64> {
            Vec::new()
        }
        fn create_events(&self, state: &Vec<u64>) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![emit(ListOut {
                values: state.clone(),
            })])
        }
    }

    fn sample() -> Vec<Vec<ChainEvent>> {
        vec![word_from([
            Num { value: 1 },
            Num { value: 2 },
            Num { value: 3 },
        ])]
    }

    #[test]
    fn commutative_handler_passes() {
        assert_trace_invariant(|| Sum, &sample());
    }

    #[test]
    #[should_panic(expected = "trace_invariant")]
    fn order_sensitive_handler_is_caught() {
        assert_trace_invariant(|| Listy, &sample());
    }

    // End-to-end: the #[trace_invariant] attribute on a commutative handler.
    // `crate = crate` makes the generated paths resolve inside obzenflow_runtime
    // itself; external crates use the default `::obzenflow_runtime`.
    #[derive(Default)]
    struct ProvenSum;
    #[crate::trace_invariant(
        crate = crate,
        new = ProvenSum,
        inputs = vec![word_from([Num { value: 5 }, Num { value: 7 }, Num { value: 9 }])]
    )]
    #[async_trait]
    impl StatefulHandler for ProvenSum {
        type State = u64;
        fn accumulate(&mut self, state: &mut u64, event: ChainEvent) {
            *state += event.payload()["value"].as_u64().unwrap_or(0);
        }
        fn initial_state(&self) -> u64 {
            0
        }
        fn create_events(&self, state: &u64) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![emit(SumOut { total: *state })])
        }
    }

    #[test]
    fn attribute_injects_trace_invariant_declaration() {
        assert!(matches!(
            ProvenSum.declared_input_order(),
            InputOrderSemantics::TraceInvariant(_)
        ));
    }
}
