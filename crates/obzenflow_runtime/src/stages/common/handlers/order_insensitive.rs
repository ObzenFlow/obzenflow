// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095l: the metamorphic commutativity trial behind `#[order_insensitive]`.
//!
//! A stage may declare its fold order-invariant (a barrier) only with a proof.
//! The proof is this trial: for each sample input word, the handler's observable
//! output must be identical under every permutation of that word. The proof is
//! bound to the declaration by the `#[order_insensitive]` attribute, which emits a
//! test calling [`assert_order_insensitive`] and is the only minter of the witness
//! a `OrderInsensitive` declaration carries.

use crate::stages::common::handler_error::HandlerError;
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

/// Project one emission result into the run-id-free observable word. On error a
/// single sentinel row is produced, so an order-dependent failure still surfaces as
/// a divergence rather than vanishing.
fn project_emission(result: Result<Vec<ChainEvent>, HandlerError>) -> Vec<ProjectedOutput> {
    match result {
        Ok(events) => events
            .iter()
            .map(|event| (event.event_type(), event.payload()))
            .collect(),
        Err(err) => vec![(format!("<handler-error: {err:?}>"), serde_json::Value::Null)],
    }
}

/// Observe a stateful handler's transition over one input word: run the same
/// protocol the supervisor runs, then project the emitted word and the carried
/// state.
///
/// The supervisor folds each event and, after each one, checks `should_emit` and
/// may `emit` mid-stream (`stages/stateful/supervisor/running.rs`), then `drain`s at
/// EOF (`stages/stateful/supervisor/draining.rs`). A batch fold plus one final
/// `create_events` misses every mid-stream emission, so a handler can look
/// order-invariant in its final output while its durable emitted facts depend on
/// order. This drives the real protocol instead: per data event `accumulate` then
/// `should_emit`/`emit`, and a terminal `drain`, accumulating every emission into
/// one observable word.
///
/// Faithfulness rules drawn from the supervisor:
/// - The handler is cloned per transition (`(*ctx.handler).clone()`); `State` is the
///   only carrier. So this clones the prototype for each step and threads one state,
///   never persisting `&mut self` across events.
/// - `emit_interval_hint` is processing-time emission, not an input-trace property,
///   so it cannot be proven by permuting inputs and is refused here (FLOWIP-120d).
///
/// The state digest is the handler's `state_digest`, or a full serialization when
/// the handler does not narrow it (FLOWIP-095l Gap 11). Comparing the state as well
/// as the output stops a fold with order-dependent state and order-invariant output
/// from being declared a barrier and silently breaking resume's `S_N`.
fn observe_transition<H>(
    make: &impl Fn() -> H,
    word: &[ChainEvent],
) -> (Vec<ProjectedOutput>, serde_json::Value)
where
    H: StatefulHandler + Clone,
    H::State: serde::Serialize,
{
    let prototype = make();
    assert!(
        prototype.emit_interval_hint().is_none(),
        "FLOWIP-095l #[order_insensitive] does not support emit_interval_hint: processing-time \
         emission is not an input-trace property, so permuting inputs cannot prove it \
         order-invariant (see FLOWIP-120d). Declare the handler OrderSensitive instead."
    );

    let mut state = prototype.initial_state();
    let mut output = Vec::new();
    for event in word {
        // Fresh clone per transition, like the supervisor; state is the carrier.
        let mut handler = prototype.clone();
        handler.accumulate(&mut state, event.clone());
        if handler.should_emit(&mut state) {
            output.extend(project_emission(prototype.clone().emit(&mut state)));
        }
    }
    // Terminal drain mirrors the supervisor's EOF path. Drain is async; block on it
    // with a minimal executor so the trial and its proc-macro test stay synchronous.
    // A barrier drain is a pure projection of state, so no tokio reactor is needed.
    let drain_handler = prototype.clone();
    output.extend(project_emission(futures::executor::block_on(
        drain_handler.drain(&state),
    )));

    let state_digest = match prototype.state_digest(&state) {
        serde_json::Value::Null => serde_json::to_value(&state).unwrap_or(serde_json::Value::Null),
        narrowed => narrowed,
    };
    (output, state_digest)
}

/// FLOWIP-095l: assert a stateful handler's observable transition is invariant
/// under input permutation. For each sample word, every permutation must produce
/// the same output word AND the same reconstructed state as the original order
/// (Gap 11: the state is checked because resume reconstructs it). Sound and
/// conservative over the sampled words, which is exactly what a
/// `#[order_insensitive]` barrier claims.
///
/// Permutations are exhaustive for words up to length 6 and a deterministic
/// sample (reversal, every rotation, adjacent swaps) beyond that, so a long word
/// still exercises non-trivial reorderings without factorial blow-up.
pub fn assert_order_insensitive<H>(make: impl Fn() -> H, words: &[Vec<ChainEvent>])
where
    H: StatefulHandler + Clone,
    H::State: serde::Serialize,
{
    // A sample with no word of length >= 2 has nothing to reorder, so the trial
    // would pass vacuously and mint a barrier with no evidence. Refuse it.
    assert!(
        words.iter().any(|word| word.len() >= 2),
        "FLOWIP-095l #[order_insensitive] needs at least one sample word with >= 2 inputs; an \
         empty or singleton sample proves nothing. Provide representative concurrent inputs in \
         `inputs = ...` (include short words, since resume frontiers are per-multiset)."
    );
    for (index, word) in words.iter().enumerate() {
        let (baseline_output, baseline_state) = observe_transition(&make, word);
        for permutation in permutations(word) {
            let (output, state) = observe_transition(&make, &permutation);
            assert!(
                output == baseline_output,
                "FLOWIP-095l #[order_insensitive] trial failed on sample word {index}: the \
                 handler's OUTPUT depends on input order. A reordering produced {output:?} but \
                 the original order produced {baseline_output:?}. This handler is not \
                 order-invariant; declare it OrderSensitive instead of #[order_insensitive]."
            );
            assert!(
                state == baseline_state,
                "FLOWIP-095l #[order_insensitive] trial failed on sample word {index}: the \
                 handler's STATE depends on input order. Its output is order-invariant but its \
                 reconstructed state is not, and resume reconstructs state, so this would break \
                 S_N. A reordering produced state {state} but the original produced \
                 {baseline_state}. Declare it OrderSensitive, narrow `state_digest` to the \
                 order-invariant part."
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
        const EVENT_TYPE: &'static str = "test.order_insensitive.num";
    }

    #[derive(Serialize, Deserialize)]
    struct SumOut {
        total: u64,
    }
    impl TypedPayload for SumOut {
        const EVENT_TYPE: &'static str = "test.order_insensitive.sum";
    }

    #[derive(Serialize, Deserialize)]
    struct ListOut {
        values: Vec<u64>,
    }
    impl TypedPayload for ListOut {
        const EVENT_TYPE: &'static str = "test.order_insensitive.list";
    }

    #[derive(Serialize, Deserialize)]
    struct CountOut {
        n: u64,
    }
    impl TypedPayload for CountOut {
        const EVENT_TYPE: &'static str = "test.order_insensitive.count";
    }

    fn emit<T: TypedPayload>(payload: T) -> ChainEvent {
        payload.to_event(WriterId::from(StageId::new()))
    }

    /// Commutative: state is a running sum, output is the total.
    #[derive(Clone, Default)]
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
    #[derive(Clone, Default)]
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
        assert_order_insensitive(|| Sum, &sample());
    }

    #[test]
    #[should_panic(expected = "order_insensitive")]
    fn order_sensitive_handler_is_caught() {
        assert_order_insensitive(|| Listy, &sample());
    }

    /// FLOWIP-095l Gap 11 regression: order-dependent STATE, order-invariant
    /// OUTPUT. State is the arrival-ordered list; output is only its length, which
    /// commutes. The pre-Gap-11 output-only trial admitted this as a barrier; the
    /// state check now rejects it.
    #[derive(Clone, Default)]
    struct CountOnly;
    #[async_trait]
    impl StatefulHandler for CountOnly {
        type State = Vec<u64>;
        fn accumulate(&mut self, state: &mut Vec<u64>, event: ChainEvent) {
            state.push(event.payload()["value"].as_u64().unwrap_or(0));
        }
        fn initial_state(&self) -> Vec<u64> {
            Vec::new()
        }
        fn create_events(&self, state: &Vec<u64>) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![emit(CountOut {
                n: state.len() as u64,
            })])
        }
    }

    #[test]
    #[should_panic(expected = "STATE depends on input order")]
    fn order_dependent_state_is_caught_even_when_output_commutes() {
        assert_order_insensitive(|| CountOnly, &sample());
    }

    // End-to-end: the #[order_insensitive] attribute on a commutative handler.
    // `crate = crate` makes the generated paths resolve inside obzenflow_runtime
    // itself; external crates use the default `::obzenflow_runtime`.
    #[derive(Clone, Default)]
    struct ProvenSum;
    #[crate::order_insensitive(
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
    fn attribute_injects_order_insensitive_declaration() {
        assert!(matches!(
            ProvenSum.declared_input_order(),
            InputOrderSemantics::OrderInsensitive(_)
        ));
    }

    /// A four-input sample, long enough for a two-input emission window to fire
    /// twice. Resume frontiers are per-multiset, so short words matter.
    fn pair_sample() -> Vec<Vec<ChainEvent>> {
        vec![word_from([
            Num { value: 1 },
            Num { value: 2 },
            Num { value: 3 },
            Num { value: 4 },
        ])]
    }

    #[derive(Serialize, Deserialize)]
    struct PairOut {
        values: Vec<u64>,
    }
    impl TypedPayload for PairOut {
        const EVENT_TYPE: &'static str = "test.order_insensitive.pair";
    }

    #[derive(Clone, Default, Serialize)]
    struct PairState {
        total: u64,
        pending: Vec<u64>,
    }

    /// FLOWIP-095l increment-2 regression: order-invariant final state and an empty
    /// final `create_events`, but order-DEPENDENT mid-stream emissions. `should_emit`
    /// fires every two inputs and `emit` writes the pair in arrival order, so
    /// `[1,2,3,4]` emits `[1,2],[3,4]` while `[2,1,3,4]` emits `[2,1],[3,4]`. The
    /// batch-fold trial admitted this as a barrier; the transition trial catches it on
    /// the emitted word. Its `state_digest` even narrows to `total`, so the state
    /// check passes honestly and the emitted-word check is the only witness.
    #[derive(Clone, Default)]
    struct PairEmitter;
    #[async_trait]
    impl StatefulHandler for PairEmitter {
        type State = PairState;
        fn accumulate(&mut self, state: &mut PairState, event: ChainEvent) {
            let n = event.payload()["value"].as_u64().unwrap_or(0);
            state.total += n;
            state.pending.push(n);
        }
        fn initial_state(&self) -> PairState {
            PairState::default()
        }
        fn should_emit(&self, state: &mut PairState) -> bool {
            state.pending.len() == 2
        }
        fn emit(&self, state: &mut PairState) -> Result<Vec<ChainEvent>, HandlerError> {
            let values = std::mem::take(&mut state.pending);
            Ok(vec![emit(PairOut { values })])
        }
        fn create_events(&self, _state: &PairState) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(Vec::new())
        }
        fn state_digest(&self, state: &PairState) -> serde_json::Value {
            serde_json::json!({ "total": state.total })
        }
    }

    #[test]
    #[should_panic(expected = "OUTPUT depends on input order")]
    fn order_dependent_mid_stream_emission_is_caught() {
        assert_order_insensitive(|| PairEmitter, &pair_sample());
    }

    /// Windowed emission that IS order-invariant: emit the running input count every
    /// two inputs. The count after k inputs is k regardless of order, so every
    /// permutation emits the same word. Proves the trial admits genuine
    /// order-invariant windowed emitters rather than rejecting all windowing.
    #[derive(Clone, Default)]
    struct RunningCount;
    #[async_trait]
    impl StatefulHandler for RunningCount {
        type State = u64;
        fn accumulate(&mut self, state: &mut u64, _event: ChainEvent) {
            *state += 1;
        }
        fn initial_state(&self) -> u64 {
            0
        }
        fn should_emit(&self, state: &mut u64) -> bool {
            (*state).is_multiple_of(2)
        }
        fn create_events(&self, state: &u64) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![emit(CountOut { n: *state })])
        }
    }

    #[test]
    fn order_invariant_windowed_emitter_passes() {
        assert_order_insensitive(|| RunningCount, &pair_sample());
    }

    /// A timer-emitting handler: `emit_interval_hint` is set, so the trial refuses it
    /// because processing-time emission is not an input-trace property (FLOWIP-120d).
    #[derive(Clone, Default)]
    struct Ticking;
    #[async_trait]
    impl StatefulHandler for Ticking {
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
        fn emit_interval_hint(&self) -> Option<std::time::Duration> {
            Some(std::time::Duration::from_millis(1))
        }
    }

    #[test]
    #[should_panic(expected = "emit_interval_hint")]
    fn timer_emitting_handler_is_refused() {
        assert_order_insensitive(|| Ticking, &pair_sample());
    }

    #[test]
    #[should_panic(expected = ">= 2 inputs")]
    fn singleton_sample_is_refused() {
        let words = vec![word_from([Num { value: 1 }])];
        assert_order_insensitive(|| Sum, &words);
    }
}
