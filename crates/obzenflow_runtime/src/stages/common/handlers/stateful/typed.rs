// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed plain-stateful authoring and runtime erasure (FLOWIP-134e).

use super::traits::StatefulHandler;
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::stateful::strategies::accumulators::trace::TraceState;
use async_trait::async_trait;
use obzenflow_core::config::LineagePolicy;
use obzenflow_core::event::provenance::causality_context::CausalInput;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, EventId, OneFactStageOutput, TypedPayload, WriterId};
use std::time::Duration;

/// The outputs of one emission and whether they close the contribution epoch.
/// [`TypedStatefulHandler::post_emit`] moves the state once they are accepted.
///
/// ```compile_fail,E0107
/// use obzenflow_runtime::stages::stateful::StatefulEmission;
///
/// let _ = StatefulEmission::<u64, u64>::RetainEpoch {
///     next_state: 0,
///     outputs: Vec::new(),
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatefulEmission<O> {
    RetainEpoch { outputs: Vec<O> },
    ResetEpoch { outputs: Vec<O> },
}

/// Opaque evidence for one successfully decoded input.
///
/// Only framework-owned facades can inspect or attach this evidence. It is
/// passed through a hidden hook so ordinary authored handlers remain value-only.
#[doc(hidden)]
pub struct TypedStatefulContribution {
    event: ChainEvent,
    lineage: LineagePolicy,
}

impl TypedStatefulContribution {
    fn new(event: ChainEvent, lineage: LineagePolicy) -> Self {
        Self { event, lineage }
    }

    pub(crate) fn event_id(&self) -> EventId {
        self.event.id
    }

    pub(crate) fn record_into(&self, trace: &mut TraceState) {
        trace.record_event(&self.event, self.lineage);
    }
}

/// Opaque identities of one accepted emission's outputs, in emission order.
///
/// Only framework-owned facades can read them. They reach the handler through
/// a hidden hook so ordinary authored handlers never see an event id.
#[doc(hidden)]
pub struct TypedStatefulOutputIds {
    outputs: Vec<CausalInput>,
}

impl TypedStatefulOutputIds {
    pub(crate) fn new(outputs: Vec<CausalInput>) -> Self {
        Self { outputs }
    }

    pub(crate) fn outputs(&self) -> &[CausalInput] {
        &self.outputs
    }
}

/// One framework invocation before typed outputs are lowered to events.
#[doc(hidden)]
pub struct TypedStatefulInvocation<O> {
    emission: StatefulEmission<O>,
    output_traces: Option<Vec<TraceState>>,
}

impl<O> TypedStatefulInvocation<O> {
    pub(crate) fn facts_only(emission: StatefulEmission<O>) -> Self {
        Self {
            emission,
            output_traces: None,
        }
    }

    pub(crate) fn with_output_traces(
        emission: StatefulEmission<O>,
        output_traces: Vec<TraceState>,
    ) -> Self {
        Self {
            emission,
            output_traces: Some(output_traces),
        }
    }

    fn into_parts(self) -> (StatefulEmission<O>, Option<Vec<TraceState>>) {
        (self.emission, self.output_traces)
    }
}

/// A terminal typed invocation has outputs and evidence but no successor state.
#[doc(hidden)]
pub struct TypedStatefulDrainInvocation<O> {
    outputs: Vec<O>,
    output_traces: Option<Vec<TraceState>>,
}

impl<O> TypedStatefulDrainInvocation<O> {
    pub(crate) fn facts_only(outputs: Vec<O>) -> Self {
        Self {
            outputs,
            output_traces: None,
        }
    }

    pub(crate) fn with_output_traces(outputs: Vec<O>, output_traces: Vec<TraceState>) -> Self {
        Self {
            outputs,
            output_traces: Some(output_traces),
        }
    }

    fn into_parts(self) -> (Vec<O>, Option<Vec<TraceState>>) {
        (self.outputs, self.output_traces)
    }
}

/// Pure typed stateful surface whose associated types own the stage contract.
///
/// The handler is immutable transition behaviour. Together with `Self::State`
/// it forms the machine, and every replay-relevant evolving value belongs in
/// that explicit state. Plain stateful handlers never receive `Effects`:
/// effectful work composes through facts, while built-in processing-time
/// facades are probed by the live supervisor clock.
///
/// Every handler states its successor, so one without `post_emit` does not
/// compile:
///
/// ```compile_fail,E0046
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::stateful::{StatefulEmission, TypedStatefulHandler};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Fact;
/// # impl TypedPayload for Fact { const EVENT_TYPE: &'static str = "doc.fact"; }
///
/// struct Totals;
///
/// impl TypedStatefulHandler for Totals {
///     type State = u64;
///     type Input = Fact;
///     type Output = Fact;
///
///     fn initial_state(&self) -> u64 {
///         0
///     }
///
///     fn accumulate(&self, total: &mut u64, _: Fact) {
///         *total += 1;
///     }
///
///     fn emit(&self, _: &u64) -> StatefulEmission<Fact> {
///         StatefulEmission::RetainEpoch { outputs: vec![Fact] }
///     }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "this stateful handler does not witness its arrow contract",
    label = "this handler does not implement the typed plain-stateful contract",
    note = "implement TypedStatefulHandler with Input and Output matching the stateful! arrow (FLOWIP-134e)",
    note = "an accumulator becomes a handler with `.emit(...)`, after which `group_by` needs `.updated_groups()` or `.all_groups()` and `conflate` needs `.updated_keys()` or `.all_keys()`"
)]
pub trait TypedStatefulHandler: Send + Sync {
    type State: Clone + Send + Sync;
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;

    fn accumulate(&self, state: &mut Self::State, input: Self::Input);

    fn should_emit(&self, _state: &Self::State) -> bool {
        false
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    /// Projects the state into outputs and returns no error. A rejected
    /// aggregate is a named fact in `Output`, and a state not ready to emit
    /// keeps `should_emit` false. An output that cannot be serialized fails
    /// the stage.
    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output>;

    /// Runs once after an accepted emission, as Ptolemy's postfire runs after fire.
    /// It moves the state past that emission.
    ///
    /// The runtime calls this once per accepted emission, including one with no
    /// outputs. It never calls it after a failed emission, from `drain`, or from
    /// `should_emit`. `state` is the state `emit` just read, so derive the successor
    /// from it alone. Write an empty body when emission leaves the state as it was.
    fn post_emit(&self, state: &mut Self::State);

    fn drain(&self, state: &Self::State) -> Vec<Self::Output> {
        match self.emit(state) {
            StatefulEmission::RetainEpoch { outputs }
            | StatefulEmission::ResetEpoch { outputs } => outputs,
        }
    }

    /// Runtime-only accumulation hook for sealed contribution evidence.
    #[doc(hidden)]
    fn accumulate_invocation(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        _contribution: TypedStatefulContribution,
    ) {
        self.accumulate(state, input);
    }

    /// Runtime-only emission hook for exact facade contribution partitions.
    #[doc(hidden)]
    fn emit_invocation(&self, state: &Self::State) -> TypedStatefulInvocation<Self::Output> {
        TypedStatefulInvocation::facts_only(self.emit(state))
    }

    /// Runtime-only post-emission hook carrying the ids of the accepted outputs.
    #[doc(hidden)]
    fn post_emit_invocation(&self, state: &mut Self::State, _output_ids: TypedStatefulOutputIds) {
        self.post_emit(state);
    }

    /// Runtime-only terminal hook for exact facade contribution partitions.
    #[doc(hidden)]
    fn drain_invocation(&self, state: &Self::State) -> TypedStatefulDrainInvocation<Self::Output> {
        TypedStatefulDrainInvocation::facts_only(self.drain(state))
    }
}

/// Erased state pairs domain state with the bounded whole-batch contribution
/// frontier. The evidence never enters the authored state type.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TypedStatefulState<S> {
    inner: S,
    trace: TraceState,
}

/// Adapter from typed authoring to the existing stateful supervisor.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TypedStatefulHandlerAdapter<H> {
    handler: H,
    lineage: LineagePolicy,
    writer_id: Option<WriterId>,
}

impl<H> TypedStatefulHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            lineage: LineagePolicy::default(),
            writer_id: None,
        }
    }

    fn writer_id(&self) -> Result<WriterId, StageFatal> {
        self.writer_id.ok_or_else(|| {
            StageFatal::new(
                StageFatalCode::Configuration,
                StageFatalReason::ConfigurationInvariant,
                "typed stateful adapter invoked before runtime writer identity installation",
            )
        })
    }

    fn lower_outputs<O>(
        &self,
        outputs: Vec<O>,
        output_traces: Option<Vec<TraceState>>,
        fallback_trace: &TraceState,
    ) -> Result<Vec<ChainEvent>, StageFatal>
    where
        O: OneFactStageOutput,
    {
        let writer_id = self.writer_id()?;
        if let Some(traces) = output_traces.as_ref() {
            if traces.len() != outputs.len() {
                return Err(StageFatal::new(
                    StageFatalCode::Protocol,
                    StageFatalReason::ProtocolInputIntegrity,
                    format!(
                        "typed_stateful_contributions: {} outputs were paired with {} contribution frontiers",
                        outputs.len(),
                        traces.len(),
                    ),
                ));
            }
        }

        outputs
            .into_iter()
            .enumerate()
            .map(|(index, output)| {
                let mut facts = output.into_facts().map_err(|error| {
                    StageFatal::new(
                        StageFatalCode::Protocol,
                        StageFatalReason::OutputSerializationFailure,
                        format!("typed stateful output serialization failed: {error}"),
                    )
                })?;
                if facts.len() != 1 {
                    return Err(StageFatal::new(
                        StageFatalCode::Protocol,
                        StageFatalReason::ProtocolInputIntegrity,
                        format!(
                            "one_fact_stage_output: `{}` lowered to {} facts instead of exactly one",
                            std::any::type_name::<O>(),
                            facts.len(),
                        ),
                    ));
                }
                let fact = facts.pop().expect("length checked above");
                let mut event =
                    fact.into_event(writer_id);
                output_traces
                    .as_ref()
                    .and_then(|traces| traces.get(index))
                    .unwrap_or(fallback_trace)
                    .apply_to_event(&mut event);
                Ok(event)
            })
            .collect()
    }
}

#[async_trait]
impl<H> StatefulHandler for TypedStatefulHandlerAdapter<H>
where
    H: TypedStatefulHandler + Send + Sync,
{
    type State = TypedStatefulState<H::State>;

    fn install_lineage_policy(&mut self, policy: LineagePolicy) {
        self.lineage = policy;
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let _ = self.try_accumulate(state, event);
    }

    fn try_accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
    ) -> Result<(), HandlerError> {
        self.writer_id().map_err(HandlerError::Fatal)?;
        let input = H::Input::try_from_event(&event)
            .map_err(|error| HandlerError::Deserialization(error.to_string()))?;
        state.trace.record_event(&event, self.lineage);
        let contribution = TypedStatefulContribution::new(event, self.lineage);
        self.handler
            .accumulate_invocation(&mut state.inner, input, contribution);
        Ok(())
    }

    fn initial_state(&self) -> Self::State {
        TypedStatefulState {
            inner: self.handler.initial_state(),
            trace: TraceState::default(),
        }
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, StageFatal> {
        self.writer_id()?;
        let invocation = self.handler.emit_invocation(&state.inner);
        let (emission, traces) = invocation.into_parts();
        let outputs = match emission {
            StatefulEmission::RetainEpoch { outputs }
            | StatefulEmission::ResetEpoch { outputs } => outputs,
        };
        self.lower_outputs(outputs, traces, &state.trace)
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        self.handler.emit_interval_hint()
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        self.handler.should_emit(&state.inner)
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, StageFatal> {
        self.writer_id()?;
        let invocation = self.handler.emit_invocation(&state.inner);
        let (emission, traces) = invocation.into_parts();
        let (outputs, reset_epoch) = match emission {
            StatefulEmission::RetainEpoch { outputs } => (outputs, false),
            StatefulEmission::ResetEpoch { outputs } => (outputs, true),
        };
        let events = self.lower_outputs(outputs, traces, &state.trace)?;
        let outputs = events.iter().map(|event| CausalInput::of(event)).collect();
        self.handler
            .post_emit_invocation(&mut state.inner, TypedStatefulOutputIds::new(outputs));
        if reset_epoch {
            state.trace.reset();
        }
        Ok(events)
    }

    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>, StageFatal> {
        self.writer_id()?;
        let invocation = self.handler.drain_invocation(&state.inner);
        let (outputs, traces) = invocation.into_parts();
        self.lower_outputs(outputs, traces, &state.trace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::{StageId, TypedPayload};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Input {
        value: u32,
    }

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "typed_stateful.input";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Output {
        value: u32,
    }

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "typed_stateful.output";
    }

    #[derive(Clone, Debug, Default)]
    struct Counter {
        post_emits: Arc<AtomicUsize>,
    }

    impl TypedStatefulHandler for Counter {
        type State = u32;
        type Input = Input;
        type Output = Output;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
            *state += input.value;
        }

        fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
            StatefulEmission::RetainEpoch {
                outputs: vec![Output { value: *state }],
            }
        }

        fn post_emit(&self, state: &mut Self::State) {
            self.post_emits.fetch_add(1, Ordering::SeqCst);
            *state = state.saturating_add(1);
        }
    }

    fn input_event(value: u32) -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            Input::event_type_name(),
            Input::payload_schema_version(),
            serde_json::json!(Input { value }),
        )
    }

    #[test]
    fn adapter_uses_runtime_writer_canonical_key_and_whole_batch_frontier() {
        let writer_id = WriterId::from(StageId::new());
        let parent = input_event(7);
        let handler = Counter::default();
        let post_emits = handler.post_emits.clone();
        let mut adapter = TypedStatefulHandlerAdapter::new(handler);
        StatefulHandler::install_writer_id(&mut adapter, writer_id);
        let mut state = StatefulHandler::initial_state(&adapter);

        StatefulHandler::try_accumulate(&mut adapter, &mut state, parent.clone())
            .expect("typed input decodes");
        let outputs = StatefulHandler::emit(&adapter, &mut state).expect("emission lowers");

        assert_eq!(post_emits.load(Ordering::SeqCst), 1);
        assert_eq!(
            state.inner, 8,
            "post_emit moves the state after the outputs lower"
        );
        assert_eq!(state.trace.parent_ids(), vec![parent.id]);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].writer_id, writer_id);
        assert_eq!(outputs[0].event_type(), Output::event_type_name());
        assert_eq!(outputs[0].causality.parent_ids, vec![parent.id]);
        assert_eq!(Output::from_event(&outputs[0]), Some(Output { value: 7 }));
    }

    #[derive(Clone, Debug, Default)]
    struct RecordsOutputIds {
        received: Arc<Mutex<Vec<Vec<CausalInput>>>>,
    }

    impl TypedStatefulHandler for RecordsOutputIds {
        type State = u32;
        type Input = Input;
        type Output = Output;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
            *state += input.value;
        }

        fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
            StatefulEmission::RetainEpoch {
                outputs: vec![Output { value: *state }, Output { value: *state + 1 }],
            }
        }

        fn post_emit(&self, _: &mut Self::State) {}

        fn post_emit_invocation(
            &self,
            state: &mut Self::State,
            output_ids: TypedStatefulOutputIds,
        ) {
            self.received
                .lock()
                .expect("recorder lock")
                .push(output_ids.outputs().to_vec());
            self.post_emit(state);
        }
    }

    #[tokio::test]
    async fn only_an_accepted_emission_hands_its_output_ids_to_the_handler() {
        let handler = RecordsOutputIds::default();
        let received = handler.received.clone();
        let mut adapter = TypedStatefulHandlerAdapter::new(handler);
        StatefulHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        let mut state = StatefulHandler::initial_state(&adapter);
        StatefulHandler::try_accumulate(&mut adapter, &mut state, input_event(3))
            .expect("typed input decodes");

        StatefulHandler::create_events(&adapter, &state).expect("projection lowers");
        StatefulHandler::drain(&adapter, &state)
            .await
            .expect("drain lowers");
        assert!(received.lock().expect("recorder lock").is_empty());

        let outputs = StatefulHandler::emit(&adapter, &mut state).expect("emission lowers");
        let output_ids: Vec<_> = outputs
            .iter()
            .map(|output| CausalInput::of(output))
            .collect();
        assert_eq!(*received.lock().expect("recorder lock"), [output_ids]);
    }

    #[test]
    fn should_emit_and_create_events_never_post_emit() {
        let handler = Counter::default();
        let post_emits = handler.post_emits.clone();
        let mut adapter = TypedStatefulHandlerAdapter::new(handler);
        StatefulHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        let mut state = StatefulHandler::initial_state(&adapter);
        StatefulHandler::try_accumulate(&mut adapter, &mut state, input_event(2))
            .expect("typed input decodes");

        assert!(!StatefulHandler::should_emit(&adapter, &mut state));
        let projected =
            StatefulHandler::create_events(&adapter, &state).expect("projection lowers");

        assert_eq!(projected.len(), 1);
        assert_eq!(post_emits.load(Ordering::SeqCst), 0);
        assert_eq!(state.inner, 2);
    }

    #[derive(Clone, Debug, Serialize)]
    struct PanicOnDecode;

    impl<'de> Deserialize<'de> for PanicOnDecode {
        fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            panic!("writer installation must be checked before decoding")
        }
    }

    impl TypedPayload for PanicOnDecode {
        const EVENT_TYPE: &'static str = "typed_stateful.panic_on_decode";
    }

    #[derive(Clone, Debug)]
    struct WorkGuard {
        calls: Arc<AtomicUsize>,
    }

    impl TypedStatefulHandler for WorkGuard {
        type State = ();
        type Input = PanicOnDecode;
        type Output = Output;

        fn initial_state(&self) -> Self::State {}

        fn accumulate(&self, _state: &mut Self::State, _input: Self::Input) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }

        fn emit(&self, _state: &Self::State) -> StatefulEmission<Self::Output> {
            StatefulEmission::RetainEpoch {
                outputs: Vec::new(),
            }
        }

        fn post_emit(&self, _: &mut Self::State) {}
    }

    #[test]
    fn adapter_fails_closed_before_decode_or_handler_work_without_writer() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut adapter = TypedStatefulHandlerAdapter::new(WorkGuard {
            calls: calls.clone(),
        });
        let mut state = StatefulHandler::initial_state(&adapter);
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            PanicOnDecode::event_type_name(),
            PanicOnDecode::payload_schema_version(),
            serde_json::json!({}),
        );

        let error = StatefulHandler::try_accumulate(&mut adapter, &mut state, event)
            .expect_err("unbound adapter must fail closed");

        let HandlerError::Fatal(fatal) = error else {
            panic!("missing runtime identity must be fatal")
        };
        assert_eq!(fatal.code, StageFatalCode::Configuration);
        assert_eq!(fatal.reason, StageFatalReason::ConfigurationInvariant);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
    struct DishonestProduct {
        first: Output,
        second: OtherOutput,
    }

    impl OneFactStageOutput for DishonestProduct {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct OtherOutput {
        value: u32,
    }

    impl TypedPayload for OtherOutput {
        const EVENT_TYPE: &'static str = "typed_stateful.other_output";
    }

    #[derive(Clone, Debug)]
    struct DishonestHandler;

    impl TypedStatefulHandler for DishonestHandler {
        type State = u32;
        type Input = Input;
        type Output = DishonestProduct;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
            *state += input.value;
        }

        fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
            StatefulEmission::RetainEpoch {
                outputs: vec![DishonestProduct {
                    first: Output { value: *state },
                    second: OtherOutput { value: *state },
                }],
            }
        }

        fn post_emit(&self, state: &mut Self::State) {
            *state = 99;
        }
    }

    #[test]
    fn dishonest_one_fact_marker_is_fatal_and_installs_no_successor() {
        let mut adapter = TypedStatefulHandlerAdapter::new(DishonestHandler);
        StatefulHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        let mut state = StatefulHandler::initial_state(&adapter);
        let parent = input_event(4);
        StatefulHandler::try_accumulate(&mut adapter, &mut state, parent.clone())
            .expect("typed input decodes");
        let before_trace = state.trace.parent_ids();

        let fatal = StatefulHandler::emit(&adapter, &mut state)
            .expect_err("two facts contradict the one-fact marker");

        assert_eq!(fatal.code, StageFatalCode::Protocol);
        assert_eq!(fatal.reason, StageFatalReason::ProtocolInputIntegrity);
        assert!(fatal.detail.contains("one_fact_stage_output"));
        assert_eq!(state.inner, 4, "failed lowering cannot install successor");
        assert_eq!(state.trace.parent_ids(), before_trace);
    }

    #[derive(Clone, Debug, Deserialize)]
    struct FailingOutput;

    impl Serialize for FailingOutput {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ))
        }
    }

    impl TypedPayload for FailingOutput {
        const EVENT_TYPE: &'static str = "typed_stateful.failing_output";
    }

    #[derive(Clone, Debug)]
    struct FailingHandler;

    impl TypedStatefulHandler for FailingHandler {
        type State = u32;
        type Input = Input;
        type Output = FailingOutput;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
            *state += input.value;
        }

        fn emit(&self, _state: &Self::State) -> StatefulEmission<Self::Output> {
            StatefulEmission::ResetEpoch {
                outputs: vec![FailingOutput],
            }
        }

        fn post_emit(&self, state: &mut Self::State) {
            *state = 99;
        }
    }

    #[tokio::test]
    async fn an_unserializable_output_is_fatal_and_skips_post_emit() {
        let mut adapter = TypedStatefulHandlerAdapter::new(FailingHandler);
        StatefulHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        let mut state = StatefulHandler::initial_state(&adapter);
        let parent = input_event(5);
        StatefulHandler::try_accumulate(&mut adapter, &mut state, parent.clone())
            .expect("typed input decodes");

        let emitted = StatefulHandler::emit(&adapter, &mut state)
            .expect_err("output serialization is deliberately invalid");
        let drained = StatefulHandler::drain(&adapter, &state)
            .await
            .expect_err("a drain lowers the same output");

        for fatal in [emitted, drained] {
            assert_eq!(fatal.code, StageFatalCode::Protocol);
            assert_eq!(fatal.reason, StageFatalReason::OutputSerializationFailure);
            assert!(fatal.detail.contains("serialization failed"));
        }
        assert_eq!(state.inner, 5);
        assert_eq!(state.trace.parent_ids(), vec![parent.id]);
    }

    #[derive(Clone, Debug)]
    struct EmptyReset;

    impl TypedStatefulHandler for EmptyReset {
        type State = u32;
        type Input = Input;
        type Output = Output;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
            *state += input.value;
        }

        fn emit(&self, _state: &Self::State) -> StatefulEmission<Self::Output> {
            StatefulEmission::ResetEpoch {
                outputs: Vec::new(),
            }
        }

        fn post_emit(&self, state: &mut Self::State) {
            *state = 42;
        }
    }

    #[test]
    fn empty_success_installs_successor_and_resets_epoch() {
        let mut adapter = TypedStatefulHandlerAdapter::new(EmptyReset);
        StatefulHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        let mut state = StatefulHandler::initial_state(&adapter);
        StatefulHandler::try_accumulate(&mut adapter, &mut state, input_event(3))
            .expect("typed input decodes");

        let outputs = StatefulHandler::emit(&adapter, &mut state).expect("empty transition");

        assert!(outputs.is_empty());
        assert_eq!(state.inner, 42);
        assert!(state.trace.parent_ids().is_empty());
    }

    #[tokio::test]
    async fn default_typed_drain_projects_once_without_post_emit() {
        let writer_id = WriterId::from(StageId::new());
        let handler = Counter::default();
        let post_emits = handler.post_emits.clone();
        let mut adapter = TypedStatefulHandlerAdapter::new(handler);
        StatefulHandler::install_writer_id(&mut adapter, writer_id);
        let mut state = StatefulHandler::initial_state(&adapter);
        let parent = input_event(6);
        StatefulHandler::try_accumulate(&mut adapter, &mut state, parent.clone())
            .expect("typed input decodes");

        let outputs = StatefulHandler::drain(&adapter, &state)
            .await
            .expect("default drain lowers ordinary emission");

        assert_eq!(post_emits.load(Ordering::SeqCst), 0);
        assert_eq!(
            state.inner, 6,
            "terminal projection cannot open a successor epoch"
        );
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].writer_id, writer_id);
        assert_eq!(outputs[0].causality.parent_ids, vec![parent.id]);
        assert!(matches!(outputs[0].payload, ChainPayload::Fact(_)));
    }

    #[derive(Clone, Debug)]
    struct OverrideDrain;

    impl TypedStatefulHandler for OverrideDrain {
        type State = u32;
        type Input = Input;
        type Output = Output;

        fn initial_state(&self) -> Self::State {
            7
        }

        fn accumulate(&self, _state: &mut Self::State, _input: Self::Input) {}

        fn emit(&self, _state: &Self::State) -> StatefulEmission<Self::Output> {
            StatefulEmission::RetainEpoch {
                outputs: vec![Output { value: 1 }],
            }
        }

        fn post_emit(&self, _: &mut Self::State) {}

        fn drain(&self, _state: &Self::State) -> Vec<Self::Output> {
            vec![Output { value: 2 }]
        }
    }

    #[tokio::test]
    async fn explicit_typed_drain_replaces_the_default_projection() {
        let mut adapter = TypedStatefulHandlerAdapter::new(OverrideDrain);
        StatefulHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        let state = StatefulHandler::initial_state(&adapter);

        let outputs = StatefulHandler::drain(&adapter, &state)
            .await
            .expect("explicit drain projection lowers");

        assert_eq!(outputs.len(), 1);
        assert_eq!(Output::from_event(&outputs[0]), Some(Output { value: 2 }));
        assert_eq!(state.inner, 7);
    }
}
