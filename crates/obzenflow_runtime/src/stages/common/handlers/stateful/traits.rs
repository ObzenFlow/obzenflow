// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler trait for stateful processing stages
//!
//! Examples: Aggregators, windowing operations, session tracking

use crate::effects::{EffectInvocationContext, Effects};
use crate::messaging::upstream_subscription::StageInputPosition;
use crate::stages::common::handler_error::HandlerError;
use async_trait::async_trait;
use obzenflow_core::event::schema::{TypedFact, TypedPayload};
use obzenflow_core::{ChainEvent, EventEnvelope, OneFactStageOutput, WriterId};
use std::time::Duration;

#[derive(Clone, Copy)]
pub struct StatefulOutputContext<'a> {
    pub writer_id: WriterId,
    pub parent: &'a EventEnvelope<ChainEvent>,
    pub recorded_flow_id: &'a str,
    pub stage_key: &'a str,
    pub input_seq: StageInputPosition,
}

/// Handler for stateful processing stages
///
/// Stateful handlers maintain internal state across events and use FSM states
/// to control when accumulated results are written to the journal.
///
/// Key principle: Accumulation (processing many events) is separate from
/// emission (writing ONE aggregated event to the journal).
///
/// # FSM States
/// - `Accumulating`: Process events, update state, write NOTHING
/// - `Emitting`: Write ONE aggregated event to journal
/// - `Draining`: Handle EOF, emit final result
///
/// # Example
/// ```ignore
/// use obzenflow_runtime::stages::common::handlers::StatefulHandler;
/// use obzenflow_core::{ChainEvent, EventId, WriterId, Result};
/// use serde_json::json;
/// use async_trait::async_trait;
///
/// #[derive(Clone, Default)]
/// struct AggregatorState {
///     count: u64,
///     sum: f64,
///     events_since_emit: u64,
/// }
///
/// struct MetricsAggregator {
///     window_size: u64,
///     writer_id: WriterId,
/// }
///
/// #[async_trait]
/// impl StatefulHandler for MetricsAggregator {
///     type State = AggregatorState;
///
///     fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
///         let value = event.payload()["value"].as_f64().unwrap_or(0.0);
///         state.count += 1;
///         state.sum += value;
///         state.events_since_emit += 1;
///     }
///
///     fn should_emit(&self, state: &mut Self::State) -> bool {
///         state.events_since_emit >= self.window_size
///     }
///
///     fn emit(&self, state: &mut Self::State) -> Option<ChainEvent> {
///         if state.count == 0 {
///             return None;
///         }
///
///         let event = ChainEvent::data(
///             EventId::new(),
///             self.writer_id.clone(),
///             "metrics",
///             json!({
///                 "avg": state.sum / state.count as f64,
///                 "count": state.count,
///                 "window": state.events_since_emit,
///             })
///         );
///
///         // Reset window counter but keep running totals
///         state.events_since_emit = 0;
///
///         Some(event)
///     }
///
///     fn initial_state(&self) -> Self::State {
///         AggregatorState::default()
///     }
///
///     async fn drain(&self, state: &Self::State) -> Result<Option<ChainEvent>> {
///         // Emit final aggregation if we have data
///         Ok(self.emit(&mut state.clone()))
///     }
/// }
/// ```
#[async_trait]
pub trait StatefulHandler: Send + Sync {
    /// The internal state type
    type State: Clone + Send + Sync;

    /// FLOWIP-010 §7: called once at stage build with the build-resolved
    /// lineage policy. Handlers that create derived events store it; the
    /// default ignores it.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}

    /// Accumulate an event into the state (called in Accumulating state)
    ///
    /// This method updates the state with the new event but does NOT
    /// write anything to the journal. Journal writes only happen
    /// when the FSM transitions to Emitting state.
    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent);

    /// Get the initial state for this handler
    fn initial_state(&self) -> Self::State;

    /// Transform accumulated state into output events
    ///
    /// Called when emission is triggered (by emission strategy or drain).
    /// Return the events you want to emit based on current state.
    ///
    /// `Ok(events)` means emission succeeded. `Err(HandlerError)` means a
    /// per-record failure occurred while creating outputs (e.g. IO or
    /// encoding problems); the supervisor will turn this into an
    /// error-marked event and continue running the stage.
    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    // --- Advanced methods with sensible defaults ---

    /// Optional idle tick interval hint for the supervisor.
    ///
    /// When set, the supervisor may periodically call `should_emit` even when no new input events
    /// arrive, enabling time-based emission.
    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    /// Check if we should transition from Accumulating to Emitting
    ///
    /// Default: false (only emit on drain, i.e., OnEOF behavior)
    /// Override this OR use .with_emission() for other strategies
    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    /// Emit the aggregated result (called in Emitting state)
    ///
    /// Default: Calls create_events()
    /// Override only if you need to modify state during emission
    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    /// Emit final result during shutdown (called in Draining state)
    ///
    /// Default: Calls create_events()
    /// Override only if drain behavior differs from normal emission
    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }
}

#[doc(hidden)]
#[async_trait]
pub trait UnifiedStatefulHandler: Send + Sync {
    type State: Clone + Send + Sync;

    /// FLOWIP-010 §7: forwarded to the wrapped handler at stage build.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}

    /// Accumulate one event. `scope` is the per-event middleware execution
    /// scope computed by the supervisor at dispatch (FLOWIP-120c H3);
    /// handlers without middleware ignore it.
    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<(), HandlerError>;

    fn initial_state(&self) -> Self::State;

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    fn emit_with_context(
        &self,
        state: &mut Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.emit(state)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    async fn drain_with_context(
        &self,
        state: &Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.drain(state).await
    }

    fn stage_logic_version(&self) -> &str {
        "1"
    }
}

#[async_trait]
impl<T: StatefulHandler + Send + Sync> UnifiedStatefulHandler for T {
    type State = T::State;

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        StatefulHandler::install_lineage_policy(self, policy)
    }

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<(), HandlerError> {
        StatefulHandler::accumulate(self, state, event);
        Ok(())
    }

    fn initial_state(&self) -> Self::State {
        StatefulHandler::initial_state(self)
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::create_events(self, state)
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        StatefulHandler::emit_interval_hint(self)
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        StatefulHandler::should_emit(self, state)
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::emit(self, state)
    }

    fn emit_with_context(
        &self,
        state: &mut Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::emit(self, state)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::drain(self, state).await
    }

    async fn drain_with_context(
        &self,
        state: &Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::drain(self, state).await
    }
}

/// Mealy-split effectful stateful surface (FLOWIP-120b): `decide` performs
/// effects and emits facts, `apply` folds each committed fact into state.
///
/// `Output` is the stage's per-fact enum, one variant per committed fact type;
/// deriving `EffectOutcomeFacts` on it generates exactly this shape. An effect
/// outcome carrier never reaches `apply` (FLOWIP-120m): the carrier is
/// transient `fx.perform` machinery, and a multi-fact product outcome folds
/// as its individual member facts, one `apply` call per fact in ordinal
/// (field) order.
///
/// This surface is input-driven only (FLOWIP-120z): `decide` is the sole
/// fact-authoring position. There is no periodic-emission or drain-emission
/// hook; a wall-clock trigger is not a function of the journal, and a
/// mutable-state emission position would evolve state without a fact.
///
/// A `decide` invocation is not transactional. Every fact whose commit
/// completes through `fx` remains durable even if `decide` later returns an
/// error. Before propagating that result, the adapter folds all such facts in
/// commit order through [`EffectfulStatefulHandler::apply`] and installs the
/// completed draft. Returning an error never rolls a committed fact back. If
/// decoding or folding a committed fact fails, the adapter leaves the prior
/// state installed and returns a stage-fatal [`HandlerError::ContractViolation`]
/// instead of the `decide` result. The committed fact remains durable, but the
/// stage cannot acknowledge the input, process later input, or complete drain
/// because its state no longer represents a fold of its durable history.
///
/// The stage arrow and `effects:` clause are the canonical operator-facing
/// contract. `Output` and `AllowedEffects` mirror those declarations so Rust
/// can check `decide` before the handler is erased. They carry no runtime
/// metadata and are never journalled.
///
/// Unlike an effectful transform's zero-sized fact-set witness, stateful
/// `Output` is inhabited because [`EffectfulStatefulHandler::apply`] receives
/// each committed fact. A one-fact stage can use its [`TypedPayload`] type
/// directly:
///
/// ```ignore
/// type Output = PaymentAuthorized;
/// type AllowedEffects = obzenflow_runtime::effect_set![AuthorizePayment];
/// ```
///
/// For a multi-fact arrow, derive [`obzenflow_core::StageOutputFacts`] on a
/// per-fact sum. Each variant must contain exactly one fact so the carrier
/// implements [`obzenflow_core::OneFactStageOutput`]:
///
/// ```ignore
/// #[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
/// enum PaymentStateFact {
///     Authorized(PaymentAuthorized),
///     Declined(PaymentDeclined),
///     Cancelled(OrderCancelled),
/// }
///
/// #[async_trait::async_trait]
/// impl obzenflow_runtime::stages::common::handlers::EffectfulStatefulHandler
///     for PaymentStatefulHandler
/// {
///     type State = PaymentState;
///     type Input = ValidatedOrder;
///     type Output = PaymentStateFact;
///     type AllowedEffects = obzenflow_runtime::effect_set![AuthorizePayment];
///
///     // `initial_state`, `decide`, and `apply` follow.
///     # fn initial_state(&self) -> Self::State { unimplemented!() }
///     # async fn decide(
///     #     &mut self,
///     #     _state: &Self::State,
///     #     _input: &Self::Input,
///     #     _fx: &mut obzenflow_runtime::effects::Effects<
///     #         Self::Output,
///     #         Self::AllowedEffects,
///     #     >,
///     # ) -> Result<
///     #     obzenflow_runtime::effects::StageCompletion<Self::Output>,
///     #     obzenflow_runtime::stages::common::handler_error::HandlerError,
///     # > {
///     #     unimplemented!()
///     # }
///     # fn apply(
///     #     &mut self,
///     #     _state: &mut Self::State,
///     #     _fact: Self::Output,
///     # ) -> Result<(), obzenflow_runtime::stages::common::handler_error::HandlerError> {
///     #     unimplemented!()
///     # }
/// }
/// ```
///
/// Middleware and policy values stay solely in the stage's `effects:` clause;
/// `AllowedEffects` mirrors effect types only.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not satisfy `EffectfulStatefulHandler` for this stage",
    label = "this handler does not match the effectful stateful contract",
    note = "implement `EffectfulStatefulHandler` with `State`, `Input`, `Output`, \
            `AllowedEffects`, `initial_state`, `decide`, and `apply`; `Input` must match the \
            arrow input, while `Output` and `AllowedEffects` mirror the canonical arrow and \
            `effects:` clause (FLOWIP-120z B9)"
)]
#[async_trait]
pub trait EffectfulStatefulHandler: Send + Sync {
    type State: Clone + Send + Sync;
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: obzenflow_core::OneFactStageOutput + Send + Sync + 'static;
    type AllowedEffects: crate::effects::EffectSet;

    fn initial_state(&self) -> Self::State;

    async fn decide(
        &mut self,
        state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> std::result::Result<crate::effects::StageCompletion<Self::Output>, HandlerError>;

    /// Fold one committed fact into state. Called once per committed `Data`
    /// fact in commit order; carriers never appear here.
    ///
    /// The fact is already durable when this method is called. Returning an
    /// error therefore contradicts the state-fold contract: the adapter keeps
    /// the previously installed state, wraps the error as
    /// [`HandlerError::ContractViolation`], and the effectful stateful
    /// supervisor fails the stage. It does not acknowledge the input or skip
    /// the fact and continue. Implementations must not rely on retry because
    /// they receive mutable access to both the handler and the draft state.
    fn apply(
        &mut self,
        state: &mut Self::State,
        fact: Self::Output,
    ) -> std::result::Result<(), HandlerError>;

    fn stage_logic_version(&self) -> &str {
        "1"
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct EffectfulStatefulHandlerAdapter<H>(pub H);

#[async_trait]
impl<H> UnifiedStatefulHandler for EffectfulStatefulHandlerAdapter<H>
where
    H: EffectfulStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    type State = H::State;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<(), HandlerError> {
        let input = H::Input::try_from_event(&event)
            .map_err(|e| HandlerError::Deserialization(e.to_string()))?;
        let effect_context = effect_context.ok_or_else(|| {
            HandlerError::Other(
                "effectful stateful handler invoked without effect context".to_string(),
            )
        })?;
        let mut fx = Effects::<H::Output, H::AllowedEffects>::new(effect_context);
        let decide_result = self.0.decide(state, &input, &mut fx).await;
        let mut draft = state.clone();
        for fact_event in fx.drain_committed_facts() {
            let fact = decode_effectful_stateful_fact::<H::Output>(&fact_event)?;
            self.0.apply(&mut draft, fact).map_err(|source| {
                HandlerError::ContractViolation(format!(
                    "effectful_stateful_apply: `apply` rejected committed event `{}` of type \
                     `{}`: {source}",
                    fact_event.id,
                    fact_event.event_type(),
                ))
            })?;
        }
        *state = draft;
        decide_result.map(|_| ())
    }

    fn initial_state(&self) -> Self::State {
        self.0.initial_state()
    }

    fn create_events(
        &self,
        _state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    // The effectful surface is input-driven only (FLOWIP-120z): no emission
    // interval and no should-emit signal, so the supervisor's emitting
    // transition is unreachable for effectful stateful stages.
    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let _ = state;
        Ok(Vec::new())
    }

    fn emit_with_context(
        &self,
        state: &mut Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let _ = state;
        Ok(Vec::new())
    }

    async fn drain(
        &self,
        _state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain_with_context(
        &self,
        state: &Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let _ = state;
        Ok(Vec::new())
    }

    fn stage_logic_version(&self) -> &str {
        self.0.stage_logic_version()
    }
}

fn decode_effectful_stateful_fact<Output>(event: &ChainEvent) -> Result<Output, HandlerError>
where
    Output: OneFactStageOutput,
{
    let fact = TypedFact::from_event(event).ok_or_else(|| {
        HandlerError::ContractViolation(format!(
            "one_fact_stage_output: committed event `{}` was not a Data event",
            event.id
        ))
    })?;
    let event_type = fact.event_type.clone();
    Output::try_from_facts(&[fact]).map_err(|error| {
        HandlerError::ContractViolation(format!(
            "one_fact_stage_output: committed event `{}` of type `{event_type}` could not be \
             reconstructed as `{}` from one fact: {error}",
            event.id,
            std::any::type_name::<Output>(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventContent;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct FirstOutput {
        value: u32,
    }

    impl TypedPayload for FirstOutput {
        const EVENT_TYPE: &'static str = "stateful.first";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct SecondOutput {
        value: u32,
    }

    impl TypedPayload for SecondOutput {
        const EVENT_TYPE: &'static str = "stateful.second";
    }

    #[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
    struct DishonestProductOutput {
        first: FirstOutput,
        second: SecondOutput,
    }

    // Manual implementations are trusted semantic assertions. This fixture
    // deliberately violates the trait law to prove the runtime safety net.
    impl OneFactStageOutput for DishonestProductOutput {}

    #[test]
    fn effectful_stateful_fact_decoder_accepts_scalar_fact() {
        let event = obzenflow_core::event::ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            FirstOutput::versioned_event_type(),
            serde_json::json!({ "value": 1 }),
        );

        let fact: FirstOutput =
            decode_effectful_stateful_fact(&event).expect("scalar fact decodes");

        assert_eq!(fact, FirstOutput { value: 1 });
        assert!(matches!(
            &event.content,
            ChainEventContent::Data { event_type, .. } if event_type == "stateful.first.v1"
        ));
    }

    #[test]
    fn effectful_stateful_fact_decoder_rejects_false_one_fact_assertion() {
        let event = obzenflow_core::event::ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            FirstOutput::versioned_event_type(),
            serde_json::json!({ "value": 1 }),
        );

        let error = decode_effectful_stateful_fact::<DishonestProductOutput>(&event)
            .expect_err("a two-field product cannot reconstruct from one committed fact");

        assert!(error.is_contract_violation());
        let message = error.to_string();
        assert!(message.contains("one_fact_stage_output"));
        assert!(message.contains("stateful.first.v1"));
        assert!(message.contains("stateful.second.v1"));
    }

    #[derive(Clone, Debug)]
    struct DecideOnly;

    #[async_trait]
    impl EffectfulStatefulHandler for DecideOnly {
        type State = u32;
        type Input = FirstOutput;
        type Output = FirstOutput;
        type AllowedEffects = crate::effect_set![];

        fn initial_state(&self) -> Self::State {
            0
        }

        async fn decide(
            &mut self,
            _state: &Self::State,
            _input: &Self::Input,
            fx: &mut Effects<Self::Output, Self::AllowedEffects>,
        ) -> std::result::Result<crate::effects::StageCompletion<Self::Output>, HandlerError>
        {
            Ok(fx.complete_empty()?)
        }

        fn apply(
            &mut self,
            state: &mut Self::State,
            _fact: Self::Output,
        ) -> std::result::Result<(), HandlerError> {
            *state += 1;
            Ok(())
        }
    }

    /// FLOWIP-120z: the effectful surface is input-driven only. The adapter
    /// reports no emission interval and never signals should-emit, so the
    /// stateful supervisor's emitting transition is unreachable for
    /// effectful stages.
    #[test]
    fn effectful_stateful_adapter_reports_no_emission_surface() {
        let adapter = EffectfulStatefulHandlerAdapter(DecideOnly);
        let mut state = adapter.initial_state();

        assert_eq!(UnifiedStatefulHandler::emit_interval_hint(&adapter), None);
        assert!(!UnifiedStatefulHandler::should_emit(&adapter, &mut state));
    }
}
