// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler trait for stateful processing stages
//!
//! Examples: Aggregators, windowing operations, session tracking

use crate::effects::{EffectBoundary, EffectHistoryScan, EffectInvocationContext, Effects};
use crate::messaging::upstream_subscription::StageInputPosition;
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use async_trait::async_trait;
use obzenflow_core::event::observability::ObservationRecorder;
use obzenflow_core::event::schema::{DecodeCommitted, TypedFact, TypedPayload};
use obzenflow_core::event::ChainPayload;
use obzenflow_core::{ChainEvent, JournalRecord, WriterId};
use std::{fmt, sync::Arc, time::Duration};

#[derive(Clone, Copy)]
pub struct StatefulOutputContext<'a> {
    pub writer_id: WriterId,
    pub parent: &'a JournalRecord<ChainPayload>,
    pub recorded_flow_id: &'a str,
    pub stage_key: &'a str,
    pub input_seq: StageInputPosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatefulTerminationKind {
    NaturalEof,
    Drain,
    BeginDrain,
    TruncatedEof,
    PoisonedEof,
    PipelineAbort,
    ForceShutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalValidation {
    Clean,
    Primary(StageFatal),
    Secondary(StageFatal),
}

/// Runtime erasure substrate for stateful supervisors.
///
/// This mutable, event-aware interface is intentionally hidden from authored
/// handlers. Plain authored stages implement [`super::TypedStatefulHandler`]
/// and enter the runtime through its sealed adapter; the few structural
/// adapters that implement this trait are pinned by a source allowlist.
#[async_trait]
#[doc(hidden)]
pub trait StatefulHandler: Send + Sync {
    /// The internal state type
    type State: Clone + Send + Sync;

    /// FLOWIP-010 §7: called once at stage build with the build-resolved
    /// lineage policy. Handlers that create derived events store it; the
    /// default ignores it.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}

    /// Installed once by the runtime before the handler is shared.
    fn install_writer_id(&mut self, _writer_id: WriterId) {}
    fn install_observation_recorder(&mut self, _recorder: Arc<dyn ObservationRecorder>) {}

    /// Accumulate an event into the state (called in Accumulating state)
    ///
    /// This method updates the state with the new event but does NOT
    /// write anything to the journal. Journal writes only happen
    /// when the FSM transitions to Emitting state.
    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent);

    /// Fallible protocol-facing accumulation seam. Ordinary handlers inherit
    /// their existing infallible behaviour; sealed collectors override this
    /// method so integrity failures reach common fatal supervision directly.
    fn try_accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
    ) -> std::result::Result<(), HandlerError> {
        self.accumulate(state, event);
        Ok(())
    }

    /// Get the initial state for this handler
    fn initial_state(&self) -> Self::State;

    /// Transform accumulated state into output events
    ///
    /// Called when emission is triggered (by emission strategy or drain).
    /// Return the events you want to emit based on current state.
    ///
    /// An emission either succeeds or fails the stage; the supervisor records
    /// the `StageFatal` before failing.
    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal>;

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
    /// Override this OR give an accumulator another strategy with `.emit(...)`
    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    /// Emit the aggregated result (called in Emitting state)
    ///
    /// Default: Calls create_events()
    /// Override only if you need to modify state during emission
    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        self.create_events(state)
    }

    /// Emit final result during shutdown (called in Draining state)
    ///
    /// Default: Calls create_events()
    /// Override only if drain behavior differs from normal emission
    async fn drain(&self, state: &Self::State) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        self.create_events(state)
    }

    fn validate_terminal(
        &self,
        _state: &Self::State,
        _kind: StatefulTerminationKind,
    ) -> TerminalValidation {
        TerminalValidation::Clean
    }

    fn finalizes_on(&self, kind: StatefulTerminationKind) -> bool {
        !matches!(kind, StatefulTerminationKind::TruncatedEof)
    }

    /// Called only after every event returned by the most recent
    /// `emit`/`drain` invocation is durable.
    fn outputs_committed(&self, _state: &mut Self::State) {}
}

#[doc(hidden)]
#[async_trait]
pub trait UnifiedStatefulHandler: Send + Sync {
    type State: Clone + Send + Sync;

    fn effect_history_scan() -> EffectHistoryScan
    where
        Self: Sized,
    {
        EffectHistoryScan::Skip
    }

    /// FLOWIP-010 §7: forwarded to the wrapped handler at stage build.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}

    /// Installed once by the runtime before the handler is shared.
    fn install_writer_id(&mut self, _writer_id: WriterId) {}
    fn install_observation_recorder(&mut self, _recorder: Arc<dyn ObservationRecorder>) {}

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
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal>;

    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        self.create_events(state)
    }

    fn emit_with_context(
        &self,
        state: &mut Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        self.emit(state)
    }

    async fn drain(&self, state: &Self::State) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        self.create_events(state)
    }

    async fn drain_with_context(
        &self,
        state: &Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        self.drain(state).await
    }

    fn validate_terminal(
        &self,
        _state: &Self::State,
        _kind: StatefulTerminationKind,
    ) -> TerminalValidation {
        TerminalValidation::Clean
    }

    fn finalizes_on(&self, kind: StatefulTerminationKind) -> bool {
        !matches!(kind, StatefulTerminationKind::TruncatedEof)
    }

    fn outputs_committed(&self, _state: &mut Self::State) {}

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

    fn install_observation_recorder(&mut self, recorder: Arc<dyn ObservationRecorder>) {
        StatefulHandler::install_observation_recorder(self, recorder);
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        StatefulHandler::install_writer_id(self, writer_id)
    }

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<(), HandlerError> {
        StatefulHandler::try_accumulate(self, state, event)
    }

    fn initial_state(&self) -> Self::State {
        StatefulHandler::initial_state(self)
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        StatefulHandler::create_events(self, state)
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        StatefulHandler::emit_interval_hint(self)
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        StatefulHandler::should_emit(self, state)
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        StatefulHandler::emit(self, state)
    }

    fn emit_with_context(
        &self,
        state: &mut Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        StatefulHandler::emit(self, state)
    }

    async fn drain(&self, state: &Self::State) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        StatefulHandler::drain(self, state).await
    }

    async fn drain_with_context(
        &self,
        state: &Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        StatefulHandler::drain(self, state).await
    }

    fn validate_terminal(
        &self,
        state: &Self::State,
        kind: StatefulTerminationKind,
    ) -> TerminalValidation {
        StatefulHandler::validate_terminal(self, state, kind)
    }

    fn finalizes_on(&self, kind: StatefulTerminationKind) -> bool {
        StatefulHandler::finalizes_on(self, kind)
    }

    fn outputs_committed(&self, state: &mut Self::State) {
        StatefulHandler::outputs_committed(self, state)
    }
}

/// Mealy-split effectful stateful surface: `decide` performs effects and
/// emits facts, `apply` folds each committed fact into state.
///
/// `Output` names the facts `decide` may emit, one fact or an `outcome![...]`
/// list of single facts, and `apply` receives each committed fact as a value
/// of it, one call per fact in commit order. An effect's answer never reaches
/// `apply` whole: a multi-fact answer folds as its member facts.
///
/// This surface is input-driven only: `decide` is the sole fact-authoring
/// position. There is no periodic-emission or drain-emission hook; a
/// wall-clock trigger is not a function of the journal, and a mutable-state
/// emission position would evolve state without a fact.
///
/// A `decide` invocation is not transactional. Every fact whose commit
/// completes through `fx` remains durable even if `decide` later returns an
/// error. Before propagating that result, the adapter folds each such fact
/// into the installed state in commit order through
/// [`EffectfulStatefulHandler::apply`]. Returning an error never rolls a
/// committed fact back. If decoding or folding a committed fact fails, the
/// adapter returns a stage-fatal [`HandlerError::ContractViolation`] instead
/// of the `decide` result. The committed fact remains durable, but the
/// stage cannot acknowledge the input, process later input, or complete drain
/// because its state no longer represents a fold of its durable history.
///
/// The stage arrow is the operator-facing output contract, and `Output` mirrors
/// it so Rust can check `decide` before the handler is erased. The command's
/// ports allow the effects the stage performs, and the row never restates them.
///
/// A one-fact stage uses its [`TypedPayload`] type directly:
///
/// ```ignore
/// type Output = PaymentAuthorized;
/// ```
///
/// A stage with several facts lists them with `outcome![...]` and tells them
/// apart in `apply` with `fold!`, whose arms are the facts' own patterns:
///
/// ```ignore
/// #[async_trait::async_trait]
/// impl obzenflow_runtime::stages::common::handlers::EffectfulStatefulHandler
///     for PaymentStatefulHandler
/// {
///     type State = PaymentState;
///     type Input = ValidatedOrder;
///     type Output =
///         obzenflow_core::outcome![PaymentAuthorized | PaymentDeclined | OrderCancelled];
///
///     // `initial_state` and `decide` follow.
///
///     fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
///         obzenflow_core::fold!(match fact {
///             PaymentAuthorized { .. } => { state.authorized += 1 }
///             PaymentDeclined { .. } | OrderCancelled { .. } => {}
///         });
///         Ok(())
///     }
/// }
/// ```
///
/// Middleware stays in the stage's `with` clause. The handler is a
/// [`Command`](crate::effects::Command), which allows every effect the ports it
/// works through declare.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not satisfy `EffectfulStatefulHandler` for this stage",
    label = "this handler does not match the effectful stateful contract",
    note = "implement `EffectfulStatefulHandler` with `State`, `Input`, `Output`, \
            `initial_state`, `decide` and `apply`; `Input` must match the arrow input, and \
            `Output` mirrors the arrow"
)]
#[async_trait]
pub trait EffectfulStatefulHandler: crate::effects::Command + Send + Sync {
    type State: Clone + Send + Sync;
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: obzenflow_core::EffectfulStageOutput + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;

    async fn decide(
        &mut self,
        state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> std::result::Result<crate::effects::StageCompletion<Self::Output>, HandlerError>;

    /// Fold one committed fact into state. Called once per committed `Data`
    /// fact in commit order.
    ///
    /// The fact is already durable when this method is called. Returning an
    /// error therefore contradicts the state-fold contract: the adapter wraps
    /// the error as [`HandlerError::ContractViolation`], and the effectful
    /// stateful supervisor fails the stage, which never reads its state again.
    /// It does not acknowledge the input or skip the fact and continue.
    /// Implementations must not rely on retry because they receive mutable
    /// access to both the handler and the state.
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
#[derive(Clone)]
pub struct EffectfulStatefulHandlerAdapter<H> {
    handler: H,
    effect_boundary: Arc<dyn EffectBoundary>,
}

impl<H> EffectfulStatefulHandlerAdapter<H> {
    pub fn new(handler: H, effect_boundary: Arc<dyn EffectBoundary>) -> Self {
        Self {
            handler,
            effect_boundary,
        }
    }

    fn install_required_effect_boundary(
        &self,
        slot: &mut Option<Arc<dyn EffectBoundary>>,
    ) -> Result<(), HandlerError> {
        if slot.is_some() {
            return Err(HandlerError::ContractViolation(
                "effectful stateful handler received an unexpected pre-installed effect boundary"
                    .to_string(),
            ));
        }
        *slot = Some(self.effect_boundary.clone());
        Ok(())
    }
}

impl<H: fmt::Debug> fmt::Debug for EffectfulStatefulHandlerAdapter<H> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EffectfulStatefulHandlerAdapter")
            .field("handler", &self.handler)
            .field("effect_boundary", &"<required>")
            .finish()
    }
}

#[async_trait]
impl<H> UnifiedStatefulHandler for EffectfulStatefulHandlerAdapter<H>
where
    H: EffectfulStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
    H::Output: DecodeCommitted,
{
    fn effect_history_scan() -> EffectHistoryScan {
        EffectHistoryScan::Actions { pass_through: None }
    }

    fn install_observation_recorder(&mut self, recorder: Arc<dyn ObservationRecorder>) {
        self.effect_boundary.install_observation_recorder(recorder);
    }

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
        let mut effect_context = effect_context.ok_or_else(|| {
            HandlerError::Other(
                "effectful stateful handler invoked without effect context".to_string(),
            )
        })?;
        self.install_required_effect_boundary(&mut effect_context.effect_boundary)?;
        let mut fx = Effects::<H::Output, H::AllowedEffects>::new(effect_context);
        let decide_result = self.handler.decide(state, &input, &mut fx).await;
        for fact_event in fx.drain_committed_facts() {
            let fact = decode_effectful_stateful_fact::<H::Output>(&fact_event)?;
            self.handler.apply(state, fact).map_err(|source| {
                HandlerError::ContractViolation(format!(
                    "effectful_stateful_apply: `apply` rejected committed event `{}` of type \
                     `{}`: {source}",
                    fact_event.id,
                    fact_event.event_type(),
                ))
            })?;
        }
        if let Some(fatal) = fx.authoring_halt_fatal() {
            return Err(HandlerError::Fatal(fatal));
        }
        if decide_result.as_ref().is_err_and(HandlerError::is_fatal) {
            return decide_result.map(|_| ());
        }
        if let Some(divergence) = fx.end_of_invocation_fatal().await? {
            return Err(HandlerError::Fatal(divergence));
        }
        decide_result.map(|_| ())
    }

    fn initial_state(&self) -> Self::State {
        self.handler.initial_state()
    }

    fn create_events(
        &self,
        _state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
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

    fn emit(&self, _state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        Ok(Vec::new())
    }

    fn emit_with_context(
        &self,
        _state: &mut Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        Ok(Vec::new())
    }

    async fn drain(
        &self,
        _state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        Ok(Vec::new())
    }

    async fn drain_with_context(
        &self,
        _state: &Self::State,
        _output_context: Option<StatefulOutputContext<'_>>,
    ) -> std::result::Result<Vec<ChainEvent>, StageFatal> {
        Ok(Vec::new())
    }

    fn stage_logic_version(&self) -> &str {
        self.handler.stage_logic_version()
    }
}

fn decode_effectful_stateful_fact<Output>(event: &ChainEvent) -> Result<Output, HandlerError>
where
    Output: DecodeCommitted,
{
    let fact = TypedFact::from_event(event).ok_or_else(|| {
        HandlerError::ContractViolation(format!(
            "effectful_stateful_output: committed event `{}` was not a Data event",
            event.id
        ))
    })?;
    Output::decode_committed(&fact).map_err(|error| {
        let declared = Output::member_fact_types()
            .iter()
            .map(|fact| fact.event_type.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        HandlerError::ContractViolation(format!(
            "effectful_stateful_output: committed event `{}` of type `{}` could not be decoded \
             as one of the `Output`'s facts ({declared}): {error}",
            event.id, fact.event_type,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug)]
    struct TestAllowAllEffectBoundary;

    #[async_trait]
    impl EffectBoundary for TestAllowAllEffectBoundary {
        async fn around_repeatable_effect(
            &self,
            _identity: &crate::effects::EffectIdentity,
            _event: &ChainEvent,
            mut operation: crate::effects::RepeatableEffectOperation,
        ) -> crate::effects::EffectBoundaryReport {
            crate::effects::EffectBoundaryReport {
                outcome: crate::effects::EffectBoundaryOutcome::Executed(operation.execute().await),
                control_events: Vec::new(),
            }
        }

        async fn around_single_use_effect(
            &self,
            _identity: &crate::effects::EffectIdentity,
            _event: &ChainEvent,
            operation: crate::effects::SingleUseEffectOperation,
        ) -> crate::effects::SingleUseEffectBoundaryReport {
            operation.execute().await.into_report(Vec::new())
        }
    }

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

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct UndeclaredOutput {
        value: u32,
    }

    impl TypedPayload for UndeclaredOutput {
        const EVENT_TYPE: &'static str = "stateful.undeclared";
    }

    #[test]
    fn effectful_stateful_fact_decoder_accepts_scalar_fact() {
        let event = obzenflow_core::event::ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            FirstOutput::event_type_name(),
            FirstOutput::payload_schema_version(),
            serde_json::json!({ "value": 1 }),
        );

        let fact: FirstOutput =
            decode_effectful_stateful_fact(&event).expect("scalar fact decodes");

        assert_eq!(fact, FirstOutput { value: 1 });
        assert!(event.is_fact());
        assert_eq!(event.event_type(), "stateful.first");
    }

    #[test]
    fn effectful_stateful_set_decodes_committed_fact() {
        let event = obzenflow_core::event::ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            SecondOutput::event_type_name(),
            SecondOutput::payload_schema_version(),
            serde_json::json!({ "value": 2 }),
        );

        let fact: obzenflow_core::outcome![FirstOutput | SecondOutput] =
            decode_effectful_stateful_fact(&event).expect("listed fact decodes");

        assert_eq!(fact, obzenflow_core::outcome(SecondOutput { value: 2 }));
    }

    #[test]
    fn undeclared_committed_fact_lists_output_facts() {
        let event = obzenflow_core::event::ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            UndeclaredOutput::event_type_name(),
            UndeclaredOutput::payload_schema_version(),
            serde_json::json!({ "value": 3 }),
        );

        let error = decode_effectful_stateful_fact::<
            obzenflow_core::outcome![FirstOutput | SecondOutput],
        >(&event)
        .expect_err("an unlisted fact does not decode");

        assert!(error.is_contract_violation());
        let message = error.to_string();
        assert!(message.contains("effectful_stateful_output"), "{message}");
        assert!(message.contains("stateful.undeclared"), "{message}");
        assert!(
            message.contains("(stateful.first, stateful.second)"),
            "{message}"
        );
    }

    #[crate::effects::command(effects = crate::effects)]
    #[derive(Clone, Debug)]
    struct DecideOnly;

    #[async_trait]
    impl EffectfulStatefulHandler for DecideOnly {
        type State = u32;
        type Input = FirstOutput;
        type Output = FirstOutput;

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
        let adapter =
            EffectfulStatefulHandlerAdapter::new(DecideOnly, Arc::new(TestAllowAllEffectBoundary));
        let mut state = adapter.initial_state();

        assert_eq!(UnifiedStatefulHandler::emit_interval_hint(&adapter), None);
        assert!(!UnifiedStatefulHandler::should_emit(&adapter, &mut state));
    }

    #[test]
    fn effectful_stateful_adapter_installs_exactly_its_required_boundary() {
        let required: Arc<dyn EffectBoundary> = Arc::new(TestAllowAllEffectBoundary);
        let adapter = EffectfulStatefulHandlerAdapter::new(DecideOnly, required.clone());
        let mut slot = None;

        adapter
            .install_required_effect_boundary(&mut slot)
            .expect("empty invocation slot accepts the adapter-owned boundary");

        let installed = slot.expect("required boundary must be installed");
        assert!(Arc::ptr_eq(&installed, &required));
    }

    #[test]
    fn effectful_stateful_adapter_rejects_a_second_boundary_authority() {
        let required: Arc<dyn EffectBoundary> = Arc::new(TestAllowAllEffectBoundary);
        let unexpected: Arc<dyn EffectBoundary> = Arc::new(TestAllowAllEffectBoundary);
        let adapter = EffectfulStatefulHandlerAdapter::new(DecideOnly, required);
        let mut slot = Some(unexpected.clone());

        let error = adapter
            .install_required_effect_boundary(&mut slot)
            .expect_err("a pre-installed boundary must fail closed");

        assert!(error.is_contract_violation());
        assert!(error.to_string().contains("unexpected pre-installed"));
        assert!(Arc::ptr_eq(
            slot.as_ref().expect("unexpected boundary remains visible"),
            &unexpected,
        ));
    }
}
