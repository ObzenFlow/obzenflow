// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Empirical verification for FLOWIP-114c: every typed decoration combination
//! across the surviving stage families is callable today. If any arm is missing,
//! this file fails to compile and the implementing PR must add the missing
//! arm before deleting the matching untyped arm.

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::{ChainEvent, TypedPayload};
    use obzenflow_runtime::effects::{
        Effect, EffectContext, EffectError, EffectSafety, Effects, StageCompletion,
    };
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::source::SourceError;
    use obzenflow_runtime::stages::common::handlers::{
        AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, EffectfulStatefulHandler,
        EffectfulTransformHandler, FiniteSourceHandler, InfiniteSourceHandler, JoinHandler,
        SinkHandler, StatefulEmission, TransformHandler, TypedStatefulHandler,
        TypedTransformHandler,
    };
    use obzenflow_runtime::stages::sink::SinkTyped;
    use obzenflow_runtime::typing::{SinkTyping, SourceTyping, TransformTyping};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct In;
    impl TypedPayload for In {
        const EVENT_TYPE: &'static str = "test.in";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Out;
    impl TypedPayload for Out {
        const EVENT_TYPE: &'static str = "test.out";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug)]
    struct Src;
    impl SourceTyping for Src {
        type Output = Out;
    }
    impl FiniteSourceHandler for Src {
        fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            Ok(None)
        }
    }

    mod qualified {
        use super::*;

        #[derive(Clone, Debug)]
        pub(super) struct Source;

        impl SourceTyping for Source {
            type Output = Out;
        }

        impl FiniteSourceHandler for Source {
            fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
                Ok(None)
            }
        }
    }

    #[derive(Clone, Debug)]
    struct AsyncSrc;
    impl SourceTyping for AsyncSrc {
        type Output = Out;
    }
    #[async_trait]
    impl AsyncFiniteSourceHandler for AsyncSrc {
        async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct InfSrc;
    impl SourceTyping for InfSrc {
        type Output = Out;
    }
    impl InfiniteSourceHandler for InfSrc {
        fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Debug)]
    struct AsyncInfSrc;
    impl SourceTyping for AsyncInfSrc {
        type Output = Out;
    }
    #[async_trait]
    impl AsyncInfiniteSourceHandler for AsyncInfSrc {
        async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Debug)]
    struct Tr;
    impl TransformTyping for Tr {
        type Input = In;
        type Output = Out;
    }
    impl TypedTransformHandler for Tr {
        type Input = In;
        type Output = Out;

        fn process(&self, _input: In) -> Result<Out, HandlerError> {
            Ok(Out)
        }
    }
    #[async_trait]
    impl TransformHandler for Tr {
        fn process(&self, _e: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct FxTr;
    #[async_trait]
    impl EffectfulTransformHandler for FxTr {
        type Input = In;
        type Output = Out;
        type AllowedEffects = obzenflow_runtime::effect_set![];

        async fn process(
            &self,
            _input: In,
            fx: &mut Effects<Self::Output, Self::AllowedEffects>,
        ) -> Result<StageCompletion<Self::Output>, HandlerError> {
            fx.emit(Out)
                .await
                .map_err(|e| HandlerError::Other(e.to_string()))?;
            Ok(fx.complete()?)
        }
    }

    #[derive(Clone, Debug)]
    struct TxFxTr;

    #[async_trait]
    impl EffectfulTransformHandler for TxFxTr {
        type Input = In;
        type Output = Out;
        type AllowedEffects = obzenflow_runtime::effect_set![TxEffect];

        async fn process(
            &self,
            _input: In,
            fx: &mut Effects<Self::Output, Self::AllowedEffects>,
        ) -> Result<StageCompletion<Self::Output>, HandlerError> {
            fx.emit(Out).await?;
            Ok(fx.complete()?)
        }
    }

    #[derive(Clone, Debug)]
    struct St;
    impl TypedStatefulHandler for St {
        type State = ();
        type Input = In;
        type Output = Out;

        fn accumulate(&self, _s: &mut Self::State, _input: In) {}
        fn initial_state(&self) -> Self::State {}
        fn emit(
            &self,
            _s: &Self::State,
        ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
            Ok(StatefulEmission::RetainEpoch {
                next_state: (),
                outputs: vec![],
            })
        }
    }

    #[derive(Clone, Debug)]
    struct FxSt;
    #[async_trait]
    impl EffectfulStatefulHandler for FxSt {
        type State = ();
        type Input = In;
        type Output = Out;
        type AllowedEffects = obzenflow_runtime::effect_set![];

        fn initial_state(&self) -> Self::State {}

        async fn decide(
            &mut self,
            _state: &Self::State,
            _input: &In,
            fx: &mut Effects<Self::Output, Self::AllowedEffects>,
        ) -> Result<StageCompletion<Self::Output>, HandlerError> {
            Ok(fx.complete_empty()?)
        }

        fn apply(
            &mut self,
            _state: &mut Self::State,
            _fact: Self::Output,
        ) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct Sn;
    impl SinkTyping for Sn {
        type Input = Out;
    }
    #[async_trait]
    impl SinkHandler for Sn {
        async fn consume(&mut self, _e: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
            Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
        }
    }

    #[derive(Clone, Debug)]
    struct Jn;
    #[async_trait]
    impl JoinHandler for Jn {
        type State = ();

        fn initial_state(&self) -> Self::State {}

        fn process_event(
            &self,
            _state: &mut Self::State,
            _event: ChainEvent,
            _source_id: obzenflow_core::StageId,
            _writer_id: obzenflow_core::WriterId,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        fn on_source_eof(
            &self,
            _state: &mut Self::State,
            _source_id: obzenflow_core::StageId,
            _writer_id: obzenflow_core::WriterId,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Debug)]
    struct TxEffect;

    #[async_trait]
    impl Effect for TxEffect {
        const EFFECT_TYPE: &'static str = "test.tx_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Transactional;

        type Outcome = Out;
        type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

        fn label(&self) -> &str {
            "tx_effect"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Out)
        }
    }

    #[test]
    fn every_stage_family_accepts_a_bound_path_in_its_maximally_decorated_form() {
        let finite = Src;
        let async_finite = AsyncSrc;
        let infinite = InfSrc;
        let async_infinite = AsyncInfSrc;
        let transform = Tr;
        let effectful_transform = FxTr;
        let stateful = St;
        let effectful_stateful = FxSt;
        let join = Jn;
        let sink = SinkTyped::new(|_out: Out| async move {});

        let _ = crate::source!(
            name: "finite",
            Out => finite with [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::async_source!(
            name: "async_finite",
            Out => async_finite with [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::infinite_source!(
            name: "infinite",
            Out => infinite with [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::async_infinite_source!(
            name: "async_infinite",
            Out => async_infinite with [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::transform!(
            name: "transform",
            In -> Out => transform,
            observers: [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::effectful_transform!(
            name: "effectful_transform",
            In -> Out => effectful_transform,
            effects: [],
            observers: [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::stateful!(
            name: "stateful",
            In -> Out => stateful,
            emit_interval = std::time::Duration::from_millis(1),
            observers: [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::effectful_stateful!(
            name: "effectful_stateful",
            In -> Out => effectful_stateful,
            effects: [],
            observers: [],
            backpressure: crate::dsl::backpressure_clause::enforced(1)
        );
        let _ = crate::join!(
            name: "join",
            catalog reference_stage: In,
            In -> Out => join,
            observers: []
        );
        let _ = crate::sink!(
            name: "sink",
            Out => sink,
            delivery: idempotent,
            observers: []
        );
    }

    // ── source! ─────────────────────────────────────────────────────────────
    #[test]
    fn source_typed_bare() {
        let _ = crate::source!(Out => Src);
    }
    #[test]
    fn source_typed_mw() {
        let _ = crate::source!(Out => Src with []);
    }
    #[test]
    fn source_typed_name() {
        let _ = crate::source!(name: "s", Out => Src);
    }
    #[test]
    fn source_typed_name_mw() {
        let _ = crate::source!(name: "s", Out => Src with []);
    }
    #[test]
    fn source_accepts_a_qualified_unit_path() {
        let _ = crate::source!(Out => qualified::Source);
    }
    #[test]
    fn source_accepts_a_function_parameter_name() {
        fn declare(handler: Src) {
            let _ = crate::source!(Out => handler);
        }

        declare(Src);
    }

    // ── async_source! ───────────────────────────────────────────────────────
    #[test]
    fn async_source_typed_bare() {
        let _ = crate::async_source!(Out => AsyncSrc);
    }
    #[test]
    fn async_source_typed_mw() {
        let _ = crate::async_source!(Out => AsyncSrc with []);
    }
    #[test]
    fn async_source_typed_name() {
        let _ = crate::async_source!(name: "s", Out => AsyncSrc);
    }
    #[test]
    fn async_source_typed_name_mw() {
        let _ = crate::async_source!(name: "s", Out => AsyncSrc with []);
    }

    // ── infinite_source! ────────────────────────────────────────────────────
    #[test]
    fn infinite_source_typed_bare() {
        let _ = crate::infinite_source!(Out => InfSrc);
    }
    #[test]
    fn infinite_source_typed_mw() {
        let _ = crate::infinite_source!(Out => InfSrc with []);
    }
    #[test]
    fn infinite_source_typed_name() {
        let _ = crate::infinite_source!(name: "s", Out => InfSrc);
    }
    #[test]
    fn infinite_source_typed_name_mw() {
        let _ = crate::infinite_source!(name: "s", Out => InfSrc with []);
    }

    // ── async_infinite_source! ──────────────────────────────────────────────
    #[test]
    fn async_infinite_source_typed_bare() {
        let _ = crate::async_infinite_source!(Out => AsyncInfSrc);
    }
    #[test]
    fn async_infinite_source_typed_mw() {
        let _ = crate::async_infinite_source!(Out => AsyncInfSrc with []);
    }
    #[test]
    fn async_infinite_source_typed_name() {
        let _ = crate::async_infinite_source!(name: "s", Out => AsyncInfSrc);
    }
    #[test]
    fn async_infinite_source_typed_name_mw() {
        let _ = crate::async_infinite_source!(name: "s", Out => AsyncInfSrc with []);
    }

    // ── transform! ──────────────────────────────────────────────────────────
    #[test]
    fn transform_typed_bare() {
        let _ = crate::transform!(In -> Out => Tr);
    }
    #[test]
    fn transform_typed_mw() {
        let _ = crate::transform!(In -> Out => Tr, observers: []);
    }
    #[test]
    fn transform_typed_name() {
        let _ = crate::transform!(name: "t", In -> Out => Tr);
    }
    #[test]
    fn transform_typed_name_mw() {
        let _ = crate::transform!(name: "t", In -> Out => Tr, observers: []);
    }

    // ── effectful_transform! ──────────────────────────────────────────
    #[test]
    fn effectful_transform_typed_bare() {
        let _ = crate::effectful_transform!(In -> Out => FxTr, effects: [], observers: []);
    }
    #[test]
    fn effectful_transform_typed_mw() {
        let _ = crate::effectful_transform!(In -> Out => FxTr, effects: [], observers: []);
    }
    #[test]
    fn effectful_transform_typed_name() {
        let _ =
            crate::effectful_transform!(name: "t", In -> Out => FxTr, effects: [], observers: []);
    }
    #[test]
    fn effectful_transform_typed_name_mw() {
        let _ =
            crate::effectful_transform!(name: "t", In -> Out => FxTr, effects: [], observers: []);
    }
    #[test]
    fn effectful_transform_transactional_effect_clause_declares_executor() {
        let descriptor = crate::effectful_transform!(
            In -> Out => TxFxTr,
            effects: [transactional(TxEffect, "tx")],
            observers: []
        );

        let declarations = descriptor.effect_declarations();
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].effect_type, TxEffect::EFFECT_TYPE);
        assert_eq!(declarations[0].safety, EffectSafety::Transactional);
        assert_eq!(declarations[0].transactional_executor, Some("tx"));
    }

    // The effect-manifest muncher cross-check lives in
    // `lowering_helper_contract_test.rs`, the sole test file licensed to
    // invoke lowering helpers directly (FLOWIP-133a helper boundary).

    // ── stateful! ───────────────────────────────────────────────────────────
    #[test]
    fn stateful_typed_bare() {
        let _ = crate::stateful!(In -> Out => St);
    }
    #[test]
    fn stateful_typed_mw() {
        let _ = crate::stateful!(In -> Out => St, observers: []);
    }
    #[test]
    fn stateful_typed_name() {
        let _ = crate::stateful!(name: "s", In -> Out => St);
    }
    #[test]
    fn stateful_typed_name_mw() {
        let _ = crate::stateful!(name: "s", In -> Out => St, observers: []);
    }

    // ── effectful_stateful! ─────────────────────────────────────────────────
    #[test]
    fn effectful_stateful_typed_bare() {
        let _ = crate::effectful_stateful!(In -> Out => FxSt, effects: [], observers: []);
    }
    #[test]
    fn effectful_stateful_typed_mw() {
        let _ = crate::effectful_stateful!(In -> Out => FxSt, effects: [], observers: []);
    }
    #[test]
    fn effectful_stateful_typed_name() {
        let _ =
            crate::effectful_stateful!(name: "s", In -> Out => FxSt, effects: [], observers: []);
    }
    #[test]
    fn effectful_stateful_typed_name_mw() {
        let _ =
            crate::effectful_stateful!(name: "s", In -> Out => FxSt, effects: [], observers: []);
    }

    // ── sink! ───────────────────────────────────────────────────────────────
    #[test]
    fn sink_typed_bare() {
        let _ = crate::sink!(Out => Sn);
    }
    #[test]
    fn sink_typed_mw() {
        let _ = crate::sink!(Out => Sn, observers: []);
    }
    #[test]
    fn sink_typed_name() {
        let _ = crate::sink!(name: "s", Out => Sn);
    }
    #[test]
    fn sink_typed_name_mw() {
        let _ = crate::sink!(name: "s", Out => Sn, observers: []);
    }
    #[test]
    fn sink_typed_delivery_clause() {
        // The clause rides the sealed closure-tier structs; a custom handler
        // implements `SinkHandler::delivery_safety` directly instead.
        let idempotent_sink = SinkTyped::new(|_out: Out| async move {});
        let _ = crate::sink!(Out => idempotent_sink, delivery: idempotent);
        let non_idempotent_sink = SinkTyped::new(|_out: Out| async move {});
        let _ = crate::sink!(Out => non_idempotent_sink, delivery: non_idempotent, observers: []);
    }
    #[test]
    fn sink_exact_contract_one_arg_closure() {
        let bare_sink = SinkTyped::new(|_out: Out| async move {});
        let _ = crate::sink!(Out => bare_sink);
        let idempotent_sink = SinkTyped::new(|_out: Out| async move {});
        let _ = crate::sink!(Out => idempotent_sink, delivery: idempotent);
        let named_sink = SinkTyped::new(|_out: Out| async move {});
        let _ = crate::sink!(name: "s", Out => named_sink, observers: []);
    }
    #[test]
    fn sink_exact_contract_delivery_closure() {
        let bare_sink = SinkTyped::with_delivery(|_out: Out, _delivery| async move {});
        let _ = crate::sink!(Out => bare_sink);
        let named_sink = SinkTyped::with_delivery(|out: Out, delivery| async move {
            let _ = (out, delivery.provenance());
        });
        let _ = crate::sink!(
            name: "s",
            Out => named_sink,
            delivery: idempotent,
            observers: []
        );
    }
}
