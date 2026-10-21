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
    use obzenflow_core::{ChainEvent, TypedPayload};
    use obzenflow_runtime::effects::{
        Call, CallRequest, EffectContext, EffectError, EffectHandler, EffectPortResolutionError,
        EffectSafety, Effects, Idempotent, SinkRedeliverySafety, StageCompletion, System,
        Transactional,
    };
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::source::SourceError;
    use obzenflow_runtime::stages::common::handlers::{
        EffectfulStatefulHandler, EffectfulTransformHandler, InlineSink, JoinReferenceView,
        SinkWriteFailure, StatefulEmission, TransformHandler, TypedAsyncFiniteSourceHandler,
        TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
        TypedJoinHandler, TypedStatefulHandler, TypedTransformHandler,
    };
    use obzenflow_runtime::typing::{SourceTyping, TransformTyping};
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

    struct OutObserver;

    impl obzenflow_adapters::middleware::SinkDeliveryObserver for OutObserver {
        type Input = Out;
    }

    #[derive(Clone, Debug)]
    struct Src;
    impl SourceTyping for Src {
        type Output = Out;
    }
    impl TypedFiniteSourceHandler for Src {
        type Output = Out;

        fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
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

        impl TypedFiniteSourceHandler for Source {
            type Output = Out;

            fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
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
    impl TypedAsyncFiniteSourceHandler for AsyncSrc {
        type Output = Out;

        async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct InfSrc;
    impl SourceTyping for InfSrc {
        type Output = Out;
    }
    impl TypedInfiniteSourceHandler for InfSrc {
        type Output = Out;

        fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Debug)]
    struct AsyncInfSrc;
    impl SourceTyping for AsyncInfSrc {
        type Output = Out;
    }
    #[async_trait]
    impl TypedAsyncInfiniteSourceHandler for AsyncInfSrc {
        type Output = Out;

        async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
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

    #[derive(Clone, Debug, EffectHandler)]
    #[effect_handler(effects = obzenflow_runtime::effects)]
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

    #[derive(Clone, Debug, EffectHandler)]
    #[effect_handler(effects = obzenflow_runtime::effects)]
    struct TxFxTr {
        #[system]
        tx: System<TxPort>,
    }

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

    #[derive(Clone, Debug, EffectHandler)]
    #[effect_handler(effects = obzenflow_runtime::effects)]
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

    #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    struct PolicyPort;

    #[derive(Clone, Debug, Serialize)]
    struct PolicyRequest;

    #[async_trait]
    impl CallRequest for PolicyRequest {
        const EFFECT_TYPE: &'static str = "test.stateful_policy_effect";
        type Safety = Idempotent;
        type Port = PolicyPort;
        type Outcome = Out;

        async fn call(&self, _: &PolicyPort, _: &mut EffectContext) -> Result<Out, EffectError> {
            Ok(Out)
        }
    }

    type PolicyEffect = Call<PolicyRequest>;

    #[derive(Clone, Debug, EffectHandler)]
    #[effect_handler(effects = obzenflow_runtime::effects)]
    struct FxPolicySt {
        #[system]
        policy: System<PolicyPort>,
    }

    fn policy_handler() -> FxPolicySt {
        FxPolicySt {
            policy: System::eager("policy", std::sync::Arc::new(PolicyPort)).unwrap(),
        }
    }

    #[async_trait]
    impl EffectfulStatefulHandler for FxPolicySt {
        type State = ();
        type Input = In;
        type Output = Out;
        type AllowedEffects = obzenflow_runtime::effect_set![PolicyEffect];

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
    #[async_trait]
    impl InlineSink for Sn {
        type Input = Out;
        fn describe(&self) -> obzenflow_runtime::stages::sink::SinkDescription {
            obzenflow_runtime::stages::sink::SinkDescription::method(
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
            )
        }

        async fn write(&mut self, _input: Out) -> Result<(), SinkWriteFailure> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct Jn;
    impl TypedJoinHandler for Jn {
        type State = ();
        type ReferenceKey = ();
        type Reference = In;
        type Stream = In;
        type Output = Out;

        fn initial_state(&self) -> Self::State {}

        fn admit_reference(
            &self,
            _reference: &Self::Reference,
        ) -> Result<Self::ReferenceKey, HandlerError> {
            Ok(())
        }

        fn process_stream(
            &self,
            _state: &mut Self::State,
            _references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
            _stream: Self::Stream,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            Ok(vec![])
        }
    }

    #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    struct TxPort;

    #[derive(Clone, Debug, Serialize)]
    struct TxRequest;

    #[async_trait]
    impl CallRequest for TxRequest {
        const EFFECT_TYPE: &'static str = "test.tx_effect";
        type Safety = Transactional;
        type Port = TxPort;
        type Outcome = Out;

        async fn call(&self, _: &TxPort, ctx: &mut EffectContext) -> Result<Out, EffectError> {
            let tx = ctx.transaction(self)?;
            tx.commit_success(&Out).await?;
            Ok(Out)
        }
    }

    type TxEffect = Call<TxRequest>;

    fn tx_system() -> System<TxPort> {
        System::deferred(
            "tx",
            || -> Result<std::sync::Arc<TxPort>, EffectPortResolutionError> {
                Err(EffectPortResolutionError::ClientConstructionFailed)
            },
        )
        .unwrap()
    }

    #[test]
    fn every_stage_family_accepts_a_bound_path_in_its_maximally_decorated_form() {
        use obzenflow_adapters::middleware::rate_limit;
        let finite = Src;
        let async_finite = AsyncSrc;
        let infinite = InfSrc;
        let async_infinite = AsyncInfSrc;
        let transform = Tr;
        let effectful_transform = FxTr;
        let stateful = St;
        let effectful_stateful = FxSt;
        let join = Jn;
        let sink = Sn;

        let _ = crate::source!(
            name: "finite",
            Out with rate_limit(10.0) => finite
        );
        let _ = crate::async_source!(
            name: "async_finite",
            Out with rate_limit(10.0) => async_finite
        );
        let _ = crate::infinite_source!(
            name: "infinite",
            Out with rate_limit(10.0) => infinite
        );
        let _ = crate::async_infinite_source!(
            name: "async_infinite",
            Out with rate_limit(10.0) => async_infinite
        );
        let _ = crate::transform!(
            name: "transform",
            In -> Out with rate_limit(10.0) => transform
        );
        let _ = crate::effectful_transform!(
            name: "effectful_transform",
            In -> Out with rate_limit(10.0) => effectful_transform
        );
        let _ = crate::stateful!(
            name: "stateful",
            In -> Out with rate_limit(10.0) => stateful
        );
        let _ = crate::effectful_stateful!(
            name: "effectful_stateful",
            In -> Out with rate_limit(10.0) => effectful_stateful
        );
        let _ = crate::join!(
            name: "join",
            catalog reference_stage: In,
            In -> Out with rate_limit(10.0) => join);
        let _ = crate::sink!(
            name: "sink",
            Out with rate_limit(10.0) => sink.idempotent());
    }

    // ── source! ─────────────────────────────────────────────────────────────
    #[test]
    fn source_typed_bare() {
        let _ = crate::source!(Out => Src);
    }
    #[test]
    fn source_typed_mw() {
        let _ = crate::source!(Out => Src );
    }
    #[test]
    fn source_typed_name() {
        let _ = crate::source!(name: "s", Out => Src);
    }
    #[test]
    fn source_typed_name_mw() {
        let _ = crate::source!(name: "s", Out => Src );
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
        let _ = crate::async_source!(Out => AsyncSrc );
    }
    #[test]
    fn async_source_typed_name() {
        let _ = crate::async_source!(name: "s", Out => AsyncSrc);
    }
    #[test]
    fn async_source_typed_name_mw() {
        let _ = crate::async_source!(name: "s", Out => AsyncSrc );
    }

    // ── infinite_source! ────────────────────────────────────────────────────
    #[test]
    fn infinite_source_typed_bare() {
        let _ = crate::infinite_source!(Out => InfSrc);
    }
    #[test]
    fn infinite_source_typed_mw() {
        let _ = crate::infinite_source!(Out => InfSrc );
    }
    #[test]
    fn infinite_source_typed_name() {
        let _ = crate::infinite_source!(name: "s", Out => InfSrc);
    }
    #[test]
    fn infinite_source_typed_name_mw() {
        let _ = crate::infinite_source!(name: "s", Out => InfSrc );
    }

    // ── async_infinite_source! ──────────────────────────────────────────────
    #[test]
    fn async_infinite_source_typed_bare() {
        let _ = crate::async_infinite_source!(Out => AsyncInfSrc);
    }
    #[test]
    fn async_infinite_source_typed_mw() {
        let _ = crate::async_infinite_source!(Out => AsyncInfSrc );
    }
    #[test]
    fn async_infinite_source_typed_name() {
        let _ = crate::async_infinite_source!(name: "s", Out => AsyncInfSrc);
    }
    #[test]
    fn async_infinite_source_typed_name_mw() {
        let _ = crate::async_infinite_source!(name: "s", Out => AsyncInfSrc );
    }

    // ── transform! ──────────────────────────────────────────────────────────
    #[test]
    fn transform_typed_bare() {
        let _ = crate::transform!(In -> Out => Tr);
    }
    #[test]
    fn transform_typed_mw() {
        let _ = crate::transform!(In -> Out => Tr);
    }
    #[test]
    fn transform_typed_name() {
        let _ = crate::transform!(name: "t", In -> Out => Tr);
    }
    #[test]
    fn transform_typed_name_mw() {
        let _ = crate::transform!(name: "t", In -> Out => Tr);
    }

    // ── effectful_transform! ──────────────────────────────────────────
    #[test]
    fn effectful_transform_typed_bare() {
        let _ = crate::effectful_transform!(In -> Out => FxTr);
    }
    #[test]
    fn effectful_transform_typed_mw() {
        let _ = crate::effectful_transform!(In -> Out => FxTr);
    }
    #[test]
    fn effectful_transform_typed_name() {
        let _ = crate::effectful_transform!(name: "t", In -> Out => FxTr);
    }
    #[test]
    fn effectful_transform_typed_name_mw() {
        let _ = crate::effectful_transform!(name: "t", In -> Out => FxTr);
    }
    #[test]
    fn effectful_transform_transactional_acknowledgement_declares_the_request() {
        let tx = tx_system();
        assert_eq!(tx.name(), "tx");
        let handler = TxFxTr { tx };
        let descriptor = crate::effectful_transform!(
            In -> Out => handler.transactional::<TxEffect>());

        let declarations = descriptor.effect_declarations();
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].effect_type(), TxRequest::EFFECT_TYPE);
        assert_eq!(declarations[0].safety(), EffectSafety::Transactional);
    }

    // ── stateful! ───────────────────────────────────────────────────────────
    #[test]
    fn stateful_typed_bare() {
        let _ = crate::stateful!(In -> Out => St);
    }
    #[test]
    fn stateful_typed_mw() {
        let _ = crate::stateful!(In -> Out => St);
    }
    #[test]
    fn stateful_typed_name() {
        let _ = crate::stateful!(name: "s", In -> Out => St);
    }
    #[test]
    fn stateful_typed_name_mw() {
        let _ = crate::stateful!(name: "s", In -> Out => St);
    }

    // ── effectful_stateful! ─────────────────────────────────────────────────
    #[test]
    fn effectful_stateful_typed_bare() {
        let _ = crate::effectful_stateful!(In -> Out => FxSt);
    }
    #[test]
    fn effectful_stateful_typed_mw() {
        let _ = crate::effectful_stateful!(In -> Out => FxSt);
    }
    #[test]
    fn effectful_stateful_typed_name() {
        let _ = crate::effectful_stateful!(name: "s", In -> Out => FxSt);
    }
    #[test]
    fn effectful_stateful_typed_name_mw() {
        let _ = crate::effectful_stateful!(name: "s", In -> Out => FxSt);
    }

    #[test]
    fn effectful_stateful_accepts_and_retains_inline_effect_policy() {
        let handler = policy_handler();
        let mut descriptor = crate::effectful_stateful!(
            In -> Out
            with obzenflow_adapters::middleware::rate_limit(1_000.0)
            => handler);

        let declarations = descriptor.effect_declarations();
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].effect_type(), PolicyRequest::EFFECT_TYPE);
        descriptor
            .prepare_middleware()
            .expect("the policy guards the stage's one effect");
        let policies = descriptor.effect_policy_attachments();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].effect_type, PolicyRequest::EFFECT_TYPE);
    }

    struct EffectAudit;
    impl obzenflow_runtime::stages::observer::EffectObserver for EffectAudit {}

    #[test]
    fn uniform_groups_compose_controls_across_effect_and_implementation_sites() {
        use obzenflow_adapters::middleware::{
            circuit_breaker, effect_observer, rate_limit, retry, MiddlewareFactory,
        };
        let limiter = rate_limit(10.0).for_effect::<PolicyEffect>();
        let audit = effect_observer("audit", EffectAudit).for_effect::<PolicyEffect>();
        let handler = policy_handler();
        let mut descriptor = crate::effectful_stateful!(
            In -> Out
            with { limiter, audit, retry(), circuit_breaker().consecutive_failures(3), effect_observer("audit", EffectAudit) }
            => handler
        );
        descriptor
            .prepare_middleware()
            .expect("a single plan spans both authored sites");
        assert_eq!(descriptor.stage_middleware_factories().len(), 1);
        assert_eq!(descriptor.effect_policy_attachments().len(), 2);
        let controls = descriptor
            .effect_policy_attachments()
            .iter()
            .find(|a| a.factory.declaration().is_control())
            .unwrap();
        assert_eq!(
            controls.factory.builtin_control().unwrap().families().len(),
            3
        );
    }

    #[test]
    fn source_and_sink_groups_mix_observers_with_controls() {
        use obzenflow_adapters::middleware::{
            circuit_breaker, rate_limit, sink_delivery_observer, source_poll_observer,
        };
        struct PollAudit;
        impl obzenflow_runtime::stages::observer::SourcePollObserver for PollAudit {}
        let mut source = crate::source!(Out with { circuit_breaker().consecutive_failures(3), source_poll_observer("audit", PollAudit), rate_limit(10.0) } => Src);
        source.prepare_middleware().unwrap();
        assert_eq!(source.stage_middleware_factories().len(), 3);
        let mut sink = crate::sink!(Out with { sink_delivery_observer("audit", OutObserver), rate_limit(10.0), circuit_breaker().consecutive_failures(3) } => Sn);
        sink.prepare_middleware().unwrap();
        assert_eq!(sink.stage_middleware_factories().len(), 3);
    }

    // ── sink! ───────────────────────────────────────────────────────────────
    #[test]
    fn sink_typed_bare() {
        let _ = crate::sink!(Out => Sn);
    }
    #[test]
    fn sink_typed_mw() {
        let _ = crate::sink!(Out => Sn);
    }
    #[test]
    fn sink_typed_name() {
        let _ = crate::sink!(name: "s", Out => Sn);
    }
    #[test]
    fn sink_typed_name_mw() {
        let _ = crate::sink!(name: "s", Out => Sn);
    }
    #[test]
    fn sink_handler_set_lowers_one_heterogeneous_binding_by_its_exact_name() {
        let inline_sink = Sn;
        let classified_sink =
            obzenflow_runtime::stages::common::handlers::WithRedeliverySafety::new(
                Sn,
                SinkRedeliverySafety::SafeToRepeat,
            );
        let pending = crate::sink!(Out => handler_set!(inline_sink, classified_sink))
            .expect("the closed handler set is valid");

        assert_eq!(
            pending.configured_sink_handler_keys(),
            Some(["inline_sink", "classified_sink"].as_slice())
        );
        let selected = pending
            .select_configured_sink_handler("classified_sink")
            .expect("the configured binding name is in the closed set");
        assert_eq!(
            selected
                .typing_metadata()
                .expect("typed sink metadata")
                .input_type,
            crate::dsl::typing::TypeHint::exact_payload::<Out>()
        );
    }

    #[test]
    fn sink_handler_set_rejects_unknown_config_before_descriptor_lowering() {
        let policy_constructions = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observer_constructions = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy_probe = std::sync::Arc::clone(&policy_constructions);
        let observer_probe = std::sync::Arc::clone(&observer_constructions);
        let first_sink = Sn;
        let second_sink = obzenflow_runtime::stages::common::handlers::WithRedeliverySafety::new(
            Sn,
            SinkRedeliverySafety::SafeToRepeat,
        );

        let pending = crate::sink!(
            Out with { ({
                policy_probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                obzenflow_adapters::middleware::rate_limit(1_000.0)
            }), ({
                observer_probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                obzenflow_adapters::middleware::sink_delivery_observer("capture-proof", OutObserver)
            }) } => handler_set!(first_sink, second_sink))
        .expect("the closed handler set is valid");
        let result = pending.select_configured_sink_handler("unknown_sink");

        assert_eq!(
            policy_constructions.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            observer_constructions.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("an unknown sink key must fail materialisation"),
        };
        assert_eq!(
            error.to_string(),
            "config error at sinks.stages.__obzenflow_binding_derived_name__.handler: expected \"first_sink\" or \"second_sink\"; got \"unknown_sink\""
        );
        match error {
            crate::dsl::FlowBuildError::InvalidSinkSelection {
                stage_name,
                selected,
                expected,
            } => {
                assert_eq!(stage_name, "__obzenflow_binding_derived_name__");
                assert_eq!(selected, "unknown_sink");
                assert_eq!(expected, "\"first_sink\" or \"second_sink\"");
            }
            error => panic!("unexpected sink selection error: {error}"),
        }
    }
    #[test]
    fn sink_handler_set_form_composes_name_and_site_clauses_once() {
        let policy_constructions = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observer_constructions = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy_probe = std::sync::Arc::clone(&policy_constructions);
        let observer_probe = std::sync::Arc::clone(&observer_constructions);
        let inline_sink = Sn;
        let classified_sink =
            obzenflow_runtime::stages::common::handlers::WithRedeliverySafety::new(
                Sn,
                SinkRedeliverySafety::SafeToRepeat,
            );

        let pending = crate::sink!(
            name: "selected_output",
            Out with { ({
                policy_probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                obzenflow_adapters::middleware::rate_limit(1_000.0)
            }), ({
                observer_probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                obzenflow_adapters::middleware::sink_delivery_observer("capture-proof", OutObserver)
            }) } => handler_set!(inline_sink, classified_sink).idempotent())
        .expect("the closed handler set is valid");
        let selected = pending
            .select_configured_sink_handler("inline_sink")
            .expect("the configured binding name is in the closed set");

        assert_eq!(selected.name(), "selected_output");
        assert_eq!(
            policy_constructions.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            observer_constructions.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(selected.stage_middleware_names().len(), 2);
        assert_eq!(
            selected
                .sink_description()
                .expect("selected sink description")
                .redelivery_safety(),
            Some(SinkRedeliverySafety::SafeToRepeat)
        );
    }
    #[test]
    fn sink_typed_redelivery_decoration() {
        let idempotent_sink = Sn;
        let _ = crate::sink!(Out => idempotent_sink.idempotent());
        let non_idempotent_sink = Sn;
        let _ = crate::sink!(Out => non_idempotent_sink.non_idempotent());
    }
    #[test]
    fn sink_exact_contract_inline_sink() {
        let bare_sink = Sn;
        let _ = crate::sink!(Out => bare_sink);
        let idempotent_sink = Sn;
        let _ = crate::sink!(Out => idempotent_sink.idempotent());
        let named_sink = Sn;
        let _ = crate::sink!(name: "s", Out => named_sink);
    }
    #[test]
    fn sink_exact_contract_named_delivery() {
        let bare_sink = Sn;
        let _ = crate::sink!(Out => bare_sink);
        let named_sink = Sn;
        let _ = crate::sink!(
            name: "s",
            Out => named_sink.idempotent());
    }
}
