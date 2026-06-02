// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Empirical verification for FLOWIP-114c: every typed decoration combination
//! across the eight stage families is callable today. If any arm is missing,
//! this file fails to compile and the implementing PR must add the missing
//! arm before deleting the matching untyped arm.

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::{ChainEvent, TypedPayload};
    use obzenflow_runtime::effects::Effects;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::source::SourceError;
    use obzenflow_runtime::stages::common::handlers::{
        AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, AsyncTransformHandler,
        EffectfulAsyncSinkHandler, EffectfulAsyncTransformHandler, EffectfulStatefulHandler,
        FiniteSourceHandler, InfiniteSourceHandler, SinkHandler, StatefulHandler, TransformHandler,
    };
    use obzenflow_runtime::typing::{SinkTyping, SourceTyping, StatefulTyping, TransformTyping};
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
    struct AsyncTr;
    impl TransformTyping for AsyncTr {
        type Input = In;
        type Output = Out;
    }
    #[async_trait]
    impl AsyncTransformHandler for AsyncTr {
        async fn process(&self, _e: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct FxTr;
    #[async_trait]
    impl EffectfulAsyncTransformHandler for FxTr {
        type Input = In;
        type Output = Out;

        async fn process(&self, _input: In, _fx: &mut Effects) -> Result<Out, HandlerError> {
            Ok(Out)
        }
    }

    #[derive(Clone, Debug)]
    struct St;
    impl StatefulTyping for St {
        type Input = In;
        type Output = Out;
    }
    #[async_trait]
    impl StatefulHandler for St {
        type State = ();
        fn accumulate(&mut self, _s: &mut Self::State, _e: ChainEvent) {}
        fn initial_state(&self) -> Self::State {}
        fn create_events(&self, _s: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Debug)]
    struct FxSt;
    #[async_trait]
    impl EffectfulStatefulHandler for FxSt {
        type State = ();
        type Input = In;
        type Output = Out;
        type Transition = ();

        fn initial_state(&self) -> Self::State {}

        async fn transition(
            &mut self,
            _state: &Self::State,
            _input: &In,
            _fx: &mut Effects,
        ) -> Result<Self::Transition, HandlerError> {
            Ok(())
        }

        fn apply(
            &mut self,
            _state: &mut Self::State,
            _input: In,
            _transition: Self::Transition,
        ) -> Result<(), HandlerError> {
            Ok(())
        }

        fn create_outputs(&self, _state: &Self::State) -> Result<Vec<Out>, HandlerError> {
            Ok(vec![])
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
            Ok(DeliveryPayload::success("sink", DeliveryMethod::Noop, None))
        }
    }

    #[derive(Clone, Debug)]
    struct FxSn;
    #[async_trait]
    impl EffectfulAsyncSinkHandler for FxSn {
        type Input = Out;

        async fn consume(
            &mut self,
            _input: Out,
            _fx: &mut Effects,
        ) -> Result<DeliveryPayload, HandlerError> {
            Ok(DeliveryPayload::success("sink", DeliveryMethod::Noop, None))
        }
    }

    // ── source! ─────────────────────────────────────────────────────────────
    #[test]
    fn source_typed_bare() {
        let _ = crate::source!(Out => Src);
    }
    #[test]
    fn source_typed_mw() {
        let _ = crate::source!(Out => Src, []);
    }
    #[test]
    fn source_typed_name() {
        let _ = crate::source!(name: "s", Out => Src);
    }
    #[test]
    fn source_typed_name_mw() {
        let _ = crate::source!(name: "s", Out => Src, []);
    }

    // ── async_source! ───────────────────────────────────────────────────────
    #[test]
    fn async_source_typed_bare() {
        let _ = crate::async_source!(Out => AsyncSrc);
    }
    #[test]
    fn async_source_typed_mw() {
        let _ = crate::async_source!(Out => AsyncSrc, []);
    }
    #[test]
    fn async_source_typed_name() {
        let _ = crate::async_source!(name: "s", Out => AsyncSrc);
    }
    #[test]
    fn async_source_typed_name_mw() {
        let _ = crate::async_source!(name: "s", Out => AsyncSrc, []);
    }

    // ── infinite_source! ────────────────────────────────────────────────────
    #[test]
    fn infinite_source_typed_bare() {
        let _ = crate::infinite_source!(Out => InfSrc);
    }
    #[test]
    fn infinite_source_typed_mw() {
        let _ = crate::infinite_source!(Out => InfSrc, []);
    }
    #[test]
    fn infinite_source_typed_name() {
        let _ = crate::infinite_source!(name: "s", Out => InfSrc);
    }
    #[test]
    fn infinite_source_typed_name_mw() {
        let _ = crate::infinite_source!(name: "s", Out => InfSrc, []);
    }

    // ── async_infinite_source! ──────────────────────────────────────────────
    #[test]
    fn async_infinite_source_typed_bare() {
        let _ = crate::async_infinite_source!(Out => AsyncInfSrc);
    }
    #[test]
    fn async_infinite_source_typed_mw() {
        let _ = crate::async_infinite_source!(Out => AsyncInfSrc, []);
    }
    #[test]
    fn async_infinite_source_typed_name() {
        let _ = crate::async_infinite_source!(name: "s", Out => AsyncInfSrc);
    }
    #[test]
    fn async_infinite_source_typed_name_mw() {
        let _ = crate::async_infinite_source!(name: "s", Out => AsyncInfSrc, []);
    }

    // ── transform! ──────────────────────────────────────────────────────────
    #[test]
    fn transform_typed_bare() {
        let _ = crate::transform!(In -> Out => Tr);
    }
    #[test]
    fn transform_typed_mw() {
        let _ = crate::transform!(In -> Out => Tr, []);
    }
    #[test]
    fn transform_typed_name() {
        let _ = crate::transform!(name: "t", In -> Out => Tr);
    }
    #[test]
    fn transform_typed_name_mw() {
        let _ = crate::transform!(name: "t", In -> Out => Tr, []);
    }

    // ── async_transform! ────────────────────────────────────────────────────
    #[test]
    fn async_transform_typed_bare() {
        let _ = crate::async_transform!(In -> Out => AsyncTr);
    }
    #[test]
    fn async_transform_typed_mw() {
        let _ = crate::async_transform!(In -> Out => AsyncTr, []);
    }
    #[test]
    fn async_transform_typed_name() {
        let _ = crate::async_transform!(name: "t", In -> Out => AsyncTr);
    }
    #[test]
    fn async_transform_typed_name_mw() {
        let _ = crate::async_transform!(name: "t", In -> Out => AsyncTr, []);
    }

    // ── effectful_async_transform! ──────────────────────────────────────────
    #[test]
    fn effectful_async_transform_typed_bare() {
        let _ = crate::effectful_async_transform!(In -> Out => FxTr);
    }
    #[test]
    fn effectful_async_transform_typed_mw() {
        let _ = crate::effectful_async_transform!(In -> Out => FxTr, []);
    }
    #[test]
    fn effectful_async_transform_typed_name() {
        let _ = crate::effectful_async_transform!(name: "t", In -> Out => FxTr);
    }
    #[test]
    fn effectful_async_transform_typed_name_mw() {
        let _ = crate::effectful_async_transform!(name: "t", In -> Out => FxTr, []);
    }

    // ── stateful! ───────────────────────────────────────────────────────────
    #[test]
    fn stateful_typed_bare() {
        let _ = crate::stateful!(In -> Out => St);
    }
    #[test]
    fn stateful_typed_mw() {
        let _ = crate::stateful!(In -> Out => St, []);
    }
    #[test]
    fn stateful_typed_name() {
        let _ = crate::stateful!(name: "s", In -> Out => St);
    }
    #[test]
    fn stateful_typed_name_mw() {
        let _ = crate::stateful!(name: "s", In -> Out => St, []);
    }

    // ── effectful_stateful! ─────────────────────────────────────────────────
    #[test]
    fn effectful_stateful_typed_bare() {
        let _ = crate::effectful_stateful!(In -> Out => FxSt);
    }
    #[test]
    fn effectful_stateful_typed_mw() {
        let _ = crate::effectful_stateful!(In -> Out => FxSt, []);
    }
    #[test]
    fn effectful_stateful_typed_name() {
        let _ = crate::effectful_stateful!(name: "s", In -> Out => FxSt);
    }
    #[test]
    fn effectful_stateful_typed_name_mw() {
        let _ = crate::effectful_stateful!(name: "s", In -> Out => FxSt, []);
    }

    // ── sink! ───────────────────────────────────────────────────────────────
    #[test]
    fn sink_typed_bare() {
        let _ = crate::sink!(Out => Sn);
    }
    #[test]
    fn sink_typed_mw() {
        let _ = crate::sink!(Out => Sn, []);
    }
    #[test]
    fn sink_typed_name() {
        let _ = crate::sink!(name: "s", Out => Sn);
    }
    #[test]
    fn sink_typed_name_mw() {
        let _ = crate::sink!(name: "s", Out => Sn, []);
    }

    // ── effectful_sink! ─────────────────────────────────────────────────────
    #[test]
    fn effectful_sink_typed_bare() {
        let _ = crate::effectful_sink!(Out => FxSn);
    }
    #[test]
    fn effectful_sink_typed_mw() {
        let _ = crate::effectful_sink!(Out => FxSn, []);
    }
    #[test]
    fn effectful_sink_typed_name() {
        let _ = crate::effectful_sink!(name: "s", Out => FxSn);
    }
    #[test]
    fn effectful_sink_typed_name_mw() {
        let _ = crate::effectful_sink!(name: "s", Out => FxSn, []);
    }
}
