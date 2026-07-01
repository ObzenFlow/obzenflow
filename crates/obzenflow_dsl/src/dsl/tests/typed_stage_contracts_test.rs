// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for FLOWIP-105g typed stage contracts.

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::{ChainEvent, StageId, TypedPayload, WriterId};
    use obzenflow_runtime::effects::Effects;
    use obzenflow_runtime::feed_plan::{FactVisibility, FeedRole};
    use obzenflow_runtime::id_conversions::StageIdExt;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::source::SourceError;
    use obzenflow_runtime::stages::common::handlers::{
        AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, AsyncTransformHandler,
        EffectfulTransformHandler, FiniteSourceHandler, InfiniteSourceHandler, InputOrderSemantics,
        JoinHandler, SinkHandler, StatefulHandler, TransformHandler,
    };
    use obzenflow_runtime::typing::{
        JoinTyping, SinkTyping, SourceTyping, StatefulTyping, TransformTyping,
    };
    use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder, TypeHintInfo};
    use serde::{Deserialize, Serialize};
    use std::any::type_name;
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::dsl::error::{EdgeTypingMismatchKind, FlowBuildError};
    use crate::dsl::stage_descriptor::OrderRole;
    use crate::dsl::stage_descriptor::StageDescriptor;
    use crate::dsl::stage_descriptor::TransformDescriptor;
    use crate::dsl::typing::{
        collect_stage_typing_info, derive_feed_plan, validate_edge_typing,
        validate_effectful_deterministic_input_order, validate_stage_typing_metadata,
        EdgeInputRole, TypeHint,
    };
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct InputEvent {
        value: String,
    }

    impl TypedPayload for InputEvent {
        const EVENT_TYPE: &'static str = "test.input";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct OutputEvent {
        value: String,
    }

    impl TypedPayload for OutputEvent {
        const EVENT_TYPE: &'static str = "test.output";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct AlternateEvent {
        value: String,
    }

    impl TypedPayload for AlternateEvent {
        const EVENT_TYPE: &'static str = "test.alternate";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ReferenceEvent {
        value: String,
    }

    impl TypedPayload for ReferenceEvent {
        const EVENT_TYPE: &'static str = "test.reference";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct StreamEvent {
        value: String,
    }

    impl TypedPayload for StreamEvent {
        const EVENT_TYPE: &'static str = "test.stream";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct JoinedEvent {
        value: String,
    }

    impl TypedPayload for JoinedEvent {
        const EVENT_TYPE: &'static str = "test.joined";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ValueEnvelope {
        value: serde_json::Value,
    }

    impl TypedPayload for ValueEnvelope {
        const EVENT_TYPE: &'static str = "test.value_envelope";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Debug)]
    struct SyncExactSource;

    impl SourceTyping for SyncExactSource {
        type Output = InputEvent;
    }

    impl FiniteSourceHandler for SyncExactSource {
        fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct AsyncExactSource;

    impl SourceTyping for AsyncExactSource {
        type Output = InputEvent;
    }

    #[async_trait]
    impl AsyncFiniteSourceHandler for AsyncExactSource {
        async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct InfiniteExactSource;

    impl SourceTyping for InfiniteExactSource {
        type Output = InputEvent;
    }

    impl InfiniteSourceHandler for InfiniteExactSource {
        fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Debug)]
    struct AsyncInfiniteExactSource;

    impl SourceTyping for AsyncInfiniteExactSource {
        type Output = InputEvent;
    }

    #[async_trait]
    impl AsyncInfiniteSourceHandler for AsyncInfiniteExactSource {
        async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Debug)]
    struct ExactTransform;

    impl TransformTyping for ExactTransform {
        type Input = InputEvent;
        type Output = OutputEvent;
    }

    #[async_trait]
    impl TransformHandler for ExactTransform {
        fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct EffectfulExactTransform;

    #[async_trait]
    impl EffectfulTransformHandler for EffectfulExactTransform {
        type Input = OutputEvent;

        async fn process(&self, input: OutputEvent, fx: &mut Effects) -> Result<(), HandlerError> {
            fx.emit(input)
                .await
                .map_err(|e| HandlerError::Other(e.to_string()))?;
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct EffectfulMultiOutputTransform;

    #[async_trait]
    impl EffectfulTransformHandler for EffectfulMultiOutputTransform {
        type Input = OutputEvent;

        async fn process(&self, input: OutputEvent, fx: &mut Effects) -> Result<(), HandlerError> {
            fx.emit(input)
                .await
                .map_err(|e| HandlerError::Other(e.to_string()))?;
            fx.emit(AlternateEvent {
                value: "alternate".to_string(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct UntypedTransform;

    #[async_trait]
    impl TransformHandler for UntypedTransform {
        fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct ExactAsyncTransform;

    impl TransformTyping for ExactAsyncTransform {
        type Input = InputEvent;
        type Output = OutputEvent;
    }

    #[async_trait]
    impl AsyncTransformHandler for ExactAsyncTransform {
        async fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct UntypedAsyncTransform;

    #[async_trait]
    impl AsyncTransformHandler for UntypedAsyncTransform {
        async fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct ExactStateful;

    impl StatefulTyping for ExactStateful {
        type Input = InputEvent;
        type Output = OutputEvent;
    }

    #[async_trait]
    impl StatefulHandler for ExactStateful {
        type State = ();

        fn accumulate(&mut self, _state: &mut Self::State, _event: ChainEvent) {}

        fn initial_state(&self) -> Self::State {}

        fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
    }

    /// FLOWIP-095l: a stateful handler declared order-insensitive (a barrier). The
    /// proof witness is minted directly here for the guard tests; production code
    /// mints it only through `#[order_insensitive]` and the trial it emits.
    #[derive(Clone, Debug)]
    struct CommutativeStateful;

    impl StatefulTyping for CommutativeStateful {
        type Input = InputEvent;
        type Output = OutputEvent;
    }

    #[async_trait]
    impl StatefulHandler for CommutativeStateful {
        type State = ();

        fn accumulate(&mut self, _state: &mut Self::State, _event: ChainEvent) {}

        fn initial_state(&self) -> Self::State {}

        fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        fn declared_input_order(&self) -> InputOrderSemantics {
            InputOrderSemantics::__order_insensitive_proven()
        }
    }

    /// FLOWIP-095l Gap 4: an order-sensitive fold that explicitly accepts running
    /// below a cycle with non-reproducible order (the escape hatch).
    #[derive(Clone, Debug)]
    struct CycleTolerantStateful;

    impl StatefulTyping for CycleTolerantStateful {
        type Input = InputEvent;
        type Output = OutputEvent;
    }

    #[async_trait]
    impl StatefulHandler for CycleTolerantStateful {
        type State = ();

        fn accumulate(&mut self, _state: &mut Self::State, _event: ChainEvent) {}

        fn initial_state(&self) -> Self::State {}

        fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        fn declared_input_order(&self) -> InputOrderSemantics {
            InputOrderSemantics::OrderSensitive
        }

        fn accepts_cycle_nondeterminism(&self) -> bool {
            true
        }
    }

    #[derive(Clone, Debug)]
    struct EffectfulProductStateful;

    #[async_trait]
    impl obzenflow_runtime::stages::common::handlers::EffectfulStatefulHandler
        for EffectfulProductStateful
    {
        type State = ();
        type Input = InputEvent;
        type Fact = OutputEvent;

        fn initial_state(&self) -> Self::State {}

        async fn decide(
            &mut self,
            _state: &Self::State,
            _input: &Self::Input,
            _fx: &mut Effects,
        ) -> Result<(), HandlerError> {
            Ok(())
        }

        fn apply(
            &mut self,
            _state: &mut Self::State,
            _fact: Self::Fact,
        ) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct ExactSink;

    impl SinkTyping for ExactSink {
        type Input = OutputEvent;
    }

    #[async_trait]
    impl SinkHandler for ExactSink {
        async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
            Ok(DeliveryPayload::success("sink", DeliveryMethod::Noop, None))
        }
    }

    #[derive(Clone, Debug)]
    struct ExactJoin;

    impl JoinTyping for ExactJoin {
        type Reference = ReferenceEvent;
        type Stream = StreamEvent;
        type Output = JoinedEvent;
    }

    #[async_trait]
    impl JoinHandler for ExactJoin {
        type State = ();

        fn initial_state(&self) -> Self::State {}

        fn process_event(
            &self,
            _state: &mut Self::State,
            _event: ChainEvent,
            _source_id: StageId,
            _writer_id: WriterId,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        fn on_source_eof(
            &self,
            _state: &mut Self::State,
            _source_id: StageId,
            _writer_id: WriterId,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
    }

    fn exact<T: TypedPayload + 'static>() -> TypeHint {
        TypeHint::exact_payload::<T>()
    }

    fn assert_one_member_output_contract<T: TypedPayload + 'static>(
        metadata: &crate::dsl::typing::StageTypingMetadata,
    ) {
        assert_eq!(metadata.output_contract, vec![exact::<T>()]);
    }

    fn assert_output_contract(
        metadata: &crate::dsl::typing::StageTypingMetadata,
        expected: Vec<TypeHint>,
    ) {
        assert_eq!(metadata.output_contract, expected);
    }

    #[test]
    fn typed_macros_attach_metadata_for_exact_handlers() {
        let source = crate::source!(name: "source", InputEvent => SyncExactSource);
        assert_eq!(
            source.typing_metadata().unwrap().output_type,
            exact::<InputEvent>()
        );
        assert_one_member_output_contract::<InputEvent>(source.typing_metadata().unwrap());

        let async_source =
            crate::async_source!(name: "async_source", InputEvent => AsyncExactSource);
        assert_eq!(
            async_source.typing_metadata().unwrap().output_type,
            exact::<InputEvent>()
        );

        let infinite = crate::infinite_source!(name: "infinite", InputEvent => InfiniteExactSource);
        assert_eq!(
            infinite.typing_metadata().unwrap().output_type,
            exact::<InputEvent>()
        );

        let async_infinite = crate::async_infinite_source!(
            name: "async_infinite",
            InputEvent => AsyncInfiniteExactSource
        );
        assert_eq!(
            async_infinite.typing_metadata().unwrap().output_type,
            exact::<InputEvent>()
        );

        let transform =
            crate::transform!(name: "transform", InputEvent -> OutputEvent => ExactTransform);
        let transform_meta = transform.typing_metadata().unwrap();
        assert_eq!(transform_meta.input_type, exact::<InputEvent>());
        assert_eq!(transform_meta.output_type, exact::<OutputEvent>());
        assert_one_member_output_contract::<OutputEvent>(transform_meta);

        let untyped_transform = crate::transform!(
            name: "untyped_transform",
            InputEvent -> OutputEvent => UntypedTransform
        );
        let untyped_transform_meta = untyped_transform.typing_metadata().unwrap();
        assert_eq!(untyped_transform_meta.input_type, exact::<InputEvent>());
        assert_eq!(untyped_transform_meta.output_type, exact::<OutputEvent>());

        let async_transform = crate::async_transform!(
            name: "async_transform",
            InputEvent -> OutputEvent => ExactAsyncTransform
        );
        let async_transform_meta = async_transform.typing_metadata().unwrap();
        assert_eq!(async_transform_meta.input_type, exact::<InputEvent>());
        assert_eq!(async_transform_meta.output_type, exact::<OutputEvent>());

        let untyped_async_transform = crate::async_transform!(
            name: "untyped_async_transform",
            InputEvent -> OutputEvent => UntypedAsyncTransform
        );
        let untyped_async_transform_meta = untyped_async_transform.typing_metadata().unwrap();
        assert_eq!(
            untyped_async_transform_meta.input_type,
            exact::<InputEvent>()
        );
        assert_eq!(
            untyped_async_transform_meta.output_type,
            exact::<OutputEvent>()
        );

        let stateful = crate::stateful!(
            name: "stateful",
            InputEvent -> OutputEvent => ExactStateful,
            emit_interval = Duration::from_secs(5)
        );
        let stateful_meta = stateful.typing_metadata().unwrap();
        assert_eq!(stateful_meta.input_type, exact::<InputEvent>());
        assert_eq!(stateful_meta.output_type, exact::<OutputEvent>());

        let sink = crate::sink!(name: "sink", OutputEvent => ExactSink);
        let sink_meta = sink.typing_metadata().unwrap();
        assert_eq!(sink_meta.input_type, exact::<OutputEvent>());
        assert!(sink_meta.output_contract.is_empty());

        let join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => ExactJoin
        );
        let join_meta = join.typing_metadata().unwrap();
        assert_eq!(join_meta.reference_type, exact::<ReferenceEvent>());
        assert_eq!(join_meta.stream_type, exact::<StreamEvent>());
        assert_eq!(join_meta.output_type, exact::<JoinedEvent>());
        assert_one_member_output_contract::<JoinedEvent>(join_meta);
        assert!(!join_meta.is_placeholder);
    }

    #[test]
    fn transform_macros_accept_explicit_output_contract_members() {
        let transform = crate::transform!(
            name: "transform",
            InputEvent -> OutputEvent, outputs: [OutputEvent, AlternateEvent] => ExactTransform
        );
        let transform_meta = transform.typing_metadata().unwrap();
        assert_eq!(transform_meta.output_type, exact::<OutputEvent>());
        assert_output_contract(
            transform_meta,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );

        let async_transform = crate::async_transform!(
            name: "async_transform",
            InputEvent -> OutputEvent, outputs: [AlternateEvent] => ExactAsyncTransform
        );
        let async_transform_meta = async_transform.typing_metadata().unwrap();
        assert_eq!(async_transform_meta.output_type, exact::<OutputEvent>());
        assert_output_contract(
            async_transform_meta,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );

        let effectful_transform = crate::effectful_transform!(
            name: "effectful_transform",
            OutputEvent -> OutputEvent, outputs: [OutputEvent, AlternateEvent] => EffectfulExactTransform,
            effects: [],
            middleware: []
        );
        let effectful_transform_meta = effectful_transform.typing_metadata().unwrap();
        assert_eq!(effectful_transform_meta.output_type, exact::<OutputEvent>());
        assert_output_contract(
            effectful_transform_meta,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );
    }

    #[test]
    fn effectful_transform_output_set_lowers_all_members_to_output_contract() {
        let effectful_transform = crate::effectful_transform!(
            name: "effectful_multi_output_transform",
            OutputEvent -> { OutputEvent, AlternateEvent } => EffectfulMultiOutputTransform,
            effects: [],
            middleware: []
        );
        let metadata = effectful_transform.typing_metadata().unwrap();

        assert_eq!(metadata.output_type, exact::<OutputEvent>());
        assert_output_contract(
            metadata,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );
    }

    #[test]
    fn source_output_set_syntax_lowers_to_stage_output_contract() {
        let source = crate::source!(
            name: "multi_output_source",
            { InputEvent, AlternateEvent } => SyncExactSource
        );
        let source_meta = source.typing_metadata().unwrap();
        assert_eq!(source_meta.output_type, exact::<InputEvent>());
        assert_output_contract(
            source_meta,
            vec![exact::<InputEvent>(), exact::<AlternateEvent>()],
        );

        let async_source = crate::async_source!(
            name: "multi_output_async_source",
            { InputEvent, AlternateEvent } => AsyncExactSource
        );
        let async_source_meta = async_source.typing_metadata().unwrap();
        assert_eq!(async_source_meta.output_type, exact::<InputEvent>());
        assert_output_contract(
            async_source_meta,
            vec![exact::<InputEvent>(), exact::<AlternateEvent>()],
        );

        let infinite_source = crate::infinite_source!(
            name: "multi_output_infinite_source",
            { InputEvent, AlternateEvent } => InfiniteExactSource
        );
        let infinite_source_meta = infinite_source.typing_metadata().unwrap();
        assert_eq!(infinite_source_meta.output_type, exact::<InputEvent>());
        assert_output_contract(
            infinite_source_meta,
            vec![exact::<InputEvent>(), exact::<AlternateEvent>()],
        );

        let async_infinite_source = crate::async_infinite_source!(
            name: "multi_output_async_infinite_source",
            { InputEvent, AlternateEvent } => AsyncInfiniteExactSource
        );
        let async_infinite_source_meta = async_infinite_source.typing_metadata().unwrap();
        assert_eq!(
            async_infinite_source_meta.output_type,
            exact::<InputEvent>()
        );
        assert_output_contract(
            async_infinite_source_meta,
            vec![exact::<InputEvent>(), exact::<AlternateEvent>()],
        );
    }

    #[test]
    fn output_set_syntax_lowers_to_stage_output_contract() {
        let transform = crate::transform!(
            name: "multi_output_transform",
            InputEvent -> { OutputEvent, AlternateEvent } => ExactTransform
        );
        let transform_meta = transform.typing_metadata().unwrap();
        assert_eq!(transform_meta.output_type, exact::<OutputEvent>());
        assert_output_contract(
            transform_meta,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );

        let async_transform = crate::async_transform!(
            name: "multi_output_async_transform",
            InputEvent -> { OutputEvent, AlternateEvent } => ExactAsyncTransform
        );
        let async_transform_meta = async_transform.typing_metadata().unwrap();
        assert_eq!(async_transform_meta.output_type, exact::<OutputEvent>());
        assert_output_contract(
            async_transform_meta,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );

        let stateful = crate::stateful!(
            name: "multi_output_stateful",
            InputEvent -> { OutputEvent, AlternateEvent } => ExactStateful,
            emit_interval = Duration::from_secs(1)
        );
        let stateful_meta = stateful.typing_metadata().unwrap();
        assert_eq!(stateful_meta.output_type, exact::<OutputEvent>());
        assert_output_contract(
            stateful_meta,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );

        let effectful_stateful = crate::effectful_stateful!(
            name: "multi_output_effectful_stateful",
            InputEvent -> { OutputEvent, AlternateEvent } => EffectfulProductStateful,
            emit_interval = Duration::from_secs(1),
            effects: [],
            middleware: []
        );
        let effectful_stateful_meta = effectful_stateful.typing_metadata().unwrap();
        assert_eq!(effectful_stateful_meta.output_type, exact::<OutputEvent>());
        assert_output_contract(
            effectful_stateful_meta,
            vec![exact::<OutputEvent>(), exact::<AlternateEvent>()],
        );

        let join = crate::join!(
            name: "multi_output_join",
            catalog reference: ReferenceEvent,
            StreamEvent -> { JoinedEvent, AlternateEvent } => ExactJoin
        );
        let join_meta = join.typing_metadata().unwrap();
        assert_eq!(join_meta.output_type, exact::<JoinedEvent>());
        assert_output_contract(
            join_meta,
            vec![exact::<JoinedEvent>(), exact::<AlternateEvent>()],
        );
    }

    #[test]
    fn effectful_stateful_scalar_output_contract_has_one_member() {
        let effectful_stateful = crate::effectful_stateful!(
            name: "effectful_stateful",
            InputEvent -> OutputEvent => EffectfulProductStateful,
            effects: [],
            middleware: []
        );
        let metadata = effectful_stateful.typing_metadata().unwrap();

        assert_eq!(metadata.output_type, exact::<OutputEvent>());
        assert_output_contract(metadata, vec![exact::<OutputEvent>()]);
    }

    #[test]
    fn collect_stage_typing_info_exports_runtime_metadata_by_stage_id() {
        let source_id = StageId::new();
        let transform_id = StageId::new();
        let join_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source".to_string(),
            crate::source!(name: "source", InputEvent => SyncExactSource),
        );
        descriptors.insert(
            "transform".to_string(),
            crate::transform!(name: "transform", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "join".to_string(),
            crate::join!(
                name: "join",
                catalog reference: ReferenceEvent,
                StreamEvent -> JoinedEvent => ExactJoin
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);
        name_to_id.insert("transform".to_string(), transform_id);
        name_to_id.insert("join".to_string(), join_id);

        let stage_typing = collect_stage_typing_info(&descriptors, &name_to_id);

        assert_eq!(stage_typing.len(), 3);
        assert_eq!(
            stage_typing.get(&source_id).unwrap().output_type,
            TypeHintInfo::Exact {
                name: type_name::<InputEvent>().to_string()
            }
        );
        assert_eq!(
            stage_typing.get(&transform_id).unwrap().input_type,
            TypeHintInfo::Exact {
                name: type_name::<InputEvent>().to_string()
            }
        );

        let join_typing = stage_typing.get(&join_id).unwrap();
        assert_eq!(
            join_typing.reference_type,
            TypeHintInfo::Exact {
                name: type_name::<ReferenceEvent>().to_string()
            }
        );
        assert_eq!(
            join_typing.stream_type,
            TypeHintInfo::Exact {
                name: type_name::<StreamEvent>().to_string()
            }
        );
        assert_eq!(
            join_typing.output_type,
            TypeHintInfo::Exact {
                name: type_name::<JoinedEvent>().to_string()
            }
        );
    }

    #[test]
    fn collect_stage_typing_info_projects_multi_output_contract_as_mixed() {
        let source_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source".to_string(),
            crate::source!(
                name: "source",
                { InputEvent, AlternateEvent } => SyncExactSource
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);

        let stage_typing = collect_stage_typing_info(&descriptors, &name_to_id);

        assert_eq!(
            stage_typing.get(&source_id).unwrap().output_type,
            TypeHintInfo::Mixed
        );
    }

    #[test]
    fn stage_macros_default_to_binding_derived_name_marker() {
        let source = crate::source!(InputEvent => SyncExactSource);
        assert_eq!(
            source.name(),
            crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL
        );

        let transform = crate::transform!(InputEvent -> OutputEvent => ExactTransform);
        assert_eq!(
            transform.name(),
            crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL
        );

        let sink = crate::sink!(OutputEvent => ExactSink);
        assert_eq!(
            sink.name(),
            crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL
        );

        let join = crate::join!(catalog reference: ReferenceEvent, StreamEvent -> JoinedEvent => ExactJoin);
        assert_eq!(
            join.name(),
            crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL
        );
    }

    #[test]
    fn placeholders_capture_metadata() {
        let source = crate::source!(name: "source", InputEvent => placeholder!("awaiting source"));
        let source_meta = source.typing_metadata().unwrap();
        assert!(source_meta.is_placeholder);
        assert_eq!(
            source_meta.placeholder_message.as_deref(),
            Some("awaiting source")
        );

        let async_transform = crate::async_transform!(
            name: "transform",
            InputEvent -> OutputEvent => placeholder!()
        );
        let async_transform_meta = async_transform.typing_metadata().unwrap();
        assert!(async_transform_meta.is_placeholder);
        assert_eq!(async_transform_meta.input_type, exact::<InputEvent>());
        assert_eq!(async_transform_meta.output_type, exact::<OutputEvent>());
    }

    /// FLOWIP-114c: validate_edge_typing returns Err with one EdgeError per
    /// mismatched forward edge.
    #[test]
    fn validate_edge_typing_reports_exact_mismatch() {
        let source_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source".to_string(),
            crate::source!(name: "source", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", OutputEvent => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_id.to_topology_id(),
            Some("source".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            sink_id.to_topology_id(),
            Some("sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(source_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let errors = validate_edge_typing(&topology, &descriptors, &name_to_id)
            .expect_err("expected an EdgeError for the mismatched edge");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].upstream_stage, "source");
        assert_eq!(errors[0].downstream_stage, "sink");
        assert_eq!(errors[0].upstream_type, type_name::<InputEvent>());
        assert_eq!(errors[0].expected_type, type_name::<OutputEvent>());
        assert_eq!(errors[0].input_role, EdgeInputRole::Input);
    }

    #[test]
    fn validate_edge_typing_accepts_member_of_upstream_output_contract() {
        use crate::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata};

        let transform_id = StageId::new();
        let sink_id = StageId::new();

        let transform_inner: Box<dyn StageDescriptor> = Box::new(TransformDescriptor {
            name: "transform".to_string(),
            handler: ExactTransform,
            middleware: vec![],
        });
        let transform_metadata = StageTypingMetadata::transform(
            TypeHint::exact::<InputEvent>(),
            TypeHint::exact::<OutputEvent>(),
            false,
            None,
        )
        .with_output_contract(vec![
            TypeHint::exact::<OutputEvent>(),
            TypeHint::exact::<InputEvent>(),
        ]);

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "transform".to_string(),
            wrap_typed_descriptor(transform_inner, transform_metadata),
        );
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", InputEvent => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("transform".to_string(), transform_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            transform_id.to_topology_id(),
            Some("transform".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            sink_id.to_topology_id(),
            Some("sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(transform_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        validate_edge_typing(&topology, &descriptors, &name_to_id)
            .expect("downstream selected type is a member of upstream output contract");
    }

    #[test]
    fn output_set_member_order_does_not_affect_edge_subscription() {
        let source_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source".to_string(),
            crate::source!(
                name: "source",
                { InputEvent, AlternateEvent } => SyncExactSource
            ),
        );
        descriptors.insert(
            "alternate_sink".to_string(),
            crate::sink!(name: "alternate_sink", AlternateEvent => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);
        name_to_id.insert("alternate_sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_id.to_topology_id(),
            Some("source".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            sink_id.to_topology_id(),
            Some("alternate_sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(source_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        validate_edge_typing(&topology, &descriptors, &name_to_id)
            .expect("any output-set member, including a non-first member, is selectable");
    }

    #[test]
    fn validate_edge_typing_distinguishes_join_reference_and_stream_roles() {
        let reference_id = StageId::new();
        let stream_id = StageId::new();
        let join_id = StageId::new();

        let mut join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => placeholder!()
        );
        join.set_reference_stage_id(reference_id);

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "reference".to_string(),
            crate::source!(name: "reference", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "stream".to_string(),
            crate::source!(name: "stream", AlternateEvent => placeholder!()),
        );
        descriptors.insert("join".to_string(), join);

        let mut name_to_id = HashMap::new();
        name_to_id.insert("reference".to_string(), reference_id);
        name_to_id.insert("stream".to_string(), stream_id);
        name_to_id.insert("join".to_string(), join_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            reference_id.to_topology_id(),
            Some("reference".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            stream_id.to_topology_id(),
            Some("stream".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            join_id.to_topology_id(),
            Some("join".to_string()),
            TopologyStageType::Join,
        );
        topology.reset_current();
        topology.add_edge(reference_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(stream_id.to_topology_id(), join_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let mut errors = validate_edge_typing(&topology, &descriptors, &name_to_id)
            .expect_err("expected EdgeErrors on join reference + stream mismatches");
        errors.sort_by_key(|err| match err.input_role {
            EdgeInputRole::Reference => 0,
            EdgeInputRole::Stream => 1,
            EdgeInputRole::Input => 2,
        });

        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].input_role, EdgeInputRole::Reference);
        assert_eq!(errors[0].upstream_type, type_name::<InputEvent>());
        assert_eq!(errors[0].expected_type, type_name::<ReferenceEvent>());

        assert_eq!(errors[1].input_role, EdgeInputRole::Stream);
        assert_eq!(errors[1].upstream_type, type_name::<AlternateEvent>());
        assert_eq!(errors[1].expected_type, type_name::<StreamEvent>());
    }

    #[test]
    fn effectful_guard_rejects_transitive_nondeterministic_fan_in() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let merge_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "merge".to_string(),
            crate::transform!(name: "merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("merge".to_string(), merge_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_a_id.to_topology_id(),
            Some("source_a".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            source_b_id.to_topology_id(),
            Some("source_b".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            merge_id.to_topology_id(),
            Some("merge".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            effectful_id.to_topology_id(),
            Some("effectful".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_edge(source_a_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(merge_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let err =
            *validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .expect_err("effectful stage below transitive fan-in must be rejected");

        match err {
            FlowBuildError::EffectfulFanInRequiresDeterministicOrder { stage_name } => {
                assert_eq!(stage_name, "effectful");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    /// FLOWIP-095d: the accept branch of the effectful fan-in guard, exercised
    /// through the real capability rather than a test stub. The topology is
    /// identical to `effectful_guard_rejects_transitive_nondeterministic_fan_in`
    /// (that test is the negative control); here the enablement walk marks the
    /// merge node and the orderer override makes the guard accept, which is
    /// exactly what the `flow!` expansion does.
    #[test]
    fn effectful_guard_accepts_effectful_stage_below_deterministic_orderer() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let orderer_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "orderer".to_string(),
            crate::transform!(name: "orderer", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("orderer".to_string(), orderer_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_a_id.to_topology_id(),
            Some("source_a".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            source_b_id.to_topology_id(),
            Some("source_b".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            orderer_id.to_topology_id(),
            Some("orderer".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            effectful_id.to_topology_id(),
            Some("effectful".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_edge(source_a_id.to_topology_id(), orderer_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), orderer_id.to_topology_id());
        topology.add_edge(orderer_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        // The negative control first: without the enablement walk the guard
        // rejects this topology.
        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_err(),
            "without enablement the fan-in must still be rejected"
        );

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(
            marked.contains(&orderer_id),
            "the walk must mark the multi-inbound stage above the effectful stage"
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);
        assert!(descriptors["orderer"].is_deterministic_input_orderer());

        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_ok(),
            "effectful stage below the enabled orderer must be accepted"
        );
    }

    /// FLOWIP-095d: a hydrating join (reference consumed to authored EOF, then
    /// stream) is a structural deterministic orderer. The phase boundary pins
    /// reference-versus-stream order with no merge machinery.
    #[test]
    fn hydrating_join_descriptor_is_structural_deterministic_orderer() {
        let descriptor = crate::dsl::stage_descriptor::JoinDescriptor {
            name: "join".to_string(),
            reference_stage_id: StageId::new(),
            reference_stage_var: None,
            handler: ExactJoin,
            middleware: vec![],
        };
        assert!(descriptor.is_deterministic_input_orderer());
    }

    /// FLOWIP-095d: a live-mode join interleaves reference and stream
    /// concurrently, so it is not a structural orderer; it needs the canonical
    /// merge (runtime enablement) to order deterministically.
    #[test]
    fn live_join_descriptor_is_not_a_structural_orderer() {
        #[derive(Clone, Debug)]
        struct LiveModeJoin;

        #[async_trait]
        impl JoinHandler for LiveModeJoin {
            type State = ();

            fn initial_state(&self) -> Self::State {}

            fn reference_mode(&self) -> obzenflow_runtime::stages::join::JoinReferenceMode {
                obzenflow_runtime::stages::join::JoinReferenceMode::Live
            }

            fn process_event(
                &self,
                _state: &mut Self::State,
                _event: ChainEvent,
                _source_id: StageId,
                _writer_id: WriterId,
            ) -> Result<Vec<ChainEvent>, HandlerError> {
                Ok(vec![])
            }

            fn on_source_eof(
                &self,
                _state: &mut Self::State,
                _source_id: StageId,
                _writer_id: WriterId,
            ) -> Result<Vec<ChainEvent>, HandlerError> {
                Ok(vec![])
            }
        }

        let descriptor = crate::dsl::stage_descriptor::JoinDescriptor {
            name: "join".to_string(),
            reference_stage_id: StageId::new(),
            reference_stage_var: None,
            handler: LiveModeJoin,
            middleware: vec![],
        };
        assert!(!descriptor.is_deterministic_input_orderer());
    }

    /// FLOWIP-095l Gap 12: a join is never a Barrier in v1. The witness minter is
    /// `pub` so the `#[order_insensitive]` attribute can mint it cross-crate, which
    /// means a Live join could hand-mint a `OrderInsensitive` declaration. The
    /// descriptor neutralizes that unproven claim: a Live join always reports
    /// `Observer`, so it can never escape the merge without a real (StatefulHandler)
    /// trial proving it.
    #[test]
    fn live_join_claiming_order_insensitive_is_still_an_observer() {
        use obzenflow_runtime::stages::common::handlers::InputOrderSemantics;

        #[derive(Clone, Debug)]
        struct LiveJoinClaimingBarrier;

        #[async_trait]
        impl JoinHandler for LiveJoinClaimingBarrier {
            type State = ();

            fn initial_state(&self) -> Self::State {}

            fn reference_mode(&self) -> obzenflow_runtime::stages::join::JoinReferenceMode {
                obzenflow_runtime::stages::join::JoinReferenceMode::Live
            }

            fn declared_input_order(&self) -> InputOrderSemantics {
                // The unproven hand-mint that Gap 12 neutralizes at the descriptor.
                InputOrderSemantics::__order_insensitive_proven()
            }

            fn process_event(
                &self,
                _state: &mut Self::State,
                _event: ChainEvent,
                _source_id: StageId,
                _writer_id: WriterId,
            ) -> Result<Vec<ChainEvent>, HandlerError> {
                Ok(vec![])
            }

            fn on_source_eof(
                &self,
                _state: &mut Self::State,
                _source_id: StageId,
                _writer_id: WriterId,
            ) -> Result<Vec<ChainEvent>, HandlerError> {
                Ok(vec![])
            }
        }

        let descriptor = crate::dsl::stage_descriptor::JoinDescriptor {
            name: "join".to_string(),
            reference_stage_id: StageId::new(),
            reference_stage_var: None,
            handler: LiveJoinClaimingBarrier,
            middleware: vec![],
        };
        assert_eq!(
            descriptor.order_role(),
            crate::dsl::stage_descriptor::OrderRole::Observer
        );
    }

    /// FLOWIP-095d guard hardening: an orderer declaration must not mask a
    /// nondeterministic fan-in above it. The stability induction needs both
    /// halves (the stage orders its inputs AND every input stream is itself
    /// deterministic), so a declared orderer whose upstream fan-in was never
    /// marked is rejected; marking the upstream too (what the flow! enablement
    /// walk does) makes the same topology acceptable.
    #[test]
    fn effectful_guard_rejects_orderer_with_nondeterministic_upstream_fan_in() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let source_c_id = StageId::new();
        let upper_merge_id = StageId::new();
        let orderer_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_c".to_string(),
            crate::source!(name: "source_c", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "upper_merge".to_string(),
            crate::transform!(name: "upper_merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "orderer".to_string(),
            crate::transform!(name: "orderer", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("source_c".to_string(), source_c_id);
        name_to_id.insert("upper_merge".to_string(), upper_merge_id);
        name_to_id.insert("orderer".to_string(), orderer_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (source_c_id, "source_c", TopologyStageType::FiniteSource),
            (upper_merge_id, "upper_merge", TopologyStageType::Transform),
            (orderer_id, "orderer", TopologyStageType::Transform),
            (effectful_id, "effectful", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(
            source_a_id.to_topology_id(),
            upper_merge_id.to_topology_id(),
        );
        topology.add_edge(
            source_b_id.to_topology_id(),
            upper_merge_id.to_topology_id(),
        );
        topology.add_edge(upper_merge_id.to_topology_id(), orderer_id.to_topology_id());
        topology.add_edge(source_c_id.to_topology_id(), orderer_id.to_topology_id());
        topology.add_edge(orderer_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        // Mark only the lower fan-in, simulating a hand-declared orderer (or
        // an externally constructed flow that skipped the enablement walk).
        let partial_mark: std::collections::HashSet<StageId> =
            std::iter::once(orderer_id).collect();
        crate::dsl::typing::wrap_deterministic_orderers(
            &mut descriptors,
            &name_to_id,
            &partial_mark,
        );
        assert!(descriptors["orderer"].is_deterministic_input_orderer());
        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_err(),
            "a declared orderer must not mask the unordered fan-in above it"
        );

        // The full enablement walk marks the upper fan-in too, which is what
        // makes the topology sound.
        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(
            marked.contains(&upper_merge_id),
            "the walk must mark the upper fan-in above the orderer"
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);
        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_ok(),
            "with both fan-ins ordered the effectful stage must be accepted"
        );
    }

    /// FLOWIP-095d guard hardening: a cycle feeding an ordered fan-in from
    /// above is rejected even after the enablement walk runs. The walk never
    /// marks cycle members, so the cycle's merge order stays timing-dependent
    /// and the ordered fan-in below it cannot produce a stable order.
    #[test]
    fn effectful_guard_rejects_cycle_above_ordered_fan_in() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let cycle_entry_id = StageId::new();
        let cycle_back_id = StageId::new();
        let merge_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "cycle_entry".to_string(),
            crate::transform!(name: "cycle_entry", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "cycle_back".to_string(),
            crate::transform!(name: "cycle_back", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "merge".to_string(),
            crate::transform!(name: "merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("cycle_entry".to_string(), cycle_entry_id);
        name_to_id.insert("cycle_back".to_string(), cycle_back_id);
        name_to_id.insert("merge".to_string(), merge_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (cycle_entry_id, "cycle_entry", TopologyStageType::Transform),
            (cycle_back_id, "cycle_back", TopologyStageType::Transform),
            (merge_id, "merge", TopologyStageType::Transform),
            (effectful_id, "effectful", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(
            source_a_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_entry_id.to_topology_id(),
            cycle_back_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_back_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(cycle_back_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(merge_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        // The full macro-path enablement: the walk marks the fan-in below the
        // cycle (it is not a cycle member) and wrapping makes it an orderer.
        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(marked.contains(&merge_id), "the walk must mark the fan-in");
        assert!(
            !marked.contains(&cycle_entry_id),
            "cycle members must never be marked"
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);

        let err =
            *validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .expect_err("an ordered fan-in fed from a cycle must be rejected");
        match err {
            FlowBuildError::EffectfulFanInRequiresDeterministicOrder { stage_name } => {
                assert_eq!(stage_name, "effectful");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    /// FLOWIP-095d guard hardening: the same cycle-above-orderer shape with a
    /// hydrating join as the structural orderer. Structural and wrapped
    /// orderers must get the same upstream scrutiny.
    #[test]
    fn effectful_guard_rejects_cycle_above_hydrating_join() {
        let reference_id = StageId::new();
        let source_s_id = StageId::new();
        let cycle_entry_id = StageId::new();
        let cycle_back_id = StageId::new();
        let join_id = StageId::new();
        let effectful_id = StageId::new();

        let mut join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => placeholder!()
        );
        join.set_reference_stage_id(reference_id);
        assert!(
            join.is_deterministic_input_orderer(),
            "premise: a hydrating join is a structural orderer"
        );

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "reference".to_string(),
            crate::source!(name: "reference", ReferenceEvent => placeholder!()),
        );
        descriptors.insert(
            "source_s".to_string(),
            crate::source!(name: "source_s", StreamEvent => placeholder!()),
        );
        descriptors.insert(
            "cycle_entry".to_string(),
            crate::transform!(name: "cycle_entry", StreamEvent -> StreamEvent => placeholder!()),
        );
        descriptors.insert(
            "cycle_back".to_string(),
            crate::transform!(name: "cycle_back", StreamEvent -> StreamEvent => placeholder!()),
        );
        descriptors.insert("join".to_string(), join);
        // Edge types are irrelevant to this validator; reuse the effectful
        // fixture handler rather than minting a JoinedEvent-typed one.
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("reference".to_string(), reference_id);
        name_to_id.insert("source_s".to_string(), source_s_id);
        name_to_id.insert("cycle_entry".to_string(), cycle_entry_id);
        name_to_id.insert("cycle_back".to_string(), cycle_back_id);
        name_to_id.insert("join".to_string(), join_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (reference_id, "reference", TopologyStageType::FiniteSource),
            (source_s_id, "source_s", TopologyStageType::FiniteSource),
            (cycle_entry_id, "cycle_entry", TopologyStageType::Transform),
            (cycle_back_id, "cycle_back", TopologyStageType::Transform),
            (join_id, "join", TopologyStageType::Join),
            (effectful_id, "effectful", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(
            source_s_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_entry_id.to_topology_id(),
            cycle_back_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_back_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(reference_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(cycle_back_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(join_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);

        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_err(),
            "a hydrating join fed from a cycle must not anchor an effectful stage"
        );
    }

    /// FLOWIP-095d guard hardening regression: nested ordered fan-ins stay
    /// accepted. The recursion through an orderer's inputs must pass when
    /// every fan-in above is itself marked and the chains bottom out at
    /// sources.
    #[test]
    fn effectful_guard_accepts_nested_ordered_fan_ins() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let source_c_id = StageId::new();
        let merge_one_id = StageId::new();
        let merge_two_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_c".to_string(),
            crate::source!(name: "source_c", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "merge_one".to_string(),
            crate::transform!(name: "merge_one", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "merge_two".to_string(),
            crate::transform!(name: "merge_two", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("source_c".to_string(), source_c_id);
        name_to_id.insert("merge_one".to_string(), merge_one_id);
        name_to_id.insert("merge_two".to_string(), merge_two_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (source_c_id, "source_c", TopologyStageType::FiniteSource),
            (merge_one_id, "merge_one", TopologyStageType::Transform),
            (merge_two_id, "merge_two", TopologyStageType::Transform),
            (effectful_id, "effectful", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(source_a_id.to_topology_id(), merge_one_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), merge_one_id.to_topology_id());
        topology.add_edge(merge_one_id.to_topology_id(), merge_two_id.to_topology_id());
        topology.add_edge(source_c_id.to_topology_id(), merge_two_id.to_topology_id());
        topology.add_edge(merge_two_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(marked.contains(&merge_one_id) && marked.contains(&merge_two_id));
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);

        validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
            .expect("nested ordered fan-ins above an effectful stage must be accepted");
    }

    /// FLOWIP-120a: the single-upstream reset. An orderer followed by an ordinary
    /// passthrough transform still yields a deterministic input order at an effectful
    /// stage two hops below the fan-in, exercising the recursion that propagates the
    /// orderer's reset through an intervening single-upstream node.
    #[test]
    fn effectful_guard_accepts_effectful_stage_two_hops_below_orderer() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let orderer_id = StageId::new();
        let passthrough_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "orderer".to_string(),
            crate::transform!(name: "orderer", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "passthrough".to_string(),
            crate::transform!(name: "passthrough", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("orderer".to_string(), orderer_id);
        name_to_id.insert("passthrough".to_string(), passthrough_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_a_id.to_topology_id(),
            Some("source_a".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            source_b_id.to_topology_id(),
            Some("source_b".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            orderer_id.to_topology_id(),
            Some("orderer".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            passthrough_id.to_topology_id(),
            Some("passthrough".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            effectful_id.to_topology_id(),
            Some("effectful".to_string()),
            TopologyStageType::Transform,
        );
        topology.reset_current();
        topology.add_edge(source_a_id.to_topology_id(), orderer_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), orderer_id.to_topology_id());
        topology.add_edge(orderer_id.to_topology_id(), passthrough_id.to_topology_id());
        topology.add_edge(
            passthrough_id.to_topology_id(),
            effectful_id.to_topology_id(),
        );
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(marked.contains(&orderer_id));
        assert!(
            !marked.contains(&passthrough_id),
            "single-inbound stages are never marked"
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);

        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_ok(),
            "effectful stage two hops below the enabled orderer must be accepted"
        );
    }

    /// FLOWIP-095d: the enablement walk marks fan-ins transitively (every
    /// fan-in above an ordered stage must itself be ordered, because the guard
    /// stops at the first orderer), skips fan-ins with no effectful
    /// descendant, and marks an effectful stage that is itself multi-inbound.
    #[test]
    fn enablement_walk_marks_transitively_and_skips_unrelated_fan_ins() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let source_c_id = StageId::new();
        let upper_merge_id = StageId::new();
        let lower_merge_id = StageId::new();
        let effectful_id = StageId::new();
        let plain_merge_id = StageId::new();
        let plain_sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_c".to_string(),
            crate::source!(name: "source_c", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "upper_merge".to_string(),
            crate::transform!(name: "upper_merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "lower_merge".to_string(),
            crate::transform!(name: "lower_merge", InputEvent -> OutputEvent => ExactTransform),
        );
        // The effectful stage is itself fed by two upstreams.
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );
        // A fan-in with only a sink below it: never marked.
        descriptors.insert(
            "plain_merge".to_string(),
            crate::transform!(name: "plain_merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "plain_sink".to_string(),
            crate::sink!(name: "plain_sink", OutputEvent => ExactSink),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("source_c".to_string(), source_c_id);
        name_to_id.insert("upper_merge".to_string(), upper_merge_id);
        name_to_id.insert("lower_merge".to_string(), lower_merge_id);
        name_to_id.insert("effectful".to_string(), effectful_id);
        name_to_id.insert("plain_merge".to_string(), plain_merge_id);
        name_to_id.insert("plain_sink".to_string(), plain_sink_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, stage_type) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (source_c_id, "source_c", TopologyStageType::FiniteSource),
            (upper_merge_id, "upper_merge", TopologyStageType::Transform),
            (lower_merge_id, "lower_merge", TopologyStageType::Transform),
            (effectful_id, "effectful", TopologyStageType::Transform),
            (plain_merge_id, "plain_merge", TopologyStageType::Transform),
            (plain_sink_id, "plain_sink", TopologyStageType::Sink),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), stage_type);
            topology.reset_current();
        }
        // upper_merge: fan-in of a+b. lower_merge: fan-in of upper_merge+c.
        // effectful: fan-in of lower_merge+c (itself multi-inbound).
        topology.add_edge(
            source_a_id.to_topology_id(),
            upper_merge_id.to_topology_id(),
        );
        topology.add_edge(
            source_b_id.to_topology_id(),
            upper_merge_id.to_topology_id(),
        );
        topology.add_edge(
            upper_merge_id.to_topology_id(),
            lower_merge_id.to_topology_id(),
        );
        topology.add_edge(
            source_c_id.to_topology_id(),
            lower_merge_id.to_topology_id(),
        );
        topology.add_edge(
            lower_merge_id.to_topology_id(),
            effectful_id.to_topology_id(),
        );
        topology.add_edge(source_c_id.to_topology_id(), effectful_id.to_topology_id());
        // plain_merge: fan-in of a+b with only a sink downstream.
        topology.add_edge(
            source_a_id.to_topology_id(),
            plain_merge_id.to_topology_id(),
        );
        topology.add_edge(
            source_b_id.to_topology_id(),
            plain_merge_id.to_topology_id(),
        );
        topology.add_edge(
            plain_merge_id.to_topology_id(),
            plain_sink_id.to_topology_id(),
        );
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );

        assert!(marked.contains(&effectful_id), "multi-inbound effectful");
        assert!(marked.contains(&lower_merge_id), "fan-in above effectful");
        assert!(
            marked.contains(&upper_merge_id),
            "transitive fan-in above fan-in"
        );
        assert!(
            !marked.contains(&plain_merge_id),
            "fan-in with no effectful descendant stays unordered"
        );
        assert!(!marked.contains(&source_a_id));

        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);
        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_ok()
        );
        assert!(
            !descriptors["plain_merge"].is_deterministic_input_orderer(),
            "unmarked descriptors stay unwrapped"
        );
    }

    /// FLOWIP-095d: cycle members are never marked, so the guard keeps
    /// rejecting an effectful stage below a cycle even with the enablement
    /// walk in front of it.
    #[test]
    fn enablement_walk_skips_cycle_members_and_guard_still_rejects() {
        let source_id = StageId::new();
        let cycle_a_id = StageId::new();
        let cycle_b_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source".to_string(),
            crate::source!(name: "source", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "cycle_a".to_string(),
            crate::transform!(name: "cycle_a", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "cycle_b".to_string(),
            crate::transform!(name: "cycle_b", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);
        name_to_id.insert("cycle_a".to_string(), cycle_a_id);
        name_to_id.insert("cycle_b".to_string(), cycle_b_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, stage_type) in [
            (source_id, "source", TopologyStageType::FiniteSource),
            (cycle_a_id, "cycle_a", TopologyStageType::Transform),
            (cycle_b_id, "cycle_b", TopologyStageType::Transform),
            (effectful_id, "effectful", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), stage_type);
            topology.reset_current();
        }
        // cycle_a is multi-inbound (source + cycle_b) and in a cycle with
        // cycle_b; the effectful stage hangs below the cycle.
        topology.add_edge(source_id.to_topology_id(), cycle_a_id.to_topology_id());
        topology.add_edge(cycle_a_id.to_topology_id(), cycle_b_id.to_topology_id());
        topology.add_edge(cycle_b_id.to_topology_id(), cycle_a_id.to_topology_id());
        topology.add_edge(cycle_b_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(
            !marked.contains(&cycle_a_id),
            "cycle members are never marked"
        );

        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);
        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_err(),
            "the guard stays the safety net for effectful stages below cycles"
        );
    }

    /// FLOWIP-095d: the orderer override must delegate typing metadata, or
    /// feed-plan derivation silently degrades behind the wrapper.
    #[test]
    fn orderer_override_preserves_typing_metadata_and_feed_plan() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let merge_id = StageId::new();
        let effectful_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "merge".to_string(),
            crate::transform!(name: "merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "effectful".to_string(),
            crate::effectful_transform!(
                name: "effectful",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("merge".to_string(), merge_id);
        name_to_id.insert("effectful".to_string(), effectful_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, stage_type) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (merge_id, "merge", TopologyStageType::Transform),
            (effectful_id, "effectful", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), stage_type);
            topology.reset_current();
        }
        topology.add_edge(source_a_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(merge_id.to_topology_id(), effectful_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let before = crate::dsl::typing::derive_feed_plan(&topology, &descriptors, &name_to_id);

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);

        assert!(
            descriptors["merge"].typing_metadata().is_some(),
            "the override must delegate typing metadata"
        );
        let after = crate::dsl::typing::derive_feed_plan(&topology, &descriptors, &name_to_id);
        for stage_id in [merge_id, effectful_id] {
            assert_eq!(
                format!("{:?}", before.input_feeds(stage_id)),
                format!("{:?}", after.input_feeds(stage_id)),
                "input feeds must be unchanged behind the override"
            );
            assert_eq!(
                format!("{:?}", before.output_contract(stage_id)),
                format!("{:?}", after.output_contract(stage_id)),
                "output contracts must be unchanged behind the override"
            );
        }
    }

    // ─── FLOWIP-114c PR D closing-pass regression tests ────────────────────

    /// A descriptor that returns no typing metadata (the legacy untyped path)
    /// must be rejected by validate_stage_typing_metadata with
    /// FlowBuildError::StageMissingTypingMetadata.
    #[test]
    fn validate_stage_typing_metadata_rejects_descriptor_with_none_metadata() {
        let descriptor: Box<dyn StageDescriptor> = Box::new(TransformDescriptor {
            name: "untyped_transform".to_string(),
            handler: ExactTransform,
            middleware: vec![],
        });
        // Sanity: the bare TransformDescriptor returns None.
        assert!(descriptor.typing_metadata().is_none());

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert("transform".to_string(), descriptor);

        let err = validate_stage_typing_metadata(&descriptors).expect_err(
            "expected StageMissingTypingMetadata for descriptor without typing metadata",
        );
        match err {
            FlowBuildError::StageMissingTypingMetadata { stage_name } => {
                assert_eq!(stage_name, "transform");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    /// A typed flow whose author somehow passed Unspecified into an applicable
    /// slot (here: a transform's input) must be rejected.
    #[test]
    fn validate_stage_typing_metadata_rejects_unspecified_on_applicable_slot() {
        // Use the typed transform!() macro to build a valid descriptor, then
        // wrap it with synthetic metadata that has Unspecified input.
        use crate::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata};

        let inner: Box<dyn StageDescriptor> = Box::new(TransformDescriptor {
            name: "synthetic".to_string(),
            handler: ExactTransform,
            middleware: vec![],
        });
        let bad_metadata = StageTypingMetadata::transform(
            TypeHint::Unspecified,
            TypeHint::exact::<OutputEvent>(),
            false,
            None,
        );
        let wrapped = wrap_typed_descriptor(inner, bad_metadata);

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert("transform".to_string(), wrapped);

        let err = validate_stage_typing_metadata(&descriptors).expect_err(
            "expected UnspecifiedTypingOnApplicableSlot for transform with Unspecified input",
        );
        match err {
            FlowBuildError::UnspecifiedTypingOnApplicableSlot { stage_name, slot } => {
                assert_eq!(stage_name, "transform");
                assert_eq!(slot, "input");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn validate_stage_typing_metadata_rejects_empty_output_contract_for_output_stage() {
        use crate::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata};

        let inner: Box<dyn StageDescriptor> = Box::new(TransformDescriptor {
            name: "synthetic".to_string(),
            handler: ExactTransform,
            middleware: vec![],
        });
        let bad_metadata = StageTypingMetadata::transform(
            TypeHint::exact::<InputEvent>(),
            TypeHint::exact::<OutputEvent>(),
            false,
            None,
        )
        .with_output_contract(Vec::new());
        let wrapped = wrap_typed_descriptor(inner, bad_metadata);

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert("transform".to_string(), wrapped);

        let err = validate_stage_typing_metadata(&descriptors).expect_err(
            "expected UnspecifiedTypingOnApplicableSlot for transform with empty output contract",
        );
        match err {
            FlowBuildError::UnspecifiedTypingOnApplicableSlot { stage_name, slot } => {
                assert_eq!(stage_name, "transform");
                assert_eq!(slot, "output");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    /// Three upstreams of three different Exact types feeding the same
    /// non-join downstream slot must produce a HeterogeneousFanIn error
    /// listing the other conflicting upstreams in `other_upstream_stages`.
    #[test]
    fn validate_edge_typing_emits_heterogeneous_fan_in_for_non_join_slot() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let source_c_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", AlternateEvent => placeholder!()),
        );
        descriptors.insert(
            "source_c".to_string(),
            crate::source!(name: "source_c", StreamEvent => placeholder!()),
        );
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", InputEvent => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("source_c".to_string(), source_c_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_a_id.to_topology_id(),
            Some("source_a".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            source_b_id.to_topology_id(),
            Some("source_b".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            source_c_id.to_topology_id(),
            Some("source_c".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            sink_id.to_topology_id(),
            Some("sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(source_a_id.to_topology_id(), sink_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), sink_id.to_topology_id());
        topology.add_edge(source_c_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let errors = validate_edge_typing(&topology, &descriptors, &name_to_id)
            .expect_err("expected EdgeErrors for heterogeneous fan-in into a non-join sink");

        // Find the HeterogeneousFanIn error (there may also be SingleEdge
        // errors for the per-edge mismatches against InputEvent).
        let het = errors
            .iter()
            .find(|e| matches!(e.kind, EdgeTypingMismatchKind::HeterogeneousFanIn { .. }))
            .expect("expected at least one HeterogeneousFanIn error");
        assert_eq!(het.downstream_stage, "sink");
        assert_eq!(het.input_role, EdgeInputRole::Input);
        if let EdgeTypingMismatchKind::HeterogeneousFanIn {
            other_upstream_stages,
            other_actual_types,
        } = &het.kind
        {
            // The focal upstream is one of source_a/b/c; the other two appear
            // in other_upstream_stages.
            assert_eq!(other_upstream_stages.len(), 2);
            assert_eq!(other_actual_types.len(), 2);
            let mut all = vec![het.upstream_stage.clone()];
            all.extend_from_slice(other_upstream_stages);
            all.sort();
            assert_eq!(all, vec!["source_a", "source_b", "source_c"]);
        } else {
            unreachable!();
        }
    }

    /// Two stages declared with the same Rust type but spelled two different
    /// ways (short name vs path-qualified) must compare as equal under the
    /// TypeId-based identity. This is the FLOWIP-114c canonical-identity
    /// regression test that 105j Background flagged.
    #[test]
    fn validate_edge_typing_treats_type_id_equivalent_spellings_as_match() {
        // Bind a path-qualified alias to the same underlying type.
        type AliasedInput = crate::dsl::tests::typed_stage_contracts_test::tests::InputEvent;

        let source_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        // Source declares the short-named `InputEvent`.
        descriptors.insert(
            "source".to_string(),
            crate::source!(name: "source", InputEvent => placeholder!()),
        );
        // Sink declares the path-qualified alias of the same type.
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", AliasedInput => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_id.to_topology_id(),
            Some("source".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            sink_id.to_topology_id(),
            Some("sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(source_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        // Despite the two different spellings, the TypeId is identical, so
        // validate_edge_typing must report no mismatch.
        validate_edge_typing(&topology, &descriptors, &name_to_id)
            .expect("identical TypeId from two spellings must validate clean");
    }

    /// FLOWIP-114c PR E: documenting the shared-envelope-as-the-new-`mixed`
    /// failure mode.
    ///
    /// If two semantically different event shapes are both spelled as one
    /// envelope in DSL slots, the validator cannot tell them apart because
    /// the type and event descriptor are one constant across the whole
    /// compilation. The flow build succeeds even though the upstream is,
    /// semantically, emitting two different things into the downstream slot.
    ///
    /// The correct pattern is per-branch alignment with concrete
    /// `TypedPayload` structs that normalise to one common envelope
    /// type, demonstrated in `examples/multi_source_ingest_demo/`.
    #[test]
    fn validate_edge_typing_accepts_shared_envelope_silently_documenting_anti_pattern() {
        // Two distinct semantic events the test author "should" model as
        // separate types (e.g. an order and a charge), but they both get
        // spelled as one envelope in DSL slots. The validator sees a single
        // TypeId and event descriptor on every edge.
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "order_source".to_string(),
            crate::source!(name: "order_source", ValueEnvelope => placeholder!()),
        );
        descriptors.insert(
            "charge_source".to_string(),
            crate::source!(name: "charge_source", ValueEnvelope => placeholder!()),
        );
        descriptors.insert(
            "consolidator".to_string(),
            crate::sink!(name: "consolidator", ValueEnvelope => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("order_source".to_string(), source_a_id);
        name_to_id.insert("charge_source".to_string(), source_b_id);
        name_to_id.insert("consolidator".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_a_id.to_topology_id(),
            Some("order_source".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            source_b_id.to_topology_id(),
            Some("charge_source".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            sink_id.to_topology_id(),
            Some("consolidator".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(source_a_id.to_topology_id(), sink_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        // The validator accepts this. That is the failure mode this test
        // exists to document: the type system is satisfied because every
        // slot's TypeId is the same, but the topology is meaningless.
        validate_edge_typing(&topology, &descriptors, &name_to_id).expect(
            "one shared envelope collapses every payload into one TypeId and event descriptor, \
             so the validator accepts this flow even though the two sources semantically emit \
             different events. This is the wrong answer. See examples/multi_source_ingest_demo/ \
             for the correct per-branch alignment pattern.",
        );
    }

    /// FLOWIP-114c Acceptance #4 + #25, passing case: a join whose stream slot
    /// has two upstreams of the same `Exact` type must build successfully. The
    /// reference-leg upstream also matches the declared reference type. The
    /// homogeneous-fan-in invariant applies independently per join leg.
    #[test]
    fn validate_edge_typing_accepts_homogeneous_fan_in_on_join_stream_leg() {
        let reference_id = StageId::new();
        let stream_a_id = StageId::new();
        let stream_b_id = StageId::new();
        let join_id = StageId::new();

        let mut join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => placeholder!()
        );
        join.set_reference_stage_id(reference_id);

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "reference".to_string(),
            crate::source!(name: "reference", ReferenceEvent => placeholder!()),
        );
        descriptors.insert(
            "stream_a".to_string(),
            crate::source!(name: "stream_a", StreamEvent => placeholder!()),
        );
        descriptors.insert(
            "stream_b".to_string(),
            crate::source!(name: "stream_b", StreamEvent => placeholder!()),
        );
        descriptors.insert("join".to_string(), join);

        let mut name_to_id = HashMap::new();
        name_to_id.insert("reference".to_string(), reference_id);
        name_to_id.insert("stream_a".to_string(), stream_a_id);
        name_to_id.insert("stream_b".to_string(), stream_b_id);
        name_to_id.insert("join".to_string(), join_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (reference_id, "reference", TopologyStageType::FiniteSource),
            (stream_a_id, "stream_a", TopologyStageType::FiniteSource),
            (stream_b_id, "stream_b", TopologyStageType::FiniteSource),
            (join_id, "join", TopologyStageType::Join),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(reference_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(stream_a_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(stream_b_id.to_topology_id(), join_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        validate_edge_typing(&topology, &descriptors, &name_to_id).expect(
            "homogeneous fan-in on a join stream leg with matching reference leg must \
             validate cleanly under FLOWIP-114c",
        );
    }

    /// FLOWIP-120b: the runtime feed plan must preserve the same logical edge
    /// grain as output contracts and metrics. A join gives a compact regression
    /// case because two feeds enter one downstream stage but select different
    /// downstream slots and payload types.
    #[test]
    fn derive_feed_plan_keys_join_feeds_by_selected_type_and_role() {
        let reference_id = StageId::new();
        let stream_id = StageId::new();
        let join_id = StageId::new();

        let mut join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => placeholder!()
        );
        join.set_reference_stage_id(reference_id);

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "reference".to_string(),
            crate::source!(name: "reference", ReferenceEvent => placeholder!()),
        );
        descriptors.insert(
            "stream".to_string(),
            crate::source!(name: "stream", StreamEvent => placeholder!()),
        );
        descriptors.insert("join".to_string(), join);

        let mut name_to_id = HashMap::new();
        name_to_id.insert("reference".to_string(), reference_id);
        name_to_id.insert("stream".to_string(), stream_id);
        name_to_id.insert("join".to_string(), join_id);

        let mut topology = TopologyBuilder::new();
        for (stage_id, name, stage_type) in [
            (reference_id, "reference", TopologyStageType::FiniteSource),
            (stream_id, "stream", TopologyStageType::FiniteSource),
            (join_id, "join", TopologyStageType::Join),
        ] {
            topology.add_stage_with_id(
                stage_id.to_topology_id(),
                Some(name.to_string()),
                stage_type,
            );
            topology.reset_current();
        }
        topology.add_edge(reference_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(stream_id.to_topology_id(), join_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let plan = derive_feed_plan(&topology, &descriptors, &name_to_id);
        assert_eq!(plan.all_feeds().len(), 2);

        let reference_type = TypeHintInfo::exact(type_name::<ReferenceEvent>());
        let reference_key = ReferenceEvent::versioned_event_type();
        let reference_feed = plan
            .all_feeds()
            .iter()
            .find(|feed| feed.key.role == FeedRole::Reference)
            .expect("reference feed");
        assert_eq!(reference_feed.key.upstream_stage, reference_id);
        assert_eq!(reference_feed.key.downstream_stage, join_id);
        assert_eq!(reference_feed.key.selected_payload_key, reference_key);
        assert_eq!(reference_feed.selected_payload.type_hint, reference_type);

        let stream_type = TypeHintInfo::exact(type_name::<StreamEvent>());
        let stream_key = StreamEvent::versioned_event_type();
        let stream_feed = plan
            .all_feeds()
            .iter()
            .find(|feed| feed.key.role == FeedRole::Stream)
            .expect("stream feed");
        assert_eq!(stream_feed.key.upstream_stage, stream_id);
        assert_eq!(stream_feed.key.downstream_stage, join_id);
        assert_eq!(stream_feed.key.selected_payload_key, stream_key);
        assert_eq!(stream_feed.selected_payload.type_hint, stream_type);

        let join_type = TypeHintInfo::exact(type_name::<JoinedEvent>());
        let join_output_key = JoinedEvent::versioned_event_type();
        let join_output = plan
            .output_contract(join_id)
            .and_then(|contract| contract.output_by_key(&join_output_key))
            .expect("join output contract member");
        assert_eq!(join_output.type_hint, join_type);
        assert_eq!(join_output.visibility, FactVisibility::Unrouted);
    }

    #[test]
    fn derive_feed_plan_selects_non_first_output_set_member() {
        let source_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source".to_string(),
            crate::source!(
                name: "source",
                { InputEvent, AlternateEvent } => SyncExactSource
            ),
        );
        descriptors.insert(
            "alternate_sink".to_string(),
            crate::sink!(name: "alternate_sink", AlternateEvent => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);
        name_to_id.insert("alternate_sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_id.to_topology_id(),
            Some("source".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            sink_id.to_topology_id(),
            Some("alternate_sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(source_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let plan = derive_feed_plan(&topology, &descriptors, &name_to_id);
        assert_eq!(plan.all_feeds().len(), 1);

        let alternate_key = AlternateEvent::versioned_event_type();
        let feed = &plan.all_feeds()[0];
        assert_eq!(feed.key.upstream_stage, source_id);
        assert_eq!(feed.key.downstream_stage, sink_id);
        assert_eq!(feed.key.selected_payload_key, alternate_key);
        assert_eq!(
            feed.selected_payload.type_hint,
            TypeHintInfo::exact(type_name::<AlternateEvent>())
        );

        let contract = plan
            .output_contract(source_id)
            .expect("source output contract");
        assert!(contract
            .output_by_key(&InputEvent::versioned_event_type())
            .is_some());
        assert_eq!(
            contract
                .output_by_key(&alternate_key)
                .expect("alternate output contract member")
                .visibility,
            FactVisibility::Routable
        );
    }

    /// FLOWIP-114c Acceptance #4 + #25, failing case: a join whose stream slot
    /// has two upstreams of differing `Exact` types must produce a
    /// HeterogeneousFanIn error with `input_role: EdgeInputRole::Stream`,
    /// naming both upstreams and both types. The dedupe rule on
    /// `validate_edge_typing` surfaces the architecturally-meaningful kind
    /// rather than a SingleEdge mismatch from the one upstream that disagrees
    /// with the declared stream type.
    #[test]
    fn validate_edge_typing_rejects_heterogeneous_fan_in_on_join_stream_leg() {
        let reference_id = StageId::new();
        let stream_a_id = StageId::new();
        let stream_b_id = StageId::new();
        let join_id = StageId::new();

        let mut join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => placeholder!()
        );
        join.set_reference_stage_id(reference_id);

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "reference".to_string(),
            crate::source!(name: "reference", ReferenceEvent => placeholder!()),
        );
        descriptors.insert(
            "stream_a".to_string(),
            crate::source!(name: "stream_a", StreamEvent => placeholder!()),
        );
        descriptors.insert(
            "stream_b".to_string(),
            crate::source!(name: "stream_b", AlternateEvent => placeholder!()),
        );
        descriptors.insert("join".to_string(), join);

        let mut name_to_id = HashMap::new();
        name_to_id.insert("reference".to_string(), reference_id);
        name_to_id.insert("stream_a".to_string(), stream_a_id);
        name_to_id.insert("stream_b".to_string(), stream_b_id);
        name_to_id.insert("join".to_string(), join_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (reference_id, "reference", TopologyStageType::FiniteSource),
            (stream_a_id, "stream_a", TopologyStageType::FiniteSource),
            (stream_b_id, "stream_b", TopologyStageType::FiniteSource),
            (join_id, "join", TopologyStageType::Join),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(reference_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(stream_a_id.to_topology_id(), join_id.to_topology_id());
        topology.add_edge(stream_b_id.to_topology_id(), join_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let errors = validate_edge_typing(&topology, &descriptors, &name_to_id).expect_err(
            "expected a HeterogeneousFanIn error on the join stream leg with differing types",
        );

        // After the validator's dedupe rule the HeterogeneousFanIn is the only
        // error reported for the (join, Stream) slot. No SingleEdge survives,
        // and the reference leg matched so no error fires there.
        assert_eq!(
            errors.len(),
            1,
            "expected exactly one EdgeError after dedupe, got {errors:?}",
        );
        let het = &errors[0];
        assert_eq!(het.downstream_stage, "join");
        assert_eq!(het.input_role, EdgeInputRole::Stream);

        // The focal upstream is the alphabetically-first by sort, so it is
        // `stream_a` with `StreamEvent` (which matches the declared stream
        // type). The other upstream is `stream_b` with `AlternateEvent`.
        assert_eq!(het.upstream_stage, "stream_a");
        assert_eq!(het.upstream_type, type_name::<StreamEvent>());

        match &het.kind {
            EdgeTypingMismatchKind::HeterogeneousFanIn {
                other_upstream_stages,
                other_actual_types,
            } => {
                assert_eq!(other_upstream_stages, &vec!["stream_b".to_string()]);
                assert_eq!(
                    other_actual_types,
                    &vec![type_name::<AlternateEvent>().to_string()]
                );
            }
            other => panic!("expected HeterogeneousFanIn kind, got {other:?}"),
        }

        // Confirm the rendered `FlowBuildError::EdgeTypingMismatch` message
        // includes both upstreams and both types under the
        // Heterogeneous-branch Display path. The dsl.rs surfacing code wraps
        // this same error so the failure the author sees is the same string
        // shape.
        let rendered = FlowBuildError::fmt_edge_typing_mismatch(
            &het.upstream_stage,
            &het.downstream_stage,
            het.input_role,
            &het.upstream_type,
            &het.expected_type,
            &het.kind,
            "Insert per-branch alignment transforms.",
        );
        assert!(
            rendered.contains("Heterogeneous fan-in"),
            "expected Heterogeneous-branch render, got: {rendered}"
        );
        assert!(rendered.contains("'stream_a'"));
        assert!(rendered.contains("'stream_b'"));
        assert!(rendered.contains(type_name::<StreamEvent>()));
        assert!(rendered.contains(type_name::<AlternateEvent>()));
    }

    #[test]
    fn order_observer_stateful_marks_its_fan_in() {
        // FLOWIP-095l: a non-effectful stateful fold is an order Observer, so the
        // two-source fan-in feeding it is marked for the canonical merge. Before
        // 095l only effects seeded the walk, leaving this fan-in unordered and its
        // reconstructed state non-reproducible.
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let fold_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "fold".to_string(),
            crate::stateful!(name: "fold", InputEvent -> OutputEvent => ExactStateful),
        );
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", OutputEvent => ExactSink),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("fold".to_string(), fold_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, stage_type) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (fold_id, "fold", TopologyStageType::Transform),
            (sink_id, "sink", TopologyStageType::Sink),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), stage_type);
            topology.reset_current();
        }
        topology.add_edge(source_a_id.to_topology_id(), fold_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), fold_id.to_topology_id());
        topology.add_edge(fold_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(
            marked.contains(&fold_id),
            "the two-source fan-in feeding a stateful observer must be marked"
        );

        // Once the orderer is wired, the guard accepts the fold's input order.
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);
        validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
            .expect("a stateful fold below a marked fan-in has deterministic input order");
    }

    /// FLOWIP-095l Tier 1 (build-time, barrier case): a commutative fold declared
    /// `#[order_insensitive]` is a Barrier, so the two-source fan-in feeding it directly
    /// is NOT marked for the canonical merge and keeps availability-driven scheduling.
    /// This pins the build-time half of `commutative_barrier_fan_in_replay_test`: the
    /// runtime fidelity there is only meaningful because the fan-in is genuinely
    /// unordered, which is asserted here. Complements
    /// `barrier_shields_its_fan_in_from_a_downstream_observer` (barrier with an observer
    /// below) by covering the barrier-as-direct-consumer shape with no observer at all.
    #[test]
    fn commutative_barrier_as_fan_in_consumer_keeps_availability_scheduling() {
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let barrier_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "barrier".to_string(),
            crate::stateful!(name: "barrier", InputEvent -> OutputEvent => CommutativeStateful),
        );
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", OutputEvent => ExactSink),
        );

        // The build classifies the barrier from its declaration, before any wrapping.
        assert_eq!(descriptors["barrier"].order_role(), OrderRole::Barrier);

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("barrier".to_string(), barrier_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, stage_type) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (barrier_id, "barrier", TopologyStageType::Transform),
            (sink_id, "sink", TopologyStageType::Sink),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), stage_type);
            topology.reset_current();
        }
        topology.add_edge(source_a_id.to_topology_id(), barrier_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), barrier_id.to_topology_id());
        topology.add_edge(barrier_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        // The barrier absorbs input order, so its fan-in is NOT marked: it keeps
        // availability-driven scheduling rather than the canonical merge.
        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(
            !marked.contains(&barrier_id),
            "a commutative barrier's fan-in must stay availability-scheduled"
        );

        // And the build accepts it: a declared barrier is neither an undeclared
        // observer nor an order-sensitive stage needing a merge.
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);
        assert!(!descriptors["barrier"].is_deterministic_input_orderer());
        crate::dsl::typing::validate_input_order_declarations(&topology, &descriptors, &name_to_id)
            .expect("a declared barrier is not an undeclared observer");
        validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
            .expect("a barrier consumer needs no deterministic input order");
    }

    #[test]
    fn decorators_forward_order_role() {
        // FLOWIP-095l Gap 6: a defaulted trait method is not auto-forwarded, so the
        // build-time wrappers forward order_role explicitly. The macro wraps the
        // descriptor (TypedStageDescriptor) and wrap_deterministic_orderers adds
        // DeterministicOrdererOverride; the Carrier role must survive both, or a
        // wrapped Barrier would silently degrade to Observer.
        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "t".to_string(),
            crate::transform!(name: "t", InputEvent -> OutputEvent => ExactTransform),
        );
        assert_eq!(descriptors["t"].order_role(), OrderRole::Carrier);

        let t_id = StageId::new();
        let mut name_to_id = HashMap::new();
        name_to_id.insert("t".to_string(), t_id);
        let marked: std::collections::HashSet<StageId> = std::iter::once(t_id).collect();
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);

        assert!(descriptors["t"].is_deterministic_input_orderer());
        assert_eq!(
            descriptors["t"].order_role(),
            OrderRole::Carrier,
            "the orderer override must forward the inner Carrier role"
        );
    }

    #[test]
    fn order_sensitive_stateful_under_cycle_is_rejected() {
        // FLOWIP-095l Gap 4: a non-effectful order Observer (a stateful fold) whose
        // input arrives through a cycle-fed fan-in cannot be made deterministic this
        // release, so the build refuses it with a distinct, correctly named error
        // rather than the effect-specific one.
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let cycle_entry_id = StageId::new();
        let cycle_back_id = StageId::new();
        let merge_id = StageId::new();
        let fold_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "cycle_entry".to_string(),
            crate::transform!(name: "cycle_entry", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "cycle_back".to_string(),
            crate::transform!(name: "cycle_back", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "merge".to_string(),
            crate::transform!(name: "merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "fold".to_string(),
            crate::stateful!(name: "fold", InputEvent -> OutputEvent => ExactStateful),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("cycle_entry".to_string(), cycle_entry_id);
        name_to_id.insert("cycle_back".to_string(), cycle_back_id);
        name_to_id.insert("merge".to_string(), merge_id);
        name_to_id.insert("fold".to_string(), fold_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (cycle_entry_id, "cycle_entry", TopologyStageType::Transform),
            (cycle_back_id, "cycle_back", TopologyStageType::Transform),
            (merge_id, "merge", TopologyStageType::Transform),
            (fold_id, "fold", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(
            source_a_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_entry_id.to_topology_id(),
            cycle_back_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_back_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(cycle_back_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(merge_id.to_topology_id(), fold_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);

        let err =
            *validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .expect_err("an order-sensitive fold fed from a cycle must be rejected");
        match err {
            FlowBuildError::OrderSensitiveStageUnderCycle { stage_name } => {
                assert_eq!(stage_name, "fold");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn barrier_shields_its_fan_in_from_a_downstream_observer() {
        // FLOWIP-095l: a order-insensitive fold is a barrier. The two-source fan-in
        // feeding it is order-insensitive, so an effect downstream of the barrier
        // does not drag that fan-in into the merge.
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let barrier_id = StageId::new();
        let effect_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "barrier".to_string(),
            crate::stateful!(name: "barrier", InputEvent -> OutputEvent => CommutativeStateful),
        );
        descriptors.insert(
            "effect".to_string(),
            crate::effectful_transform!(
                name: "effect",
                OutputEvent -> OutputEvent => EffectfulExactTransform,
                effects: [],
                middleware: []
            ),
        );
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", OutputEvent => ExactSink),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("barrier".to_string(), barrier_id);
        name_to_id.insert("effect".to_string(), effect_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (barrier_id, "barrier", TopologyStageType::Transform),
            (effect_id, "effect", TopologyStageType::Transform),
            (sink_id, "sink", TopologyStageType::Sink),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(source_a_id.to_topology_id(), barrier_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), barrier_id.to_topology_id());
        topology.add_edge(barrier_id.to_topology_id(), effect_id.to_topology_id());
        topology.add_edge(effect_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(
            !marked.contains(&barrier_id),
            "a barrier's fan-in must not be marked: the barrier absorbs input order"
        );
        // The build accepts it: a declared barrier needs no further declaration.
        crate::dsl::typing::validate_input_order_declarations(&topology, &descriptors, &name_to_id)
            .expect("a declared barrier is not an undeclared observer");
    }

    /// FLOWIP-095l Gap 14: a sink whose destination is order-sensitive
    /// (`DestinationOrder::Ordered`) is an Observer and seeds the canonical merge at
    /// its upstream fan-in; an order-insensitive sink (the default) is a Carrier and
    /// does not.
    #[test]
    fn ordered_sink_seeds_its_fan_in() {
        #[derive(Clone, Debug)]
        struct OrderedSink;
        impl SinkTyping for OrderedSink {
            type Input = OutputEvent;
        }
        #[async_trait]
        impl SinkHandler for OrderedSink {
            async fn consume(
                &mut self,
                _event: ChainEvent,
            ) -> Result<DeliveryPayload, HandlerError> {
                Ok(DeliveryPayload::success("sink", DeliveryMethod::Noop, None))
            }
            fn destination_order(
                &self,
            ) -> obzenflow_runtime::stages::common::handlers::DestinationOrder {
                obzenflow_runtime::stages::common::handlers::DestinationOrder::Ordered
            }
        }

        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let ordered_id = StageId::new();
        let insensitive_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", OutputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", OutputEvent => placeholder!()),
        );
        descriptors.insert(
            "ordered".to_string(),
            crate::sink!(name: "ordered", OutputEvent => OrderedSink),
        );
        descriptors.insert(
            "insensitive".to_string(),
            crate::sink!(name: "insensitive", OutputEvent => ExactSink),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("ordered".to_string(), ordered_id);
        name_to_id.insert("insensitive".to_string(), insensitive_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (ordered_id, "ordered", TopologyStageType::Sink),
            (insensitive_id, "insensitive", TopologyStageType::Sink),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(source_a_id.to_topology_id(), ordered_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), ordered_id.to_topology_id());
        topology.add_edge(
            source_a_id.to_topology_id(),
            insensitive_id.to_topology_id(),
        );
        topology.add_edge(
            source_b_id.to_topology_id(),
            insensitive_id.to_topology_id(),
        );
        let topology = topology.build_unchecked().unwrap();

        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        assert!(
            marked.contains(&ordered_id),
            "an Ordered sink seeds the canonical merge at its fan-in"
        );
        assert!(
            !marked.contains(&insensitive_id),
            "an Insensitive sink stays a Carrier and does not seed the merge"
        );
    }

    #[test]
    fn undeclared_stateful_in_fan_in_cone_is_rejected() {
        // FLOWIP-095l Gap 2: an undeclared stateful fold below a multi-source
        // fan-in must declare; the build refuses it with a clear message.
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let fold_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "fold".to_string(),
            crate::stateful!(name: "fold", InputEvent -> OutputEvent => ExactStateful),
        );
        descriptors.insert(
            "sink".to_string(),
            crate::sink!(name: "sink", OutputEvent => ExactSink),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("fold".to_string(), fold_id);
        name_to_id.insert("sink".to_string(), sink_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (fold_id, "fold", TopologyStageType::Transform),
            (sink_id, "sink", TopologyStageType::Sink),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(source_a_id.to_topology_id(), fold_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), fold_id.to_topology_id());
        topology.add_edge(fold_id.to_topology_id(), sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let err = *crate::dsl::typing::validate_input_order_declarations(
            &topology,
            &descriptors,
            &name_to_id,
        )
        .expect_err("an undeclared stateful fold in a fan-in cone must be rejected");
        match err {
            FlowBuildError::UndeclaredInputOrderSemantics { stage_name } => {
                assert_eq!(stage_name, "fold");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn cycle_tolerant_stateful_builds_with_opt_in() {
        // FLOWIP-095l Gap 4: the exact cycle topology that rejects a plain
        // order-sensitive fold (see order_sensitive_stateful_under_cycle_is_rejected)
        // builds when the handler overrides accepts_cycle_nondeterminism, exercising
        // the opt-in branch end to end.
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let cycle_entry_id = StageId::new();
        let cycle_back_id = StageId::new();
        let merge_id = StageId::new();
        let fold_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source_a".to_string(),
            crate::source!(name: "source_a", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "source_b".to_string(),
            crate::source!(name: "source_b", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "cycle_entry".to_string(),
            crate::transform!(name: "cycle_entry", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "cycle_back".to_string(),
            crate::transform!(name: "cycle_back", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "merge".to_string(),
            crate::transform!(name: "merge", InputEvent -> OutputEvent => ExactTransform),
        );
        descriptors.insert(
            "fold".to_string(),
            crate::stateful!(name: "fold", InputEvent -> OutputEvent => CycleTolerantStateful),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source_a".to_string(), source_a_id);
        name_to_id.insert("source_b".to_string(), source_b_id);
        name_to_id.insert("cycle_entry".to_string(), cycle_entry_id);
        name_to_id.insert("cycle_back".to_string(), cycle_back_id);
        name_to_id.insert("merge".to_string(), merge_id);
        name_to_id.insert("fold".to_string(), fold_id);

        let mut topology = TopologyBuilder::new();
        for (id, name, role) in [
            (source_a_id, "source_a", TopologyStageType::FiniteSource),
            (source_b_id, "source_b", TopologyStageType::FiniteSource),
            (cycle_entry_id, "cycle_entry", TopologyStageType::Transform),
            (cycle_back_id, "cycle_back", TopologyStageType::Transform),
            (merge_id, "merge", TopologyStageType::Transform),
            (fold_id, "fold", TopologyStageType::Transform),
        ] {
            topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), role);
            topology.reset_current();
        }
        topology.add_edge(
            source_a_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_entry_id.to_topology_id(),
            cycle_back_id.to_topology_id(),
        );
        topology.add_edge(
            cycle_back_id.to_topology_id(),
            cycle_entry_id.to_topology_id(),
        );
        topology.add_edge(cycle_back_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(source_b_id.to_topology_id(), merge_id.to_topology_id());
        topology.add_edge(merge_id.to_topology_id(), fold_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        crate::dsl::typing::validate_input_order_declarations(&topology, &descriptors, &name_to_id)
            .expect("a declared fold is not an undeclared observer");
        let marked = crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        crate::dsl::typing::wrap_deterministic_orderers(&mut descriptors, &name_to_id, &marked);
        crate::dsl::typing::validate_effectful_deterministic_input_order(
            &topology,
            &descriptors,
            &name_to_id,
        )
        .expect("an opted-in fold below a cycle builds instead of being rejected");
    }
}
