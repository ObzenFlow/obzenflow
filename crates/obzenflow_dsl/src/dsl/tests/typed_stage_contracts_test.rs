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
        EffectfulTransformHandler, FiniteSourceHandler, InfiniteSourceHandler, JoinHandler,
        SinkHandler, StatefulHandler, TransformHandler,
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
    use crate::dsl::stage_descriptor::StageDescriptor;
    use crate::dsl::stage_descriptor::TransformDescriptor;
    use crate::dsl::typing::{
        collect_stage_typing_info, derive_feed_plan, validate_edge_typing,
        validate_effectful_deterministic_input_order, validate_stage_typing_metadata,
        EdgeInputRole, TypeHint,
    };
    // FLOWIP-120a: imports for the deterministic-orderer test fixture below.
    use crate::dsl::StageCreationResult;
    use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
    use obzenflow_adapters::middleware::MiddlewareFactory;
    use obzenflow_core::event::context::StageType as CoreStageType;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use obzenflow_runtime::stages::common::stage_handle::BoxedStageHandle;
    use obzenflow_runtime::stages::StageResources;
    use std::sync::Arc;

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
        type Output = OutputEvent;

        async fn process(
            &self,
            input: OutputEvent,
            _fx: &mut Effects,
        ) -> Result<OutputEvent, HandlerError> {
            Ok(input)
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

    /// FLOWIP-120a test fixture: a descriptor that declares itself a deterministic
    /// input orderer, standing in for the runtime-enforced capability FLOWIP-095d
    /// will supply. No production descriptor sets this flag, so this test-only stub
    /// is the only way to exercise the guard's accept branch. It never builds a
    /// handle, because the deterministic-order guard inspects descriptor flags only.
    struct DeterministicOrdererStub {
        name: String,
    }

    #[async_trait]
    impl StageDescriptor for DeterministicOrdererStub {
        fn name(&self) -> &str {
            &self.name
        }

        fn stage_type(&self) -> CoreStageType {
            CoreStageType::Transform
        }

        fn is_deterministic_input_orderer(&self) -> bool {
            true
        }

        async fn create_handle_with_flow_middleware(
            self: Box<Self>,
            _config: StageConfig,
            _resources: StageResources,
            _flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
            _control_middleware: Arc<ControlMiddlewareAggregator>,
        ) -> StageCreationResult<BoxedStageHandle> {
            unimplemented!(
                "DeterministicOrdererStub is a build-guard test fixture and never builds a handle"
            )
        }
    }

    /// FLOWIP-120a: the accept branch of the effectful fan-in guard. An effectful
    /// stage whose immediate upstream is a declared deterministic orderer must pass,
    /// because the orderer resets the propagated deterministic-input-order property
    /// for its descendants. This pins the orderer flag as the sole discriminator: the
    /// topology is identical to `effectful_guard_rejects_transitive_nondeterministic_fan_in`
    /// except the merge node is an orderer, and that test is the negative control.
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
            Box::new(DeterministicOrdererStub {
                name: "orderer".to_string(),
            }),
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

        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_ok(),
            "effectful stage below a deterministic orderer must be accepted"
        );
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
            Box::new(DeterministicOrdererStub {
                name: "orderer".to_string(),
            }),
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

        assert!(
            validate_effectful_deterministic_input_order(&topology, &descriptors, &name_to_id)
                .is_ok(),
            "effectful stage two hops below a deterministic orderer must be accepted"
        );
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
}
