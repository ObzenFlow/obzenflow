// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for FLOWIP-105g typed stage contracts.

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::{ChainEvent, StageId, TypedPayload, WriterId};
    use obzenflow_runtime::id_conversions::StageIdExt;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::source::SourceError;
    use obzenflow_runtime::stages::common::handlers::{
        AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, AsyncTransformHandler,
        FiniteSourceHandler, InfiniteSourceHandler, JoinHandler, SinkHandler, StatefulHandler,
        TransformHandler,
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
        collect_stage_typing_info, validate_edge_typing, validate_stage_typing_metadata,
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

    fn exact<T: 'static>() -> TypeHint {
        TypeHint::exact::<T>()
    }

    #[test]
    fn typed_macros_attach_metadata_for_exact_handlers() {
        let source = crate::source!(name: "source", InputEvent => SyncExactSource);
        assert_eq!(
            source.typing_metadata().unwrap().output_type,
            exact::<InputEvent>()
        );

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
        assert_eq!(
            sink.typing_metadata().unwrap().input_type,
            exact::<OutputEvent>()
        );

        let join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => ExactJoin
        );
        let join_meta = join.typing_metadata().unwrap();
        assert_eq!(join_meta.reference_type, exact::<ReferenceEvent>());
        assert_eq!(join_meta.stream_type, exact::<StreamEvent>());
        assert_eq!(join_meta.output_type, exact::<JoinedEvent>());
        assert!(!join_meta.is_placeholder);
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

    /// FLOWIP-114c PR E: documenting the `serde_json::Value`-as-the-new-
    /// `mixed` failure mode.
    ///
    /// If two semantically different event shapes are both spelled as
    /// `serde_json::Value` in DSL slots, the validator cannot tell them
    /// apart because `TypeId::of::<serde_json::Value>()` is one constant
    /// across the whole compilation. The flow build succeeds even though
    /// the upstream is, semantically, emitting two different things into
    /// the downstream slot. This is exactly what the PR D bulk migration
    /// accidentally enabled before PR E cleaned it up, and exactly why
    /// the `no_serde_value_in_dsl_slots` lint exists alongside this test.
    ///
    /// The correct pattern is per-branch alignment with concrete
    /// `TypedPayload` structs that normalise to one common envelope
    /// type, demonstrated in `examples/multi_source_ingest_demo/`.
    #[test]
    fn validate_edge_typing_accepts_value_into_value_silently_documenting_anti_pattern() {
        // Two distinct semantic events the test author "should" model as
        // separate types (e.g. an order and a charge), but they both get
        // spelled `serde_json::Value` in DSL slots. The validator sees a
        // single TypeId on every edge.
        let source_a_id = StageId::new();
        let source_b_id = StageId::new();
        let sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "order_source".to_string(),
            // allow-serde-value: deliberate anti-pattern demonstration (Value-as-blessed-answer).
            crate::source!(name: "order_source", serde_json::Value => placeholder!()),
        );
        descriptors.insert(
            "charge_source".to_string(),
            // allow-serde-value: see above; deliberate anti-pattern demonstration.
            crate::source!(name: "charge_source", serde_json::Value => placeholder!()),
        );
        descriptors.insert(
            "consolidator".to_string(),
            // allow-serde-value: see above; deliberate anti-pattern demonstration.
            crate::sink!(name: "consolidator", serde_json::Value => placeholder!()),
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
            "serde_json::Value collapses every payload into one TypeId, so the validator \
             accepts this flow even though the two sources semantically emit different \
             events. This is the wrong answer. See examples/multi_source_ingest_demo/ for \
             the correct per-branch alignment pattern, and the lint at \
             obzenflow_dsl/tests/no_serde_value_in_dsl_slots.rs which enforces the policy \
             at build time.",
        );
    }
}
