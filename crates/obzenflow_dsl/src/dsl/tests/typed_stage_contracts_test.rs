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
    use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::dsl::stage_descriptor::StageDescriptor;
    use crate::dsl::typing::{collect_edge_warnings, EdgeInputRole, TypeHint};

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
    struct InternalMixedMarker;

    #[derive(Clone, Debug)]
    struct MixedTransform;

    impl TransformTyping for MixedTransform {
        type Input = InternalMixedMarker;
        type Output = OutputEvent;
    }

    #[async_trait]
    impl TransformHandler for MixedTransform {
        fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
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
            Ok(DeliveryPayload::success(
                "sink",
                DeliveryMethod::Noop,
                None,
            ))
        }
    }

    #[derive(Clone, Debug)]
    struct MixedAuditSink;

    #[async_trait]
    impl SinkHandler for MixedAuditSink {
        async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
            Ok(DeliveryPayload::success(
                "audit",
                DeliveryMethod::Noop,
                None,
            ))
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

    fn exact(name: &str) -> TypeHint {
        TypeHint::Exact(name.to_string())
    }

    #[test]
    fn typed_macros_attach_metadata_for_exact_handlers() {
        let source = crate::source!(name: "source", InputEvent => SyncExactSource);
        assert_eq!(
            source.typing_metadata().unwrap().output_type,
            exact("InputEvent")
        );

        let async_source = crate::async_source!(name: "async_source", InputEvent => AsyncExactSource);
        assert_eq!(
            async_source.typing_metadata().unwrap().output_type,
            exact("InputEvent")
        );

        let infinite = crate::infinite_source!(name: "infinite", InputEvent => InfiniteExactSource);
        assert_eq!(
            infinite.typing_metadata().unwrap().output_type,
            exact("InputEvent")
        );

        let async_infinite = crate::async_infinite_source!(
            name: "async_infinite",
            InputEvent => AsyncInfiniteExactSource
        );
        assert_eq!(
            async_infinite.typing_metadata().unwrap().output_type,
            exact("InputEvent")
        );

        let transform = crate::transform!(name: "transform", InputEvent -> OutputEvent => ExactTransform);
        let transform_meta = transform.typing_metadata().unwrap();
        assert_eq!(transform_meta.input_type, exact("InputEvent"));
        assert_eq!(transform_meta.output_type, exact("OutputEvent"));

        let async_transform = crate::async_transform!(
            name: "async_transform",
            InputEvent -> OutputEvent => ExactAsyncTransform
        );
        let async_transform_meta = async_transform.typing_metadata().unwrap();
        assert_eq!(async_transform_meta.input_type, exact("InputEvent"));
        assert_eq!(async_transform_meta.output_type, exact("OutputEvent"));

        let stateful = crate::stateful!(
            name: "stateful",
            InputEvent -> OutputEvent => ExactStateful,
            emit_interval = Duration::from_secs(5)
        );
        let stateful_meta = stateful.typing_metadata().unwrap();
        assert_eq!(stateful_meta.input_type, exact("InputEvent"));
        assert_eq!(stateful_meta.output_type, exact("OutputEvent"));

        let sink = crate::sink!(name: "sink", OutputEvent => ExactSink);
        assert_eq!(sink.typing_metadata().unwrap().input_type, exact("OutputEvent"));

        let join = crate::join!(
            name: "join",
            catalog reference: ReferenceEvent,
            StreamEvent -> JoinedEvent => ExactJoin
        );
        let join_meta = join.typing_metadata().unwrap();
        assert_eq!(join_meta.reference_type, exact("ReferenceEvent"));
        assert_eq!(join_meta.stream_type, exact("StreamEvent"));
        assert_eq!(join_meta.output_type, exact("JoinedEvent"));
        assert!(!join_meta.is_placeholder);
    }

    #[test]
    fn stage_macros_default_to_binding_derived_name_marker() {
        let source = crate::source!(InputEvent => SyncExactSource);
        assert_eq!(source.name(), crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL);

        let transform = crate::transform!(InputEvent -> OutputEvent => ExactTransform);
        assert_eq!(
            transform.name(),
            crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL
        );

        let sink = crate::sink!(OutputEvent => ExactSink);
        assert_eq!(sink.name(), crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL);

        let join =
            crate::join!(catalog reference: ReferenceEvent, StreamEvent -> JoinedEvent => ExactJoin);
        assert_eq!(join.name(), crate::dsl::stage_descriptor::BINDING_DERIVED_NAME_SENTINEL);
    }

    #[test]
    fn placeholders_and_mixed_inputs_capture_metadata() {
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
        assert_eq!(async_transform_meta.input_type, exact("InputEvent"));
        assert_eq!(async_transform_meta.output_type, exact("OutputEvent"));

        let mixed_sink =
            crate::sink!(name: "audit", mixed => placeholder!("route everything"));
        let mixed_sink_meta = mixed_sink.typing_metadata().unwrap();
        assert!(mixed_sink_meta.is_placeholder);
        assert_eq!(mixed_sink_meta.input_type, TypeHint::Mixed);
        assert_eq!(
            mixed_sink_meta.placeholder_message.as_deref(),
            Some("route everything")
        );
    }

    #[test]
    fn mixed_input_macros_only_assert_exact_positions() {
        let transform =
            crate::transform!(name: "router", mixed -> OutputEvent => MixedTransform);
        let transform_meta = transform.typing_metadata().unwrap();
        assert_eq!(transform_meta.input_type, TypeHint::Mixed);
        assert_eq!(transform_meta.output_type, exact("OutputEvent"));
        assert!(!transform_meta.is_placeholder);

        let sink = crate::sink!(name: "audit", mixed => MixedAuditSink);
        let sink_meta = sink.typing_metadata().unwrap();
        assert_eq!(sink_meta.input_type, TypeHint::Mixed);
        assert_eq!(sink_meta.output_type, TypeHint::Unspecified);
        assert!(!sink_meta.is_placeholder);
    }

    #[test]
    fn collect_edge_warnings_reports_exact_mismatch_and_suppresses_mixed() {
        let source_id = StageId::new();
        let exact_sink_id = StageId::new();
        let mixed_sink_id = StageId::new();

        let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        descriptors.insert(
            "source".to_string(),
            crate::source!(name: "source", InputEvent => placeholder!()),
        );
        descriptors.insert(
            "exact_sink".to_string(),
            crate::sink!(name: "exact_sink", OutputEvent => placeholder!()),
        );
        descriptors.insert(
            "mixed_sink".to_string(),
            crate::sink!(name: "mixed_sink", mixed => placeholder!()),
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("source".to_string(), source_id);
        name_to_id.insert("exact_sink".to_string(), exact_sink_id);
        name_to_id.insert("mixed_sink".to_string(), mixed_sink_id);

        let mut topology = TopologyBuilder::new();
        topology.add_stage_with_id(
            source_id.to_topology_id(),
            Some("source".to_string()),
            TopologyStageType::FiniteSource,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            exact_sink_id.to_topology_id(),
            Some("exact_sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_stage_with_id(
            mixed_sink_id.to_topology_id(),
            Some("mixed_sink".to_string()),
            TopologyStageType::Sink,
        );
        topology.reset_current();
        topology.add_edge(source_id.to_topology_id(), exact_sink_id.to_topology_id());
        topology.add_edge(source_id.to_topology_id(), mixed_sink_id.to_topology_id());
        let topology = topology.build_unchecked().unwrap();

        let warnings = collect_edge_warnings(&topology, &descriptors, &name_to_id);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].upstream_stage, "source");
        assert_eq!(warnings[0].downstream_stage, "exact_sink");
        assert_eq!(warnings[0].upstream_type, "InputEvent");
        assert_eq!(warnings[0].expected_type, "OutputEvent");
        assert_eq!(warnings[0].input_role, EdgeInputRole::Input);
    }

    #[test]
    fn collect_edge_warnings_distinguishes_join_reference_and_stream_roles() {
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

        let mut warnings = collect_edge_warnings(&topology, &descriptors, &name_to_id);
        warnings.sort_by_key(|warning| match warning.input_role {
            EdgeInputRole::Reference => 0,
            EdgeInputRole::Stream => 1,
            EdgeInputRole::Input => 2,
        });

        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0].input_role, EdgeInputRole::Reference);
        assert_eq!(warnings[0].upstream_type, "InputEvent");
        assert_eq!(warnings[0].expected_type, "ReferenceEvent");

        assert_eq!(warnings[1].input_role, EdgeInputRole::Stream);
        assert_eq!(warnings[1].upstream_type, "AlternateEvent");
        assert_eq!(warnings[1].expected_type, "StreamEvent");
    }
}
