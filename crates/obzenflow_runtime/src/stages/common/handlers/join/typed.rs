// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed join authoring and runtime erasure (FLOWIP-134f).

use super::traits::{sealed, UnifiedJoinHandler};
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::join::config::{JoinReferenceMode, DEFAULT_REFERENCE_BATCH_CAP};
use async_trait::async_trait;
use obzenflow_core::config::LineagePolicy;
use obzenflow_core::event::context::CompositeActivationContext;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::{ChainEventFactory, StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, OneFactStageOutput, StageId, TypedPayload, WriterId};
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Clone, Debug)]
struct ReferenceProjectionEntry<R> {
    value: R,
    activations: Vec<CompositeActivationContext>,
}

/// Read-only typed access to the current reference projection.
///
/// Every successful `select` privately records the exact projection entry
/// used by the invocation. The adapter attaches that evidence to every fact
/// returned by the invocation; authored code never handles envelope metadata.
pub struct JoinReferenceView<'a, K, R> {
    projection: &'a HashMap<K, ReferenceProjectionEntry<R>>,
    selected_activations: Vec<CompositeActivationContext>,
}

impl<'a, K, R> JoinReferenceView<'a, K, R>
where
    K: Eq + Hash,
    R: Clone,
{
    fn new(projection: &'a HashMap<K, ReferenceProjectionEntry<R>>) -> Self {
        Self {
            projection,
            selected_activations: Vec::new(),
        }
    }

    /// Select the current value for `key`, recording its exact durable
    /// contribution evidence when present.
    pub fn select(&mut self, key: &K) -> Option<R> {
        let entry = self.projection.get(key)?;
        self.selected_activations
            .extend(entry.activations.iter().cloned());
        Some(entry.value.clone())
    }

    fn into_selected_activations(self) -> Vec<CompositeActivationContext> {
        self.selected_activations
    }
}

/// One framework invocation before facts are lowered into runtime envelopes.
///
/// Constructors are intentionally not public. Ordinary authored handlers can
/// return facts only; sealed framework joins may additionally author protocol
/// control such as StrictJoin's Poison EOF.
#[doc(hidden)]
pub struct TypedJoinInvocation<O> {
    outputs: Vec<O>,
    framework_eof: Option<EofKind>,
}

impl<O> TypedJoinInvocation<O> {
    fn facts_only(outputs: Vec<O>) -> Self {
        Self {
            outputs,
            framework_eof: None,
        }
    }

    pub(crate) fn with_framework_eof(outputs: Vec<O>, kind: EofKind) -> Self {
        Self {
            outputs,
            framework_eof: Some(kind),
        }
    }

    pub(crate) fn facts_only_for_framework(outputs: Vec<O>) -> Self {
        Self::facts_only(outputs)
    }

    fn into_parts(self) -> (Vec<O>, Option<EofKind>) {
        (self.outputs, self.framework_eof)
    }
}

/// Pure typed join surface whose associated types witness the `join!` arrow.
#[diagnostic::on_unimplemented(
    message = "this join handler does not witness its arrow contract",
    label = "this handler does not implement the typed join contract",
    note = "implement TypedJoinHandler with Reference, Stream, and Output matching the join! arrow (FLOWIP-134f)"
)]
pub trait TypedJoinHandler: Send + Sync {
    type State: Clone + Send + Sync;
    type ReferenceKey: Eq + Hash + Clone + Send + Sync;
    type Reference: TypedPayload + Clone + Send + Sync + 'static;
    type Stream: TypedPayload + Send + Sync + 'static;
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::FiniteEof
    }

    fn admit_reference(
        &self,
        reference: &Self::Reference,
    ) -> Result<Self::ReferenceKey, HandlerError>;

    fn process_stream(
        &self,
        state: &mut Self::State,
        references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
        stream: Self::Stream,
    ) -> Result<Vec<Self::Output>, HandlerError>;

    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        Ok(Vec::new())
    }

    fn drain(
        &self,
        _state: &Self::State,
        _references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        Ok(Vec::new())
    }

    /// Runtime-only seam for sealed framework control authoring.
    #[doc(hidden)]
    fn process_stream_invocation(
        &self,
        state: &mut Self::State,
        references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
        stream: Self::Stream,
    ) -> Result<TypedJoinInvocation<Self::Output>, HandlerError> {
        self.process_stream(state, references, stream)
            .map(TypedJoinInvocation::facts_only)
    }
}

/// Erased state couples authored state to the runtime-owned reference
/// projection. Projection values and evidence remain inaccessible to authors.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TypedJoinState<S, K, R> {
    inner: S,
    projection: HashMap<K, ReferenceProjectionEntry<R>>,
}

/// Adapter from typed join authoring to the sealed join supervisor surface.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TypedJoinHandlerAdapter<H> {
    handler: H,
    reference_mode: JoinReferenceMode,
    lineage: LineagePolicy,
    writer_id: Option<WriterId>,
}

impl<H> TypedJoinHandlerAdapter<H>
where
    H: TypedJoinHandler,
{
    /// Samples `reference_mode` exactly once. All later topology and runtime
    /// decisions read the cached value from this adapter.
    pub fn new(handler: H) -> Self {
        let reference_mode = handler.reference_mode();
        Self {
            handler,
            reference_mode,
            lineage: LineagePolicy::default(),
            writer_id: None,
        }
    }

    fn writer_id(&self) -> Result<WriterId, HandlerError> {
        self.writer_id.ok_or_else(|| {
            HandlerError::Fatal(StageFatal::new(
                StageFatalCode::Configuration,
                StageFatalReason::ConfigurationInvariant,
                "typed join adapter invoked before runtime writer identity installation",
            ))
        })
    }

    fn lower_outputs<O>(
        &self,
        outputs: Vec<O>,
        parent: Option<&ChainEvent>,
        selected_activations: Vec<CompositeActivationContext>,
        framework_eof: Option<EofKind>,
    ) -> Result<Vec<ChainEvent>, HandlerError>
    where
        O: OneFactStageOutput,
    {
        let writer_id = self.writer_id()?;
        let mut events = Vec::with_capacity(outputs.len() + usize::from(framework_eof.is_some()));

        for output in outputs {
            let mut facts = output.into_facts().map_err(|error| {
                HandlerError::Other(format!("typed join output serialization failed: {error}"))
            })?;
            if facts.len() != 1 {
                return Err(HandlerError::Fatal(StageFatal::new(
                    StageFatalCode::Protocol,
                    StageFatalReason::ProtocolInputIntegrity,
                    format!(
                        "one_fact_stage_output: `{}` lowered to {} facts instead of exactly one",
                        std::any::type_name::<O>(),
                        facts.len(),
                    ),
                )));
            }
            let fact = facts.pop().expect("length checked above");
            let event = match parent {
                Some(parent) => ChainEventFactory::derived_data_event(
                    writer_id,
                    parent,
                    fact.event_type,
                    fact.payload,
                    self.lineage,
                ),
                None => ChainEventFactory::data_event(writer_id, fact.event_type, fact.payload),
            };
            let event = event
                .try_with_composite_activations(selected_activations.clone())
                .map_err(|error| {
                    HandlerError::Fatal(StageFatal::new(
                        StageFatalCode::Protocol,
                        StageFatalReason::ProtocolInputIntegrity,
                        format!("typed_join_contributions: {error}"),
                    ))
                })?;
            events.push(event);
        }

        if let Some(kind) = framework_eof {
            events.push(ChainEventFactory::eof_event_with_kind(writer_id, kind));
        }
        Ok(events)
    }
}

impl<H> sealed::Sealed for TypedJoinHandlerAdapter<H> where H: TypedJoinHandler {}

#[async_trait]
impl<H> UnifiedJoinHandler for TypedJoinHandlerAdapter<H>
where
    H: TypedJoinHandler + Send + Sync,
{
    type State = TypedJoinState<H::State, H::ReferenceKey, H::Reference>;

    fn install_lineage_policy(&mut self, policy: LineagePolicy) {
        self.lineage = policy;
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn initial_state(&self) -> Self::State {
        TypedJoinState {
            inner: self.handler.initial_state(),
            projection: HashMap::new(),
        }
    }

    fn process_reference(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        self.writer_id()?;
        let reference = H::Reference::try_from_event(&event)
            .map_err(|error| HandlerError::Deserialization(error.to_string()))?;
        let key = self.handler.admit_reference(&reference)?;
        state.projection.insert(
            key,
            ReferenceProjectionEntry {
                value: reference,
                activations: event.composite_activations().to_vec(),
            },
        );
        Ok(Vec::new())
    }

    fn process_stream(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        self.writer_id()?;
        let stream = H::Stream::try_from_event(&event)
            .map_err(|error| HandlerError::Deserialization(error.to_string()))?;
        let mut references = JoinReferenceView::new(&state.projection);
        let invocation =
            self.handler
                .process_stream_invocation(&mut state.inner, &mut references, stream)?;
        let selected = references.into_selected_activations();
        let (outputs, framework_eof) = invocation.into_parts();
        self.lower_outputs(outputs, Some(&event), selected, framework_eof)
    }

    fn reference_mode(&self) -> JoinReferenceMode {
        self.reference_mode
    }

    fn reference_batch_cap(&self) -> Option<usize> {
        Some(DEFAULT_REFERENCE_BATCH_CAP)
    }

    fn on_stream_eof(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        self.writer_id()?;
        let mut references = JoinReferenceView::new(&state.projection);
        let outputs = self
            .handler
            .on_stream_eof(&mut state.inner, &mut references)?;
        let selected = references.into_selected_activations();
        self.lower_outputs(outputs, Some(&event), selected, None)
    }

    async fn drain(
        &self,
        state: &Self::State,
        parent: Option<&ChainEvent>,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        self.writer_id()?;
        let mut references = JoinReferenceView::new(&state.projection);
        let outputs = self.handler.drain(&state.inner, &mut references)?;
        let selected = references.into_selected_activations();
        self.lower_outputs(outputs, parent, selected, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::join::StrictJoinBuilder;
    use obzenflow_core::event::context::CompositeActivationContext;
    use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
    use obzenflow_core::event::ChainEventContent;
    use obzenflow_core::id::CompositeId;
    use obzenflow_core::{OneFactStageOutput, StageOutputFacts};
    use serde::de::{self, Deserializer};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestReference {
        key: String,
        value: String,
        reject: bool,
    }

    impl TypedPayload for TestReference {
        const EVENT_TYPE: &'static str = "typed_join.reference";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStream {
        keys: Vec<String>,
        fanout: usize,
    }

    impl TypedPayload for TestStream {
        const EVENT_TYPE: &'static str = "typed_join.stream";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOutput {
        selected_values: Vec<String>,
        ordinal: usize,
    }

    impl TypedPayload for TestOutput {
        const EVENT_TYPE: &'static str = "typed_join.output";
    }

    #[derive(Clone, Debug)]
    struct ProjectionJoin;

    impl TypedJoinHandler for ProjectionJoin {
        type State = ();
        type ReferenceKey = String;
        type Reference = TestReference;
        type Stream = TestStream;
        type Output = TestOutput;

        fn initial_state(&self) -> Self::State {}

        fn admit_reference(
            &self,
            reference: &Self::Reference,
        ) -> Result<Self::ReferenceKey, HandlerError> {
            if reference.reject {
                return Err(HandlerError::Validation("rejected reference".to_string()));
            }
            Ok(reference.key.clone())
        }

        fn process_stream(
            &self,
            _state: &mut Self::State,
            references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
            stream: Self::Stream,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            let selected_values = stream
                .keys
                .iter()
                .filter_map(|key| references.select(key))
                .map(|reference| reference.value)
                .collect::<Vec<_>>();
            Ok((0..stream.fanout)
                .map(|ordinal| TestOutput {
                    selected_values: selected_values.clone(),
                    ordinal,
                })
                .collect())
        }

        fn on_stream_eof(
            &self,
            _state: &mut Self::State,
            references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            let selected_values = references
                .select(&"terminal".to_string())
                .map(|reference| vec![reference.value])
                .unwrap_or_default();
            Ok(vec![TestOutput {
                selected_values,
                ordinal: 0,
            }])
        }
    }

    fn activation(label: &str, event: &ChainEvent, sequence: u64) -> CompositeActivationContext {
        CompositeActivationContext::new(
            CompositeId::new("typed-join:test"),
            event.id,
            label,
            sequence,
        )
    }

    fn reference_event(
        writer: WriterId,
        key: &str,
        value: &str,
        reject: bool,
        activation_label: &str,
        sequence: u64,
    ) -> ChainEvent {
        let event = TestReference {
            key: key.to_string(),
            value: value.to_string(),
            reject,
        }
        .to_event(writer);
        let evidence = activation(activation_label, &event, sequence);
        event
            .try_with_composite_activations(vec![evidence])
            .expect("valid reference activation")
    }

    fn stream_event(
        writer: WriterId,
        keys: &[&str],
        fanout: usize,
        activation_label: &str,
    ) -> ChainEvent {
        let event = TestStream {
            keys: keys.iter().map(|key| (*key).to_string()).collect(),
            fanout,
        }
        .to_event(writer);
        let evidence = activation(activation_label, &event, 999);
        event
            .try_with_composite_activations(vec![evidence])
            .expect("valid stream activation")
    }

    fn configured_adapter<H: TypedJoinHandler>(handler: H) -> TypedJoinHandlerAdapter<H> {
        let mut adapter = TypedJoinHandlerAdapter::new(handler);
        adapter.install_writer_id(WriterId::from(StageId::new()));
        adapter
    }

    fn live_scope() -> obzenflow_core::MiddlewareExecutionScope {
        obzenflow_core::MiddlewareExecutionScope::LiveHandler
    }

    fn has_activation(event: &ChainEvent, label: &str) -> bool {
        event
            .composite_activations()
            .iter()
            .any(|activation| activation.entry_port == label)
    }

    #[test]
    fn failed_same_key_admission_leaves_value_and_evidence_unchanged() {
        let reference_writer = WriterId::from(StageId::new());
        let stream_writer = WriterId::from(StageId::new());
        let adapter = configured_adapter(ProjectionJoin);
        let mut state = adapter.initial_state();
        let source = StageId::new();

        adapter
            .process_reference(
                &mut state,
                reference_event(reference_writer, "k", "old", false, "old", 1),
                source,
                reference_writer,
                live_scope(),
            )
            .expect("initial reference admission");

        let error = adapter
            .process_reference(
                &mut state,
                reference_event(reference_writer, "k", "rejected", true, "rejected", 2),
                source,
                reference_writer,
                live_scope(),
            )
            .expect_err("same-key rejected admission must surface");
        assert!(matches!(error, HandlerError::Validation(_)));

        let output = adapter
            .process_stream(
                &mut state,
                stream_event(stream_writer, &["k"], 1, "stream"),
                source,
                stream_writer,
                live_scope(),
            )
            .expect("stream processing after failed replacement");
        assert_eq!(output.len(), 1);
        assert_eq!(
            TestOutput::try_from_event(&output[0])
                .expect("typed output")
                .selected_values,
            vec!["old".to_string()]
        );
        assert!(has_activation(&output[0], "old"));
        assert!(!has_activation(&output[0], "rejected"));
    }

    #[test]
    fn successful_same_key_admission_atomically_replaces_value_and_evidence() {
        let reference_writer = WriterId::from(StageId::new());
        let stream_writer = WriterId::from(StageId::new());
        let adapter = configured_adapter(ProjectionJoin);
        let mut state = adapter.initial_state();
        let source = StageId::new();

        for event in [
            reference_event(reference_writer, "k", "old", false, "old", 1),
            reference_event(reference_writer, "k", "new", false, "new", 2),
        ] {
            adapter
                .process_reference(&mut state, event, source, reference_writer, live_scope())
                .expect("reference admission");
        }

        let output = adapter
            .process_stream(
                &mut state,
                stream_event(stream_writer, &["k"], 1, "stream"),
                source,
                stream_writer,
                live_scope(),
            )
            .expect("stream processing");
        let value = TestOutput::try_from_event(&output[0]).expect("typed output");
        assert_eq!(value.selected_values, vec!["new".to_string()]);
        assert!(!has_activation(&output[0], "old"));
        assert!(has_activation(&output[0], "new"));
        assert!(has_activation(&output[0], "stream"));
    }

    #[test]
    fn exact_multi_key_selection_is_shared_by_every_fanout_fact() {
        let reference_writer = WriterId::from(StageId::new());
        let stream_writer = WriterId::from(StageId::new());
        let join_writer = WriterId::from(StageId::new());
        let mut adapter = TypedJoinHandlerAdapter::new(ProjectionJoin);
        adapter.install_writer_id(join_writer);
        let mut state = adapter.initial_state();
        let source = StageId::new();

        for (key, label, sequence) in [("k1", "one", 1), ("k2", "two", 2), ("k3", "three", 3)] {
            adapter
                .process_reference(
                    &mut state,
                    reference_event(reference_writer, key, label, false, label, sequence),
                    source,
                    reference_writer,
                    live_scope(),
                )
                .expect("reference admission");
        }

        let output = adapter
            .process_stream(
                &mut state,
                stream_event(stream_writer, &["k1", "k2"], 2, "stream"),
                source,
                stream_writer,
                live_scope(),
            )
            .expect("fanout stream processing");
        assert_eq!(output.len(), 2);
        for (ordinal, event) in output.iter().enumerate() {
            assert_eq!(event.writer_id, join_writer);
            assert_eq!(event.event_type(), TestOutput::versioned_event_type());
            let value = TestOutput::try_from_event(event).expect("typed output");
            assert_eq!(value.ordinal, ordinal);
            assert_eq!(value.selected_values, vec!["one", "two"]);
            assert!(has_activation(event, "one"));
            assert!(has_activation(event, "two"));
            assert!(!has_activation(event, "three"));
            assert!(has_activation(event, "stream"));
        }
    }

    #[test]
    fn missing_selection_adds_no_reference_evidence() {
        let writer = WriterId::from(StageId::new());
        let adapter = configured_adapter(ProjectionJoin);
        let mut state = adapter.initial_state();
        let source = StageId::new();
        adapter
            .process_reference(
                &mut state,
                reference_event(writer, "present", "value", false, "present", 1),
                source,
                writer,
                live_scope(),
            )
            .expect("reference admission");

        let output = adapter
            .process_stream(
                &mut state,
                stream_event(writer, &["missing"], 1, "stream"),
                source,
                writer,
                live_scope(),
            )
            .expect("missing selection is valid");
        assert!(!has_activation(&output[0], "present"));
        assert!(has_activation(&output[0], "stream"));
    }

    #[test]
    fn terminal_hook_gets_a_fresh_exact_reference_view() {
        let writer = WriterId::from(StageId::new());
        let adapter = configured_adapter(ProjectionJoin);
        let mut state = adapter.initial_state();
        let source = StageId::new();
        adapter
            .process_reference(
                &mut state,
                reference_event(writer, "terminal", "final", false, "terminal", 1),
                source,
                writer,
                live_scope(),
            )
            .expect("terminal reference admission");

        let eof = ChainEventFactory::eof_event_with_kind(writer, EofKind::Natural);
        let output = adapter
            .on_stream_eof(&mut state, eof, source, writer)
            .expect("terminal hook");
        assert_eq!(output.len(), 1);
        assert_eq!(
            TestOutput::try_from_event(&output[0])
                .expect("typed terminal output")
                .selected_values,
            vec!["final".to_string()]
        );
        assert!(has_activation(&output[0], "terminal"));
    }

    static DECODE_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Debug, Serialize)]
    struct DecodeProbe {
        key: String,
    }

    impl<'de> Deserialize<'de> for DecodeProbe {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            DECODE_CALLS.fetch_add(1, Ordering::SeqCst);
            let value = serde_json::Value::deserialize(deserializer)?;
            let key = value
                .get("key")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| de::Error::custom("missing key"))?;
            Ok(Self {
                key: key.to_string(),
            })
        }
    }

    impl TypedPayload for DecodeProbe {
        const EVENT_TYPE: &'static str = "typed_join.decode_probe";
    }

    #[derive(Clone, Debug)]
    struct WorkGuard {
        calls: Arc<AtomicUsize>,
    }

    impl TypedJoinHandler for WorkGuard {
        type State = ();
        type ReferenceKey = String;
        type Reference = DecodeProbe;
        type Stream = TestStream;
        type Output = TestOutput;

        fn initial_state(&self) -> Self::State {}

        fn admit_reference(
            &self,
            reference: &Self::Reference,
        ) -> Result<Self::ReferenceKey, HandlerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(reference.key.clone())
        }

        fn process_stream(
            &self,
            _state: &mut Self::State,
            _references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
            _stream: Self::Stream,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }

    #[test]
    fn adapter_fails_closed_before_decode_or_handler_work_without_writer() {
        DECODE_CALLS.store(0, Ordering::SeqCst);
        let calls = Arc::new(AtomicUsize::new(0));
        let adapter = TypedJoinHandlerAdapter::new(WorkGuard {
            calls: calls.clone(),
        });
        let mut state = adapter.initial_state();
        let writer = WriterId::from(StageId::new());
        let event = ChainEventFactory::data_event(
            writer,
            DecodeProbe::versioned_event_type(),
            serde_json::json!({ "key": "k" }),
        );

        let error = adapter
            .process_reference(&mut state, event, StageId::new(), writer, live_scope())
            .expect_err("unbound adapter must fail closed");
        let HandlerError::Fatal(fatal) = error else {
            panic!("missing writer identity must be fatal")
        };
        assert_eq!(fatal.code, StageFatalCode::Configuration);
        assert_eq!(fatal.reason, StageFatalReason::ConfigurationInvariant);
        assert_eq!(DECODE_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[derive(Clone, Debug)]
    struct SamePayloadJoin;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct SamePayload {
        key: String,
    }

    impl TypedPayload for SamePayload {
        const EVENT_TYPE: &'static str = "typed_join.same_payload";
    }

    impl TypedJoinHandler for SamePayloadJoin {
        type State = ();
        type ReferenceKey = String;
        type Reference = SamePayload;
        type Stream = SamePayload;
        type Output = TestOutput;

        fn initial_state(&self) -> Self::State {}

        fn admit_reference(
            &self,
            reference: &Self::Reference,
        ) -> Result<Self::ReferenceKey, HandlerError> {
            Ok(reference.key.clone())
        }

        fn process_stream(
            &self,
            _state: &mut Self::State,
            references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
            stream: Self::Stream,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            Ok(vec![TestOutput {
                selected_values: references
                    .select(&stream.key)
                    .map(|value| vec![value.key])
                    .unwrap_or_default(),
                ordinal: 0,
            }])
        }
    }

    #[test]
    fn structural_branch_dispatch_works_when_both_sides_share_source_and_type() {
        let writer = WriterId::from(StageId::new());
        let shared_source = StageId::new();
        let adapter = configured_adapter(SamePayloadJoin);
        let mut state = adapter.initial_state();
        adapter
            .process_reference(
                &mut state,
                SamePayload {
                    key: "shared".to_string(),
                }
                .to_event(writer),
                shared_source,
                writer,
                live_scope(),
            )
            .expect("reference branch");
        let output = adapter
            .process_stream(
                &mut state,
                SamePayload {
                    key: "shared".to_string(),
                }
                .to_event(writer),
                shared_source,
                writer,
                live_scope(),
            )
            .expect("stream branch");
        assert_eq!(
            TestOutput::try_from_event(&output[0])
                .expect("typed output")
                .selected_values,
            vec!["shared".to_string()]
        );
    }

    #[derive(Clone, Debug)]
    struct ModeCounter {
        reads: Arc<AtomicUsize>,
    }

    impl TypedJoinHandler for ModeCounter {
        type State = ();
        type ReferenceKey = String;
        type Reference = TestReference;
        type Stream = TestStream;
        type Output = TestOutput;

        fn initial_state(&self) -> Self::State {}

        fn reference_mode(&self) -> JoinReferenceMode {
            self.reads.fetch_add(1, Ordering::SeqCst);
            JoinReferenceMode::Live
        }

        fn admit_reference(
            &self,
            reference: &Self::Reference,
        ) -> Result<Self::ReferenceKey, HandlerError> {
            Ok(reference.key.clone())
        }

        fn process_stream(
            &self,
            _state: &mut Self::State,
            _references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
            _stream: Self::Stream,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn reference_mode_is_sampled_once_and_then_cached() {
        let reads = Arc::new(AtomicUsize::new(0));
        let adapter = TypedJoinHandlerAdapter::new(ModeCounter {
            reads: reads.clone(),
        });
        assert_eq!(adapter.reference_mode(), JoinReferenceMode::Live);
        assert_eq!(adapter.reference_mode(), JoinReferenceMode::Live);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct OtherOutput {
        value: String,
    }

    impl TypedPayload for OtherOutput {
        const EVENT_TYPE: &'static str = "typed_join.other_output";
    }

    #[derive(Clone, Debug, StageOutputFacts)]
    struct DishonestProduct {
        first: TestOutput,
        second: OtherOutput,
    }

    impl OneFactStageOutput for DishonestProduct {}

    #[derive(Clone, Debug)]
    struct DishonestJoin;

    impl TypedJoinHandler for DishonestJoin {
        type State = ();
        type ReferenceKey = String;
        type Reference = TestReference;
        type Stream = TestStream;
        type Output = DishonestProduct;

        fn initial_state(&self) -> Self::State {}

        fn admit_reference(
            &self,
            reference: &Self::Reference,
        ) -> Result<Self::ReferenceKey, HandlerError> {
            Ok(reference.key.clone())
        }

        fn process_stream(
            &self,
            _state: &mut Self::State,
            _references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
            _stream: Self::Stream,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            Ok(vec![DishonestProduct {
                first: TestOutput {
                    selected_values: Vec::new(),
                    ordinal: 0,
                },
                second: OtherOutput {
                    value: "second".to_string(),
                },
            }])
        }
    }

    #[test]
    fn dishonest_one_fact_output_is_rejected_as_fatal_before_commit() {
        let writer = WriterId::from(StageId::new());
        let adapter = configured_adapter(DishonestJoin);
        let mut state = adapter.initial_state();
        let error = adapter
            .process_stream(
                &mut state,
                stream_event(writer, &[], 1, "stream"),
                StageId::new(),
                writer,
                live_scope(),
            )
            .expect_err("two facts violate OneFactStageOutput");
        let HandlerError::Fatal(fatal) = error else {
            panic!("dishonest output must become a fatal protocol error")
        };
        assert_eq!(fatal.code, StageFatalCode::Protocol);
        assert_eq!(fatal.reason, StageFatalReason::ProtocolInputIntegrity);
        assert!(fatal.detail.contains("one_fact_stage_output"));
    }

    #[test]
    fn strict_join_poison_stays_on_the_hidden_framework_lane() {
        let handler = StrictJoinBuilder::<TestReference, TestStream, TestOutput>::new()
            .catalog_key(|reference| reference.key.clone())
            .stream_key(|stream| stream.keys[0].clone())
            .build(|reference, _stream| TestOutput {
                selected_values: vec![reference.value],
                ordinal: 0,
            });
        let join_writer = WriterId::from(StageId::new());
        let stream_writer = WriterId::from(StageId::new());
        let mut adapter = TypedJoinHandlerAdapter::new(handler);
        adapter.install_writer_id(join_writer);
        let mut state = adapter.initial_state();
        let output = adapter
            .process_stream(
                &mut state,
                TestStream {
                    keys: vec!["missing".to_string()],
                    fanout: 1,
                }
                .to_event(stream_writer),
                StageId::new(),
                stream_writer,
                live_scope(),
            )
            .expect("strict miss emits framework poison");
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].writer_id, join_writer);
        assert!(matches!(
            &output[0].content,
            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                kind: EofKind::Poison,
                ..
            })
        ));
    }
}
