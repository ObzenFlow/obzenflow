// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::{Arc, Mutex};

use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::{StageFatalCode, StageFatalReason, StageFatalRecorded};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, StageId, SystemId, TypedPayload, WriterId};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::__private::TypedJoinHandlerAdapter;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::stages::common::control_strategies::JonestownSignalStrategy;
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::common::handlers::{JoinReferenceView, TypedJoinHandler};
use obzenflow_runtime::stages::join::handle::JoinHandleExt;
use obzenflow_runtime::stages::join::{JoinBuilder, JoinConfig, JoinReferenceMode, JoinState};
use obzenflow_runtime::stages::resources_builder::StageResourcesBuilder;
use obzenflow_runtime::supervised_base::SupervisorBuilder;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};
use serde::{Deserialize, Serialize};

fn make_eof_event(writer: WriterId, seq: u64) -> ChainEvent {
    make_eof_event_with_kind(writer, seq, EofKind::Natural)
}

fn make_eof_event_with_kind(writer: WriterId, seq: u64, kind: EofKind) -> ChainEvent {
    let mut eof = ChainEventFactory::eof_event_with_kind(writer, kind);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        ref mut writer_id,
        ref mut writer_seq,
        ..
    }) = eof.content
    {
        *writer_id = Some(writer);
        *writer_seq = Some(SeqNo(seq));
    }
    eof
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogRow {
    key: String,
    value: String,
}

impl TypedPayload for CatalogRow {
    const EVENT_TYPE: &'static str = "test.catalog";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamRow {
    key: String,
}

impl TypedPayload for StreamRow {
    const EVENT_TYPE: &'static str = "test.stream";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct JoinedRow {
    value: String,
    key: String,
}

impl TypedPayload for JoinedRow {
    const EVENT_TYPE: &'static str = "test.joined";
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_processes_stream_without_reference_eof() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    let mut topo_builder = TopologyBuilder::new();
    topo_builder.add_stage_with_id(
        reference_stage.to_topology_id(),
        Some("reference".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        stream_stage.to_topology_id(),
        Some("stream".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        join_stage.to_topology_id(),
        Some("join".to_string()),
        TopologyStageType::Join,
    );
    topo_builder.reset_current();
    topo_builder.add_edge(
        reference_stage.to_topology_id(),
        join_stage.to_topology_id(),
    );
    topo_builder.add_edge(stream_stage.to_topology_id(), join_stage.to_topology_id());
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_path = tmp.path().join("reference.log");
    let stream_path = tmp.path().join("stream.log");
    let join_path = tmp.path().join("join.log");

    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(reference_path, JournalOwner::stage(reference_stage))
            .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(stream_path, JournalOwner::stage(stream_stage))
            .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(join_path, JournalOwner::stage(join_stage))
            .expect("create join disk journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        join_stage,
        join_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let mut error_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    for (id, journal) in stage_journals.iter() {
        error_journals.insert(*id, journal.clone());
    }

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits data but never EOF.
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(
            CatalogRow {
                key: "k1".into(),
                value: "v1".into(),
            }
            .to_event(reference_writer),
            None,
        )
        .await
        .expect("append reference data");

    // Stream emits one matching record and EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamRow { key: "k1".into() }.to_event(stream_writer), None)
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
        .await
        .expect("append stream eof");

    let mut resources_set = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let handler =
        obzenflow_runtime::stages::join::InnerJoinBuilder::<CatalogRow, StreamRow, JoinedRow>::new(
        )
        .catalog_key(|c| c.key.clone())
        .stream_key(|s| s.key.clone())
        .build(|catalog, stream| JoinedRow {
            value: catalog.value,
            key: stream.key,
        });

    let control = Arc::new(JonestownSignalStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_no_ref_eof",
        "live_reference_join",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        TypedJoinHandlerAdapter::new(handler),
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
    )
    .expect("build join builder")
    .build()
    .await
    .expect("build join supervisor");

    handle.initialize().await.expect("initialize join");
    handle.ready().await.expect("ready join");

    handle
        .wait_for_completion()
        .await
        .expect("join supervisor should complete");

    let events = join_journal
        .read_causally_ordered()
        .await
        .expect("read join journal");
    let joined_env = events
        .iter()
        .find(|env| JoinedRow::from_event(&env.event).is_some())
        .expect("joined output envelope present");

    // FLOWIP-071h: fan-in outputs must preserve ancestry from both contributors.
    let reference_key = WriterId::from(reference_stage).to_string();
    let stream_key = WriterId::from(stream_stage).to_string();
    assert_ne!(
        joined_env.vector_clock.get(&reference_key),
        0,
        "joined output vector clock must include reference writer ancestry"
    );
    assert_ne!(
        joined_env.vector_clock.get(&stream_key),
        0,
        "joined output vector clock must include stream writer ancestry"
    );
    let joined: Vec<JoinedRow> = events
        .iter()
        .filter_map(|env| JoinedRow::from_event(&env.event))
        .collect();

    assert_eq!(
        joined,
        vec![JoinedRow {
            value: "v1".into(),
            key: "k1".into()
        }]
    );
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefEvent {
    id: u64,
}

impl TypedPayload for RefEvent {
    const EVENT_TYPE: &'static str = "test.ref";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamEvent {
    id: u64,
}

impl TypedPayload for StreamEvent {
    const EVENT_TYPE: &'static str = "test.stream_event";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StreamObservedRefs {
    refs_seen: usize,
}

impl TypedPayload for StreamObservedRefs {
    const EVENT_TYPE: &'static str = "test.stream_observed_refs";
}

#[derive(Clone, Debug)]
struct CountReferenceEventsJoin;

impl TypedJoinHandler for CountReferenceEventsJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = RefEvent;
    type Stream = StreamEvent;
    type Output = StreamObservedRefs;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::Live
    }

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        Ok(reference.id)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, u64, RefEvent>,
        _stream: StreamEvent,
    ) -> Result<Vec<StreamObservedRefs>, HandlerError> {
        let refs_seen = (0..1024u64)
            .filter(|id| references.select(id).is_some())
            .count();
        Ok(vec![StreamObservedRefs { refs_seen }])
    }
}

#[derive(Clone, Debug)]
struct EmitOnStreamEofJoin;

impl TypedJoinHandler for EmitOnStreamEofJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = RefEvent;
    type Stream = StreamEvent;
    type Output = StreamObservedRefs;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::Live
    }

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        Ok(reference.id)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u64, RefEvent>,
        _stream: StreamEvent,
    ) -> Result<Vec<StreamObservedRefs>, HandlerError> {
        Ok(Vec::new())
    }

    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, u64, RefEvent>,
    ) -> Result<Vec<StreamObservedRefs>, HandlerError> {
        let refs_seen = (0..1024u64)
            .filter(|id| references.select(id).is_some())
            .count();
        Ok(vec![StreamObservedRefs { refs_seen }])
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TerminalStep {
    step: String,
}

impl TypedPayload for TerminalStep {
    const EVENT_TYPE: &'static str = "test.join_terminal_step";
}

#[derive(Clone, Debug)]
struct TerminalOrderJoin {
    mode: JoinReferenceMode,
    calls: Arc<Mutex<Vec<String>>>,
}

impl TypedJoinHandler for TerminalOrderJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = RefEvent;
    type Stream = StreamEvent;
    type Output = TerminalStep;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> JoinReferenceMode {
        self.mode
    }

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        Ok(reference.id)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u64, RefEvent>,
        _stream: StreamEvent,
    ) -> Result<Vec<TerminalStep>, HandlerError> {
        Ok(Vec::new())
    }

    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, u64, RefEvent>,
    ) -> Result<Vec<TerminalStep>, HandlerError> {
        self.calls
            .lock()
            .expect("terminal calls lock")
            .push("hook".to_string());
        let _selected = references.select(&0);
        Ok(vec![TerminalStep {
            step: "hook".to_string(),
        }])
    }

    fn drain(
        &self,
        _state: &Self::State,
        references: &mut JoinReferenceView<'_, u64, RefEvent>,
    ) -> Result<Vec<TerminalStep>, HandlerError> {
        self.calls
            .lock()
            .expect("terminal calls lock")
            .push("drain".to_string());
        let _selected = references.select(&0);
        Ok(vec![TerminalStep {
            step: "drain".to_string(),
        }])
    }
}

async fn run_typed_join_case<H>(
    mode: JoinReferenceMode,
    handler: H,
    reference_data: bool,
    reference_eof_kind: Option<EofKind>,
    stream_data: bool,
    stream_eof_kind: Option<EofKind>,
    direct_drain: bool,
) -> (Result<(), String>, bool, Vec<ChainEvent>, WriterId)
where
    H: TypedJoinHandler<Reference = RefEvent, Stream = StreamEvent>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let flow_id = FlowId::new();
    let system_id = SystemId::new();
    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    let mut topology = TopologyBuilder::new();
    for (stage, name, stage_type) in [
        (
            reference_stage,
            "reference",
            TopologyStageType::FiniteSource,
        ),
        (stream_stage, "stream", TopologyStageType::FiniteSource),
        (join_stage, "join", TopologyStageType::Join),
    ] {
        topology.add_stage_with_id(stage.to_topology_id(), Some(name.to_string()), stage_type);
        topology.reset_current();
    }
    topology.add_edge(
        reference_stage.to_topology_id(),
        join_stage.to_topology_id(),
    );
    topology.add_edge(stream_stage.to_topology_id(), join_stage.to_topology_id());
    let topology = Arc::new(topology.build_unchecked().expect("build terminal topology"));

    let temp = tempfile::tempdir().expect("terminal tempdir");
    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            temp.path().join("reference.log"),
            JournalOwner::stage(reference_stage),
        )
        .expect("reference journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            temp.path().join("stream.log"),
            JournalOwner::stage(stream_stage),
        )
        .expect("stream journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            temp.path().join("join.log"),
            JournalOwner::stage(join_stage),
        )
        .expect("join journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(reference_stage, reference_journal.clone());
    stage_journals.insert(stream_stage, stream_journal.clone());
    stage_journals.insert(join_stage, join_journal.clone());
    let error_journals = stage_journals.clone();
    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            temp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("system journal"),
    );

    let reference_writer = WriterId::from(reference_stage);
    if reference_data {
        reference_journal
            .append(RefEvent { id: 0 }.to_event(reference_writer), None)
            .await
            .expect("reference data");
    }
    if let Some(kind) = reference_eof_kind {
        reference_journal
            .append(make_eof_event_with_kind(reference_writer, 1, kind), None)
            .await
            .expect("reference eof");
    }

    let stream_writer = WriterId::from(stream_stage);
    if stream_data {
        stream_journal
            .append(StreamEvent { id: 1 }.to_event(stream_writer), None)
            .await
            .expect("stream data");
    }
    if let Some(kind) = stream_eof_kind {
        stream_journal
            .append(make_eof_event_with_kind(stream_writer, 1, kind), None)
            .await
            .expect("stream eof");
    }

    let mut resources = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("terminal resources");
    let join_resources = resources
        .take_stage_resources(join_stage)
        .expect("terminal join resources");

    let control = Arc::new(JonestownSignalStrategy);
    let mut config = JoinConfig::new(
        join_stage,
        "typed_terminal_order",
        "live_reference_join",
        reference_stage,
        stream_stage,
    );
    config.reference_mode = mode;
    config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        TypedJoinHandlerAdapter::new(handler),
        config,
        join_resources,
        reference_journal,
        vec![(stream_stage, stream_journal)],
        control,
    )
    .expect("terminal join builder")
    .build()
    .await
    .expect("terminal join supervisor");
    handle.initialize().await.expect("initialize terminal join");
    handle.ready().await.expect("ready terminal join");
    if direct_drain {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if matches!(
                    handle.current_state(),
                    JoinState::Live | JoinState::Enriching
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("join reaches a state that accepts direct drain");
        handle.begin_drain().await.expect("begin direct drain");
    }
    let state_receiver = handle.state_receiver();
    let completion = handle
        .wait_for_completion()
        .await
        .map_err(|error| error.to_string());
    let final_state = state_receiver.borrow().clone();
    let failed = matches!(final_state, JoinState::Failed(_));

    let events = join_journal
        .read_causally_ordered()
        .await
        .expect("terminal join journal")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect();
    (completion, failed, events, WriterId::from(join_stage))
}

async fn run_terminal_finalizer_case(
    mode: JoinReferenceMode,
    stream_eof_kind: Option<EofKind>,
) -> (Vec<String>, Vec<ChainEvent>, WriterId) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler = TerminalOrderJoin {
        mode,
        calls: calls.clone(),
    };
    let reference_eof_kind = (mode == JoinReferenceMode::FiniteEof).then_some(EofKind::Natural);
    let direct_drain = stream_eof_kind.is_none();
    let (completion, failed, events, writer_id) = run_typed_join_case(
        mode,
        handler,
        true,
        reference_eof_kind,
        true,
        stream_eof_kind,
        direct_drain,
    )
    .await;
    completion.expect("terminal join completion");
    assert!(!failed, "terminal join must drain successfully");
    let calls = calls.lock().expect("terminal calls lock").clone();
    (calls, events, writer_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_terminal_hooks_follow_the_mode_and_eof_matrix() {
    for mode in [JoinReferenceMode::FiniteEof, JoinReferenceMode::Live] {
        for kind in [EofKind::Natural, EofKind::Poison] {
            let (calls, events, join_writer) = run_terminal_finalizer_case(mode, Some(kind)).await;
            assert_eq!(calls, vec!["hook", "drain"], "mode={mode:?}, kind={kind:?}");
            let steps = events
                .iter()
                .filter_map(TerminalStep::from_event)
                .map(|step| step.step)
                .collect::<Vec<_>>();
            assert_eq!(steps, vec!["hook", "drain"]);

            let drain_index = events
                .iter()
                .position(|event| {
                    TerminalStep::from_event(event).is_some_and(|step| step.step == "drain")
                })
                .expect("drain fact");
            let stage_eof_index = events
                .iter()
                .position(|event| event.writer_id == join_writer && event.is_eof())
                .expect("join-authored stage EOF");
            assert!(
                drain_index < stage_eof_index,
                "terminal facts precede stage EOF"
            );
        }

        let (calls, events, _join_writer) =
            run_terminal_finalizer_case(mode, Some(EofKind::Truncated)).await;
        assert!(
            calls.is_empty(),
            "Truncated suppresses hook and drain in {mode:?}"
        );
        assert!(events
            .iter()
            .all(|event| TerminalStep::from_event(event).is_none()));

        let (calls, events, _join_writer) = run_terminal_finalizer_case(mode, None).await;
        assert_eq!(
            calls,
            vec!["drain"],
            "direct drain has no EOF hook in {mode:?}"
        );
        let steps = events
            .iter()
            .filter_map(TerminalStep::from_event)
            .map(|step| step.step)
            .collect::<Vec<_>>();
        assert_eq!(steps, vec!["drain"]);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorPosition {
    AdmitReference,
    ProcessStream,
    OnStreamEof,
    Drain,
}

#[derive(Clone, Debug)]
struct ErrorMatrixJoin {
    mode: JoinReferenceMode,
    position: ErrorPosition,
    fatal: bool,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl ErrorMatrixJoin {
    fn result<T>(&self, position: ErrorPosition, value: T) -> Result<T, HandlerError> {
        if self.position != position {
            return Ok(value);
        }
        if self.fatal {
            Err(HandlerError::Fatal(StageFatal::new(
                StageFatalCode::Protocol,
                StageFatalReason::ProtocolInputIntegrity,
                format!("typed join fatal at {position:?}"),
            )))
        } else {
            Err(HandlerError::Domain(format!(
                "typed join non-fatal at {position:?}"
            )))
        }
    }

    fn record(&self, call: &'static str) {
        self.calls.lock().expect("error calls lock").push(call);
    }
}

impl TypedJoinHandler for ErrorMatrixJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = RefEvent;
    type Stream = StreamEvent;
    type Output = TerminalStep;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> JoinReferenceMode {
        self.mode
    }

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        self.record("admit_reference");
        self.result(ErrorPosition::AdmitReference, reference.id)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u64, RefEvent>,
        _stream: StreamEvent,
    ) -> Result<Vec<TerminalStep>, HandlerError> {
        self.record("process_stream");
        self.result(ErrorPosition::ProcessStream, Vec::new())
    }

    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u64, RefEvent>,
    ) -> Result<Vec<TerminalStep>, HandlerError> {
        self.record("on_stream_eof");
        self.result(ErrorPosition::OnStreamEof, Vec::new())
    }

    fn drain(
        &self,
        _state: &Self::State,
        _references: &mut JoinReferenceView<'_, u64, RefEvent>,
    ) -> Result<Vec<TerminalStep>, HandlerError> {
        self.record("drain");
        self.result(ErrorPosition::Drain, Vec::new())
    }
}

fn error_matrix_handler(
    mode: JoinReferenceMode,
    position: ErrorPosition,
    fatal: bool,
) -> (ErrorMatrixJoin, Arc<Mutex<Vec<&'static str>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    (
        ErrorMatrixJoin {
            mode,
            position,
            fatal,
            calls: calls.clone(),
        },
        calls,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_join_errors_follow_the_locked_mode_matrix() {
    for mode in [JoinReferenceMode::FiniteEof, JoinReferenceMode::Live] {
        let reference_eof = (mode == JoinReferenceMode::FiniteEof).then_some(EofKind::Natural);

        let (handler, calls) = error_matrix_handler(mode, ErrorPosition::AdmitReference, false);
        let (completion, failed, events, _writer) = run_typed_join_case(
            mode,
            handler,
            true,
            reference_eof,
            true,
            Some(EofKind::Natural),
            false,
        )
        .await;
        match mode {
            JoinReferenceMode::FiniteEof => {
                assert!(failed, "finite admission error is terminal")
            }
            JoinReferenceMode::Live => {
                completion.expect("live admission error remains per-record");
                assert!(!failed, "live admission error remains per-record");
            }
        }
        assert_eq!(
            calls
                .lock()
                .expect("error calls lock")
                .iter()
                .filter(|call| **call == "admit_reference")
                .count(),
            1
        );
        assert!(events
            .iter()
            .all(|event| StageFatalRecorded::try_from_event(event).is_err()));

        for position in [
            ErrorPosition::AdmitReference,
            ErrorPosition::ProcessStream,
            ErrorPosition::OnStreamEof,
            ErrorPosition::Drain,
        ] {
            let (handler, _calls) = error_matrix_handler(mode, position, true);
            let (_completion, failed, events, _writer) = run_typed_join_case(
                mode,
                handler,
                true,
                reference_eof,
                true,
                Some(EofKind::Natural),
                false,
            )
            .await;
            assert!(failed, "Fatal from {position:?} must fail a {mode:?} join");
            let fatal = events
                .iter()
                .filter_map(|event| StageFatalRecorded::try_from_event(event).ok())
                .collect::<Vec<_>>();
            assert_eq!(
                fatal.len(),
                1,
                "Fatal from {position:?} must have one durable record in {mode:?}"
            );
            assert_eq!(fatal[0].code, StageFatalCode::Protocol);
            assert_eq!(fatal[0].reason, StageFatalReason::ProtocolInputIntegrity);
            assert!(fatal[0].detail.contains(&format!("{position:?}")));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_on_source_eof_outputs_carry_reference_and_stream_ancestry() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    let mut topo_builder = TopologyBuilder::new();
    topo_builder.add_stage_with_id(
        reference_stage.to_topology_id(),
        Some("reference".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        stream_stage.to_topology_id(),
        Some("stream".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        join_stage.to_topology_id(),
        Some("join".to_string()),
        TopologyStageType::Join,
    );
    topo_builder.reset_current();
    topo_builder.add_edge(
        reference_stage.to_topology_id(),
        join_stage.to_topology_id(),
    );
    topo_builder.add_edge(stream_stage.to_topology_id(), join_stage.to_topology_id());
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_path = tmp.path().join("reference.log");
    let stream_path = tmp.path().join("stream.log");
    let join_path = tmp.path().join("join.log");

    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(reference_path, JournalOwner::stage(reference_stage))
            .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(stream_path, JournalOwner::stage(stream_stage))
            .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(join_path, JournalOwner::stage(join_stage))
            .expect("create join disk journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        join_stage,
        join_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let mut error_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    for (id, journal) in stage_journals.iter() {
        error_journals.insert(*id, journal.clone());
    }

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits data, never EOF (Live mode).
    let reference_writer = WriterId::from(reference_stage);
    for i in 0..2u64 {
        reference_journal
            .append(RefEvent { id: i }.to_event(reference_writer), None)
            .await
            .expect("append reference data");
    }

    // Stream emits one record + EOF. Handler emits only during on_source_eof().
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamEvent { id: 1 }.to_event(stream_writer), None)
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
        .await
        .expect("append stream eof");

    let mut resources_set = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let control = Arc::new(JonestownSignalStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_on_source_eof_ancestry",
        "live_reference_join",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        TypedJoinHandlerAdapter::new(EmitOnStreamEofJoin),
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
    )
    .expect("build join builder")
    .build()
    .await
    .expect("build join supervisor");

    handle.initialize().await.expect("initialize join");
    handle.ready().await.expect("ready join");

    handle
        .wait_for_completion()
        .await
        .expect("join supervisor should complete");

    let events = join_journal
        .read_causally_ordered()
        .await
        .expect("read join journal");
    let observed_env = events
        .iter()
        .find(|env| StreamObservedRefs::from_event(&env.event).is_some())
        .expect("expected an on_source_eof output event");
    let observed =
        StreamObservedRefs::from_event(&observed_env.event).expect("parse observed payload");

    assert_eq!(observed, StreamObservedRefs { refs_seen: 2 });

    // FLOWIP-071h: outputs emitted at merge boundaries must preserve ancestry from all contributors.
    let reference_key = reference_writer.to_string();
    let stream_key = stream_writer.to_string();
    assert_ne!(
        observed_env.vector_clock.get(&reference_key),
        0,
        "on_source_eof output vector clock must include reference writer ancestry"
    );
    assert_ne!(
        observed_env.vector_clock.get(&stream_key),
        0,
        "on_source_eof output vector clock must include stream writer ancestry"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_reference_batch_cap_prevents_stream_starvation() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    let mut topo_builder = TopologyBuilder::new();
    topo_builder.add_stage_with_id(
        reference_stage.to_topology_id(),
        Some("reference".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        stream_stage.to_topology_id(),
        Some("stream".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        join_stage.to_topology_id(),
        Some("join".to_string()),
        TopologyStageType::Join,
    );
    topo_builder.reset_current();
    topo_builder.add_edge(
        reference_stage.to_topology_id(),
        join_stage.to_topology_id(),
    );
    topo_builder.add_edge(stream_stage.to_topology_id(), join_stage.to_topology_id());
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_path = tmp.path().join("reference.log");
    let stream_path = tmp.path().join("stream.log");
    let join_path = tmp.path().join("join.log");

    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(reference_path, JournalOwner::stage(reference_stage))
            .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(stream_path, JournalOwner::stage(stream_stage))
            .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(join_path, JournalOwner::stage(join_stage))
            .expect("create join disk journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        join_stage,
        join_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let mut error_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    for (id, journal) in stage_journals.iter() {
        error_journals.insert(*id, journal.clone());
    }

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits many records, never EOF.
    let reference_writer = WriterId::from(reference_stage);
    for i in 0..10u64 {
        reference_journal
            .append(RefEvent { id: i }.to_event(reference_writer), None)
            .await
            .expect("append reference data");
    }

    // Stream emits one record + EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamEvent { id: 1 }.to_event(stream_writer), None)
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
        .await
        .expect("append stream eof");

    let mut resources_set = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let control = Arc::new(JonestownSignalStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_batch_cap",
        "live_reference_join",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.reference_batch_cap = Some(3);
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        TypedJoinHandlerAdapter::new(CountReferenceEventsJoin),
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
    )
    .expect("build join builder")
    .build()
    .await
    .expect("build join supervisor");

    handle.initialize().await.expect("initialize join");
    handle.ready().await.expect("ready join");

    handle
        .wait_for_completion()
        .await
        .expect("join supervisor should complete");

    let events = join_journal
        .read_causally_ordered()
        .await
        .expect("read join journal");
    let observed: Vec<StreamObservedRefs> = events
        .iter()
        .filter_map(|env| StreamObservedRefs::from_event(&env.event))
        .collect();

    assert_eq!(observed, vec![StreamObservedRefs { refs_seen: 3 }]);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_forwards_reference_eof() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    let mut topo_builder = TopologyBuilder::new();
    topo_builder.add_stage_with_id(
        reference_stage.to_topology_id(),
        Some("reference".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        stream_stage.to_topology_id(),
        Some("stream".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        join_stage.to_topology_id(),
        Some("join".to_string()),
        TopologyStageType::Join,
    );
    topo_builder.reset_current();
    topo_builder.add_edge(
        reference_stage.to_topology_id(),
        join_stage.to_topology_id(),
    );
    topo_builder.add_edge(stream_stage.to_topology_id(), join_stage.to_topology_id());
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("reference.log"),
            JournalOwner::stage(reference_stage),
        )
        .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("stream.log"),
            JournalOwner::stage(stream_stage),
        )
        .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(tmp.path().join("join.log"), JournalOwner::stage(join_stage))
            .expect("create join disk journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        join_stage,
        join_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let mut error_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    for (id, journal) in stage_journals.iter() {
        error_journals.insert(*id, journal.clone());
    }

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits data + EOF (in Live mode EOF should be forwarded but not drive completion).
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(
            CatalogRow {
                key: "k1".into(),
                value: "v1".into(),
            }
            .to_event(reference_writer),
            None,
        )
        .await
        .expect("append reference data");
    reference_journal
        .append(make_eof_event(reference_writer, 1), None)
        .await
        .expect("append reference eof");

    // Stream emits one matching record and EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamRow { key: "k1".into() }.to_event(stream_writer), None)
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
        .await
        .expect("append stream eof");

    let mut resources_set = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let handler =
        obzenflow_runtime::stages::join::InnerJoinBuilder::<CatalogRow, StreamRow, JoinedRow>::new(
        )
        .catalog_key(|c| c.key.clone())
        .stream_key(|s| s.key.clone())
        .build(|catalog, stream| JoinedRow {
            value: catalog.value,
            key: stream.key,
        });

    let control = Arc::new(JonestownSignalStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_reference_eof_forwarded",
        "live_reference_join",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        TypedJoinHandlerAdapter::new(handler),
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
    )
    .expect("build join builder")
    .build()
    .await
    .expect("build join supervisor");

    handle.initialize().await.expect("initialize join");
    handle.ready().await.expect("ready join");

    handle
        .wait_for_completion()
        .await
        .expect("join supervisor should complete");

    let events = join_journal
        .read_causally_ordered()
        .await
        .expect("read join journal");

    let joined: Vec<JoinedRow> = events
        .iter()
        .filter_map(|env| JoinedRow::from_event(&env.event))
        .collect();
    assert_eq!(
        joined,
        vec![JoinedRow {
            value: "v1".into(),
            key: "k1".into()
        }]
    );

    let saw_reference_eof = events.iter().any(|env| {
        env.event.writer_id == reference_writer
            && matches!(
                &env.event.content,
                ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
            )
    });
    assert!(saw_reference_eof, "expected reference EOF to be forwarded");
}

#[derive(Clone, Debug)]
struct ReferenceErrorJoin;

impl TypedJoinHandler for ReferenceErrorJoin {
    type State = ();
    type ReferenceKey = String;
    type Reference = CatalogRow;
    type Stream = StreamRow;
    type Output = JoinedRow;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::Live
    }

    fn admit_reference(&self, _reference: &Self::Reference) -> Result<String, HandlerError> {
        Err(HandlerError::Remote("boom".to_string()))
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, String, CatalogRow>,
        stream: StreamRow,
    ) -> Result<Vec<JoinedRow>, HandlerError> {
        Ok(vec![JoinedRow {
            value: "ok".into(),
            key: stream.key,
        }])
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_reference_errors_are_per_record() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    let mut topo_builder = TopologyBuilder::new();
    topo_builder.add_stage_with_id(
        reference_stage.to_topology_id(),
        Some("reference".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        stream_stage.to_topology_id(),
        Some("stream".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        join_stage.to_topology_id(),
        Some("join".to_string()),
        TopologyStageType::Join,
    );
    topo_builder.reset_current();
    topo_builder.add_edge(
        reference_stage.to_topology_id(),
        join_stage.to_topology_id(),
    );
    topo_builder.add_edge(stream_stage.to_topology_id(), join_stage.to_topology_id());
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("reference.log"),
            JournalOwner::stage(reference_stage),
        )
        .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("stream.log"),
            JournalOwner::stage(stream_stage),
        )
        .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(tmp.path().join("join.log"), JournalOwner::stage(join_stage))
            .expect("create join disk journal"),
    );
    let join_error_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("join_error.log"),
            JournalOwner::stage(join_stage),
        )
        .expect("create join error disk journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        join_stage,
        join_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let mut error_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    error_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    error_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    error_journals.insert(
        join_stage,
        join_error_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits one record that will fail the handler.
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(
            CatalogRow {
                key: "k1".into(),
                value: "v1".into(),
            }
            .to_event(reference_writer),
            None,
        )
        .await
        .expect("append reference data");

    // Stream emits one record + EOF (must still be processed).
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamRow { key: "k1".into() }.to_event(stream_writer), None)
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
        .await
        .expect("append stream eof");

    let mut resources_set = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let control = Arc::new(JonestownSignalStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_reference_error_per_record",
        "live_reference_join",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        TypedJoinHandlerAdapter::new(ReferenceErrorJoin),
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
    )
    .expect("build join builder")
    .build()
    .await
    .expect("build join supervisor");

    handle.initialize().await.expect("initialize join");
    handle.ready().await.expect("ready join");

    handle
        .wait_for_completion()
        .await
        .expect("join supervisor should complete");

    let output_events = join_journal
        .read_causally_ordered()
        .await
        .expect("read join journal");
    let joined: Vec<JoinedRow> = output_events
        .iter()
        .filter_map(|env| JoinedRow::from_event(&env.event))
        .collect();
    assert_eq!(
        joined,
        vec![JoinedRow {
            value: "ok".into(),
            key: "k1".into()
        }]
    );

    let error_events = join_error_journal
        .read_causally_ordered()
        .await
        .expect("read join error journal");
    let saw_reference_error = error_events.iter().any(|env| {
        env.event.writer_id == reference_writer
            && CatalogRow::from_event(&env.event).is_some()
            && matches!(
                env.event.processing_info.status,
                ProcessingStatus::Error { .. }
            )
    });
    assert!(
        saw_reference_error,
        "expected reference error event in error journal"
    );
}
