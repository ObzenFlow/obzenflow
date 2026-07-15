// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Cardinality, admission, and observation for single-use effect operations.

use super::support::*;
use obzenflow_core::event::EventEnvelope;
use obzenflow_core::journal::{Journal, JournalError, JournalReader};
use obzenflow_core::{FlowId, JournalId, JournalOwner, JournalWriterId, TypedPayload};
use obzenflow_runtime::backpressure::BackpressureWriter;
use obzenflow_runtime::effects::{
    Effect, EffectCommitHandle, EffectContext, EffectDeclaration, EffectInvocationContext,
    EffectPortRegistry, Effects, TransactionalEffectPort,
};
use obzenflow_runtime::execution::{RuntimeExecution, RuntimeMode};
use obzenflow_runtime::feed_plan::StageOutputContract;
use obzenflow_runtime::messaging::upstream_subscription::StageInputPosition;
use serde_json::{json, Value};

struct AppendOnlyJournal {
    id: JournalId,
    owner: JournalOwner,
}

impl AppendOnlyJournal {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner,
        }
    }
}

struct EmptyJournalReader;

#[async_trait]
impl JournalReader<ChainEvent> for EmptyJournalReader {
    async fn next(&mut self) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(None)
    }

    fn position(&self) -> u64 {
        0
    }

    fn is_at_end(&self) -> bool {
        true
    }
}

#[async_trait]
impl Journal<ChainEvent> for AppendOnlyJournal {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        Some(&self.owner)
    }

    async fn append(
        &self,
        event: ChainEvent,
        _parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<EventEnvelope<ChainEvent>, JournalError> {
        Ok(EventEnvelope::new(JournalWriterId::from(self.id), event))
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::EventId,
    ) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(None)
    }

    async fn reader_from(
        &self,
        _position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Ok(Box::new(EmptyJournalReader))
    }

    async fn read_last_n(
        &self,
        _count: usize,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct TransactionProbeOutput {
    value: u64,
}

impl TypedPayload for TransactionProbeOutput {
    const EVENT_TYPE: &'static str = "test.transaction_probe_output";
}

#[derive(Clone, Debug)]
struct TransactionProbe;

#[async_trait]
impl Effect for TransactionProbe {
    const EFFECT_TYPE: &'static str = "effect.retry";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Transactional;

    type Outcome = TransactionProbeOutput;

    fn label(&self) -> &str {
        "transaction_probe"
    }

    fn canonical_input(&self) -> Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        panic!("transaction probe must execute through its transactional port")
    }
}

struct TransactionProbePort {
    calls: Arc<AtomicUsize>,
    trace: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl TransactionalEffectPort<TransactionProbe> for TransactionProbePort {
    async fn execute_and_commit(
        &self,
        _effect: TransactionProbe,
        _ctx: &mut EffectContext,
        commit: EffectCommitHandle<TransactionProbeOutput>,
    ) -> Result<TransactionProbeOutput, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.trace.lock().unwrap().push("execute".to_string());
        let error = EffectError::Timeout("single attempt".to_string());
        commit.commit_failure(&error).await?;
        Err(error)
    }
}

fn effects_with_boundary(
    stage_id: StageId,
    boundary: PerEffectPolicyBoundary,
    calls: Arc<AtomicUsize>,
    trace: Arc<Mutex<Vec<String>>>,
) -> Effects {
    let writer_id = WriterId::from(stage_id);
    let parent = EventEnvelope::new(
        JournalWriterId::new(),
        ChainEventFactory::data_event(writer_id, "test.input", json!({})),
    );
    let mut effect_ports = EffectPortRegistry::new();
    effect_ports.insert::<dyn TransactionalEffectPort<TransactionProbe>>(
        "tx",
        Arc::new(TransactionProbePort { calls, trace }),
    );

    Effects::new(EffectInvocationContext {
        flow_id: FlowId::new(),
        stage_id,
        stage_key: "single_use_effect_boundary".to_string(),
        writer_id,
        input_seq: StageInputPosition(1),
        lineage: obzenflow_core::config::LineagePolicy::default(),
        stage_logic_version: "test-v1".to_string(),
        data_journal: Arc::new(AppendOnlyJournal::new(JournalOwner::stage(stage_id))),
        flow_context: None,
        observers: None,
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        parent,
        effect_history: None,
        runtime_execution: RuntimeExecution::new(RuntimeMode::Live, None),
        effect_ports,
        effect_declarations: vec![EffectDeclaration::transactional_effect::<TransactionProbe>(
            "tx",
        )],
        synthesized_outcomes: Vec::new(),
        output_contract: StageOutputContract::empty(),
        backpressure_writer: BackpressureWriter::disabled(),
        emit_enabled: false,
        effect_boundary: Some(Arc::new(boundary)),
        boundary_control_events: Arc::new(Mutex::new(Vec::new())),
    })
}

struct TracingPolicy {
    label: &'static str,
    trace: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EffectPolicy for TracingPolicy {
    fn label(&self) -> &'static str {
        self.label
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        self.trace
            .lock()
            .unwrap()
            .push(format!("{}:admit", self.label));
        PolicyAdmission::Admit
    }

    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {
        let outcome = match attempt {
            EffectAttemptOutcome::Executed(Ok(_)) => "ok".to_string(),
            EffectAttemptOutcome::Executed(Err(_)) => "error".to_string(),
            EffectAttemptOutcome::SkippedBy(label) => format!("skipped:{label}"),
            EffectAttemptOutcome::RejectedBy(cause) => {
                format!("rejected:{}", cause.source)
            }
        };
        self.trace
            .lock()
            .unwrap()
            .push(format!("{}:observe:{outcome}", self.label));
    }
}

struct RejectingPolicy {
    trace: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EffectPolicy for RejectingPolicy {
    fn label(&self) -> &'static str {
        "test.rejector"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        self.trace
            .lock()
            .unwrap()
            .push("rejector:admit".to_string());
        PolicyAdmission::Reject(MiddlewareAbortCause {
            source: EffectFailureSource::new("test.rejector"),
            code: EffectFailureCode::new("rejected"),
            message: "rejected before execution".to_string(),
            retry: RetryDisposition::NotRetryable,
            event: None,
        })
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

#[tokio::test]
async fn single_use_operation_bypasses_recovery_and_observes_once_in_reverse_order() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let outer = EffectPolicyAttachment::neutral(Arc::new(TracingPolicy {
        label: "outer",
        trace: trace.clone(),
    }));
    let (breaker, control, stage_id) = retrying_breaker_fixture(
        CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .build(),
    );
    let inner = EffectPolicyAttachment::neutral(Arc::new(TracingPolicy {
        label: "inner",
        trace: trace.clone(),
    }));
    let boundary = boundary_with_chain(vec![outer, breaker, inner]);

    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = effects_with_boundary(stage_id, boundary, calls.clone(), trace.clone());
    let error = effects.perform(TransactionProbe).await;
    assert!(
        matches!(error, Err(EffectError::RecordedFailure { .. })),
        "the committed transactional failure must remain the terminal result, got {error:?}"
    );

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        trace.lock().unwrap().as_slice(),
        [
            "outer:admit",
            "inner:admit",
            "execute",
            "inner:observe:error",
            "outer:observe:error",
        ]
    );
    let snapshotters = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = snapshotters[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.failures_total, 1);
}

#[tokio::test]
async fn single_use_rejection_runs_no_effect_and_finalizes_admitted_policies() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let admitted = EffectPolicyAttachment::neutral(Arc::new(TracingPolicy {
        label: "admitted",
        trace: trace.clone(),
    }));
    let rejecting = EffectPolicyAttachment::neutral(Arc::new(RejectingPolicy {
        trace: trace.clone(),
    }));
    let boundary = boundary_with_chain(vec![admitted, rejecting]);

    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = effects_with_boundary(StageId::new(), boundary, calls.clone(), trace.clone());
    let error = effects.perform(TransactionProbe).await;
    assert!(
        matches!(error, Err(EffectError::BoundaryRejected { .. })),
        "the rejecting policy must remain the terminal result, got {error:?}"
    );

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        trace.lock().unwrap().as_slice(),
        [
            "admitted:admit",
            "rejector:admit",
            "admitted:observe:rejected:test.rejector",
        ]
    );
}
