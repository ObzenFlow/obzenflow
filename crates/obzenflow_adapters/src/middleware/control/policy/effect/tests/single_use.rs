// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Cardinality, admission, and observation for single-use effect operations.

use super::support::*;
use crate::middleware::{EffectResilience, RateLimiter, RateLimiterBuilder};
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

use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, EffectfulTransformHandlerAdapter, UnifiedTransformHandler,
};

struct AppendOnlyJournal {
    id: JournalId,
    owner: JournalOwner,
    fail_append: bool,
}

impl AppendOnlyJournal {
    fn new(owner: JournalOwner, fail_append: bool) -> Self {
        Self {
            id: JournalId::new(),
            owner,
            fail_append,
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
        if self.fail_append {
            return Err(JournalError::Implementation {
                message: "injected transactional append failure".to_string(),
                source: "test journal rejected append".into(),
            });
        }
        Ok(EventEnvelope::new(JournalWriterId::from(self.id), event))
    }

    async fn append_group(
        &self,
        _group_id: &str,
        events: Vec<ChainEvent>,
        _parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        if self.fail_append {
            return Err(JournalError::Implementation {
                message: "injected transactional terminal-group failure".to_string(),
                source: "test journal rejected atomic group".into(),
            });
        }
        Ok(events
            .into_iter()
            .map(|event| EventEnvelope::new(JournalWriterId::from(self.id), event))
            .collect())
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct TransactionProbeInput;

impl TypedPayload for TransactionProbeInput {
    const EVENT_TYPE: &'static str = "test.transaction_probe_input";
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
    mode: TransactionProbeMode,
}

#[derive(Clone, Copy)]
enum TransactionProbeMode {
    CommitTimeout,
    AppendFailure,
    MissingCommit,
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
        match self.mode {
            TransactionProbeMode::CommitTimeout => {
                let error = EffectError::Timeout("single attempt".to_string());
                commit.commit_failure(&error).await?;
                Err(error)
            }
            TransactionProbeMode::AppendFailure => {
                let output = TransactionProbeOutput { value: 1 };
                commit.commit_success(&output).await?;
                Ok(output)
            }
            TransactionProbeMode::MissingCommit => Ok(TransactionProbeOutput { value: 1 }),
        }
    }
}

fn effect_context_with_boundary(
    stage_id: StageId,
    boundary: PerEffectPolicyBoundary,
    calls: Arc<AtomicUsize>,
    trace: Arc<Mutex<Vec<String>>>,
    mode: TransactionProbeMode,
) -> EffectInvocationContext {
    let writer_id = WriterId::from(stage_id);
    let parent = EventEnvelope::new(
        JournalWriterId::new(),
        ChainEventFactory::data_event(
            writer_id,
            TransactionProbeInput::versioned_event_type(),
            json!(TransactionProbeInput),
        ),
    );
    let mut effect_ports = EffectPortRegistry::new();
    effect_ports
        .insert::<dyn TransactionalEffectPort<TransactionProbe>>(
            "tx",
            Arc::new(TransactionProbePort { calls, trace, mode }),
        )
        .expect("transaction probe port registration is unique");

    EffectInvocationContext {
        flow_id: FlowId::new(),
        stage_id,
        stage_key: "single_use_effect_boundary".to_string(),
        writer_id,
        input_seq: StageInputPosition(1),
        lineage: obzenflow_core::config::LineagePolicy::default(),
        stage_logic_version: "test-v1".to_string(),
        data_journal: Arc::new(AppendOnlyJournal::new(
            JournalOwner::stage(stage_id),
            matches!(mode, TransactionProbeMode::AppendFailure),
        )),
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
        output_contract: StageOutputContract::empty(),
        backpressure_writer: BackpressureWriter::disabled(),
        emit_enabled: false,
        effect_boundary: Some(Arc::new(boundary)),
    }
}

#[derive(Clone, Debug)]
struct TransactionProbeHandler {
    terminal_error: Arc<Mutex<Option<EffectError>>>,
}

#[async_trait]
impl EffectfulTransformHandler for TransactionProbeHandler {
    type Input = TransactionProbeInput;
    type Output = TransactionProbeOutput;
    type AllowedEffects = obzenflow_runtime::effect_set![TransactionProbe];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        match fx.perform(TransactionProbe).await {
            Ok(_) => Ok(fx.complete()?),
            Err(error) => {
                *self.terminal_error.lock().unwrap() = Some(error);
                Err(HandlerError::Other(
                    "transaction probe reached its expected terminal error".to_string(),
                ))
            }
        }
    }
}

async fn invoke_with_boundary(
    stage_id: StageId,
    boundary: PerEffectPolicyBoundary,
    calls: Arc<AtomicUsize>,
    trace: Arc<Mutex<Vec<String>>>,
) -> EffectError {
    invoke_with_boundary_mode(
        stage_id,
        boundary,
        calls,
        trace,
        TransactionProbeMode::CommitTimeout,
    )
    .await
}

async fn invoke_with_boundary_mode(
    stage_id: StageId,
    boundary: PerEffectPolicyBoundary,
    calls: Arc<AtomicUsize>,
    trace: Arc<Mutex<Vec<String>>>,
    mode: TransactionProbeMode,
) -> EffectError {
    let context = effect_context_with_boundary(stage_id, boundary, calls, trace, mode);
    let input = context.parent.event.clone();
    let terminal_error = Arc::new(Mutex::new(None));
    let adapter = EffectfulTransformHandlerAdapter(TransactionProbeHandler {
        terminal_error: terminal_error.clone(),
    });

    let result = UnifiedTransformHandler::process(
        &adapter,
        input,
        Some(context),
        obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
    )
    .await;
    assert!(result.is_err(), "the transaction probe must fail");
    let terminal_error = terminal_error
        .lock()
        .unwrap()
        .take()
        .expect("handler must retain the terminal effect error");
    terminal_error
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
        PolicyAdmission::Reject(Box::new(MiddlewareAbortCause {
            source: EffectFailureSource::new("test.rejector"),
            code: EffectFailureCode::new("rejected"),
            message: "rejected before execution".to_string(),
            retry: RetryDisposition::NotRetryable,
            event: None,
        }))
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
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
    let error = invoke_with_boundary(StageId::new(), boundary, calls.clone(), trace.clone()).await;
    assert!(
        matches!(error, EffectError::BoundaryRejected { .. }),
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

#[tokio::test]
async fn effect_resilience_guards_transactional_calls_without_retrying_them() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(3)
            .open_for(Duration::from_secs(1))
            .build()
            .expect("transactional test breaker"),
    )
    .rate_limit_each_attempt(RateLimiter::per_second(100.0).unwrap())
    .build()
    .expect("transactional resilience configuration");
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let resilience = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Transactional,
    )
    .expect("breaker-only transactional resilience should materialize");
    let boundary = boundary_with_chain(vec![resilience]);

    let calls = Arc::new(AtomicUsize::new(0));
    let error = invoke_with_boundary(
        stage_id,
        boundary,
        calls.clone(),
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    assert!(matches!(error, EffectError::RecordedFailure { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    assert_eq!(breaker.len(), 1);
    let breaker = breaker[0].1();
    assert_eq!(breaker.requests_total, 1);
    assert_eq!(breaker.failures_total, 1);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 1);
}

#[tokio::test]
async fn transactional_missing_commit_consumes_attempt_without_health_sample() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .open_for(Duration::from_secs(1))
            .build()
            .expect("transactional test breaker"),
    )
    .build()
    .expect("transactional resilience configuration");
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let resilience = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Transactional,
    )
    .expect("transactional resilience should materialize");
    let boundary = boundary_with_chain(vec![resilience]);

    let calls = Arc::new(AtomicUsize::new(0));
    let error = invoke_with_boundary_mode(
        stage_id,
        boundary,
        calls.clone(),
        Arc::new(Mutex::new(Vec::new())),
        TransactionProbeMode::MissingCommit,
    )
    .await;
    assert!(matches!(
        error,
        EffectError::TransactionalCommitMissing { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let breaker = breaker[0].1();
    assert_eq!(breaker.requests_total, 1);
    assert_eq!(breaker.successes_total, 0);
    assert_eq!(breaker.failures_total, 0);
    assert!(matches!(
        breaker.state,
        obzenflow_runtime::control_plane::CircuitBreakerState::Closed
    ));
}

#[tokio::test]
async fn transactional_terminal_group_failure_preserves_physical_success_sample() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .open_for(Duration::from_secs(1))
            .build()
            .expect("transactional test breaker"),
    )
    .build()
    .expect("transactional resilience configuration");
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let resilience = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Transactional,
    )
    .expect("transactional resilience should materialize");
    let boundary = boundary_with_chain(vec![resilience]);

    let calls = Arc::new(AtomicUsize::new(0));
    let error = invoke_with_boundary_mode(
        stage_id,
        boundary,
        calls.clone(),
        Arc::new(Mutex::new(Vec::new())),
        TransactionProbeMode::AppendFailure,
    )
    .await;
    assert!(matches!(error, EffectError::Journal(_)), "{error:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let breaker = breaker[0].1();
    assert_eq!(breaker.requests_total, 1);
    assert_eq!(
        breaker.successes_total, 1,
        "terminal persistence is outside the protected transactional call and cannot reclassify its prepared success"
    );
    assert_eq!(breaker.failures_total, 0);
    assert!(matches!(
        breaker.state,
        obzenflow_runtime::control_plane::CircuitBreakerState::Closed
    ));
}

#[test]
fn effect_resilience_rejects_retry_for_transactional_effects_at_materialization() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(3)
            .build()
            .expect("transactional test breaker"),
    )
    .retry(Retry::fixed(Duration::from_millis(1)).max_attempts(2))
    .build()
    .expect("retry configuration is intrinsically valid");
    let config = test_stage_config(factory.as_ref());
    let control = Arc::new(ControlMiddlewareAggregator::new());

    let error = match materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Transactional,
    ) {
        Ok(_) => panic!("transactional effects cannot acquire retry authority"),
        Err(error) => error,
    };
    assert!(error.contains("retry is not eligible"), "{error}");
}

#[test]
fn effect_resilience_and_standalone_limiter_cannot_share_one_effect_key() {
    let resilience = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(3)
            .build()
            .expect("duplicate test breaker"),
    )
    .rate_limit_each_attempt(RateLimiter::per_second(100.0).unwrap())
    .build()
    .expect("duplicate test resilience");
    let standalone = RateLimiterBuilder::new(100.0).build();
    let config = test_stage_config_for_factories(&[resilience.as_ref(), standalone.as_ref()]);
    let control = Arc::new(ControlMiddlewareAggregator::new());

    materialize_effect_attachment(
        resilience.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Idempotent,
    )
    .expect("aggregate should claim the effect limiter key");
    let error = match materialize_effect_attachment(
        standalone.as_ref(),
        &config,
        &control,
        1,
        EffectSafety::Idempotent,
    ) {
        Ok(_) => panic!("a second effect limiter must fail"),
        Err(error) => error,
    };
    assert!(error.contains("already registered"), "{error}");
}
