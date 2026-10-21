// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::execution::{RuntimeExecution, RuntimeMode};
use obzenflow_core::event::CausalFrontier;
use std::sync::atomic::AtomicBool;

const STAGE_KEY: &str = "effect_stage";

struct WindowBoundary {
    invariant: InvariantEvidenceBoundary,
}

#[async_trait]
impl EffectBoundary for WindowBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        operation.execute().await.into_report(Vec::new())
    }

    async fn around_affine_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: AffineEffectOperation,
    ) -> AffineEffectBoundaryReport {
        if identity.effect_type == InvariantAffineEffect::EFFECT_TYPE {
            return self
                .invariant
                .around_affine_effect(identity, event, operation)
                .await;
        }
        operation.execute().await.into_report(Vec::new())
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Repeatable,
    Affine,
    EscapeControls,
    Capture,
    Transactional,
}

const ACTIONS: [Action; 5] = [
    Action::Repeatable,
    Action::Affine,
    Action::EscapeControls,
    Action::Capture,
    Action::Transactional,
];

struct Window {
    journal: Arc<MemoryJournal<ChainEvent>>,
    erased: Arc<dyn Journal<ChainEvent>>,
    history: CurrentRunHistory,
    execution: RuntimeExecution,
    flow_id: FlowId,
    stage_id: StageId,
    ledger: System<dyn TransactionalLedger>,
    transactional: EffectDeclaration,
    ports: EffectPortRegistry,
    calls: Arc<AtomicUsize>,
    injected_failure: bool,
}

impl Window {
    fn new() -> Self {
        let stage_id = StageId::new();
        Self::over(
            MemoryJournal::new(JournalOwner::stage(stage_id)),
            stage_id,
            false,
        )
    }

    fn failing_first_outcome() -> Self {
        let stage_id = StageId::new();
        Self::over(
            MemoryJournal::failing_group(JournalOwner::stage(stage_id), "effect-outcome:v1:"),
            stage_id,
            true,
        )
    }

    fn over(journal: MemoryJournal<ChainEvent>, stage_id: StageId, injected_failure: bool) -> Self {
        let journal = Arc::new(journal);
        let erased: Arc<dyn Journal<ChainEvent>> = journal.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let ledger = transactional_ledger(Arc::new(TransactionalCountingPort {
            calls: calls.clone(),
            commit: true,
        }));
        let transactional = transactional_declaration(&ledger);
        let ports = fixtures()
            .registry
            .merged_with(&EffectPortRegistry::collect_from_declarations([&transactional]).unwrap());
        Self {
            history: CurrentRunHistory::new(erased.clone()),
            journal,
            erased,
            execution: RuntimeExecution::new(RuntimeMode::Live, None),
            flow_id: FlowId::new(),
            stage_id,
            ledger,
            transactional,
            ports,
            calls,
            injected_failure,
        }
    }

    fn context(&self, input: u64) -> EffectInvocationContext {
        EffectInvocationContext {
            flow_id: self.flow_id,
            stage_id: self.stage_id,
            stage_key: STAGE_KEY.to_string(),
            writer_id: WriterId::from(self.stage_id),
            input_seq: StageInputPosition(input),
            lineage: obzenflow_core::config::LineagePolicy::default(),
            stage_logic_version: "test-v1".to_string(),
            data_journal: self.erased.clone(),
            flow_context: None,
            observers: None,
            instrumentation: None,
            heartbeat_state: None,
            parent: parent_envelope(WriterId::from(self.stage_id)).into(),
            effect_history: None,
            current_run_history: self.history.clone(),
            runtime_execution: self.execution.clone(),
            effect_ports: self.ports.clone(),
            effect_declarations: vec![
                EffectDeclaration::named(&fixtures().counting),
                EffectDeclaration::named_at_least_once(&fixtures().affine_counting),
                EffectDeclaration::named_at_least_once(&fixtures().invariant_affine),
                self.transactional.clone(),
            ],
            output_contract: StageOutputContract::empty(),
            backpressure_writer: BackpressureWriter::disabled(),
            emit_enabled: false,
            effect_boundary: Some(Arc::new(WindowBoundary {
                invariant: InvariantEvidenceBoundary {
                    include_preterminal: true,
                    breaker_free: false,
                    consults: Arc::new(AtomicUsize::new(0)),
                },
            })),
            catch_up: None,
        }
    }

    fn cursor(&self, input: u64, ordinal: u32) -> EffectCursor {
        EffectCursor::new(
            self.flow_id.to_string(),
            STAGE_KEY,
            input,
            EffectOrdinal::new(ordinal),
        )
    }

    async fn perform(&self, core: &mut EffectsCore, input: u64, action: Action) {
        let result = match action {
            Action::Repeatable => core
                .perform(CountingEffect {
                    binding: bound(),
                    value: input,
                    label: "window",
                    calls: self.calls.clone(),
                })
                .await
                .map(drop),
            Action::Affine => core
                .perform(AffineCountingEffect {
                    binding: bound(),
                    calls: self.calls.clone(),
                })
                .await
                .map(drop),
            Action::EscapeControls => match core
                .perform(InvariantAffineEffect {
                    binding: bound(),
                    calls: self.calls.clone(),
                })
                .await
            {
                Err(EffectError::EffectTargetInvariantViolation { .. }) => Ok(()),
                other => other.map(drop),
            },
            Action::Capture => core.capture("window", input).await.map(drop),
            Action::Transactional => core
                .call_request(&self.ledger, TransactionalCounting { value: input })
                .await
                .map(drop),
        };
        match result {
            Ok(()) => {}
            Err(EffectError::Journal(_)) if self.injected_failure => {}
            Err(error) => panic!("{action:?} at input {input} failed: {error:?}"),
        }
    }

    async fn run(&self, input: u64, actions: &[Action]) {
        let mut core = EffectsCore::new(self.context(input));
        for action in actions {
            self.perform(&mut core, input, *action).await;
        }
        core.settle_without_action()
            .await
            .expect("an input without an action settles");
        assert!(
            core.settlement_divergence().await.is_none(),
            "a live input leaves no unused current-run history"
        );
    }
}

type ComparableHistory = (
    CausalFrontier,
    Vec<EffectRecord>,
    Vec<EffectAttemptStarted>,
    Vec<(EffectAttemptOrdinal, EventId)>,
    Option<EffectRecoveryAbandoned>,
    Option<Option<EffectAttemptOrdinal>>,
    Vec<EventId>,
    Vec<(EffectAttemptOrdinal, Vec<EventId>)>,
);

fn comparable(history: &EffectCursorHistory) -> ComparableHistory {
    (
        history.causal.clone(),
        history.records.clone(),
        history.attempts.clone(),
        history
            .attempt_events
            .iter()
            .map(|(attempt, event)| (*attempt, event.id))
            .collect(),
        history.abandonment.clone(),
        history.terminal_attempt,
        history
            .terminal_group_events
            .iter()
            .map(|event| event.id)
            .collect(),
        history
            .escape_control_batches
            .iter()
            .map(|(attempt, events)| (*attempt, events.iter().map(|event| event.id).collect()))
            .collect(),
    )
}

async fn assert_matches_scan(
    history: &CurrentRunHistory,
    journal: &Arc<dyn Journal<ChainEvent>>,
    cursor: &EffectCursor,
) {
    let scanned = current_cursor_history(journal, cursor).await;
    let folded = history.cursor_history(journal, cursor).await;
    match (scanned, folded) {
        (Ok(scanned), Ok(folded)) => assert_eq!(
            comparable(&scanned),
            comparable(&folded),
            "current-run history for {cursor:?} differs from the full scan"
        ),
        (Err(_), Err(_)) => {}
        (scanned, folded) => {
            panic!("cursor {cursor:?}: full scan {scanned:?}, current-run history {folded:?}")
        }
    }
}

fn generated_inputs(seed: u64, inputs: u64) -> Vec<Vec<Action>> {
    let mut rng = fastrand::Rng::with_seed(seed);
    (0..inputs)
        .map(|_| {
            let count = rng.usize(0..=3);
            (0..count)
                .map(|_| ACTIONS[rng.usize(0..ACTIONS.len())])
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn current_run_history_answers_every_window_lookup_as_the_full_scan_does() {
    for seed in 0..12 {
        let window = if seed % 3 == 2 {
            Window::failing_first_outcome()
        } else {
            Window::new()
        };
        for (index, actions) in generated_inputs(seed, 10).iter().enumerate() {
            let input = index as u64 + 1;
            window.run(input, actions).await;
            let ordinals = u32::try_from(actions.len().max(1)).expect("few actions per input");
            for ordinal in 0..=ordinals {
                assert_matches_scan(
                    &window.history,
                    &window.erased,
                    &window.cursor(input, ordinal),
                )
                .await;
            }
            assert!(
                window.history.retained_cursors().await <= ordinals as usize,
                "seed {seed}: only input {input}'s cursors stay retained"
            );
        }
        assert_eq!(
            window.execution.effect_cursor_coordinator().held_cursors(),
            0,
            "seed {seed}: settled cursors keep no lock entry"
        );
    }
}

#[tokio::test]
async fn current_run_history_answers_a_resumed_recovery_as_the_full_scan_does() {
    let stage_id = StageId::new();
    let parent = parent_envelope(WriterId::from(stage_id));
    let (completed_history, in_doubt_history) = affine_scope_matrix_histories(&parent).await;
    for (archived, boundary) in [
        (
            completed_history,
            Arc::new(PanicBoundary) as Arc<dyn EffectBoundary>,
        ),
        (
            in_doubt_history,
            Arc::new(RecoveryRejectingBoundary {
                consults: Arc::new(AtomicUsize::new(0)),
            }) as Arc<dyn EffectBoundary>,
        ),
    ] {
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let erased: Arc<dyn Journal<ChainEvent>> = journal.clone();
        let mut ctx = invocation_context_with_mode(
            journal.clone(),
            parent.clone(),
            Some(archived),
            EffectRuntimeMode::Resume,
            EffectPortRegistry::new(),
        );
        ctx.effect_boundary = Some(boundary);
        let history = ctx.current_run_history.clone();
        let mut core = EffectsCore::new(ctx);
        let outcome = core
            .perform(AffineCountingEffect {
                binding: bound(),
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .await;
        assert!(
            matches!(outcome, Ok(_) | Err(EffectError::RecoveryAbandoned { .. })),
            "a resumed affine cursor reproduces or abandons, got {outcome:?}"
        );
        let cursor = cursor_started_in(&journal);
        assert_matches_scan(&history, &erased, &cursor).await;
        let next = EffectCursor::new(
            cursor.recorded_flow_id.clone(),
            cursor.stage_key.clone(),
            cursor.input_seq,
            EffectOrdinal::new(cursor.effect_ordinal.get() + 1),
        );
        assert_matches_scan(&history, &erased, &next).await;
    }
}

#[tokio::test]
async fn a_lookup_for_a_settled_input_halts_authoring_without_executing() {
    let window = Window::new();
    window.run(1, &[Action::Repeatable]).await;
    window.run(2, &[Action::Repeatable]).await;
    let calls_before = window.calls.load(Ordering::SeqCst);

    let refused = window
        .history
        .cursor_history(&window.erased, &window.cursor(1, 1))
        .await
        .expect_err("a settled input is outside the window");
    assert!(
        matches!(
            &refused,
            CurrentRunFailure::History(EffectError::EffectProvenanceMismatch(detail))
                if detail.contains("input 1") && detail.contains("through 1")
        ),
        "the refusal names the input and the settled watermark: {refused:?}"
    );

    let mut late = EffectsCore::new(window.context(1));
    let error = late
        .perform(CountingEffect {
            binding: bound(),
            value: 1,
            label: "late",
            calls: window.calls.clone(),
        })
        .await
        .expect_err("a settled input cannot perform again");
    assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    assert!(
        late.authoring_halt_fatal().is_some(),
        "the refusal closes authoring whatever the handler does"
    );
    assert_eq!(window.calls.load(Ordering::SeqCst), calls_before);

    assert!(
        window
            .history
            .cursor_history(&window.erased, &window.cursor(3, 0))
            .await
            .is_err(),
        "a refused lookup leaves the history refusing"
    );
}

#[tokio::test]
async fn a_record_for_a_settled_input_refuses_the_next_lookup() {
    let window = Window::new();
    window.run(1, &[Action::Repeatable]).await;
    window.run(2, &[Action::Repeatable]).await;
    let late_fact = window
        .journal
        .events()
        .into_iter()
        .find(|record| {
            record
                .envelope
                .provenance
                .event
                .effect_provenance
                .as_ref()
                .is_some_and(|provenance| provenance.cursor == window.cursor(1, 0))
        })
        .expect("input 1 recorded its outcome")
        .authored();
    window
        .erased
        .append(late_fact, AppendOptions::default())
        .await
        .expect("the test journal accepts the late record");

    let refused = window
        .history
        .cursor_history(&window.erased, &window.cursor(3, 0))
        .await
        .expect_err("a record for a settled input fails closed");
    assert!(matches!(
        refused,
        CurrentRunFailure::History(EffectError::EffectProvenanceMismatch(_))
    ));
}

#[tokio::test]
async fn current_run_history_reads_each_record_once() {
    let window = Window::new();
    let reads = Arc::new(AtomicUsize::new(0));
    let counted: Arc<dyn Journal<ChainEvent>> = Arc::new(CountedReads {
        inner: window.journal.clone(),
        reads: reads.clone(),
    });
    let history = CurrentRunHistory::new(counted.clone());
    for input in 1..=8 {
        let mut ctx = window.context(input);
        ctx.data_journal = counted.clone();
        ctx.current_run_history = history.clone();
        let mut core = EffectsCore::new(ctx);
        window.perform(&mut core, input, Action::Affine).await;
        window.perform(&mut core, input, Action::Repeatable).await;
        assert!(core.settlement_divergence().await.is_none());
    }
    assert_eq!(
        reads.load(Ordering::SeqCst),
        window.journal.events().len(),
        "every committed record is decoded once across all lookups"
    );
}

#[tokio::test]
async fn an_uncommitted_journal_tail_fails_the_lookup_before_any_call() {
    let window = Window::new();
    let torn = Arc::new(AtomicBool::new(false));
    let erased: Arc<dyn Journal<ChainEvent>> = Arc::new(TornTail {
        inner: window.journal.clone(),
        torn: torn.clone(),
    });
    let history = CurrentRunHistory::new(erased.clone());
    let context = |input: u64| {
        let mut ctx = window.context(input);
        ctx.data_journal = erased.clone();
        ctx.current_run_history = history.clone();
        ctx
    };

    let mut first = EffectsCore::new(context(1));
    window.perform(&mut first, 1, Action::Repeatable).await;
    torn.store(true, Ordering::SeqCst);

    assert!(current_cursor_history(&erased, &window.cursor(1, 1))
        .await
        .is_err());
    assert!(matches!(
        history.cursor_history(&erased, &window.cursor(1, 1)).await,
        Err(CurrentRunFailure::Journal(EffectError::Journal(_)))
    ));

    let calls_before = window.calls.load(Ordering::SeqCst);
    let mut second = EffectsCore::new(context(2));
    let error = second
        .perform(CountingEffect {
            binding: bound(),
            value: 2,
            label: "after-torn-tail",
            calls: window.calls.clone(),
        })
        .await
        .expect_err("an unconfirmed journal end fails the lookup");
    assert!(matches!(error, EffectError::Journal(_)));
    assert_eq!(window.calls.load(Ordering::SeqCst), calls_before);
    assert!(
        second.settlement_divergence().await.is_some(),
        "the end-of-input check stops the stage, as the full scan does"
    );
}

#[tokio::test]
async fn current_run_history_answers_only_for_the_journal_it_folds() {
    let window = Window::new();
    let other: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::new(JournalOwner::stage(window.stage_id)));
    let mut ctx = window.context(1);
    ctx.current_run_history = CurrentRunHistory::new(other);
    let mut core = EffectsCore::new(ctx);
    let error = core
        .perform(CountingEffect {
            binding: bound(),
            value: 1,
            label: "mismatched",
            calls: window.calls.clone(),
        })
        .await
        .expect_err("a history folded from another journal cannot answer");
    assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    assert!(core.authoring_halt_fatal().is_some());
    assert_eq!(window.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn another_stages_outcome_fact_passes_through_unclaimed() {
    let window = Window::new();
    let upstream_stage = StageId::new();
    let upstream = Arc::new(MemoryJournal::new(JournalOwner::stage(upstream_stage)));
    let mut upstream_core = EffectsCore::new(invocation_context(
        upstream.clone(),
        parent_envelope(WriterId::from(upstream_stage)),
        None,
    ));
    upstream_core
        .perform(CountingEffect {
            binding: bound(),
            value: 1,
            label: "upstream",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("upstream outcome");
    let upstream_fact = upstream
        .events()
        .into_iter()
        .find(|record| {
            record
                .envelope
                .provenance
                .event
                .effect_provenance
                .as_ref()
                .is_some_and(|provenance| !provenance.fact_owner.is_framework())
        })
        .expect("the upstream outcome is a user-owned fact")
        .authored();

    window.run(1, &[Action::Repeatable]).await;
    window.run(2, &[Action::Repeatable]).await;
    window
        .erased
        .append(upstream_fact, AppendOptions::default())
        .await
        .expect("a passed-through fact keeps its upstream provenance");
    window.run(3, &[Action::Repeatable]).await;
    for ordinal in 0..=1 {
        assert_matches_scan(&window.history, &window.erased, &window.cursor(3, ordinal)).await;
    }

    let upstream_start = {
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(upstream_stage)));
        let mut core = EffectsCore::new(invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(upstream_stage)),
            None,
        ));
        core.perform(AffineCountingEffect {
            binding: bound(),
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("upstream affine outcome");
        journal
            .events()
            .into_iter()
            .find(|record| EffectAttemptStarted::matches_event_type(&record.event_type()))
            .expect("affine Start")
            .authored()
    };
    window
        .erased
        .append(upstream_start, AppendOptions::default())
        .await
        .expect("the test journal accepts foreign framework evidence");
    assert!(
        matches!(
            window
                .history
                .cursor_history(&window.erased, &window.cursor(4, 0))
                .await,
            Err(CurrentRunFailure::History(_))
        ),
        "framework evidence for another stage's cursor fails closed"
    );
}

#[tokio::test]
async fn corrupt_current_run_history_halts_authoring() {
    let window = Window::new();
    window.run(1, &[Action::Affine]).await;
    let mut core = EffectsCore::new(window.context(2));
    window.perform(&mut core, 2, Action::Affine).await;
    let start = window
        .journal
        .events()
        .into_iter()
        .rev()
        .find(|record| EffectAttemptStarted::matches_event_type(&record.event_type()))
        .expect("input 2 recorded a Start")
        .authored();
    window
        .erased
        .append(start, AppendOptions::default())
        .await
        .expect("the test journal accepts a duplicate Start");
    assert_matches_scan(&window.history, &window.erased, &window.cursor(2, 1)).await;

    let calls_before = window.calls.load(Ordering::SeqCst);
    let error = core
        .perform(CountingEffect {
            binding: bound(),
            value: 2,
            label: "after-duplicate-start",
            calls: window.calls.clone(),
        })
        .await
        .expect_err("a duplicate Start is corrupt current-run history");
    assert!(matches!(error, EffectError::EffectProvenanceMismatch(_)));
    assert!(core.authoring_halt_fatal().is_some());
    assert_eq!(window.calls.load(Ordering::SeqCst), calls_before);
}

#[tokio::test]
async fn cursor_lock_entries_release_after_their_last_holder() {
    let coordinator = Arc::new(EffectCursorCoordinator::default());
    let cursor = EffectCursor::new("flow", STAGE_KEY, 1, EffectOrdinal::new(0));

    let held = coordinator.lock(&cursor).await.expect("first holder");
    let waiter = tokio::spawn({
        let coordinator = coordinator.clone();
        let cursor = cursor.clone();
        async move {
            let queued = coordinator.lock(&cursor).await.expect("queued holder");
            drop(queued);
        }
    });
    tokio::task::yield_now().await;
    assert_eq!(coordinator.held_cursors(), 1);
    drop(held);
    waiter
        .await
        .expect("the queued holder acquires after release");
    assert_eq!(coordinator.held_cursors(), 0);

    let held = coordinator.lock(&cursor).await.expect("holder");
    let cancelled = tokio::spawn({
        let coordinator = coordinator.clone();
        let cursor = cursor.clone();
        async move {
            let never = coordinator.lock(&cursor).await.expect("never acquired");
            drop(never);
        }
    });
    tokio::task::yield_now().await;
    cancelled.abort();
    assert!(cancelled.await.is_err_and(|error| error.is_cancelled()));
    drop(held);
    assert_eq!(
        coordinator.held_cursors(),
        0,
        "a waiter cancelled in the queue leaves no lock entry"
    );
}

struct CountedReads {
    inner: Arc<MemoryJournal<ChainEvent>>,
    reads: Arc<AtomicUsize>,
}

struct CountedReader {
    inner: MemoryJournalReader<ChainEvent>,
    reads: Arc<AtomicUsize>,
}

#[async_trait]
impl obzenflow_core::journal::JournalStorageReader<ChainEvent> for CountedReader {
    async fn storage_next(&mut self) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        let next = self.inner.storage_next().await?;
        if next.is_some() {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        Ok(next)
    }

    fn storage_position(&self) -> u64 {
        self.inner.storage_position()
    }

    fn storage_is_at_end(&self) -> bool {
        self.inner.storage_is_at_end()
    }
}

#[async_trait]
impl obzenflow_core::journal::JournalStorage<ChainEvent> for CountedReads {
    fn storage_id(&self) -> &JournalId {
        self.inner.storage_id()
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.inner.storage_owner()
    }

    async fn storage_append(
        &self,
        event: ChainEvent,
        options: AppendOptions<ChainEvent>,
    ) -> Result<JournalRecord<ChainPayload>, JournalError> {
        self.inner.storage_append(event, options).await
    }

    async fn storage_append_group(
        &self,
        group_id: &str,
        events: Vec<ChainEvent>,
        options: AppendOptions<ChainEvent>,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner
            .storage_append_group(group_id, events, options)
            .await
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.storage_read_all_unordered().await
    }

    async fn storage_read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.storage_read_event(event_id).await
    }

    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Ok(Box::new(CountedReader {
            inner: MemoryJournalReader {
                events: self.inner.events.clone(),
                position: position as usize,
            },
            reads: self.reads.clone(),
        }))
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.storage_read_last_n(count).await
    }
}

struct TornTail {
    inner: Arc<MemoryJournal<ChainEvent>>,
    torn: Arc<AtomicBool>,
}

struct TornTailReader {
    inner: MemoryJournalReader<ChainEvent>,
    torn: Arc<AtomicBool>,
}

#[async_trait]
impl obzenflow_core::journal::JournalStorageReader<ChainEvent> for TornTailReader {
    async fn storage_next(&mut self) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.storage_next().await
    }

    fn storage_position(&self) -> u64 {
        self.inner.storage_position()
    }

    fn storage_is_at_end(&self) -> bool {
        !self.torn.load(Ordering::SeqCst) && self.inner.storage_is_at_end()
    }
}

#[async_trait]
impl obzenflow_core::journal::JournalStorage<ChainEvent> for TornTail {
    fn storage_id(&self) -> &JournalId {
        self.inner.storage_id()
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.inner.storage_owner()
    }

    async fn storage_append(
        &self,
        event: ChainEvent,
        options: AppendOptions<ChainEvent>,
    ) -> Result<JournalRecord<ChainPayload>, JournalError> {
        self.inner.storage_append(event, options).await
    }

    async fn storage_append_group(
        &self,
        group_id: &str,
        events: Vec<ChainEvent>,
        options: AppendOptions<ChainEvent>,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner
            .storage_append_group(group_id, events, options)
            .await
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        if self.torn.load(Ordering::SeqCst) {
            return Err(JournalError::Implementation {
                message: "Corrupt record at the journal tail: missing commit trailer".to_string(),
                source: "torn tail".into(),
            });
        }
        self.inner.storage_read_all_unordered().await
    }

    async fn storage_read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.storage_read_event(event_id).await
    }

    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Ok(Box::new(TornTailReader {
            inner: MemoryJournalReader {
                events: self.inner.events.clone(),
                position: position as usize,
            },
            torn: self.torn.clone(),
        }))
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.storage_read_last_n(count).await
    }
}
