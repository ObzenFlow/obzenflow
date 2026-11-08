// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::provenance::causality_context::CausalInput;
use obzenflow_core::event::RecoveryCompletedEventParams;
use tokio::sync::Notify;

fn scheduled(identity: &EffectIdentity, event: &ChainEvent) -> ChainEvent {
    ChainEventFactory::retry_scheduled(
        WriterId::from(StageId::new()),
        identity.cursor.clone(),
        2,
        5_000,
        CausalInput::of(event),
    )
}

fn completed(identity: &EffectIdentity, event: &ChainEvent) -> ChainEvent {
    ChainEventFactory::recovery_completed(
        WriterId::from(StageId::new()),
        RecoveryCompletedEventParams {
            cursor: identity.cursor.clone(),
            total_attempts: 1,
            backoff_elapsed_ms: 5_000,
            recovery_elapsed_ms: 5_000,
        },
        CausalInput::of(event),
    )
}

struct CommitThenCall {
    committed: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl EffectBoundary for CommitThenCall {
    async fn around_repeatable_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        operation
            .progress()
            .commit(vec![scheduled(identity, event)])
            .await
            .expect("rows before the wait commit");
        self.committed.notify_one();
        self.release.notified().await;
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: vec![completed(identity, event)],
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
        let progress = operation.progress();
        let execution = operation.execute().await;
        progress
            .commit(vec![scheduled(identity, event)])
            .await
            .expect("rows before the wait commit");
        self.committed.notify_one();
        self.release.notified().await;
        execution.into_report(vec![completed(identity, event)])
    }
}

struct RefusedCommit {
    rows: fn(&EffectIdentity, &ChainEvent) -> ChainEvent,
}

#[async_trait]
impl EffectBoundary for RefusedCommit {
    async fn around_repeatable_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        let committed = operation
            .progress()
            .commit(vec![(self.rows)(identity, event)])
            .await;
        assert!(committed.is_err(), "the commit is refused");
        AbortingBoundary
            .around_repeatable_effect(identity, event, operation)
            .await
    }

    async fn around_single_use_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        AbortingBoundary
            .around_single_use_effect(identity, event, operation)
            .await
    }
}

fn data_row(_identity: &EffectIdentity, event: &ChainEvent) -> ChainEvent {
    ChainEventFactory::data_event(
        event.writer_id,
        "test.smuggled",
        std::num::NonZeroU32::MIN,
        json!({}),
    )
}

fn group_ids(journal: &MemoryJournal<ChainEvent>) -> Vec<String> {
    let mut groups: Vec<String> = Vec::new();
    for record in journal.events() {
        if let Some(group) = record.envelope.provenance.journal.journal_group_id.clone() {
            if groups.last() != Some(&group) {
                groups.push(group);
            }
        }
    }
    groups
}

fn group_rows(journal: &MemoryJournal<ChainEvent>, group: &str) -> Vec<String> {
    journal
        .events()
        .into_iter()
        .filter(|record| {
            record
                .envelope
                .provenance
                .journal
                .journal_group_id
                .as_deref()
                == Some(group)
        })
        .map(|record| record.event_type().to_string())
        .collect()
}

struct Waiting {
    journal: Arc<MemoryJournal<ChainEvent>>,
    cursor: EffectCursor,
    committed: Arc<Notify>,
    release: Arc<Notify>,
    ctx: EffectInvocationContext,
}

fn waiting() -> Waiting {
    let stage = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage)));
    let committed = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut ctx = invocation_context(journal.clone(), parent_envelope(stage.into()), None);
    ctx.effect_boundary = Some(Arc::new(CommitThenCall {
        committed: committed.clone(),
        release: release.clone(),
    }));
    let cursor = EffectCursor::new(
        ctx.flow_id.to_string(),
        ctx.stage_key.clone(),
        ctx.input_seq.0,
        0_u32,
    );
    Waiting {
        journal,
        cursor,
        committed,
        release,
        ctx,
    }
}

#[tokio::test]
async fn rows_before_breaker_wait_committed() {
    let Waiting {
        journal,
        cursor,
        committed,
        release,
        ctx,
    } = waiting();
    let calls = Arc::new(AtomicUsize::new(0));
    let call = tokio::spawn({
        let calls = calls.clone();
        async move { EffectsCore::new(ctx).perform(counting(&calls)).await }
    });

    committed.notified().await;
    let progress = effect_recovery_progress_group_id(&cursor, 1).to_string();
    assert_eq!(group_ids(&journal), std::slice::from_ref(&progress));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    release.notify_one();
    call.await
        .expect("call task")
        .expect("the waited call succeeds");
    assert_eq!(
        group_ids(&journal),
        [progress, effect_outcome_group_id(&cursor).to_string()]
    );
}

#[tokio::test]
async fn waited_call_outcome_group_holds_completion() {
    let Waiting {
        journal,
        cursor,
        release,
        ctx,
        ..
    } = waiting();
    release.notify_one();
    EffectsCore::new(ctx)
        .perform(counting(&Arc::new(AtomicUsize::new(0))))
        .await
        .expect("the waited call succeeds");

    assert_eq!(
        group_rows(
            &journal,
            effect_recovery_progress_group_id(&cursor, 1).as_str()
        ),
        ["runtime.retry.scheduled"]
    );
    let outcome = group_rows(&journal, effect_outcome_group_id(&cursor).as_str());
    assert!(outcome.contains(&"runtime.resilience.attempts_reported".to_string()));
    assert_eq!(
        outcome.last().map(String::as_str),
        Some("test.counting_output")
    );
}

#[tokio::test]
async fn progress_rows_with_data_refused() {
    let stage = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage)));
    let mut ctx = invocation_context(journal.clone(), parent_envelope(stage.into()), None);
    ctx.effect_boundary = Some(Arc::new(RefusedCommit { rows: data_row }));
    let calls = Arc::new(AtomicUsize::new(0));

    let error = EffectsCore::new(ctx)
        .perform(counting(&calls))
        .await
        .expect_err("a batch holding Data ends the call");

    assert!(matches!(
        error,
        EffectError::EffectProvenanceMismatch(ref message) if message.contains("contains Data")
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(journal.events().is_empty());
}

#[tokio::test]
async fn failed_progress_commit_ends_call() {
    let stage = StageId::new();
    let journal = Arc::new(MemoryJournal::failing_group(
        JournalOwner::stage(stage),
        "effect-recovery-progress:v1:",
    ));
    let mut ctx = invocation_context(journal.clone(), parent_envelope(stage.into()), None);
    ctx.effect_boundary = Some(Arc::new(RefusedCommit { rows: scheduled }));
    let calls = Arc::new(AtomicUsize::new(0));

    let error = EffectsCore::new(ctx)
        .perform(counting(&calls))
        .await
        .expect_err("a journal failure before the wait ends the call");

    assert!(matches!(error, EffectError::Journal(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(effect_records(&journal).is_empty());
}

async fn kill_during_wait(effect: impl Effect + 'static) -> Waiting {
    let Waiting {
        journal,
        cursor,
        committed,
        release,
        ctx,
    } = waiting();
    let call = tokio::spawn(async move { EffectsCore::new(ctx).perform(effect).await });
    committed.notified().await;
    call.abort();
    assert!(matches!(call.await, Err(error) if error.is_cancelled()));
    let stage = StageId::new();
    Waiting {
        journal,
        cursor,
        committed,
        release,
        ctx: invocation_context(
            Arc::new(MemoryJournal::new(JournalOwner::stage(stage))),
            parent_envelope(stage.into()),
            None,
        ),
    }
}

#[tokio::test]
async fn killed_wait_preserves_opened_row() {
    let repeatable = kill_during_wait(counting(&Arc::new(AtomicUsize::new(0)))).await;
    let erased: Arc<dyn Journal<ChainEvent>> = repeatable.journal.clone();
    let history = current_cursor_history(&erased, &repeatable.cursor)
        .await
        .expect("rows for a cursor with no outcome are accepted");
    assert_eq!(history.progress_batches.len(), 1);
    assert!(matches!(history.select(), EffectHistorySelection::Miss));

    let affine = kill_during_wait(AffineCountingEffect {
        calls: Arc::new(AtomicUsize::new(0)),
        binding: bound(),
    })
    .await;
    let erased: Arc<dyn Journal<ChainEvent>> = affine.journal.clone();
    let history = current_cursor_history(&erased, &affine.cursor)
        .await
        .expect("an open Start beside committed rows is accepted");
    assert_eq!(history.progress_batches.len(), 1);
    assert!(matches!(
        history.select(),
        EffectHistorySelection::InDoubt(ref attempts) if attempts.len() == 1
    ));
}

#[tokio::test]
async fn resume_after_killed_wait_resends_once() {
    let killed = kill_during_wait(counting(&Arc::new(AtomicUsize::new(0)))).await;
    let erased: Arc<dyn Journal<ChainEvent>> = killed.journal.clone();
    let history = current_cursor_history(&erased, &killed.cursor)
        .await
        .expect("the killed run's rows are accepted");
    assert!(matches!(history.select(), EffectHistorySelection::Miss));
    let stage = StageId::new();
    let resumed = Arc::new(MemoryJournal::new(JournalOwner::stage(stage)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context_with_mode(
        resumed.clone(),
        parent_envelope(stage.into()),
        Some(Arc::new(EffectHistory::with_actions_for_test(
            killed.cursor.recorded_flow_id.clone(),
            Vec::new(),
            HashMap::new(),
        ))),
        EffectRuntimeMode::Resume,
        EffectPortRegistry::new(),
    );
    ctx.effect_boundary = Some(Arc::new(TestAllowAllEffectBoundary));

    EffectsCore::new(ctx)
        .perform(counting(&calls))
        .await
        .expect("resume sends the waiting call");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(effect_records(&resumed).len(), 1);
}

#[tokio::test]
async fn replay_waited_call_reconstructs_outcome() {
    let Waiting {
        journal,
        cursor,
        release,
        ctx,
        ..
    } = waiting();
    release.notify_one();
    EffectsCore::new(ctx)
        .perform(counting(&Arc::new(AtomicUsize::new(0))))
        .await
        .expect("the waited call succeeds");
    let erased: Arc<dyn Journal<ChainEvent>> = journal.clone();
    let history = current_cursor_history(&erased, &cursor)
        .await
        .expect("the live run's rows index");

    let stage = StageId::new();
    let replay = Arc::new(MemoryJournal::new(JournalOwner::stage(stage)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        replay.clone(),
        parent_envelope(stage.into()),
        Some(Arc::new(
            EffectHistory::from_cursor_history_for_test(
                cursor.recorded_flow_id.clone(),
                cursor.clone(),
                history,
            )
            .expect("history indexes"),
        )),
    );
    ctx.effect_boundary = Some(Arc::new(PanicBoundary));

    EffectsCore::new(ctx)
        .perform(counting(&calls))
        .await
        .expect("replay rebuilds the waited outcome");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(group_ids(&replay), group_ids(&journal));
    assert_eq!(
        group_rows(
            &replay,
            effect_recovery_progress_group_id(&cursor, 1).as_str()
        ),
        ["runtime.retry.scheduled"]
    );
}
