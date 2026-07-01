// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime execution strategy (FLOWIP-120r).
//!
//! One runtime-owned authority for the question "is this work reconstructing
//! recorded history, or is it live?". The strategy is selected once from the
//! operator verb at bootstrap; every consumer then queries the strategy
//! instead of branching on `replay_archive.is_some()` or the retired
//! `EffectRuntimeMode`.
//!
//! FLOWIP-120r ships [`Live`] and [`Replay`]. FLOWIP-120n adds the operational
//! `Resume` strategy (continue-live, position-based) plus a `ResumeControl`
//! handle, without changing the read-only query trait below.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use obzenflow_core::journal::ArchiveStatus;
use obzenflow_core::{MiddlewareExecutionScope, ReaderGeneration, StageId};

use crate::messaging::upstream_subscription::StageInputPosition;
use crate::replay::ReplayArchive;

/// The operator's run verb. This is the only input that picks a strategy.
///
/// Distinct from the presentation `obzenflow_infra::RunMode`, which is a
/// banner-and-footer surface and is never read for a runtime decision.
/// FLOWIP-120n adds `Resume` (`--resume-from`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMode {
    /// No replay verb: do live work, reconstruct nothing.
    Live,
    /// `--replay-from`: reconstruct a bounded recording and drain at its end.
    Replay,
    /// `--resume-from`: catch up on the recording, then continue live from the
    /// recorded high-water mark (FLOWIP-120n).
    Resume,
}

/// A positioned point of execution: a stage plus the merged delivery rank of
/// the input being processed. The position is the canonical-merge output rank
/// (FLOWIP-095d), unique and deterministic per stage, so there is no per-input
/// key to disambiguate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionPosition {
    pub stage_id: StageId,
    pub position: StageInputPosition,
    /// The delivered event's generation (FLOWIP-120n), read from the
    /// subscription at delivery. `None` where delivery carries no generation
    /// (the effect context, per F7, and pre-resume call sites). `Live` and
    /// `Replay` ignore it.
    pub generation: Option<ReaderGeneration>,
}

/// Where a caller's execution position comes from, resolved once by
/// [`RuntimeExecution::handler_scope_for`] before it dispatches to the
/// strategy. Data and cause-carrying flow control are positioned; genuinely
/// position-less work falls back to the stage-level answer.
#[derive(Debug, Clone, Copy)]
pub enum ExecutionPositionSource {
    /// A data delivery, carrying its own merged rank.
    Data {
        stage_id: StageId,
        position: StageInputPosition,
        /// The delivered event's generation (FLOWIP-120n); `None` outside
        /// resume-aware call sites.
        generation: Option<ReaderGeneration>,
    },
    /// A flow-control signal (EOF, drain, progress); it inherits the position
    /// of the data that caused it when there is one.
    FlowControl {
        stage_id: StageId,
        cause: Option<ExecutionPosition>,
    },
    /// Stage startup or teardown: no causing data, answered at stage level.
    StageLifecycle { stage_id: StageId },
}

/// Whether a source is reconstructing recorded input or polling live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceExecutionPhase {
    Live,
    Replaying,
}

/// What a source does when its recorded input is exhausted. In FLOWIP-120r a
/// replay always terminates; FLOWIP-120n's resume returns `ContinueLive` so an
/// infinite source hands off to live polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceReplayExhaustion {
    /// Stop reconstructing; the source reaches its normal terminal.
    Terminate,
    /// Hand off to live polling (FLOWIP-120n resume).
    ContinueLive,
}

/// Whether a stage's heartbeat task should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatExecutionPolicy {
    Active,
    Suppressed,
    /// Resume catch-up: spawn the task but emit nothing until the stage's
    /// frontier crosses to live (FLOWIP-120n). The runner re-queries per tick.
    DormantUntilLive,
}

/// One implementation per run mode. These impls are the only place the
/// live / replay / resume behaviours differ. Every method is `&self`: the
/// trait is read-only. Consumers call it; they never match on a mode.
///
/// FLOWIP-120n adds a `Resume` impl whose state evolves during the run; that
/// state lives behind interior mutability and is mutated through a separate
/// `ResumeControl` handle, so this query trait stays read-only.
pub trait ExecutionStrategy: std::fmt::Debug + Send + Sync {
    /// Middleware execution scope for positioned work.
    fn scope_at(&self, at: ExecutionPosition) -> MiddlewareExecutionScope;
    /// Middleware execution scope for position-less work (lifecycle, causeless
    /// flow control).
    fn stage_scope(&self, stage: StageId) -> MiddlewareExecutionScope;
    /// Whether a missing recorded effect outcome at this position is corruption
    /// (fail loud) rather than a live gap (run the effect).
    fn missing_outcome_is_corruption(&self, at: ExecutionPosition) -> bool;
    /// Whether a source is reconstructing or polling live.
    fn source_phase_for(&self, stage: StageId) -> SourceExecutionPhase;
    /// What a source does once its recorded input is exhausted.
    fn source_replay_exhausted(&self, stage: StageId) -> SourceReplayExhaustion;
    /// Whether a stage's heartbeat task should run.
    fn heartbeat_policy_for(&self, stage: StageId) -> HeartbeatExecutionPolicy;
}

/// Live execution: reconstruct nothing.
#[derive(Debug)]
struct Live;

impl ExecutionStrategy for Live {
    fn scope_at(&self, _at: ExecutionPosition) -> MiddlewareExecutionScope {
        MiddlewareExecutionScope::LiveHandler
    }
    fn stage_scope(&self, _stage: StageId) -> MiddlewareExecutionScope {
        MiddlewareExecutionScope::LiveHandler
    }
    fn missing_outcome_is_corruption(&self, _at: ExecutionPosition) -> bool {
        false
    }
    fn source_phase_for(&self, _stage: StageId) -> SourceExecutionPhase {
        SourceExecutionPhase::Live
    }
    fn source_replay_exhausted(&self, _stage: StageId) -> SourceReplayExhaustion {
        // A live source never reconstructs, so this is never reached; the
        // terminal answer is harmless if it is.
        SourceReplayExhaustion::Terminate
    }
    fn heartbeat_policy_for(&self, _stage: StageId) -> HeartbeatExecutionPolicy {
        HeartbeatExecutionPolicy::Active
    }
}

/// Bounded reconstruction of a recording that drains at its end.
///
/// `incomplete` is set from archive status at construction: a sealed archive is
/// strict (a missing outcome is corruption, scope `StrictReplayHandler`), an
/// incomplete one is lenient (a missing outcome runs live, scope
/// `ResumeHandler`, matching today's `allow_incomplete_archive`). Both drain.
#[derive(Debug)]
struct Replay {
    incomplete: bool,
}

impl Replay {
    fn reconstruction_scope(&self) -> MiddlewareExecutionScope {
        if self.incomplete {
            MiddlewareExecutionScope::ResumeHandler
        } else {
            MiddlewareExecutionScope::StrictReplayHandler
        }
    }
}

impl ExecutionStrategy for Replay {
    fn scope_at(&self, _at: ExecutionPosition) -> MiddlewareExecutionScope {
        self.reconstruction_scope()
    }
    fn stage_scope(&self, _stage: StageId) -> MiddlewareExecutionScope {
        self.reconstruction_scope()
    }
    fn missing_outcome_is_corruption(&self, _at: ExecutionPosition) -> bool {
        !self.incomplete
    }
    fn source_phase_for(&self, _stage: StageId) -> SourceExecutionPhase {
        SourceExecutionPhase::Replaying
    }
    fn source_replay_exhausted(&self, _stage: StageId) -> SourceReplayExhaustion {
        SourceReplayExhaustion::Terminate
    }
    fn heartbeat_policy_for(&self, _stage: StageId) -> HeartbeatExecutionPolicy {
        HeartbeatExecutionPolicy::Suppressed
    }
}

/// Shared state of a resume run (FLOWIP-120n). Owned jointly by the `Resume`
/// strategy (reads) and the [`ResumeControl`] handle (writes), so the
/// read-only [`ExecutionStrategy`] trait stays read-only.
///
/// The per-stage frontier generation starts at 0 (recorded) and advances to
/// the announced value when the stage's catch-up watermark is delivered; a
/// stage is live once its frontier reaches `resume_generation`.
#[derive(Debug)]
pub struct ResumeState {
    resume_generation: ReaderGeneration,
    frontier_by_stage: Mutex<HashMap<StageId, ReaderGeneration>>,
    /// Sources registered as infinite at build; only these continue live at
    /// archive exhaustion. A finite source under resume behaves as replay.
    infinite_sources: Mutex<HashSet<StageId>>,
    /// Per-stage maximum recorded effect `input_seq`, registered at
    /// `EffectHistory` load (FLOWIP-120n F7). A cursor miss at or below the
    /// mark is a torn prefix; beyond it, a live call.
    recorded_effect_seq_max: Mutex<HashMap<StageId, StageInputPosition>>,
    /// Per-stage recorded delivered-data maxima, the F15 fail-closed
    /// validation input. Sources register theirs at driver exhaustion.
    recorded_delivered_high_water: Mutex<HashMap<StageId, u64>>,
}

impl ResumeState {
    fn new(resume_generation: ReaderGeneration) -> Self {
        Self {
            resume_generation,
            frontier_by_stage: Mutex::new(HashMap::new()),
            infinite_sources: Mutex::new(HashSet::new()),
            recorded_effect_seq_max: Mutex::new(HashMap::new()),
            recorded_delivered_high_water: Mutex::new(HashMap::new()),
        }
    }

    fn recorded_effect_seq_max(&self, stage: StageId) -> Option<StageInputPosition> {
        self.recorded_effect_seq_max
            .lock()
            .expect("resume effect high-water lock poisoned")
            .get(&stage)
            .copied()
    }

    fn frontier(&self, stage: StageId) -> ReaderGeneration {
        self.frontier_by_stage
            .lock()
            .expect("resume frontier lock poisoned")
            .get(&stage)
            .copied()
            .unwrap_or_default()
    }

    fn stage_is_live(&self, stage: StageId) -> bool {
        self.frontier(stage) >= self.resume_generation
    }

    fn is_infinite_source(&self, stage: StageId) -> bool {
        self.infinite_sources
            .lock()
            .expect("resume source registry lock poisoned")
            .contains(&stage)
    }
}

/// Write handle for resume state. Supervisors record a stage's generation
/// boundary through this when the merge delivers its catch-up watermark, and
/// source builders register infinite sources at build.
#[derive(Debug, Clone)]
pub struct ResumeControl(Arc<ResumeState>);

impl ResumeControl {
    /// The generation this resume run enters (max recorded + 1).
    pub fn resume_generation(&self) -> ReaderGeneration {
        self.0.resume_generation
    }

    /// Advance a stage's frontier to `generation`. Monotonic: a stale write
    /// never regresses the frontier.
    pub fn record_generation_boundary(&self, stage: StageId, generation: ReaderGeneration) {
        let mut frontier = self
            .0
            .frontier_by_stage
            .lock()
            .expect("resume frontier lock poisoned");
        let entry = frontier.entry(stage).or_default();
        if generation > *entry {
            *entry = generation;
        }
    }

    /// Register a source as infinite, making `ContinueLive` its exhaustion
    /// answer.
    pub fn register_infinite_source(&self, stage: StageId) {
        self.0
            .infinite_sources
            .lock()
            .expect("resume source registry lock poisoned")
            .insert(stage);
    }

    /// Register a stage's maximum recorded effect `input_seq`, from the
    /// surviving `EffectHistory` index at load (F7).
    pub fn record_effect_high_water(&self, stage: StageId, max: StageInputPosition) {
        self.0
            .recorded_effect_seq_max
            .lock()
            .expect("resume effect high-water lock poisoned")
            .insert(stage, max);
    }

    /// Register a stage's recorded delivered-data maximum, the F15 validation
    /// input. Sources register theirs at replay-driver exhaustion.
    pub fn record_delivered_high_water(&self, stage: StageId, count: u64) {
        self.0
            .recorded_delivered_high_water
            .lock()
            .expect("resume delivered high-water lock poisoned")
            .insert(stage, count);
    }

    /// The recorded delivered-data maximum for a stage, when known.
    pub fn recorded_delivered_high_water(&self, stage: StageId) -> Option<u64> {
        self.0
            .recorded_delivered_high_water
            .lock()
            .expect("resume delivered high-water lock poisoned")
            .get(&stage)
            .copied()
    }
}

/// Catch-up reconstruction that continues live from the recorded high-water
/// mark (FLOWIP-120n). Reconstruction scope is `ResumeHandler`, the lenient
/// mode: the common resume input is a `Cancelled` SIGINT archive whose tail
/// may be torn.
///
/// Until FLOWIP-120n's dispatch carries a per-event generation, both scope
/// queries answer from the per-stage frontier; the per-event comparison lands
/// with the generation field on [`ExecutionPosition`].
#[derive(Debug)]
struct Resume {
    state: Arc<ResumeState>,
}

impl ExecutionStrategy for Resume {
    fn scope_at(&self, at: ExecutionPosition) -> MiddlewareExecutionScope {
        // The phase is the event's generation, carried in band (F1): a late
        // prefix event still in flight after its stage hands off stays
        // reconstruction-scoped because the generation rides the dispatch.
        // A generation-less dispatch falls back to the stage frontier.
        match at.generation {
            Some(generation) if generation >= self.state.resume_generation => {
                MiddlewareExecutionScope::LiveHandler
            }
            Some(_) => MiddlewareExecutionScope::ResumeHandler,
            None => self.stage_scope(at.stage_id),
        }
    }
    fn stage_scope(&self, stage: StageId) -> MiddlewareExecutionScope {
        if self.state.stage_is_live(stage) {
            MiddlewareExecutionScope::LiveHandler
        } else {
            MiddlewareExecutionScope::ResumeHandler
        }
    }
    fn missing_outcome_is_corruption(&self, at: ExecutionPosition) -> bool {
        // The effect-miss decision is positional, never generational (F7): a
        // miss at or below the stage's recorded mark is a torn prefix, beyond
        // it a live call. No registered mark means no recorded effects, so
        // every miss is live.
        match self.state.recorded_effect_seq_max(at.stage_id) {
            Some(max) => at.position <= max,
            None => false,
        }
    }
    fn source_phase_for(&self, stage: StageId) -> SourceExecutionPhase {
        if self.state.stage_is_live(stage) {
            SourceExecutionPhase::Live
        } else {
            SourceExecutionPhase::Replaying
        }
    }
    fn source_replay_exhausted(&self, stage: StageId) -> SourceReplayExhaustion {
        if self.state.is_infinite_source(stage) {
            SourceReplayExhaustion::ContinueLive
        } else {
            SourceReplayExhaustion::Terminate
        }
    }
    fn heartbeat_policy_for(&self, stage: StageId) -> HeartbeatExecutionPolicy {
        if self.state.stage_is_live(stage) {
            HeartbeatExecutionPolicy::Active
        } else {
            HeartbeatExecutionPolicy::DormantUntilLive
        }
    }
}

/// A sealed archive is a finished recording: a missing recorded outcome in it
/// is genuine corruption. An archive that is neither completed nor cancelled is
/// only ever reconstructed leniently, and then only under
/// `allow_incomplete_archive`.
fn archive_is_sealed(status: ArchiveStatus) -> bool {
    matches!(status, ArchiveStatus::Completed | ArchiveStatus::Cancelled)
}

/// Whether a replay over this archive runs a missing outcome live rather than
/// treating it as corruption. Reproduces the old
/// `EffectRuntimeMode::from_replay_archive` policy: sealed or
/// non-`allow_incomplete` archives are strict; an unsealed archive admitted
/// under `allow_incomplete_archive` is lenient.
fn replay_incomplete(status: ArchiveStatus, allow_incomplete: bool) -> bool {
    !archive_is_sealed(status) && allow_incomplete
}

/// The runtime-owned execution authority. Holds the chosen strategy and the
/// archive as an I/O resource, and forwards every replay-versus-live query to
/// the strategy. This is the callers' surface; the strategy itself never sees
/// an [`ExecutionPositionSource`].
#[derive(Clone)]
pub struct RuntimeExecution {
    strategy: Arc<dyn ExecutionStrategy>,
    archive: Option<Arc<dyn ReplayArchive>>,
    /// Present only under `RuntimeMode::Resume` (FLOWIP-120n).
    resume: Option<ResumeControl>,
}

impl std::fmt::Debug for RuntimeExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeExecution")
            .field("strategy", &self.strategy)
            .field(
                "archive",
                &self.archive.as_ref().map(|a| a.archive_flow_id()),
            )
            .finish()
    }
}

impl RuntimeExecution {
    /// The one strategy-selection point. The verb picks the strategy; archive
    /// status is read exactly once here, only to set `Replay`'s `incomplete`
    /// flag. FLOWIP-120n adds the `RuntimeMode::Resume` arm and its strategy.
    pub fn new(mode: RuntimeMode, archive: Option<Arc<dyn ReplayArchive>>) -> Self {
        let mut resume = None;
        let strategy: Arc<dyn ExecutionStrategy> = match mode {
            RuntimeMode::Live => Arc::new(Live),
            RuntimeMode::Replay => {
                let incomplete = archive
                    .as_deref()
                    .map(|a| replay_incomplete(a.archive_status(), a.allow_incomplete_archive()))
                    .unwrap_or(false);
                Arc::new(Replay { incomplete })
            }
            RuntimeMode::Resume => {
                let archive_ref = archive
                    .as_deref()
                    .expect("--resume-from requires an archive; config layer must reject earlier");
                let resume_generation =
                    ReaderGeneration(archive_ref.max_recorded_generation().0 + 1);
                let state = Arc::new(ResumeState::new(resume_generation));
                resume = Some(ResumeControl(Arc::clone(&state)));
                Arc::new(Resume { state })
            }
        };
        Self {
            strategy,
            archive,
            resume,
        }
    }

    /// The resume write handle; `None` outside `RuntimeMode::Resume`.
    pub fn resume_control(&self) -> Option<&ResumeControl> {
        self.resume.as_ref()
    }

    /// Migration bridge from the legacy `EffectRuntimeMode`, for unit tests
    /// that still construct contexts from an explicit mode. Removed together
    /// with `EffectRuntimeMode` in the final 120r step.
    #[cfg(test)]
    pub(crate) fn from_effect_runtime_mode(
        mode: crate::effects::EffectRuntimeMode,
        archive: Option<Arc<dyn ReplayArchive>>,
    ) -> Self {
        use crate::effects::EffectRuntimeMode;
        let strategy: Arc<dyn ExecutionStrategy> = match mode {
            EffectRuntimeMode::Live => Arc::new(Live),
            EffectRuntimeMode::ReplayStrict => Arc::new(Replay { incomplete: false }),
            EffectRuntimeMode::ResumeIncomplete => Arc::new(Replay { incomplete: true }),
        };
        Self {
            strategy,
            archive,
            resume: None,
        }
    }

    /// The archive object, for I/O and audit only: source readers,
    /// effect-history load, metadata, replay-lifecycle facts. Never a
    /// live-versus-replay predicate; the strategy methods answer that.
    pub fn archive_for_io(&self) -> Option<&Arc<dyn ReplayArchive>> {
        self.archive.as_ref()
    }

    /// Resolve a loose position source to a scope, mode-independently, then
    /// dispatch to the strategy's positioned or position-less answer.
    pub fn handler_scope_for(&self, source: ExecutionPositionSource) -> MiddlewareExecutionScope {
        match source {
            ExecutionPositionSource::Data {
                stage_id,
                position,
                generation,
            } => self.strategy.scope_at(ExecutionPosition {
                stage_id,
                position,
                generation,
            }),
            ExecutionPositionSource::FlowControl { stage_id, cause } => match cause {
                Some(at) => self.strategy.scope_at(at),
                None => self.strategy.stage_scope(stage_id),
            },
            ExecutionPositionSource::StageLifecycle { stage_id } => {
                self.strategy.stage_scope(stage_id)
            }
        }
    }

    /// Scope for a dispatch that may carry a delivered position. A positioned
    /// dispatch is judged at its position; a position-less one (for example a
    /// join observer in 120r) falls back to the stage-level answer. The
    /// position is retained so FLOWIP-120n's position-based resume strategy
    /// needs no re-threading at these sites.
    pub fn dispatch_scope(
        &self,
        stage_id: StageId,
        position: Option<StageInputPosition>,
        generation: Option<ReaderGeneration>,
    ) -> MiddlewareExecutionScope {
        match position {
            Some(p) => self.scope_at(ExecutionPosition {
                stage_id,
                position: p,
                generation,
            }),
            None => self.stage_scope(stage_id),
        }
    }

    /// Whether positioned work at `at` is reconstructing recorded history.
    pub fn is_reconstructing(&self, at: ExecutionPosition) -> bool {
        self.strategy.scope_at(at).is_deterministic_replay()
    }

    /// Middleware execution scope for positioned work.
    pub fn scope_at(&self, at: ExecutionPosition) -> MiddlewareExecutionScope {
        self.strategy.scope_at(at)
    }

    /// Middleware execution scope for position-less work.
    pub fn stage_scope(&self, stage: StageId) -> MiddlewareExecutionScope {
        self.strategy.stage_scope(stage)
    }

    /// Whether a missing recorded effect outcome at this position is corruption.
    pub fn missing_outcome_is_corruption(&self, at: ExecutionPosition) -> bool {
        self.strategy.missing_outcome_is_corruption(at)
    }

    /// Whether a source is reconstructing or polling live.
    pub fn source_phase_for(&self, stage: StageId) -> SourceExecutionPhase {
        self.strategy.source_phase_for(stage)
    }

    /// What a source does once its recorded input is exhausted.
    pub fn source_replay_exhausted(&self, stage: StageId) -> SourceReplayExhaustion {
        self.strategy.source_replay_exhausted(stage)
    }

    /// Whether a stage's heartbeat task should run.
    pub fn heartbeat_policy_for(&self, stage: StageId) -> HeartbeatExecutionPolicy {
        self.strategy.heartbeat_policy_for(stage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::MiddlewareExecutionScope as Scope;

    fn at() -> ExecutionPosition {
        ExecutionPosition {
            stage_id: StageId::new(),
            position: StageInputPosition(0),
            generation: None,
        }
    }

    #[test]
    fn live_strategy_answers() {
        let s = Live;
        assert_eq!(s.scope_at(at()), Scope::LiveHandler);
        assert_eq!(s.stage_scope(StageId::new()), Scope::LiveHandler);
        assert!(!s.missing_outcome_is_corruption(at()));
        assert_eq!(
            s.source_phase_for(StageId::new()),
            SourceExecutionPhase::Live
        );
        assert_eq!(
            s.heartbeat_policy_for(StageId::new()),
            HeartbeatExecutionPolicy::Active
        );
        assert!(!s.scope_at(at()).is_deterministic_replay());
    }

    #[test]
    fn replay_sealed_strategy_answers() {
        let s = Replay { incomplete: false };
        assert_eq!(s.scope_at(at()), Scope::StrictReplayHandler);
        assert_eq!(s.stage_scope(StageId::new()), Scope::StrictReplayHandler);
        assert!(s.missing_outcome_is_corruption(at()));
        assert_eq!(
            s.source_phase_for(StageId::new()),
            SourceExecutionPhase::Replaying
        );
        assert_eq!(
            s.source_replay_exhausted(StageId::new()),
            SourceReplayExhaustion::Terminate
        );
        assert_eq!(
            s.heartbeat_policy_for(StageId::new()),
            HeartbeatExecutionPolicy::Suppressed
        );
        assert!(s.scope_at(at()).is_deterministic_replay());
    }

    #[test]
    fn replay_incomplete_strategy_answers() {
        let s = Replay { incomplete: true };
        // Reconstruction scope yet a missing outcome that runs live: the two
        // queries deliberately diverge for the incomplete-archive case.
        assert_eq!(s.scope_at(at()), Scope::ResumeHandler);
        assert!(!s.missing_outcome_is_corruption(at()));
        assert!(s.scope_at(at()).is_deterministic_replay());
        assert_eq!(
            s.source_replay_exhausted(StageId::new()),
            SourceReplayExhaustion::Terminate
        );
    }

    #[test]
    fn resume_strategy_answers_track_the_frontier() {
        let state = Arc::new(ResumeState::new(ReaderGeneration(1)));
        let control = ResumeControl(Arc::clone(&state));
        let s = Resume {
            state: Arc::clone(&state),
        };
        let stage = StageId::new();
        let positioned = ExecutionPosition {
            stage_id: stage,
            position: StageInputPosition(3),
            generation: None,
        };

        // Catch-up: reconstruction scope, replaying source, dormant heartbeat,
        // lenient effect miss.
        assert_eq!(s.scope_at(positioned), Scope::ResumeHandler);
        assert_eq!(s.stage_scope(stage), Scope::ResumeHandler);
        assert!(s.scope_at(positioned).is_deterministic_replay());
        assert!(!s.missing_outcome_is_corruption(positioned));
        assert_eq!(s.source_phase_for(stage), SourceExecutionPhase::Replaying);
        assert_eq!(
            s.heartbeat_policy_for(stage),
            HeartbeatExecutionPolicy::DormantUntilLive
        );

        // Only registered infinite sources continue live at exhaustion.
        assert_eq!(
            s.source_replay_exhausted(stage),
            SourceReplayExhaustion::Terminate
        );
        control.register_infinite_source(stage);
        assert_eq!(
            s.source_replay_exhausted(stage),
            SourceReplayExhaustion::ContinueLive
        );

        // Crossing the boundary flips every per-stage answer to live.
        control.record_generation_boundary(stage, ReaderGeneration(1));
        assert_eq!(s.scope_at(positioned), Scope::LiveHandler);
        assert_eq!(s.stage_scope(stage), Scope::LiveHandler);
        assert_eq!(s.source_phase_for(stage), SourceExecutionPhase::Live);
        assert_eq!(
            s.heartbeat_policy_for(stage),
            HeartbeatExecutionPolicy::Active
        );

        // The frontier is monotonic: a stale boundary never regresses it.
        control.record_generation_boundary(stage, ReaderGeneration(0));
        assert_eq!(s.stage_scope(stage), Scope::LiveHandler);

        // Other stages stay reconstruction-scoped: the flip is per stage.
        let other = StageId::new();
        assert_eq!(s.stage_scope(other), Scope::ResumeHandler);
    }

    #[test]
    fn resume_scope_at_reads_the_event_generation() {
        let state = Arc::new(ResumeState::new(ReaderGeneration(1)));
        let control = ResumeControl(Arc::clone(&state));
        let s = Resume {
            state: Arc::clone(&state),
        };
        let stage = StageId::new();
        let at = |generation: Option<ReaderGeneration>| ExecutionPosition {
            stage_id: stage,
            position: StageInputPosition(3),
            generation,
        };

        // Stage frontier still 0: a recorded-generation event reconstructs, a
        // live-generation event runs live, and a generation-less dispatch
        // falls back to the frontier.
        assert_eq!(
            s.scope_at(at(Some(ReaderGeneration(0)))),
            Scope::ResumeHandler
        );
        assert_eq!(
            s.scope_at(at(Some(ReaderGeneration(1)))),
            Scope::LiveHandler
        );
        assert_eq!(s.scope_at(at(None)), Scope::ResumeHandler);

        // After the boundary the frontier fallback flips, while a late prefix
        // event still in flight stays reconstruction-scoped: the generation
        // rides the dispatch, so there is no race (F1).
        control.record_generation_boundary(stage, ReaderGeneration(1));
        assert_eq!(s.scope_at(at(None)), Scope::LiveHandler);
        assert_eq!(
            s.scope_at(at(Some(ReaderGeneration(0)))),
            Scope::ResumeHandler
        );
    }

    #[test]
    fn resume_effect_miss_is_positional_not_generational() {
        let state = Arc::new(ResumeState::new(ReaderGeneration(1)));
        let control = ResumeControl(Arc::clone(&state));
        let s = Resume {
            state: Arc::clone(&state),
        };
        let stage = StageId::new();
        let at = |p: u64| ExecutionPosition {
            stage_id: stage,
            position: StageInputPosition(p),
            generation: None,
        };

        // No registered mark: every miss is a live call.
        assert!(!s.missing_outcome_is_corruption(at(1)));

        // With the mark, a miss within the recorded range is a torn prefix
        // and one beyond it is live, regardless of the stage frontier.
        control.record_effect_high_water(stage, StageInputPosition(5));
        assert!(s.missing_outcome_is_corruption(at(1)));
        assert!(s.missing_outcome_is_corruption(at(5)));
        assert!(!s.missing_outcome_is_corruption(at(6)));
        control.record_generation_boundary(stage, ReaderGeneration(1));
        assert!(s.missing_outcome_is_corruption(at(5)));
        assert!(!s.missing_outcome_is_corruption(at(6)));
    }

    #[test]
    fn replay_incomplete_derivation_matches_old_policy() {
        // Sealed archives are strict regardless of the flag.
        assert!(!replay_incomplete(ArchiveStatus::Completed, true));
        assert!(!replay_incomplete(ArchiveStatus::Cancelled, true));
        // Unsealed is lenient only under allow_incomplete_archive.
        assert!(replay_incomplete(ArchiveStatus::Failed, true));
        assert!(replay_incomplete(ArchiveStatus::Unknown, true));
        assert!(!replay_incomplete(ArchiveStatus::Failed, false));
        assert!(!replay_incomplete(ArchiveStatus::Unknown, false));
    }

    #[test]
    fn live_runtime_execution_is_never_reconstructing() {
        let exec = RuntimeExecution::new(RuntimeMode::Live, None);
        assert!(exec.archive_for_io().is_none());
        assert!(!exec.is_reconstructing(at()));
        assert_eq!(
            exec.heartbeat_policy_for(StageId::new()),
            HeartbeatExecutionPolicy::Active
        );
    }

    #[test]
    fn handler_scope_for_resolves_every_source() {
        let exec = RuntimeExecution::new(RuntimeMode::Live, None);
        let sid = StageId::new();
        let p = StageInputPosition(7);
        // Live answers LiveHandler for every source shape; the point of this
        // test is that resolution dispatches without panicking and a causeless
        // flow-control / lifecycle row falls back to the stage-level answer.
        assert_eq!(
            exec.handler_scope_for(ExecutionPositionSource::Data {
                stage_id: sid,
                position: p,
                generation: None
            }),
            Scope::LiveHandler
        );
        assert_eq!(
            exec.handler_scope_for(ExecutionPositionSource::FlowControl {
                stage_id: sid,
                cause: Some(ExecutionPosition {
                    stage_id: sid,
                    position: p,
                    generation: None
                })
            }),
            Scope::LiveHandler
        );
        assert_eq!(
            exec.handler_scope_for(ExecutionPositionSource::FlowControl {
                stage_id: sid,
                cause: None
            }),
            Scope::LiveHandler
        );
        assert_eq!(
            exec.handler_scope_for(ExecutionPositionSource::StageLifecycle { stage_id: sid }),
            Scope::LiveHandler
        );
    }
}
