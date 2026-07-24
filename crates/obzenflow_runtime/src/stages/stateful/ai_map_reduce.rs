// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-owned keyed collector used by the AI map-reduce composite lowering.
//!
//! The collector is completion-driven and keyed by the outer input event ID (`job_key`).
//! It consumes framework-internal transport events:
//! - `AiMapReducePlanningManifest` authored by chunk and forwarded through map
//! - `AiMapReduceTaggedPartial<serde_json::Value>` from the map stage
//! - `AiMapReduceChunkFailed` from the map stage on terminal failure
//!
//! The public typed contract remains `Partial -> Collected` even though the runtime
//! delivery surface includes the internal manifest / wrapper payloads. This is
//! intentional (FLOWIP-086z-part-2).

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handler_error::StageFatal;
use crate::stages::common::handlers::{
    StatefulHandler, StatefulTerminationKind, TerminalValidation,
};
use crate::typing::StatefulTyping;
use async_trait::async_trait;
use obzenflow_core::ai::{
    canonical_json_bytes_v1, AiMapReduceChunkFailed, AiMapReduceJobFailed,
    AiMapReducePlanningManifest, AiMapReduceReduceInput, AiMapReduceTaggedPartial,
    ChunkPlanningSummary,
};
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::context::CompositeActivationContext;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, EventId, TypedPayload};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_JOB_TTL: Duration = Duration::from_secs(300);
const DEFAULT_MAX_OPEN_JOBS: usize = 100;

const JOB_FAILED_EVENT_TYPE: &str = "ai.map_reduce.collect_failed.v1";
const JOB_TTL_EXPIRED_EVENT_TYPE: &str = "ai.map_reduce.job_ttl_expired.v1";
const JOB_CAPACITY_EXCEEDED_EVENT_TYPE: &str = "ai.map_reduce.max_open_jobs_exceeded.v1";

#[derive(Debug, Clone, Serialize)]
struct JobFailurePayload {
    job_key: EventId,
    reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

#[derive(Clone)]
#[allow(clippy::type_complexity)]
struct PlanningHook<Collected>(Arc<dyn Fn(&mut Collected, &ChunkPlanningSummary) + Send + Sync>);

impl<Collected> fmt::Debug for PlanningHook<Collected> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PlanningHook").field(&"<closure>").finish()
    }
}

#[derive(Clone)]
#[allow(clippy::type_complexity)]
struct ManifestHook<Collected>(
    Arc<
        dyn Fn(&mut Collected, &AiMapReducePlanningManifest) -> Result<(), HandlerError>
            + Send
            + Sync,
    >,
);

impl<Collected> fmt::Debug for ManifestHook<Collected> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ManifestHook").field(&"<closure>").finish()
    }
}

#[derive(Clone)]
#[allow(clippy::type_complexity)]
struct Accumulator<Partial, Collected>(
    Arc<dyn Fn(&mut Collected, &Partial) + Send + Sync + 'static>,
);

impl<Partial, Collected> fmt::Debug for Accumulator<Partial, Collected> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Accumulator").field(&"<closure>").finish()
    }
}

#[derive(Debug, Clone)]
struct PendingJob<Partial, Collected> {
    manifest: Option<AiMapReducePlanningManifest>,
    // Optional before the manifest arrives; validated once both surfaces are present.
    declared_chunk_count: Option<usize>,
    seen_chunk_indexes: BTreeSet<usize>,
    partials_by_index: Vec<Option<Partial>>,
    collected: Collected,
    created_at: Instant,
    lineage_parent: ChainEvent,
    planning_applied: bool,
    queued_for_emit: bool,
}

#[derive(Debug, Clone)]
pub struct CollectByInputState<Partial, Collected> {
    jobs: HashMap<EventId, PendingJob<Partial, Collected>>,
    ready: VecDeque<EventId>,
    pending_errors: VecDeque<ChainEvent>,
}

impl<Partial, Collected> CollectByInputState<Partial, Collected> {
    fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            ready: VecDeque::new(),
            pending_errors: VecDeque::new(),
        }
    }
}

#[derive(Clone)]
pub struct CollectByInput<Partial, Collected> {
    initial: Collected,
    accumulate: Accumulator<Partial, Collected>,
    planning_hook: Option<PlanningHook<Collected>>,
    manifest_hook: Option<ManifestHook<Collected>>,
    job_ttl: Duration,
    max_open_jobs: usize,
    lineage: obzenflow_core::config::LineagePolicy,
    _phantom: PhantomData<Partial>,
}

#[doc(hidden)]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SeededAiMapReduceAccumulator<Seed, Collected> {
    job_key: Option<EventId>,
    seed: Option<Seed>,
    collected: Collected,
    planning: ChunkPlanningSummary,
}

impl<Seed, Collected> Serialize for SeededAiMapReduceAccumulator<Seed, Collected>
where
    Seed: Serialize,
    Collected: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let job_key = self
            .job_key
            .ok_or_else(|| serde::ser::Error::custom("collector output has no job_key"))?;
        let seed = self
            .seed
            .as_ref()
            .ok_or_else(|| serde::ser::Error::custom("collector output has no seed"))?;
        AiMapReduceReduceInput {
            job_key,
            seed,
            collected: &self.collected,
            planning: self.planning.clone(),
        }
        .serialize(serializer)
    }
}

impl<Seed, Collected> TypedPayload for SeededAiMapReduceAccumulator<Seed, Collected>
where
    Seed: Serialize + DeserializeOwned,
    Collected: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.map_reduce.reduce_input";
    const SCHEMA_VERSION: u32 = 2;
}

#[doc(hidden)]
#[derive(Clone)]
pub struct SeededCollectByInput<Partial, Seed, Collected> {
    initial: Collected,
    accumulate: Accumulator<Partial, Collected>,
    planning_hook: Option<PlanningHook<Collected>>,
    composite_id: Option<obzenflow_core::id::CompositeId>,
    lineage: obzenflow_core::config::LineagePolicy,
    _phantom: PhantomData<Seed>,
}

const COLLECTOR_MAX_OPEN_JOBS: usize = 100;
const COLLECTOR_MAX_CHUNKS_PER_JOB: usize = 4096;
const COLLECTOR_MAX_FACT_BYTES: usize = 4 * 1024 * 1024;
const COLLECTOR_MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectorPhase {
    Open,
    FailureSealed,
    SuccessReady,
    HandoffInFlight,
}

#[derive(Debug, Clone)]
enum StoredDispositionKind {
    Success(serde_json::Value),
    Failure,
}

#[derive(Debug, Clone)]
struct StoredDisposition {
    canonical: Vec<u8>,
    kind: StoredDispositionKind,
}

#[derive(Debug, Clone)]
struct StoredManifest {
    manifest: AiMapReducePlanningManifest,
    canonical: Vec<u8>,
    parent: ChainEvent,
}

#[derive(Debug, Clone)]
struct CollectorJob {
    activation: CompositeActivationContext,
    manifest: Option<StoredManifest>,
    chunk_count: Option<usize>,
    dispositions: BTreeMap<usize, StoredDisposition>,
    phase: CollectorPhase,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct SeededCollectByInputState<Partial, Seed, Collected> {
    jobs: BTreeMap<EventId, CollectorJob>,
    ready: VecDeque<EventId>,
    in_flight: Option<EventId>,
    retained_bytes: usize,
    _phantom: PhantomData<(Partial, Seed, Collected)>,
}

impl<Partial, Seed, Collected> SeededCollectByInputState<Partial, Seed, Collected> {
    fn new() -> Self {
        Self {
            jobs: BTreeMap::new(),
            ready: VecDeque::new(),
            in_flight: None,
            retained_bytes: 0,
            _phantom: PhantomData,
        }
    }
}

impl<Partial, Seed, Collected> fmt::Debug for SeededCollectByInput<Partial, Seed, Collected> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeededCollectByInput")
            .finish_non_exhaustive()
    }
}

impl<Partial, Collected> fmt::Debug for CollectByInput<Partial, Collected>
where
    Collected: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectByInput")
            .field("job_ttl", &self.job_ttl)
            .field("max_open_jobs", &self.max_open_jobs)
            .finish()
    }
}

impl<Partial, Collected> CollectByInput<Partial, Collected> {
    pub fn new<F>(initial: Collected, accumulate: F) -> Self
    where
        F: Fn(&mut Collected, &Partial) + Send + Sync + 'static,
    {
        Self {
            initial,
            accumulate: Accumulator(Arc::new(accumulate)),
            planning_hook: None,
            manifest_hook: None,
            job_ttl: DEFAULT_JOB_TTL,
            max_open_jobs: DEFAULT_MAX_OPEN_JOBS,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            _phantom: PhantomData,
        }
    }

    pub fn with_planning_summary<F>(mut self, hook: F) -> Self
    where
        F: Fn(&mut Collected, &ChunkPlanningSummary) + Send + Sync + 'static,
    {
        self.planning_hook = Some(PlanningHook(Arc::new(hook)));
        self
    }

    pub fn with_manifest_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&mut Collected, &AiMapReducePlanningManifest) -> Result<(), HandlerError>
            + Send
            + Sync
            + 'static,
    {
        self.manifest_hook = Some(ManifestHook(Arc::new(hook)));
        self
    }

    /// Wrap this collector so its output payload becomes `AiMapReduceReduceInput<Seed, Collected>`.
    ///
    /// The reduce handler surface for map-reduce is `(Seed, Collected) -> Out`.
    /// This helper injects the seed payload (from the planning manifest) into the
    /// collected output so downstream stages do not need to reconstruct it.
    pub fn with_seed<Seed>(self) -> SeededCollectByInput<Partial, Seed, Collected>
    where
        Seed: DeserializeOwned + Serialize + Send + Sync + 'static,
        Partial: DeserializeOwned + Send + Sync + 'static,
        Collected: Clone + Serialize + TypedPayload + Send + Sync + 'static,
    {
        let CollectByInput {
            initial,
            accumulate,
            planning_hook,
            manifest_hook: _,
            job_ttl: _,
            max_open_jobs: _,
            lineage,
            _phantom: _,
        } = self;

        SeededCollectByInput {
            initial,
            accumulate,
            planning_hook,
            composite_id: None,
            lineage,
            _phantom: PhantomData,
        }
    }

    pub fn with_job_ttl(mut self, ttl: Duration) -> Self {
        self.job_ttl = ttl;
        self
    }

    pub fn with_max_open_jobs(mut self, max: usize) -> Self {
        self.max_open_jobs = max;
        self
    }

    fn queue_job_if_ready(state: &mut CollectByInputState<Partial, Collected>, job_key: EventId) {
        let Some(job) = state.jobs.get_mut(&job_key) else {
            return;
        };

        let Some(manifest) = job.manifest.as_ref() else {
            return;
        };

        if (manifest.chunk_count == 0 || job.seen_chunk_indexes.len() == manifest.chunk_count)
            && !job.queued_for_emit
        {
            job.queued_for_emit = true;
            state.ready.push_back(job_key);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fail_job(
        &self,
        state: &mut CollectByInputState<Partial, Collected>,
        job_key: EventId,
        parent: &ChainEvent,
        reason: impl Into<String>,
        event_type: &'static str,
        kind: ErrorKind,
        details: Option<serde_json::Value>,
    ) {
        let reason = reason.into();

        let payload = JobFailurePayload {
            job_key,
            reason: reason.clone(),
            details,
        };

        let payload =
            serde_json::to_value(payload).unwrap_or_else(|_| json!({ "job_key": job_key }));
        let error_event = ChainEventFactory::derived_data_event(
            parent.writer_id,
            parent,
            event_type,
            payload,
            self.lineage,
        )
        .mark_as_error(reason, kind);
        state.pending_errors.push_back(error_event);

        state.jobs.remove(&job_key);
    }

    fn push_internal_decode_error(
        state: &mut CollectByInputState<Partial, Collected>,
        event: &ChainEvent,
        reason: impl Into<String>,
    ) {
        let err = event
            .clone()
            .mark_as_error(reason, ErrorKind::Deserialization);
        state.pending_errors.push_back(err);
    }

    fn open_or_get_job<'a>(
        &self,
        state: &'a mut CollectByInputState<Partial, Collected>,
        job_key: EventId,
        parent: &ChainEvent,
        declared_chunk_count: Option<usize>,
    ) -> Option<&'a mut PendingJob<Partial, Collected>>
    where
        Collected: Clone,
    {
        if let Some(job) = state.jobs.get_mut(&job_key) {
            // Validate declared chunk count coherence if provided.
            if let Some(count) = declared_chunk_count {
                if let Some(existing) = job.declared_chunk_count {
                    if existing != count {
                        self.fail_job(
                            state,
                            job_key,
                            parent,
                            format!(
                                "ai_map_reduce: conflicting chunk_count for job (existing={existing}, incoming={count})"
                            ),
                            JOB_FAILED_EVENT_TYPE,
                            ErrorKind::PermanentFailure,
                            None,
                        );
                        return None;
                    }
                } else {
                    job.declared_chunk_count = Some(count);
                    job.partials_by_index = (0..count).map(|_| None).collect();
                }
            }

            return state.jobs.get_mut(&job_key);
        }

        if state.jobs.len() >= self.max_open_jobs {
            self.fail_job(
                state,
                job_key,
                parent,
                format!(
                    "ai_map_reduce: max_open_jobs exceeded (max={}, open={})",
                    self.max_open_jobs,
                    state.jobs.len()
                ),
                JOB_CAPACITY_EXCEEDED_EVENT_TYPE,
                ErrorKind::PermanentFailure,
                Some(json!({ "max_open_jobs": self.max_open_jobs, "open_jobs": state.jobs.len() })),
            );
            return None;
        }

        let job = PendingJob {
            manifest: None,
            declared_chunk_count,
            seen_chunk_indexes: BTreeSet::new(),
            partials_by_index: declared_chunk_count
                .map(|count| (0..count).map(|_| None).collect())
                .unwrap_or_default(),
            collected: self.initial.clone(),
            created_at: Instant::now(),
            lineage_parent: parent.clone(),
            planning_applied: false,
            queued_for_emit: false,
        };

        state.jobs.insert(job_key, job);
        state.jobs.get_mut(&job_key)
    }
}

impl<Partial, Collected> StatefulTyping for CollectByInput<Partial, Collected> {
    type Input = Partial;
    type Output = Collected;
}

impl<Partial, Seed, Collected> StatefulTyping for SeededCollectByInput<Partial, Seed, Collected> {
    type Input = Partial;
    type Output = AiMapReduceReduceInput<Seed, Collected>;
}

impl<Partial, Seed, Collected> SeededCollectByInput<Partial, Seed, Collected> {
    /// Bind the generated collector to its exact enclosing composite.
    #[doc(hidden)]
    pub fn with_composite_id(mut self, composite_id: obzenflow_core::id::CompositeId) -> Self {
        self.composite_id = Some(composite_id);
        self
    }

    fn protocol_fatal(detail: impl Into<String>) -> HandlerError {
        HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            detail,
        ))
    }

    fn resource_fatal(detail: impl Into<String>) -> HandlerError {
        HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Resource,
            StageFatalReason::ResourceExhaustion,
            detail,
        ))
    }

    fn activation(&self, event: &ChainEvent) -> Result<CompositeActivationContext, HandlerError> {
        let matching = event
            .composite_activations()
            .iter()
            .filter(|activation| {
                self.composite_id
                    .as_ref()
                    .is_none_or(|composite_id| &activation.composite_id == composite_id)
            })
            .collect::<Vec<_>>();
        let [activation] = matching.as_slice() else {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce collector expected exactly one matching composite activation on event {}, found {}",
                event.id,
                matching.len()
            )));
        };
        Ok((*activation).clone())
    }

    fn ensure_fact_bound(bytes: &[u8]) -> Result<(), HandlerError> {
        if bytes.len() > COLLECTOR_MAX_FACT_BYTES {
            return Err(Self::resource_fatal(format!(
                "ai_map_reduce collector fact is {} bytes; v1 limit is {}",
                bytes.len(),
                COLLECTOR_MAX_FACT_BYTES
            )));
        }
        Ok(())
    }

    fn ensure_charge(
        state: &SeededCollectByInputState<Partial, Seed, Collected>,
        bytes: usize,
    ) -> Result<usize, HandlerError> {
        let retained = state
            .retained_bytes
            .checked_add(bytes)
            .ok_or_else(|| Self::resource_fatal("collector retained-byte counter overflow"))?;
        if retained > COLLECTOR_MAX_RETAINED_BYTES {
            return Err(Self::resource_fatal(format!(
                "ai_map_reduce collector would retain {retained} bytes; v1 stage limit is {}",
                COLLECTOR_MAX_RETAINED_BYTES
            )));
        }
        Ok(retained)
    }

    fn open_job_count(state: &SeededCollectByInputState<Partial, Seed, Collected>) -> usize {
        state
            .jobs
            .values()
            .filter(|job| {
                matches!(
                    job.phase,
                    CollectorPhase::Open | CollectorPhase::FailureSealed
                )
            })
            .count()
    }

    fn ensure_job<'a>(
        state: &'a mut SeededCollectByInputState<Partial, Seed, Collected>,
        job_key: EventId,
        activation: &CompositeActivationContext,
    ) -> Result<&'a mut CollectorJob, HandlerError> {
        if !state.jobs.contains_key(&job_key) {
            if Self::open_job_count(state) >= COLLECTOR_MAX_OPEN_JOBS {
                return Err(Self::resource_fatal(format!(
                    "ai_map_reduce collector open-job limit {} exceeded",
                    COLLECTOR_MAX_OPEN_JOBS
                )));
            }
            state.jobs.insert(
                job_key,
                CollectorJob {
                    activation: activation.clone(),
                    manifest: None,
                    chunk_count: None,
                    dispositions: BTreeMap::new(),
                    phase: CollectorPhase::Open,
                    retained_bytes: 0,
                },
            );
        }

        let job = state
            .jobs
            .get_mut(&job_key)
            .expect("collector job was inserted above");
        if job.activation != *activation {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce collector activation disagrees for job {job_key}"
            )));
        }
        if matches!(
            job.phase,
            CollectorPhase::SuccessReady | CollectorPhase::HandoffInFlight
        ) {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce collector received a fact after job {job_key} closed"
            )));
        }
        Ok(job)
    }

    fn validate_job_key(
        job_key: EventId,
        activation: &CompositeActivationContext,
    ) -> Result<(), HandlerError> {
        if job_key != activation.activation {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce collector job_key {job_key} does not equal durable activation {}",
                activation.activation
            )));
        }
        Ok(())
    }

    fn validate_chunk_count(chunk_count: usize, terminal: bool) -> Result<(), HandlerError> {
        if terminal && chunk_count == 0 {
            return Err(Self::protocol_fatal(
                "ai_map_reduce terminal disposition has chunk_count=0",
            ));
        }
        if chunk_count > COLLECTOR_MAX_CHUNKS_PER_JOB {
            return Err(Self::resource_fatal(format!(
                "ai_map_reduce job declares {chunk_count} chunks; v1 limit is {}",
                COLLECTOR_MAX_CHUNKS_PER_JOB
            )));
        }
        Ok(())
    }

    fn maybe_close(
        state: &mut SeededCollectByInputState<Partial, Seed, Collected>,
        job_key: EventId,
    ) {
        let Some(job) = state.jobs.get_mut(&job_key) else {
            return;
        };
        let Some(manifest) = job.manifest.as_ref() else {
            return;
        };
        if job.dispositions.len() == manifest.manifest.chunk_count {
            job.phase = CollectorPhase::SuccessReady;
            state.ready.push_back(job_key);
        }
    }

    fn accumulate_protocol(
        &self,
        state: &mut SeededCollectByInputState<Partial, Seed, Collected>,
        event: ChainEvent,
    ) -> Result<(), HandlerError> {
        let ChainEventContent::Data {
            event_type,
            payload,
        } = &event.content
        else {
            return Err(Self::protocol_fatal(
                "ai_map_reduce collector received a non-Data event",
            ));
        };
        let activation = self.activation(&event)?;
        let canonical =
            canonical_json_bytes_v1(event_type, &activation, payload.clone()).map_err(|error| {
                Self::protocol_fatal(format!(
                    "ai_map_reduce collector canonicalisation failed: {error}"
                ))
            })?;
        Self::ensure_fact_bound(&canonical)?;

        if AiMapReducePlanningManifest::event_type_matches(event_type) {
            let manifest =
                AiMapReducePlanningManifest::try_from_event(&event).map_err(|error| {
                    Self::protocol_fatal(format!(
                        "ai_map_reduce planning manifest decode failed: {error}"
                    ))
                })?;
            Self::validate_job_key(manifest.job_key, &activation)?;
            Self::validate_chunk_count(manifest.chunk_count, false)?;

            if let Some(existing) = state
                .jobs
                .get(&manifest.job_key)
                .and_then(|job| job.manifest.as_ref())
            {
                return if existing.canonical == canonical {
                    Ok(())
                } else {
                    Err(Self::protocol_fatal(format!(
                        "ai_map_reduce collector received conflicting manifests for job {}",
                        manifest.job_key
                    )))
                };
            }
            let charged = canonical.len();
            let retained_after = Self::ensure_charge(state, charged)?;
            {
                let job = Self::ensure_job(state, manifest.job_key, &activation)?;
                if let Some(count) = job.chunk_count {
                    if count != manifest.chunk_count {
                        return Err(Self::protocol_fatal(format!(
                            "ai_map_reduce manifest count {} disagrees with terminal count {count} for job {}",
                            manifest.chunk_count, manifest.job_key
                        )));
                    }
                }
                let job_retained_after = job
                    .retained_bytes
                    .checked_add(charged)
                    .ok_or_else(|| Self::resource_fatal("collector job byte counter overflow"))?;
                if job.chunk_count.is_none() {
                    job.chunk_count = Some(manifest.chunk_count);
                }
                job.retained_bytes = job_retained_after;
                job.manifest = Some(StoredManifest {
                    manifest: manifest.clone(),
                    canonical,
                    parent: event,
                });
            }
            state.retained_bytes = retained_after;
            Self::maybe_close(state, manifest.job_key);
            return Ok(());
        }

        let (job_key, chunk_index, chunk_count, disposition) =
            if AiMapReduceTaggedPartial::<serde_json::Value>::event_type_matches(event_type) {
                let tagged = AiMapReduceTaggedPartial::<serde_json::Value>::try_from_event(&event)
                    .map_err(|error| {
                        Self::protocol_fatal(format!(
                            "ai_map_reduce tagged partial decode failed: {error}"
                        ))
                    })?;
                (
                    tagged.job_key,
                    tagged.chunk_index,
                    tagged.chunk_count,
                    StoredDispositionKind::Success(tagged.partial),
                )
            } else if AiMapReduceChunkFailed::event_type_matches(event_type) {
                let failed = AiMapReduceChunkFailed::try_from_event(&event).map_err(|error| {
                    Self::protocol_fatal(format!(
                        "ai_map_reduce chunk failure decode failed: {error}"
                    ))
                })?;
                (
                    failed.job_key,
                    failed.chunk_index,
                    failed.chunk_count,
                    StoredDispositionKind::Failure,
                )
            } else {
                return Err(Self::protocol_fatal(format!(
                    "ai_map_reduce collector received undeclared event type '{event_type}'"
                )));
            };

        Self::validate_job_key(job_key, &activation)?;
        Self::validate_chunk_count(chunk_count, true)?;
        if chunk_index >= chunk_count {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce chunk index {chunk_index} is outside 0..{chunk_count} for job {job_key}"
            )));
        }

        if let Some(existing) = state
            .jobs
            .get(&job_key)
            .and_then(|job| job.dispositions.get(&chunk_index))
        {
            return if existing.canonical == canonical {
                Ok(())
            } else {
                Err(Self::protocol_fatal(format!(
                    "ai_map_reduce collector received conflicting dispositions for job {job_key} chunk {chunk_index}"
                )))
            };
        }
        let charged = canonical.len();
        let retained_after = Self::ensure_charge(state, charged)?;
        let job = Self::ensure_job(state, job_key, &activation)?;
        if let Some(count) = job.chunk_count {
            if count != chunk_count {
                return Err(Self::protocol_fatal(format!(
                    "ai_map_reduce terminal count {chunk_count} disagrees with count {count} for job {job_key}"
                )));
            }
        }
        let job_retained_after = job
            .retained_bytes
            .checked_add(charged)
            .ok_or_else(|| Self::resource_fatal("collector job byte counter overflow"))?;
        if job.chunk_count.is_none() {
            job.chunk_count = Some(chunk_count);
        }
        let is_failure = matches!(disposition, StoredDispositionKind::Failure);
        job.dispositions.insert(
            chunk_index,
            StoredDisposition {
                canonical,
                kind: disposition,
            },
        );
        job.retained_bytes = job_retained_after;
        if is_failure {
            job.phase = CollectorPhase::FailureSealed;
        }
        state.retained_bytes = retained_after;
        Self::maybe_close(state, job_key);
        Ok(())
    }
}

#[async_trait]
impl<Partial, Collected> StatefulHandler for CollectByInput<Partial, Collected>
where
    Partial: DeserializeOwned + Clone + Send + Sync + 'static,
    Collected: Clone + Serialize + TypedPayload + Send + Sync + 'static,
{
    type State = CollectByInputState<Partial, Collected>;

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
    }

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let ChainEventContent::Data { event_type, .. } = &event.content else {
            return;
        };

        // --------------------------------------------------------------------
        // Planning manifest (chunk -> map -> collect)
        // --------------------------------------------------------------------
        if AiMapReducePlanningManifest::event_type_matches(event_type) {
            let manifest = match AiMapReducePlanningManifest::try_from_event(&event) {
                Ok(v) => v,
                Err(err) => {
                    Self::push_internal_decode_error(
                        state,
                        &event,
                        format!("ai_map_reduce: planning manifest decode failed: {err}"),
                    );
                    return;
                }
            };

            let job_key = manifest.job_key;

            let Some(job) =
                self.open_or_get_job(state, job_key, &event, Some(manifest.chunk_count))
            else {
                return;
            };

            if let Some(existing) = job.manifest.as_ref() {
                if existing.chunk_count != manifest.chunk_count
                    || existing.planning != manifest.planning
                {
                    self.fail_job(
                        state,
                        job_key,
                        &event,
                        "ai_map_reduce: conflicting planning manifest for job",
                        JOB_FAILED_EVENT_TYPE,
                        ErrorKind::PermanentFailure,
                        None,
                    );
                    return;
                }

                // Duplicate manifest, same content: ignore.
                return;
            }

            // Validate against any chunk_count already observed from partial wrappers.
            if let Some(declared) = job.declared_chunk_count {
                if declared != manifest.chunk_count {
                    self.fail_job(
                        state,
                        job_key,
                        &event,
                        format!(
                            "ai_map_reduce: manifest chunk_count mismatch (manifest={}, partials={declared})",
                            manifest.chunk_count
                        ),
                        JOB_FAILED_EVENT_TYPE,
                        ErrorKind::PermanentFailure,
                        None,
                    );
                    return;
                }
            }

            job.manifest = Some(manifest.clone());
            job.lineage_parent = event.clone();

            if !job.planning_applied {
                if let Some(hook) = self.planning_hook.as_ref() {
                    (hook.0)(&mut job.collected, &manifest.planning);
                }
                job.planning_applied = true;
            }

            let manifest_hook_err = self
                .manifest_hook
                .as_ref()
                .and_then(|hook| (hook.0)(&mut job.collected, &manifest).err());

            if let Some(err) = manifest_hook_err {
                self.fail_job(
                    state,
                    job_key,
                    &event,
                    format!("ai_map_reduce: manifest hook failed: {err}"),
                    JOB_FAILED_EVENT_TYPE,
                    ErrorKind::Deserialization,
                    None,
                );
                return;
            }

            Self::queue_job_if_ready(state, job_key);
            return;
        }

        // --------------------------------------------------------------------
        // Tagged partial (map -> collect)
        // --------------------------------------------------------------------
        if AiMapReduceTaggedPartial::<serde_json::Value>::event_type_matches(event_type) {
            let tagged = match AiMapReduceTaggedPartial::<serde_json::Value>::try_from_event(&event)
            {
                Ok(v) => v,
                Err(err) => {
                    Self::push_internal_decode_error(
                        state,
                        &event,
                        format!("ai_map_reduce: tagged partial decode failed: {err}"),
                    );
                    return;
                }
            };

            if tagged.chunk_count == 0 {
                self.fail_job(
                    state,
                    tagged.job_key,
                    &event,
                    "ai_map_reduce: tagged partial has chunk_count=0",
                    JOB_FAILED_EVENT_TYPE,
                    ErrorKind::PermanentFailure,
                    None,
                );
                return;
            }

            if tagged.chunk_index >= tagged.chunk_count {
                self.fail_job(
                    state,
                    tagged.job_key,
                    &event,
                    format!(
                        "ai_map_reduce: tagged partial chunk_index out of range (index={}, count={})",
                        tagged.chunk_index, tagged.chunk_count
                    ),
                    JOB_FAILED_EVENT_TYPE,
                    ErrorKind::PermanentFailure,
                    None,
                );
                return;
            }

            let partial: Partial = match serde_json::from_value(tagged.partial.clone()) {
                Ok(v) => v,
                Err(err) => {
                    Self::push_internal_decode_error(
                        state,
                        &event,
                        format!("ai_map_reduce: partial decode failed: {err}"),
                    );
                    self.fail_job(
                        state,
                        tagged.job_key,
                        &event,
                        "ai_map_reduce: partial decode failed (job failed)",
                        JOB_FAILED_EVENT_TYPE,
                        ErrorKind::Deserialization,
                        None,
                    );
                    return;
                }
            };

            let manifest_chunk_count = {
                let Some(job) =
                    self.open_or_get_job(state, tagged.job_key, &event, Some(tagged.chunk_count))
                else {
                    return;
                };
                job.manifest.as_ref().map(|manifest| manifest.chunk_count)
            };

            if let Some(manifest_chunk_count) = manifest_chunk_count {
                if manifest_chunk_count != tagged.chunk_count {
                    self.fail_job(
                        state,
                        tagged.job_key,
                        &event,
                        format!(
                            "ai_map_reduce: tagged partial chunk_count mismatch (manifest={}, partial={})",
                            manifest_chunk_count, tagged.chunk_count
                        ),
                        JOB_FAILED_EVENT_TYPE,
                        ErrorKind::PermanentFailure,
                        None,
                    );
                    return;
                }
            }

            let Some(job) =
                self.open_or_get_job(state, tagged.job_key, &event, Some(tagged.chunk_count))
            else {
                return;
            };

            // Deduplicate by (job_key, chunk_index).
            if job.seen_chunk_indexes.contains(&tagged.chunk_index) {
                return;
            }

            job.seen_chunk_indexes.insert(tagged.chunk_index);
            if tagged.chunk_index >= job.partials_by_index.len() {
                self.fail_job(
                    state,
                    tagged.job_key,
                    &event,
                    "ai_map_reduce: tagged partial chunk_index out of range for collector storage",
                    JOB_FAILED_EVENT_TYPE,
                    ErrorKind::PermanentFailure,
                    None,
                );
                return;
            }
            job.partials_by_index[tagged.chunk_index] = Some(partial);
            job.lineage_parent = event.clone();

            Self::queue_job_if_ready(state, tagged.job_key);
            return;
        }

        // --------------------------------------------------------------------
        // Terminal chunk failure marker (map -> collect)
        // --------------------------------------------------------------------
        if AiMapReduceChunkFailed::event_type_matches(event_type) {
            let failed = match AiMapReduceChunkFailed::try_from_event(&event) {
                Ok(v) => v,
                Err(err) => {
                    Self::push_internal_decode_error(
                        state,
                        &event,
                        format!("ai_map_reduce: chunk_failed decode failed: {err}"),
                    );
                    return;
                }
            };

            // Record the marker as a structured failure in the collector's error journal.
            let err = event
                .clone()
                .mark_as_error(format!("{:?}", failed.cause), ErrorKind::PermanentFailure);
            state.pending_errors.push_back(err);

            state.jobs.remove(&failed.job_key);
        }
    }

    fn initial_state(&self) -> Self::State {
        CollectByInputState::new()
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        if !state.pending_errors.is_empty() || !state.ready.is_empty() {
            return true;
        }

        state
            .jobs
            .values()
            .any(|job| job.created_at.elapsed() >= self.job_ttl)
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(err) = state.pending_errors.pop_front() {
            return Ok(vec![err]);
        }

        while let Some(job_key) = state.ready.pop_front() {
            let Some(mut job) = state.jobs.remove(&job_key) else {
                continue;
            };

            for partial in job.partials_by_index.iter().flatten() {
                (self.accumulate.0)(&mut job.collected, partial);
            }

            let payload = serde_json::to_value(job.collected).map_err(|err| {
                HandlerError::Validation(format!("ai_map_reduce: collected encode failed: {err}"))
            })?;

            let event_type = Collected::versioned_event_type();
            let out = ChainEventFactory::derived_data_event(
                job.lineage_parent.writer_id,
                &job.lineage_parent,
                event_type,
                payload,
                self.lineage,
            );

            return Ok(vec![out]);
        }

        if let Some(expired_key) = state.jobs.iter().find_map(|(k, job)| {
            if job.created_at.elapsed() >= self.job_ttl {
                Some(*k)
            } else {
                None
            }
        }) {
            let job = state
                .jobs
                .remove(&expired_key)
                .expect("expired job present");
            self.fail_job(
                state,
                expired_key,
                &job.lineage_parent,
                "ai_map_reduce: job TTL expired",
                JOB_TTL_EXPIRED_EVENT_TYPE,
                ErrorKind::PermanentFailure,
                Some(json!({ "job_ttl_ms": self.job_ttl.as_millis() as u64 })),
            );
        }

        Ok(Vec::new())
    }

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        // Drain is conservative: emit only complete jobs; fail incomplete ones.
        let mut out = Vec::new();

        for (job_key, job) in state.jobs.iter() {
            if let Some(manifest) = job.manifest.as_ref() {
                if manifest.chunk_count == 0 || job.seen_chunk_indexes.len() == manifest.chunk_count
                {
                    let mut collected = job.collected.clone();
                    for partial in job.partials_by_index.iter().flatten() {
                        (self.accumulate.0)(&mut collected, partial);
                    }

                    let payload = serde_json::to_value(collected).map_err(|err| {
                        HandlerError::Validation(format!(
                            "ai_map_reduce: collected encode failed during drain: {err}"
                        ))
                    })?;
                    let event_type = Collected::versioned_event_type();
                    out.push(ChainEventFactory::derived_data_event(
                        job.lineage_parent.writer_id,
                        &job.lineage_parent,
                        event_type,
                        payload,
                        self.lineage,
                    ));
                    continue;
                }
            }

            // Completion is impossible once the stage drains.
            let payload = serde_json::to_value(JobFailurePayload {
                job_key: *job_key,
                reason: "ai_map_reduce: job incomplete at drain".to_string(),
                details: job.manifest.as_ref().map(|m| {
                    json!({
                        "chunk_count": m.chunk_count,
                        "partials_seen": job.seen_chunk_indexes.len(),
                    })
                }),
            })
            .unwrap_or_else(|_| json!({ "job_key": job_key }));

            out.push(
                ChainEventFactory::derived_data_event(
                    job.lineage_parent.writer_id,
                    &job.lineage_parent,
                    JOB_FAILED_EVENT_TYPE,
                    payload,
                    self.lineage,
                )
                .mark_as_error(
                    "ai_map_reduce: job incomplete at drain",
                    ErrorKind::PermanentFailure,
                ),
            );
        }

        Ok(out)
    }
}

#[async_trait]
impl<Partial, Seed, Collected> StatefulHandler for SeededCollectByInput<Partial, Seed, Collected>
where
    Partial: DeserializeOwned + Clone + Send + Sync + 'static,
    Seed: DeserializeOwned + Serialize + Clone + Send + Sync + 'static,
    Collected: DeserializeOwned + Clone + Serialize + Send + Sync + 'static,
{
    type State = SeededCollectByInputState<Partial, Seed, Collected>;

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
    }

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        self.accumulate_protocol(state, event)
            .expect("generated collector must be driven through try_accumulate");
    }

    fn try_accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
    ) -> Result<(), HandlerError> {
        self.accumulate_protocol(state, event)
    }

    fn initial_state(&self) -> Self::State {
        SeededCollectByInputState::new()
    }

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        state.in_flight.is_none() && !state.ready.is_empty()
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if state.in_flight.is_some() {
            return Err(Self::protocol_fatal(
                "ai_map_reduce collector attempted a second handoff before commit",
            ));
        }
        let Some(job_key) = state.ready.pop_front() else {
            return Ok(Vec::new());
        };
        let job = state.jobs.get_mut(&job_key).ok_or_else(|| {
            Self::protocol_fatal(format!(
                "ai_map_reduce collector ready queue names missing job {job_key}"
            ))
        })?;
        if !matches!(job.phase, CollectorPhase::SuccessReady) {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce collector ready job {job_key} is in phase {:?}",
                job.phase
            )));
        }
        let stored_manifest = job.manifest.as_ref().ok_or_else(|| {
            Self::protocol_fatal(format!(
                "ai_map_reduce collector ready job {job_key} has no manifest"
            ))
        })?;
        let manifest = &stored_manifest.manifest;
        if job.dispositions.len() != manifest.chunk_count {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce collector ready job {job_key} has {} of {} dispositions",
                job.dispositions.len(),
                manifest.chunk_count
            )));
        }

        let failed_indices = job
            .dispositions
            .iter()
            .filter_map(|(index, disposition)| {
                matches!(disposition.kind, StoredDispositionKind::Failure).then_some(*index)
            })
            .collect::<Vec<_>>();

        let output = if failed_indices.is_empty() {
            let seed: Seed =
                serde_json::from_value(manifest.seed_payload.clone()).map_err(|error| {
                    Self::protocol_fatal(format!(
                        "ai_map_reduce seed decode failed for type '{}': {error}",
                        manifest.seed_event_type
                    ))
                })?;
            let mut collected = self.initial.clone();
            if let Some(hook) = self.planning_hook.as_ref() {
                (hook.0)(&mut collected, &manifest.planning);
            }
            for index in 0..manifest.chunk_count {
                let disposition = job.dispositions.get(&index).ok_or_else(|| {
                    Self::protocol_fatal(format!(
                        "ai_map_reduce collector closure is missing chunk {index} for job {job_key}"
                    ))
                })?;
                let StoredDispositionKind::Success(value) = &disposition.kind else {
                    return Err(Self::protocol_fatal(format!(
                        "ai_map_reduce collector all-success closure contains failure at {index}"
                    )));
                };
                let partial: Partial = serde_json::from_value(value.clone()).map_err(|error| {
                    Self::protocol_fatal(format!(
                        "ai_map_reduce partial decode failed at chunk {index}: {error}"
                    ))
                })?;
                (self.accumulate.0)(&mut collected, &partial);
            }
            let payload = AiMapReduceReduceInput {
                job_key,
                seed,
                collected,
                planning: manifest.planning.clone(),
            };
            ChainEventFactory::derived_data_event(
                stored_manifest.parent.writer_id,
                &stored_manifest.parent,
                AiMapReduceReduceInput::<Seed, Collected>::versioned_event_type(),
                serde_json::to_value(payload).map_err(|error| {
                    Self::protocol_fatal(format!(
                        "ai_map_reduce reduce input encode failed: {error}"
                    ))
                })?,
                self.lineage,
            )
        } else {
            let payload = AiMapReduceJobFailed {
                job_key,
                chunk_count: manifest.chunk_count,
                failed_indices,
            };
            ChainEventFactory::derived_data_event(
                stored_manifest.parent.writer_id,
                &stored_manifest.parent,
                AiMapReduceJobFailed::versioned_event_type(),
                serde_json::to_value(payload).map_err(|error| {
                    Self::protocol_fatal(format!(
                        "ai_map_reduce job failure encode failed: {error}"
                    ))
                })?,
                self.lineage,
            )
        };

        job.phase = CollectorPhase::HandoffInFlight;
        state.in_flight = Some(job_key);
        Ok(vec![output])
    }

    fn outputs_committed(&self, state: &mut Self::State) {
        let Some(job_key) = state.in_flight.take() else {
            return;
        };
        let job = state
            .jobs
            .remove(&job_key)
            .expect("in-flight collector job must remain retained through commit");
        state.retained_bytes = state
            .retained_bytes
            .checked_sub(job.retained_bytes)
            .expect("collector retained-byte accounting underflow");
    }

    fn validate_terminal(
        &self,
        state: &Self::State,
        kind: StatefulTerminationKind,
    ) -> TerminalValidation {
        if state.jobs.is_empty() {
            return TerminalValidation::Clean;
        }
        if matches!(
            kind,
            StatefulTerminationKind::PipelineAbort | StatefulTerminationKind::ForceShutdown
        ) {
            return TerminalValidation::Clean;
        }
        let fatal = StageFatal::new(
            StageFatalCode::Termination,
            StageFatalReason::IncompleteTermination,
            format!(
                "ai_map_reduce collector terminated with {} incomplete job(s)",
                state.jobs.len()
            ),
        );
        if matches!(
            kind,
            StatefulTerminationKind::TruncatedEof | StatefulTerminationKind::PoisonedEof
        ) {
            TerminalValidation::Secondary(fatal)
        } else {
            TerminalValidation::Primary(fatal)
        }
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::WriterId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestPartial {
        value: u32,
    }

    impl TypedPayload for TestPartial {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.partial";
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct TestCollected {
        values: Vec<u32>,
        input_items_total: usize,
        planned_items_total: usize,
        excluded_items_total: usize,
    }

    impl TypedPayload for TestCollected {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.collected";
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestSeed {
        seed: bool,
    }

    fn writer_id() -> WriterId {
        WriterId::from(StageId::new())
    }

    fn manifest_event(job_key: EventId, chunk_count: usize) -> ChainEvent {
        let manifest = AiMapReducePlanningManifest {
            job_key,
            chunk_count,
            planning: ChunkPlanningSummary {
                input_items_total: 5,
                planned_items_total: 3,
                excluded_items_total: 2,
            },
            seed_payload: serde_json::json!({ "seed": true }),
            seed_event_type: "seed.event".to_string(),
        };

        ChainEventFactory::data_event_from(
            writer_id(),
            AiMapReducePlanningManifest::versioned_event_type(),
            &manifest,
        )
        .expect("manifest serialization should succeed")
    }

    fn tagged_partial_event(
        job_key: EventId,
        chunk_index: usize,
        chunk_count: usize,
        partial: TestPartial,
    ) -> ChainEvent {
        let tagged = AiMapReduceTaggedPartial::<serde_json::Value> {
            job_key,
            chunk_index,
            chunk_count,
            partial: serde_json::to_value(partial).expect("partial serialization should succeed"),
        };

        ChainEventFactory::data_event_from(
            writer_id(),
            AiMapReduceTaggedPartial::<serde_json::Value>::versioned_event_type(),
            &tagged,
        )
        .expect("tagged partial serialization should succeed")
    }

    fn chunk_failed_event(
        job_key: EventId,
        chunk_index: usize,
        chunk_count: usize,
        reason: &str,
    ) -> ChainEvent {
        let failed = AiMapReduceChunkFailed {
            job_key,
            chunk_index,
            chunk_count,
            cause: obzenflow_core::ai::AiMapReduceRoleFailure::Logic {
                logic: obzenflow_core::ai::AiRoleLogicFailure::Parse {
                    message: reason.to_string(),
                },
            },
        };

        ChainEventFactory::data_event_from(
            writer_id(),
            AiMapReduceChunkFailed::versioned_event_type(),
            &failed,
        )
        .expect("chunk_failed serialization should succeed")
    }

    fn activated(event: ChainEvent, job_key: EventId) -> ChainEvent {
        event
            .try_with_composite_activations(vec![CompositeActivationContext::new(
                obzenflow_core::id::CompositeId::new("ai_map_reduce:test"),
                job_key,
                "in",
                1,
            )])
            .expect("fixture activation is consistent")
    }

    fn seeded_collector() -> SeededCollectByInput<TestPartial, TestSeed, TestCollected> {
        CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |acc, partial| acc.values.push(partial.value),
        )
        .with_planning_summary(|acc, planning| {
            acc.input_items_total = planning.input_items_total;
            acc.planned_items_total = planning.planned_items_total;
            acc.excluded_items_total = planning.excluded_items_total;
        })
        .with_seed::<TestSeed>()
    }

    #[test]
    fn collector_emits_when_manifest_and_all_partials_arrive() {
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |acc, partial| acc.values.push(partial.value),
        )
        .with_planning_summary(|acc, planning| {
            acc.input_items_total = planning.input_items_total;
            acc.planned_items_total = planning.planned_items_total;
            acc.excluded_items_total = planning.excluded_items_total;
        });

        let mut state = collector.initial_state();
        let job_key = EventId::new();

        // Partial arrives before manifest.
        collector.accumulate(
            &mut state,
            tagged_partial_event(job_key, 0, 2, TestPartial { value: 10 }),
        );
        collector.accumulate(&mut state, manifest_event(job_key, 2));
        collector.accumulate(
            &mut state,
            tagged_partial_event(job_key, 1, 2, TestPartial { value: 20 }),
        );

        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_type(), TestCollected::versioned_event_type());

        let collected: TestCollected =
            serde_json::from_value(out[0].payload()).expect("collected decode");
        assert_eq!(collected.values, vec![10, 20]);
        assert_eq!(collected.input_items_total, 5);
        assert_eq!(collected.planned_items_total, 3);
        assert_eq!(collected.excluded_items_total, 2);
    }

    #[test]
    fn collector_with_seed_emits_reduce_input_with_seed_and_planning() {
        let mut collector = seeded_collector();

        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector
            .try_accumulate(
                &mut state,
                activated(
                    tagged_partial_event(job_key, 0, 1, TestPartial { value: 10 }),
                    job_key,
                ),
            )
            .expect("generated partial is valid");
        collector
            .try_accumulate(&mut state, activated(manifest_event(job_key, 1), job_key))
            .expect("generated manifest is valid");

        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].event_type(),
            AiMapReduceReduceInput::<TestSeed, TestCollected>::versioned_event_type()
        );

        let decoded = AiMapReduceReduceInput::<TestSeed, TestCollected>::try_from_event(&out[0])
            .expect("reduce input decode");

        assert_eq!(decoded.seed, TestSeed { seed: true });
        assert_eq!(decoded.collected.values, vec![10]);
        assert_eq!(decoded.planning.input_items_total, 5);
        assert_eq!(decoded.planning.planned_items_total, 3);
        assert_eq!(decoded.planning.excluded_items_total, 2);
    }

    #[test]
    fn generated_collector_accepts_exact_duplicates_and_rejects_conflicts() {
        let mut collector = seeded_collector();
        let mut state = collector.initial_state();
        let job_key = EventId::new();
        let first = activated(
            tagged_partial_event(job_key, 0, 2, TestPartial { value: 10 }),
            job_key,
        );

        collector
            .try_accumulate(&mut state, first.clone())
            .expect("first disposition is accepted");
        collector
            .try_accumulate(&mut state, first)
            .expect("byte-identical duplicate is idempotent");
        let error = collector
            .try_accumulate(
                &mut state,
                activated(
                    tagged_partial_event(job_key, 0, 2, TestPartial { value: 11 }),
                    job_key,
                ),
            )
            .expect_err("same-index payload drift is corruption");

        assert!(matches!(error, HandlerError::Fatal(_)));
        assert_eq!(
            state.jobs[&job_key].dispositions.len(),
            1,
            "the conflicting duplicate must not mutate retained state"
        );
    }

    #[test]
    fn generated_collector_rejects_conflicting_manifests() {
        let mut collector = seeded_collector();
        let mut state = collector.initial_state();
        let job_key = EventId::new();
        collector
            .try_accumulate(&mut state, activated(manifest_event(job_key, 2), job_key))
            .expect("first manifest is accepted");

        let error = collector
            .try_accumulate(&mut state, activated(manifest_event(job_key, 3), job_key))
            .expect_err("manifest drift is corruption");
        assert!(matches!(error, HandlerError::Fatal(_)));
        assert_eq!(
            state.jobs[&job_key]
                .manifest
                .as_ref()
                .expect("first manifest remains")
                .manifest
                .chunk_count,
            2
        );
    }

    #[test]
    fn generated_collector_rejects_byte_counter_failures_before_mutating_state() {
        let mut collector = seeded_collector();
        let mut state = collector.initial_state();
        let job_key = EventId::new();
        state.retained_bytes = COLLECTOR_MAX_RETAINED_BYTES;

        let error = collector
            .try_accumulate(&mut state, activated(manifest_event(job_key, 2), job_key))
            .expect_err("the stage retained-byte limit must reject before opening a job");
        assert!(matches!(error, HandlerError::Fatal(_)));
        assert!(state.jobs.is_empty());
        assert_eq!(state.retained_bytes, COLLECTOR_MAX_RETAINED_BYTES);

        state.retained_bytes = 0;
        collector
            .try_accumulate(&mut state, activated(manifest_event(job_key, 2), job_key))
            .expect("the fixture manifest is valid");
        {
            let job = state
                .jobs
                .get_mut(&job_key)
                .expect("manifest opened the job");
            job.retained_bytes = usize::MAX;
        }
        state.retained_bytes = 0;

        let error = collector
            .try_accumulate(
                &mut state,
                activated(
                    tagged_partial_event(job_key, 0, 2, TestPartial { value: 10 }),
                    job_key,
                ),
            )
            .expect_err("the job counter overflow must reject before storing a disposition");
        assert!(matches!(error, HandlerError::Fatal(_)));
        let job = state.jobs.get(&job_key).expect("the original job remains");
        assert!(job.dispositions.is_empty());
        assert_eq!(job.retained_bytes, usize::MAX);
        assert_eq!(state.retained_bytes, 0);
    }

    #[test]
    fn generated_collector_retains_ready_state_until_output_commit() {
        let mut collector = seeded_collector();
        let mut state = collector.initial_state();
        let job_key = EventId::new();
        collector
            .try_accumulate(&mut state, activated(manifest_event(job_key, 1), job_key))
            .expect("manifest is accepted");
        collector
            .try_accumulate(
                &mut state,
                activated(
                    tagged_partial_event(job_key, 0, 1, TestPartial { value: 10 }),
                    job_key,
                ),
            )
            .expect("terminal disposition is accepted");

        let output = collector.emit(&mut state).expect("ready handoff emits");
        assert_eq!(output.len(), 1);
        assert!(state.jobs.contains_key(&job_key));
        assert_eq!(state.in_flight, Some(job_key));
        assert!(matches!(
            collector.validate_terminal(&state, StatefulTerminationKind::NaturalEof),
            TerminalValidation::Primary(_)
        ));

        collector.outputs_committed(&mut state);
        assert!(!state.jobs.contains_key(&job_key));
        assert!(state.in_flight.is_none());
        assert!(matches!(
            collector.validate_terminal(&state, StatefulTerminationKind::NaturalEof),
            TerminalValidation::Clean
        ));
    }

    #[test]
    fn generated_collector_rejects_payload_job_key_that_differs_from_activation() {
        let mut collector = seeded_collector();
        let mut state = collector.initial_state();
        let payload_job = EventId::new();
        let activation_job = EventId::new();

        let error = collector
            .try_accumulate(
                &mut state,
                activated(manifest_event(payload_job, 0), activation_job),
            )
            .expect_err("payload identity cannot override activation identity");
        assert!(matches!(error, HandlerError::Fatal(_)));
        assert!(state.jobs.is_empty());
    }

    #[test]
    fn collector_deduplicates_partials_by_chunk_index() {
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |acc, partial| acc.values.push(partial.value),
        );
        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector.accumulate(&mut state, manifest_event(job_key, 2));
        collector.accumulate(
            &mut state,
            tagged_partial_event(job_key, 0, 2, TestPartial { value: 1 }),
        );
        collector.accumulate(
            &mut state,
            tagged_partial_event(job_key, 0, 2, TestPartial { value: 1 }),
        );
        collector.accumulate(
            &mut state,
            tagged_partial_event(job_key, 1, 2, TestPartial { value: 2 }),
        );

        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        let collected: TestCollected =
            serde_json::from_value(out[0].payload()).expect("collected decode");
        assert_eq!(collected.values, vec![1, 2]);
    }

    #[test]
    fn collector_emits_for_zero_chunk_manifest() {
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |acc, partial| acc.values.push(partial.value),
        )
        .with_planning_summary(|acc, planning| {
            acc.input_items_total = planning.input_items_total;
            acc.planned_items_total = planning.planned_items_total;
            acc.excluded_items_total = planning.excluded_items_total;
        });

        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector.accumulate(&mut state, manifest_event(job_key, 0));

        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        let collected: TestCollected =
            serde_json::from_value(out[0].payload()).expect("collected decode");
        assert!(collected.values.is_empty());
        assert_eq!(collected.input_items_total, 5);
        assert_eq!(collected.planned_items_total, 3);
        assert_eq!(collected.excluded_items_total, 2);
    }

    #[test]
    fn collector_emits_error_on_chunk_failed_marker() {
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |_acc, _partial| {},
        );
        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector.accumulate(
            &mut state,
            chunk_failed_event(job_key, 0, 2, "terminal map failure"),
        );

        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].event_type(),
            AiMapReduceChunkFailed::versioned_event_type()
        );
        assert!(matches!(
            out[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));
        assert_eq!(
            out[0].processing_info.status.kind(),
            Some(&ErrorKind::PermanentFailure)
        );
    }

    #[test]
    fn collector_fails_when_max_open_jobs_exceeded() {
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |_acc, _partial| {},
        )
        .with_max_open_jobs(1);

        let mut state = collector.initial_state();
        let job1 = EventId::new();
        let job2 = EventId::new();

        collector.accumulate(
            &mut state,
            tagged_partial_event(job1, 0, 1, TestPartial { value: 1 }),
        );

        collector.accumulate(
            &mut state,
            tagged_partial_event(job2, 0, 1, TestPartial { value: 2 }),
        );

        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_type(), JOB_CAPACITY_EXCEEDED_EVENT_TYPE);
        assert!(matches!(
            out[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));

        // The first job remains open.
        assert!(state.jobs.contains_key(&job1));
    }

    #[test]
    fn collector_fails_when_job_ttl_expires() {
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |_acc, _partial| {},
        )
        .with_job_ttl(Duration::from_secs(1));

        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector.accumulate(&mut state, manifest_event(job_key, 1));
        {
            let job = state.jobs.get_mut(&job_key).expect("job created");
            job.created_at = Instant::now() - Duration::from_secs(10);
        }

        // First emit run detects expiry and enqueues an error.
        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert!(out.is_empty());
        assert!(!state.pending_errors.is_empty());

        // Second emit returns the queued error event.
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_type(), JOB_TTL_EXPIRED_EVENT_TYPE);
        assert_eq!(
            out[0].processing_info.status.kind(),
            Some(&ErrorKind::PermanentFailure)
        );
    }

    #[test]
    fn collector_fails_on_out_of_range_chunk_index() {
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |_acc, _partial| {},
        );
        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector.accumulate(
            &mut state,
            tagged_partial_event(job_key, 2, 2, TestPartial { value: 1 }),
        );

        assert!(collector.should_emit(&mut state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_type(), JOB_FAILED_EVENT_TYPE);
        assert!(matches!(
            out[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));
    }
}
