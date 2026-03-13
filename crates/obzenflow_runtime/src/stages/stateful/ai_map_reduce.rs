// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-owned keyed collector used by the AI map-reduce composite lowering.
//!
//! The collector is completion-driven and keyed by the outer input event ID (`job_key`).
//! It consumes framework-internal transport events:
//! - `AiMapReducePlanningManifest` from the chunk stage
//! - `AiMapReduceTaggedPartial<serde_json::Value>` from the map stage
//! - `AiMapReduceChunkFailed` from the map stage on terminal failure
//!
//! The public typed contract remains `Partial -> Collected` even though the runtime
//! delivery surface includes the internal manifest / wrapper payloads. This is
//! intentional (FLOWIP-086z-part-2).

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::StatefulHandler;
use crate::typing::StatefulTyping;
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiMapReduceChunkFailed, AiMapReducePlanningManifest, AiMapReduceReduceInput,
    AiMapReduceTaggedPartial, ChunkPlanningSummary,
};
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, EventId, TypedPayload};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use std::collections::{BTreeSet, HashMap, VecDeque};
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
struct PlanningHook<Collected>(Arc<dyn Fn(&mut Collected, &ChunkPlanningSummary) + Send + Sync>);

impl<Collected> fmt::Debug for PlanningHook<Collected> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PlanningHook").field(&"<closure>").finish()
    }
}

#[derive(Clone)]
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
    _phantom: PhantomData<Partial>,
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
    pub fn with_seed<Seed>(
        self,
    ) -> CollectByInput<Partial, AiMapReduceReduceInput<Seed, Collected>>
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
            job_ttl,
            max_open_jobs,
            _phantom: _,
        } = self;

        let wrapped = AiMapReduceReduceInput::<Seed, Collected> {
            seed: None,
            collected: initial,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let inner_accumulate = accumulate.0;
        let inner_planning_hook = planning_hook.map(|hook| hook.0);

        let mut out = CollectByInput::<Partial, AiMapReduceReduceInput<Seed, Collected>>::new(
            wrapped,
            move |acc, partial| {
                (inner_accumulate)(&mut acc.collected, partial);
            },
        )
        .with_manifest_hook(|acc, manifest| {
            let seed: Seed = serde_json::from_value(manifest.seed_payload.clone()).map_err(
                |err| {
                    HandlerError::Deserialization(format!(
                        "ai_map_reduce: seed decode failed (seed_event_type={}): {err}",
                        manifest.seed_event_type
                    ))
                },
            )?;
            acc.seed = Some(seed);
            Ok(())
        })
        .with_planning_summary(move |acc, planning| {
            acc.planning = planning.clone();
            if let Some(hook) = inner_planning_hook.as_ref() {
                (hook)(&mut acc.collected, planning);
            }
        });

        out.job_ttl = job_ttl;
        out.max_open_jobs = max_open_jobs;
        out
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

        if manifest.chunk_count == 0 || job.seen_chunk_indexes.len() == manifest.chunk_count {
            if !job.queued_for_emit {
                job.queued_for_emit = true;
                state.ready.push_back(job_key);
            }
        }
    }

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
        let error_event =
            ChainEventFactory::derived_data_event(parent.writer_id, parent, event_type, payload)
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

#[async_trait]
impl<Partial, Collected> StatefulHandler for CollectByInput<Partial, Collected>
where
    Partial: DeserializeOwned + Clone + Send + Sync + 'static,
    Collected: Clone + Serialize + TypedPayload + Send + Sync + 'static,
{
    type State = CollectByInputState<Partial, Collected>;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let ChainEventContent::Data { event_type, .. } = &event.content else {
            return;
        };

        // --------------------------------------------------------------------
        // Planning manifest (chunk -> collect)
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

            let manifest_hook_err = self.manifest_hook.as_ref().and_then(|hook| {
                (hook.0)(&mut job.collected, &manifest).err()
            });

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
                .mark_as_error(failed.reason, ErrorKind::PermanentFailure);
            state.pending_errors.push_back(err);

            state.jobs.remove(&failed.job_key);
        }
    }

    fn initial_state(&self) -> Self::State {
        CollectByInputState::new()
    }

    fn should_emit(&self, state: &Self::State) -> bool {
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
            reason: reason.to_string(),
        };

        ChainEventFactory::data_event_from(
            writer_id(),
            AiMapReduceChunkFailed::versioned_event_type(),
            &failed,
        )
        .expect("chunk_failed serialization should succeed")
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

        assert!(collector.should_emit(&state));
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
        let mut collector = CollectByInput::<TestPartial, TestCollected>::new(
            TestCollected::default(),
            |acc, partial| acc.values.push(partial.value),
        )
        .with_planning_summary(|acc, planning| {
            acc.input_items_total = planning.input_items_total;
            acc.planned_items_total = planning.planned_items_total;
            acc.excluded_items_total = planning.excluded_items_total;
        })
        .with_seed::<TestSeed>();

        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector.accumulate(
            &mut state,
            tagged_partial_event(job_key, 0, 1, TestPartial { value: 10 }),
        );
        collector.accumulate(&mut state, manifest_event(job_key, 1));

        assert!(collector.should_emit(&state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].event_type(),
            AiMapReduceReduceInput::<TestSeed, TestCollected>::versioned_event_type()
        );

        let decoded = AiMapReduceReduceInput::<TestSeed, TestCollected>::try_from_event(&out[0])
            .expect("reduce input decode");

        assert_eq!(decoded.seed, Some(TestSeed { seed: true }));
        assert_eq!(decoded.collected.values, vec![10]);
        assert_eq!(decoded.planning.input_items_total, 5);
        assert_eq!(decoded.planning.planned_items_total, 3);
        assert_eq!(decoded.planning.excluded_items_total, 2);
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

        assert!(collector.should_emit(&state));
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

        assert!(collector.should_emit(&state));
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

        assert!(collector.should_emit(&state));
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

        assert!(collector.should_emit(&state));
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
        assert!(collector.should_emit(&state));
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

        assert!(collector.should_emit(&state));
        let out = collector.emit(&mut state).expect("emit should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_type(), JOB_FAILED_EVENT_TYPE);
        assert!(matches!(
            out[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));
    }
}
