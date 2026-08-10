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
use crate::stages::common::handlers::stateful::traits::StatefulHandler;
use crate::stages::common::handlers::{StatefulTerminationKind, TerminalValidation};
use async_trait::async_trait;
use obzenflow_core::ai::{
    canonical_json_bytes_v1, AiMapReduceChunkFailed, AiMapReduceJobFailed,
    AiMapReducePlanningManifest, AiMapReduceReduceInput, AiMapReduceTaggedPartial,
    ChunkPlanningSummary,
};
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::context::CompositeActivationContext;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, EventId, TypedPayload};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

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
struct Accumulator<Partial, Collected>(
    Arc<dyn Fn(&mut Collected, &Partial) + Send + Sync + 'static>,
);

impl<Partial, Collected> fmt::Debug for Accumulator<Partial, Collected> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Accumulator").field(&"<closure>").finish()
    }
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
    pending_error: Option<HandlerError>,
    _phantom: PhantomData<(Partial, Seed, Collected)>,
}

impl<Partial, Seed, Collected> SeededCollectByInputState<Partial, Seed, Collected> {
    fn new() -> Self {
        Self {
            jobs: BTreeMap::new(),
            ready: VecDeque::new(),
            in_flight: None,
            retained_bytes: 0,
            pending_error: None,
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

impl<Partial, Seed, Collected> SeededCollectByInput<Partial, Seed, Collected> {
    #[doc(hidden)]
    pub fn new<F>(initial: Collected, accumulate: F) -> Self
    where
        F: Fn(&mut Collected, &Partial) + Send + Sync + 'static,
    {
        Self {
            initial,
            accumulate: Accumulator(Arc::new(accumulate)),
            planning_hook: None,
            composite_id: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            _phantom: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn with_planning_summary<F>(mut self, hook: F) -> Self
    where
        F: Fn(&mut Collected, &ChunkPlanningSummary) + Send + Sync + 'static,
    {
        self.planning_hook = Some(PlanningHook(Arc::new(hook)));
        self
    }

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

        let Some(job) = state.jobs.get_mut(&job_key) else {
            return Err(Self::protocol_fatal(format!(
                "ai_map_reduce collector lost newly opened job {job_key}"
            )));
        };
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
        if state.pending_error.is_none() {
            state.pending_error = self.accumulate_protocol(state, event).err();
        }
    }

    fn try_accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
    ) -> Result<(), HandlerError> {
        if let Some(error) = state.pending_error.as_ref() {
            return Err(error.clone());
        }
        self.accumulate_protocol(state, event)
    }

    fn initial_state(&self) -> Self::State {
        SeededCollectByInputState::new()
    }

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        state.pending_error.is_some() || (state.in_flight.is_none() && !state.ready.is_empty())
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(error) = state.pending_error.take() {
            return Err(error);
        }
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
        let Some(job) = state.jobs.remove(&job_key) else {
            state.pending_error = Some(Self::protocol_fatal(format!(
                "ai_map_reduce collector lost in-flight job {job_key} before commit"
            )));
            return;
        };
        let Some(retained_bytes) = state.retained_bytes.checked_sub(job.retained_bytes) else {
            state.pending_error = Some(Self::protocol_fatal(format!(
                "ai_map_reduce collector retained-byte accounting underflow after job {job_key}"
            )));
            return;
        };
        state.retained_bytes = retained_bytes;
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

    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(error) = state.pending_error.as_ref() {
            return Err(error.clone());
        }
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        SeededCollectByInput::<TestPartial, TestSeed, TestCollected>::new(
            TestCollected::default(),
            |acc, partial| acc.values.push(partial.value),
        )
        .with_planning_summary(|acc, planning| {
            acc.input_items_total = planning.input_items_total;
            acc.planned_items_total = planning.planned_items_total;
            acc.excluded_items_total = planning.excluded_items_total;
        })
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
    fn generated_collector_closes_a_failed_job_with_indexed_domain_output() {
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
                    chunk_failed_event(job_key, 0, 1, "terminal map failure"),
                    job_key,
                ),
            )
            .expect("failed chunk disposition is accepted");

        let out = collector
            .emit(&mut state)
            .expect("failed job handoff emits");
        let failed = AiMapReduceJobFailed::try_from_event(&out[0])
            .expect("collector emits the typed failed-job terminal");
        assert_eq!(failed.job_key, job_key);
        assert_eq!(failed.chunk_count, 1);
        assert_eq!(failed.failed_indices, vec![0]);
    }

    #[test]
    fn infallible_accumulate_surfaces_protocol_errors_without_panicking() {
        let mut collector = seeded_collector();
        let mut state = collector.initial_state();
        let job_key = EventId::new();

        collector.accumulate(&mut state, manifest_event(job_key, 0));

        assert!(collector.should_emit(&mut state));
        assert!(matches!(
            collector.emit(&mut state),
            Err(HandlerError::Fatal(_))
        ));
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
}
