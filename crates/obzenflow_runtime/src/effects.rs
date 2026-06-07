// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe user effects.

use async_trait::async_trait;
use obzenflow_core::event::context::FlowContext;
pub use obzenflow_core::event::payloads::effect_payload::{
    effect_outcome_group_id, framework_effect_event_type, is_framework_effect_event_type,
    EffectCursor, EffectDescriptor, EffectOutcomePayload, EffectProvenance, EffectRecord,
    CAPTURE_EVENT_TYPE, EFFECT_RECORD_EVENT_TYPE,
};
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::event::{ChainEventContent, ChainEventFactory, SystemEvent};
use obzenflow_core::journal::{ArchiveStatus, Journal};
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, FlowId, StageId, WriterId};
use ring::digest::{digest, SHA256};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::StageInputPosition;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::replay::{ReplayArchive, ReplayError};
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::output_committer::{CommitOptions, OutputCommitter};

pub struct EffectBoundaryContext {
    inner: Box<dyn Any + Send>,
}

impl EffectBoundaryContext {
    pub fn new<T>(inner: T) -> Self
    where
        T: Any + Send,
    {
        Self {
            inner: Box::new(inner),
        }
    }

    pub fn downcast<T>(self) -> Result<T, Self>
    where
        T: Any + Send,
    {
        match self.inner.downcast::<T>() {
            Ok(value) => Ok(*value),
            Err(inner) => Err(Self { inner }),
        }
    }
}

pub enum EffectBoundaryAction {
    Continue,
    Skip(Vec<ChainEvent>),
    Abort,
}

pub struct EffectBoundaryStart {
    pub action: EffectBoundaryAction,
    pub context: EffectBoundaryContext,
    pub control_events: Vec<ChainEvent>,
}

pub trait EffectBoundaryMiddleware: Send + Sync {
    fn before_effect(&self, event: &ChainEvent) -> EffectBoundaryStart;

    fn after_effect(
        &self,
        context: EffectBoundaryContext,
        event: &ChainEvent,
        outputs: &[ChainEvent],
    ) -> Vec<ChainEvent>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectSafety {
    Idempotent,
    NonIdempotentRequiresKey,
    Transactional,
}

#[derive(Debug, Error)]
pub enum EffectError {
    #[error("effect serialization failed: {0}")]
    Serialization(String),

    #[error("effect journal write failed: {0}")]
    Journal(String),

    #[error("missing recorded effect for cursor {cursor:?}")]
    MissingRecordedEffect { cursor: EffectCursor },

    #[error("duplicate recorded effect for cursor {cursor:?}")]
    DuplicateRecordedEffect { cursor: EffectCursor },

    #[error("effect descriptor mismatch for cursor {cursor:?}: expected {expected}, recorded {recorded}")]
    DescriptorMismatch {
        cursor: EffectCursor,
        expected: String,
        recorded: String,
    },

    #[error("recorded effect failure: {error_type}: {error_message}")]
    RecordedFailure {
        error_type: String,
        error_message: String,
        retryable: bool,
    },

    #[error("effect provenance mismatch: {0}")]
    EffectProvenanceMismatch(String),

    #[error("non-idempotent effect '{effect_type}' has no idempotency key")]
    MissingIdempotencyKey { effect_type: String },

    #[error("effectful stage '{stage_key}' performed undeclared effect '{effect_type}'")]
    UndeclaredEffect {
        stage_key: String,
        effect_type: String,
    },

    #[error("effectful stage '{stage_key}' emitted undeclared output fact '{event_type}'")]
    UndeclaredOutput {
        stage_key: String,
        event_type: String,
    },

    #[error("effectful stage '{stage_key}' cannot emit output facts from this handler context")]
    EmitUnsupported { stage_key: String },

    #[error("effect port '{name}' for type '{type_name}' is not registered")]
    MissingEffectPort {
        type_name: &'static str,
        name: String,
    },

    #[error(
        "transactional effect '{effect_type}' using executor '{executor}' returned without committing an effect result"
    )]
    TransactionalCommitMissing {
        effect_type: String,
        executor: String,
    },

    #[error("effect execution failed: {0}")]
    Execution(String),

    #[error("effect replay archive failed: {0}")]
    ReplayArchive(String),
}

impl EffectError {
    pub fn retryable(&self) -> bool {
        match self {
            EffectError::RecordedFailure { retryable, .. } => *retryable,
            EffectError::EffectProvenanceMismatch(_) => false,
            EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::MissingEffectPort { .. }
            | EffectError::TransactionalCommitMissing { .. } => false,
            EffectError::Serialization(_)
            | EffectError::Journal(_)
            | EffectError::Execution(_)
            | EffectError::ReplayArchive(_) => true,
        }
    }

    fn error_type(&self) -> String {
        match self {
            EffectError::Serialization(_) => "serialization",
            EffectError::Journal(_) => "journal",
            EffectError::MissingRecordedEffect { .. } => "missing_recorded_effect",
            EffectError::DuplicateRecordedEffect { .. } => "duplicate_recorded_effect",
            EffectError::DescriptorMismatch { .. } => "descriptor_mismatch",
            EffectError::RecordedFailure { error_type, .. } => return error_type.clone(),
            EffectError::EffectProvenanceMismatch(_) => "effect_provenance_mismatch",
            EffectError::MissingIdempotencyKey { .. } => "missing_idempotency_key",
            EffectError::UndeclaredEffect { .. } => "undeclared_effect",
            EffectError::UndeclaredOutput { .. } => "undeclared_output",
            EffectError::EmitUnsupported { .. } => "emit_unsupported",
            EffectError::MissingEffectPort { .. } => "missing_effect_port",
            EffectError::TransactionalCommitMissing { .. } => "transactional_commit_missing",
            EffectError::Execution(_) => "execution",
            EffectError::ReplayArchive(_) => "replay_archive",
        }
        .to_string()
    }

    fn error_message(&self) -> String {
        self.to_string()
    }
}

impl From<ReplayError> for EffectError {
    fn from(value: ReplayError) -> Self {
        Self::ReplayArchive(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EffectPortKey {
    type_id: TypeId,
    name: String,
}

impl EffectPortKey {
    pub fn of<T>(name: impl Into<String>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EffectPortRequirement {
    key: EffectPortKey,
    pub type_name: &'static str,
    pub name: String,
}

impl EffectPortRequirement {
    pub fn of<T>(name: impl Into<String>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let name = name.into();
        Self {
            key: EffectPortKey::of::<T>(name.clone()),
            type_name: std::any::type_name::<T>(),
            name,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyKeyPolicy {
    NotRequired,
    Required,
}

#[derive(Debug, Clone)]
pub struct EffectDeclaration {
    pub effect_type: &'static str,
    pub safety: EffectSafety,
    pub idempotency_key_policy: IdempotencyKeyPolicy,
    pub required_ports: Vec<EffectPortRequirement>,
    pub transactional_executor: Option<&'static str>,
}

impl EffectDeclaration {
    pub fn of<E>() -> Self
    where
        E: Effect,
    {
        let idempotency_key_policy = match E::SAFETY {
            EffectSafety::Idempotent | EffectSafety::Transactional => {
                IdempotencyKeyPolicy::NotRequired
            }
            EffectSafety::NonIdempotentRequiresKey => IdempotencyKeyPolicy::Required,
        };

        Self {
            effect_type: E::EFFECT_TYPE,
            safety: E::SAFETY,
            idempotency_key_policy,
            required_ports: E::required_ports(),
            transactional_executor: None,
        }
    }

    pub fn idempotent(effect_type: &'static str) -> Self {
        Self {
            effect_type,
            safety: EffectSafety::Idempotent,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: Vec::new(),
            transactional_executor: None,
        }
    }

    pub fn non_idempotent_with_key(effect_type: &'static str) -> Self {
        Self {
            effect_type,
            safety: EffectSafety::NonIdempotentRequiresKey,
            idempotency_key_policy: IdempotencyKeyPolicy::Required,
            required_ports: Vec::new(),
            transactional_executor: None,
        }
    }

    pub fn transactional(effect_type: &'static str, executor: &'static str) -> Self {
        Self {
            effect_type,
            safety: EffectSafety::Transactional,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: Vec::new(),
            transactional_executor: Some(executor),
        }
    }

    pub fn transactional_effect<E>(executor: &'static str) -> Self
    where
        E: Effect,
    {
        let mut required_ports = E::required_ports();
        required_ports.push(EffectPortRequirement::of::<dyn TransactionalEffectPort<E>>(
            executor,
        ));

        Self {
            effect_type: E::EFFECT_TYPE,
            safety: EffectSafety::Transactional,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports,
            transactional_executor: Some(executor),
        }
    }

    pub fn require_port<T>(mut self, name: impl Into<String>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.required_ports
            .push(EffectPortRequirement::of::<T>(name));
        self
    }
}

#[derive(Clone, Default)]
pub struct EffectPortRegistry {
    ports: Arc<HashMap<EffectPortKey, Arc<dyn Any + Send + Sync>>>,
}

impl std::fmt::Debug for EffectPortRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectPortRegistry")
            .field("ports", &self.ports.len())
            .finish()
    }
}

impl EffectPortRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<T>(&mut self, name: impl Into<String>, port: Arc<T>)
    where
        T: ?Sized + Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.ports).insert(EffectPortKey::of::<T>(name), Arc::new(port));
    }

    pub fn with_port<T>(mut self, name: impl Into<String>, port: Arc<T>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.insert(name, port);
        self
    }

    pub fn get<T>(&self, name: &str) -> Option<Arc<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.ports
            .get(&EffectPortKey {
                type_id: TypeId::of::<T>(),
                name: name.to_string(),
            })
            .and_then(|port| port.downcast_ref::<Arc<T>>())
            .cloned()
    }

    pub fn contains_requirement(&self, requirement: &EffectPortRequirement) -> bool {
        self.ports.contains_key(&requirement.key)
    }
}

#[derive(Clone)]
pub struct EffectContext {
    is_replaying: bool,
    flow_id: FlowId,
    stage_key: String,
    input_seq: StageInputPosition,
    ports: EffectPortRegistry,
}

impl EffectContext {
    pub fn is_replaying(&self) -> bool {
        self.is_replaying
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
    }

    pub fn stage_key(&self) -> &str {
        &self.stage_key
    }

    pub fn input_seq(&self) -> StageInputPosition {
        self.input_seq
    }

    pub fn now(&self) -> u64 {
        self.input_seq.0
    }

    pub fn deterministic_id(&self, label: &str, ordinal: u32) -> EventId {
        deterministic_event_id(
            &self.flow_id.to_string(),
            &format!("{}:{label}", self.stage_key),
            self.input_seq,
            ordinal,
        )
    }

    pub fn rng(&self, label: &str) -> fastrand::Rng {
        let material = format!(
            "{}:{}:{}:{label}",
            self.flow_id, self.stage_key, self.input_seq.0
        );
        let hash = digest(&SHA256, material.as_bytes());
        let mut seed = [0u8; 8];
        seed.copy_from_slice(&hash.as_ref()[..8]);
        fastrand::Rng::with_seed(u64::from_be_bytes(seed))
    }

    pub fn port<T>(&self, name: &str) -> Result<Arc<T>, EffectError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.ports
            .get(name)
            .ok_or_else(|| EffectError::MissingEffectPort {
                type_name: std::any::type_name::<T>(),
                name: name.to_string(),
            })
    }

    pub fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }
}

#[async_trait]
pub trait Effect: Clone + std::fmt::Debug + Send + Sync + 'static {
    const EFFECT_TYPE: &'static str;
    const SCHEMA_VERSION: u32;
    const SAFETY: EffectSafety;

    type Output: Serialize + DeserializeOwned + Send + Sync + 'static;

    fn label(&self) -> &str;

    fn canonical_input(&self) -> Value;

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError>;

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        None
    }

    fn required_ports() -> Vec<EffectPortRequirement> {
        Vec::new()
    }
}

#[async_trait]
pub trait TransactionalEffectPort<E: Effect>: Send + Sync {
    async fn execute_and_commit(
        &self,
        effect: E,
        ctx: &mut EffectContext,
        commit: EffectCommitHandle<E::Output>,
    ) -> Result<E::Output, EffectError>;
}

pub struct EffectCommitHandle<T> {
    inner: Arc<EffectCommitHandleInner<T>>,
}

impl<T> Clone for EffectCommitHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct EffectCommitHandleInner<T> {
    writer_id: WriterId,
    data_journal: Arc<dyn Journal<ChainEvent>>,
    parent: EventEnvelope<ChainEvent>,
    cursor: EffectCursor,
    descriptor_hash: String,
    descriptor: EffectDescriptor,
    committed: Mutex<Option<EffectOutcomePayload>>,
    _marker: PhantomData<T>,
}

impl<T> EffectCommitHandle<T>
where
    T: Serialize + Send + Sync + 'static,
{
    fn new(
        writer_id: WriterId,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        parent: EventEnvelope<ChainEvent>,
        cursor: EffectCursor,
        descriptor_hash: String,
        descriptor: EffectDescriptor,
    ) -> Self {
        Self {
            inner: Arc::new(EffectCommitHandleInner {
                writer_id,
                data_journal,
                parent,
                cursor,
                descriptor_hash,
                descriptor,
                committed: Mutex::new(None),
                _marker: PhantomData,
            }),
        }
    }

    pub async fn commit_success(&self, output: &T) -> Result<(), EffectError> {
        let output =
            serde_json::to_value(output).map_err(|e| EffectError::Serialization(e.to_string()))?;
        self.commit_outcome(EffectOutcomePayload::Succeeded { output }, None)
            .await
    }

    pub async fn commit_failure(&self, error: &EffectError) -> Result<(), EffectError> {
        self.commit_outcome(
            EffectOutcomePayload::Failed {
                error_type: error.error_type(),
                error_message: error.error_message(),
                retryable: error.retryable(),
            },
            Some(error),
        )
        .await
    }

    async fn commit_outcome(
        &self,
        outcome: EffectOutcomePayload,
        source_error: Option<&EffectError>,
    ) -> Result<(), EffectError> {
        {
            let mut committed = self
                .inner
                .committed
                .lock()
                .expect("effect commit handle lock poisoned");
            if committed.is_some() {
                return Err(EffectError::Execution(
                    "transactional effect commit handle used more than once".to_string(),
                ));
            }
            *committed = Some(outcome.clone());
        }

        let record = EffectRecord {
            cursor: self.inner.cursor.clone(),
            descriptor_hash: self.inner.descriptor_hash.clone(),
            descriptor: self.inner.descriptor.clone(),
            outcome,
        };

        if let Err(err) = append_effect_record(
            &self.inner.data_journal,
            self.inner.writer_id,
            &self.inner.parent,
            record,
            source_error,
        )
        .await
        {
            let mut committed = self
                .inner
                .committed
                .lock()
                .expect("effect commit handle lock poisoned");
            *committed = None;
            return Err(err);
        }

        Ok(())
    }

    /// The outcome committed through this handle, or `None` if the port never
    /// committed. This is the source of truth for the live transactional return
    /// value, so a live run decodes the same outcome a replay would.
    fn committed_outcome(&self) -> Option<EffectOutcomePayload> {
        self.inner
            .committed
            .lock()
            .expect("effect commit handle lock poisoned")
            .clone()
    }
}

#[async_trait]
pub trait EffectHistoryReader: Send {
    async fn read_effect(
        &mut self,
        cursor: &EffectCursor,
    ) -> Result<Option<EffectRecord>, EffectError>;
}

#[async_trait]
pub trait EffectHistoryStore: Send + Sync {
    async fn open_effect_history(
        &self,
        stage_key: &str,
    ) -> Result<Box<dyn EffectHistoryReader>, EffectError>;
}

#[derive(Clone, Debug)]
pub struct EffectHistory {
    recorded_flow_id: String,
    records: Arc<Vec<EffectRecord>>,
    index: Arc<HashMap<EffectCursor, usize>>,
}

impl EffectHistory {
    pub async fn load(
        archive: &Arc<dyn ReplayArchive>,
        stage_key: &str,
    ) -> Result<Self, EffectError> {
        let mut reader = archive.open_effect_history(stage_key).await?;
        let mut records = Vec::new();

        while let Some(envelope) = reader
            .next()
            .await
            .map_err(|e| EffectError::ReplayArchive(e.to_string()))?
        {
            if let Some(record) = effect_record_from_event(&envelope.event)? {
                records.push(record);
            }
        }

        Self::from_records(archive.archive_flow_id().to_string(), records)
    }

    pub fn from_records(
        recorded_flow_id: String,
        records: Vec<EffectRecord>,
    ) -> Result<Self, EffectError> {
        let mut index = HashMap::new();
        for (position, record) in records.iter().enumerate() {
            if index.insert(record.cursor.clone(), position).is_some() {
                return Err(EffectError::DuplicateRecordedEffect {
                    cursor: record.cursor.clone(),
                });
            }
        }

        Ok(Self {
            recorded_flow_id,
            records: Arc::new(records),
            index: Arc::new(index),
        })
    }

    // FLOWIP-120a: the cursor IS the index key (populated in `from_records`), so a
    // found record's stored cursor equals the lookup cursor by construction; a
    // "found under a different cursor" state is unrepresentable and needs no explicit
    // cursor-mismatch error. The two real divergence axes are handled elsewhere: an
    // absent cursor yields `None` (a `MissingRecordedEffect` under strict replay), and
    // a present-but-diverged record is caught by the independent `descriptor_hash`
    // check (`DescriptorMismatch`).
    fn find(&self, cursor: &EffectCursor) -> Option<&EffectRecord> {
        self.index
            .get(cursor)
            .and_then(|position| self.records.get(*position))
    }

    pub fn recorded_flow_id(&self) -> &str {
        &self.recorded_flow_id
    }
}

#[async_trait]
impl EffectHistoryReader for EffectHistory {
    async fn read_effect(
        &mut self,
        cursor: &EffectCursor,
    ) -> Result<Option<EffectRecord>, EffectError> {
        Ok(self.find(cursor).cloned())
    }
}

#[derive(Clone)]
pub struct EffectInvocationContext {
    pub flow_id: FlowId,
    pub stage_id: StageId,
    pub stage_key: String,
    pub writer_id: WriterId,
    pub input_seq: StageInputPosition,
    pub stage_logic_version: String,
    pub data_journal: Arc<dyn Journal<ChainEvent>>,
    pub flow_context: Option<FlowContext>,
    pub system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    pub instrumentation: Option<Arc<StageInstrumentation>>,
    pub heartbeat_state: Option<Arc<HeartbeatState>>,
    pub parent: EventEnvelope<ChainEvent>,
    pub effect_history: Option<Arc<EffectHistory>>,
    pub effect_runtime_mode: EffectRuntimeMode,
    pub effect_ports: EffectPortRegistry,
    pub effect_declarations: Vec<EffectDeclaration>,
    pub output_contract: StageOutputContract,
    pub emit_enabled: bool,
    pub effect_boundary: Option<Arc<dyn EffectBoundaryMiddleware>>,
    pub boundary_control_events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl EffectInvocationContext {
    pub fn push_boundary_control_events(&self, mut events: Vec<ChainEvent>) {
        if events.is_empty() {
            return;
        }

        let mut buffer = self
            .boundary_control_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        buffer.append(&mut events);
    }

    pub fn drain_boundary_control_events(&self) -> Vec<ChainEvent> {
        let mut buffer = self
            .boundary_control_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut *buffer)
    }

    pub fn effect_declaration(
        &self,
        effect_type: &'static str,
    ) -> Result<EffectDeclaration, EffectError> {
        self.effect_declarations
            .iter()
            .find(|declaration| declaration.effect_type == effect_type)
            .cloned()
            .ok_or_else(|| EffectError::UndeclaredEffect {
                stage_key: self.stage_key.clone(),
                effect_type: effect_type.to_string(),
            })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum EffectRuntimeMode {
    #[default]
    Live,
    ReplayStrict,
    ResumeIncomplete,
}

impl EffectRuntimeMode {
    pub fn from_replay_archive(archive: Option<&dyn ReplayArchive>) -> Self {
        let Some(archive) = archive else {
            return Self::Live;
        };

        if matches!(
            archive.archive_status(),
            ArchiveStatus::Completed | ArchiveStatus::Cancelled
        ) {
            Self::ReplayStrict
        } else if archive.allow_incomplete_archive() {
            Self::ResumeIncomplete
        } else {
            Self::ReplayStrict
        }
    }
}

/// Map a stage's effect runtime mode onto the handler-level middleware execution
/// scope (FLOWIP-120a). Live runs reconstruct nothing, so handler middleware runs
/// live. Strict replay and incomplete-archive resume both reconstruct the handler
/// shell from recorded events, so handler-level control middleware must suppress
/// its side effects. Live work that resume performs happens at the effect boundary,
/// which is scoped separately as `LiveEffectBoundary`.
impl From<EffectRuntimeMode> for obzenflow_core::MiddlewareExecutionScope {
    fn from(mode: EffectRuntimeMode) -> Self {
        match mode {
            EffectRuntimeMode::Live => Self::LiveHandler,
            EffectRuntimeMode::ReplayStrict => Self::StrictReplayHandler,
            EffectRuntimeMode::ResumeIncomplete => Self::ResumeHandler,
        }
    }
}

pub struct Effects {
    ctx: EffectInvocationContext,
    next_effect_ordinal: u32,
    next_output_ordinal: u32,
}

impl Effects {
    pub fn new(ctx: EffectInvocationContext) -> Self {
        Self {
            ctx,
            next_effect_ordinal: 0,
            next_output_ordinal: 0,
        }
    }

    pub fn is_replaying(&self) -> bool {
        !matches!(self.ctx.effect_runtime_mode, EffectRuntimeMode::Live)
    }

    pub async fn emit<T>(&mut self, fact: T) -> Result<(), EffectError>
    where
        T: TypedPayload,
    {
        if !self.ctx.emit_enabled {
            return Err(EffectError::EmitUnsupported {
                stage_key: self.ctx.stage_key.clone(),
            });
        }

        let event_type = T::versioned_event_type();
        if !self.ctx.output_contract.is_empty()
            && !self.ctx.output_contract.contains_event_type(&event_type)
        {
            return Err(EffectError::UndeclaredOutput {
                stage_key: self.ctx.stage_key.clone(),
                event_type,
            });
        }

        let recorded_flow_id = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.recorded_flow_id().to_string())
            .unwrap_or_else(|| self.ctx.flow_id.to_string());
        let output_ordinal = self.next_output_ordinal;
        self.next_output_ordinal = self.next_output_ordinal.saturating_add(1);
        let event = deterministic_typed_output_event(
            self.ctx.writer_id,
            &self.ctx.parent.event,
            fact,
            &recorded_flow_id,
            &self.ctx.stage_key,
            self.ctx.input_seq,
            output_ordinal,
        )?;
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),
            system_journal: self.ctx.system_journal.as_ref(),
            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: Some(&self.ctx.output_contract),
        };
        committer
            .commit_prebuilt(
                event,
                Some(&self.ctx.parent),
                CommitOptions {
                    count_output: true,
                    validate_output_contract: true,
                },
            )
            .await
            .map_err(|e| EffectError::Journal(e.to_string()))?;
        Ok(())
    }

    pub async fn perform<E>(&mut self, effect: E) -> Result<E::Output, EffectError>
    where
        E: Effect,
    {
        let declaration = self.ctx.effect_declaration(E::EFFECT_TYPE)?;
        let effect_ordinal = self.next_effect_ordinal;
        self.next_effect_ordinal = self.next_effect_ordinal.saturating_add(1);

        let descriptor = descriptor_for_effect(
            &effect,
            self.ctx.stage_logic_version.clone(),
            E::EFFECT_TYPE,
            E::SCHEMA_VERSION,
        )?;
        let descriptor_hash = descriptor_hash(&descriptor)?;
        let recorded_flow_id = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.recorded_flow_id.clone())
            .unwrap_or_else(|| self.ctx.flow_id.to_string());
        let cursor = EffectCursor {
            recorded_flow_id,
            stage_key: self.ctx.stage_key.clone(),
            input_seq: self.ctx.input_seq.0,
            effect_ordinal,
        };

        if let Some(history) = &self.ctx.effect_history {
            if let Some(record) = history.find(&cursor) {
                return self.replay_record_output(record, cursor, descriptor_hash);
            }

            if matches!(
                self.ctx.effect_runtime_mode,
                EffectRuntimeMode::ReplayStrict
            ) {
                return Err(EffectError::MissingRecordedEffect { cursor });
            }
        }

        if matches!(E::SAFETY, EffectSafety::Transactional) {
            return self
                .perform_transactional(effect, declaration, cursor, descriptor_hash, descriptor)
                .await;
        }

        let boundary_start = if let Some(boundary) = &self.ctx.effect_boundary {
            let EffectBoundaryStart {
                action,
                context,
                control_events,
            } = boundary.before_effect(&self.ctx.parent.event);
            match action {
                EffectBoundaryAction::Continue => Some(context),
                EffectBoundaryAction::Skip(results) => {
                    let output_event = results.into_iter().next().ok_or_else(|| {
                        EffectError::Execution(
                            "effect boundary skipped without a fallback output".to_string(),
                        )
                    })?;
                    self.ctx.push_boundary_control_events(control_events);
                    let output: E::Output = serde_json::from_value(output_event.payload())
                        .map_err(|e| EffectError::Serialization(e.to_string()))?;
                    let output_value = serde_json::to_value(&output)
                        .map_err(|e| EffectError::Serialization(e.to_string()))?;
                    self.append_record(
                        EffectRecord {
                            cursor,
                            descriptor_hash,
                            descriptor,
                            outcome: EffectOutcomePayload::Succeeded {
                                output: output_value,
                            },
                        },
                        None,
                    )
                    .await?;
                    return Ok(output);
                }
                EffectBoundaryAction::Abort => {
                    self.ctx.push_boundary_control_events(control_events);
                    let err =
                        EffectError::Execution("effect boundary aborted execution".to_string());
                    self.append_record(
                        EffectRecord {
                            cursor,
                            descriptor_hash,
                            descriptor,
                            outcome: EffectOutcomePayload::Failed {
                                error_type: err.error_type(),
                                error_message: err.error_message(),
                                retryable: err.retryable(),
                            },
                        },
                        Some(&err),
                    )
                    .await?;
                    return Err(err);
                }
            }
        } else {
            None
        };

        if matches!(E::SAFETY, EffectSafety::NonIdempotentRequiresKey)
            && effect.idempotency_key().is_none()
        {
            return Err(EffectError::MissingIdempotencyKey {
                effect_type: E::EFFECT_TYPE.to_string(),
            });
        }

        let mut effect_ctx = self.live_effect_context();

        match effect.execute(&mut effect_ctx).await {
            Ok(output) => {
                let output_value = serde_json::to_value(&output)
                    .map_err(|e| EffectError::Serialization(e.to_string()))?;
                if let (Some(boundary), Some(boundary_context)) =
                    (&self.ctx.effect_boundary, boundary_start)
                {
                    let output_event = ChainEventFactory::derived_data_event(
                        self.ctx.writer_id,
                        &self.ctx.parent.event,
                        E::EFFECT_TYPE,
                        output_value.clone(),
                    );
                    let control_events = boundary.after_effect(
                        boundary_context,
                        &self.ctx.parent.event,
                        std::slice::from_ref(&output_event),
                    );
                    self.ctx.push_boundary_control_events(control_events);
                }
                self.append_record(
                    EffectRecord {
                        cursor,
                        descriptor_hash,
                        descriptor,
                        outcome: EffectOutcomePayload::Succeeded {
                            output: output_value,
                        },
                    },
                    None,
                )
                .await?;
                Ok(output)
            }
            Err(err) => {
                if let (Some(boundary), Some(boundary_context)) =
                    (&self.ctx.effect_boundary, boundary_start)
                {
                    let error_event = self.ctx.parent.event.clone().mark_as_error(
                        err.to_string(),
                        obzenflow_core::event::status::processing_status::ErrorKind::Remote,
                    );
                    let control_events = boundary.after_effect(
                        boundary_context,
                        &self.ctx.parent.event,
                        std::slice::from_ref(&error_event),
                    );
                    self.ctx.push_boundary_control_events(control_events);
                }
                self.append_record(
                    EffectRecord {
                        cursor,
                        descriptor_hash,
                        descriptor,
                        outcome: EffectOutcomePayload::Failed {
                            error_type: err.error_type(),
                            error_message: err.error_message(),
                            retryable: err.retryable(),
                        },
                    },
                    Some(&err),
                )
                .await?;
                Err(err)
            }
        }
    }

    pub async fn capture<T>(&mut self, label: &'static str, value: T) -> Result<T, EffectError>
    where
        T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let effect_ordinal = self.next_effect_ordinal;
        self.next_effect_ordinal = self.next_effect_ordinal.saturating_add(1);
        let descriptor = EffectDescriptor {
            effect_type: "obzenflow.capture".to_string(),
            label: label.to_string(),
            schema_version: 1,
            stage_logic_version: self.ctx.stage_logic_version.clone(),
            canonical_input_hash: hash_json_value(&Value::String(label.to_string()))?,
        };
        let descriptor_hash = descriptor_hash(&descriptor)?;
        let recorded_flow_id = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.recorded_flow_id.clone())
            .unwrap_or_else(|| self.ctx.flow_id.to_string());
        let cursor = EffectCursor {
            recorded_flow_id,
            stage_key: self.ctx.stage_key.clone(),
            input_seq: self.ctx.input_seq.0,
            effect_ordinal,
        };

        if let Some(history) = &self.ctx.effect_history {
            if let Some(record) = history.find(&cursor) {
                return self.replay_record_output(record, cursor, descriptor_hash);
            }

            if matches!(
                self.ctx.effect_runtime_mode,
                EffectRuntimeMode::ReplayStrict
            ) {
                return Err(EffectError::MissingRecordedEffect { cursor });
            }
        }

        let output =
            serde_json::to_value(&value).map_err(|e| EffectError::Serialization(e.to_string()))?;
        self.append_record(
            EffectRecord {
                cursor,
                descriptor_hash,
                descriptor,
                outcome: EffectOutcomePayload::Succeeded { output },
            },
            None,
        )
        .await?;
        Ok(value)
    }

    async fn perform_transactional<E>(
        &self,
        effect: E,
        declaration: EffectDeclaration,
        cursor: EffectCursor,
        descriptor_hash: String,
        descriptor: EffectDescriptor,
    ) -> Result<E::Output, EffectError>
    where
        E: Effect,
    {
        let executor =
            declaration
                .transactional_executor
                .ok_or_else(|| EffectError::MissingEffectPort {
                    type_name: std::any::type_name::<dyn TransactionalEffectPort<E>>(),
                    name: "<missing transactional executor>".to_string(),
                })?;
        let port = self
            .ctx
            .effect_ports
            .get::<dyn TransactionalEffectPort<E>>(executor)
            .ok_or_else(|| EffectError::MissingEffectPort {
                type_name: std::any::type_name::<dyn TransactionalEffectPort<E>>(),
                name: executor.to_string(),
            })?;

        let mut effect_ctx = self.live_effect_context();
        let commit = EffectCommitHandle::new(
            self.ctx.writer_id,
            self.ctx.data_journal.clone(),
            self.ctx.parent.clone(),
            cursor,
            descriptor_hash,
            descriptor,
        );
        let commit_observer = commit.clone();
        // The port commits its outcome through the handle. Its returned value is
        // intentionally not used as the live result: the committed record is the single
        // source of truth, so a live run decodes the same journaled outcome a replay
        // would, and a port that commits one outcome but returns another cannot make
        // live output diverge from replay output (FLOWIP-120a).
        let port_result = port
            .execute_and_commit(effect, &mut effect_ctx, commit)
            .await;

        let Some(outcome) = commit_observer.committed_outcome() else {
            // No commit through the handle. Surface the port's own error if it produced
            // one, otherwise the contract-violation error.
            return Err(match port_result {
                Err(err) => err,
                Ok(_) => EffectError::TransactionalCommitMissing {
                    effect_type: E::EFFECT_TYPE.to_string(),
                    executor: executor.to_string(),
                },
            });
        };

        decode_effect_outcome::<E::Output>(&outcome)
    }

    fn replay_record_output<T>(
        &self,
        record: &EffectRecord,
        cursor: EffectCursor,
        descriptor_hash: String,
    ) -> Result<T, EffectError>
    where
        T: DeserializeOwned,
    {
        if record.descriptor_hash != descriptor_hash {
            return Err(EffectError::DescriptorMismatch {
                cursor,
                expected: descriptor_hash,
                recorded: record.descriptor_hash.clone(),
            });
        }

        decode_effect_outcome(&record.outcome)
    }

    fn live_effect_context(&self) -> EffectContext {
        EffectContext {
            is_replaying: false,
            flow_id: self.ctx.flow_id,
            stage_key: self.ctx.stage_key.clone(),
            input_seq: self.ctx.input_seq,
            ports: self.ctx.effect_ports.clone(),
        }
    }

    async fn append_record(
        &self,
        record: EffectRecord,
        source_error: Option<&EffectError>,
    ) -> Result<(), EffectError> {
        append_effect_record(
            &self.ctx.data_journal,
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            source_error,
        )
        .await
    }
}

// FLOWIP-120a: this append is intentionally write-time-unchecked for duplicate
// cursors. Callers reach it only after `history.find(&cursor)` returned `None`
// (see `Effects::perform`), and the cursor is deterministic per
// `(recorded_flow_id, stage_key, input_seq, effect_ordinal)`, where `effect_ordinal`
// is the per-input monotonic counter, unique while it has not saturated `u32`. A
// second append at the same cursor can therefore arise only from archive corruption,
// a non-atomic partial write, or ordinal saturation, each of which
// `EffectHistory::from_records` detects fail-loud on the next load (the same load
// that feeds replay). A read-before-write duplicate check is deliberately avoided
// here because it would add a TOCTOU window without removing one. Load-bearing
// ordering invariant: every live append on resume is preceded by
// `EffectHistory::load` (the transform, stateful, and sink FSMs load effect history
// before processing); if a future stage appended live before loading history, this
// fail-loud backstop would no longer cover it.
async fn append_effect_record(
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    writer_id: WriterId,
    parent: &EventEnvelope<ChainEvent>,
    record: EffectRecord,
    source_error: Option<&EffectError>,
) -> Result<(), EffectError> {
    let event_type = framework_effect_event_type(&record.descriptor.effect_type);
    let provenance = EffectProvenance::from_record(&record, true);
    let payload =
        serde_json::to_value(&record).map_err(|e| EffectError::Serialization(e.to_string()))?;
    let mut event =
        ChainEventFactory::derived_data_event(writer_id, &parent.event, event_type, payload)
            .with_effect_provenance(provenance);
    if let Some(err) = source_error {
        event.processing_info.status =
            obzenflow_core::event::status::processing_status::ProcessingStatus::error_with_kind(
                err.to_string(),
                Some(obzenflow_core::event::status::processing_status::ErrorKind::Remote),
            );
    }

    // FLOWIP-120b Step 1: route the framework effect/capture record append
    // through the shared OutputCommitter. With only the journal handle present,
    // this is the same credit-free, unenriched, unmirrored append it was before,
    // but it now shares the one commit path the supervisor drain uses.
    let committer = OutputCommitter {
        data_journal,
        flow_context: None,
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        output_contract: None,
    };
    committer
        .commit_prebuilt(event, Some(parent), CommitOptions::default())
        .await
        .map_err(|e| EffectError::Journal(e.to_string()))?;
    Ok(())
}

fn effect_record_from_event(event: &ChainEvent) -> Result<Option<EffectRecord>, EffectError> {
    match &event.content {
        ChainEventContent::Data {
            event_type,
            payload,
        } if is_framework_effect_event_type(event_type) => {
            let provenance = event.effect_provenance.as_ref().ok_or_else(|| {
                EffectError::EffectProvenanceMismatch(format!(
                    "reserved framework effect event `{event_type}` is missing effect_provenance"
                ))
            })?;
            if !provenance.framework_owned {
                return Err(EffectError::EffectProvenanceMismatch(format!(
                    "reserved framework effect event `{event_type}` is not marked framework_owned"
                )));
            }

            let record: EffectRecord = serde_json::from_value(payload.clone())
                .map_err(|e| EffectError::Serialization(e.to_string()))?;
            validate_effect_record_provenance(event_type, &record, provenance)?;
            Ok(Some(record))
        }
        _ => Ok(None),
    }
}

fn validate_effect_record_provenance(
    event_type: &str,
    record: &EffectRecord,
    provenance: &EffectProvenance,
) -> Result<(), EffectError> {
    let expected_event_type = framework_effect_event_type(&record.descriptor.effect_type);
    if event_type != expected_event_type {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "reserved framework effect event type `{event_type}` does not match record descriptor `{}` (expected `{expected_event_type}`)",
            record.descriptor.effect_type
        )));
    }

    if provenance.cursor != record.cursor {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance cursor does not match effect record cursor".to_string(),
        ));
    }
    if provenance.descriptor_hash != record.descriptor_hash {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance descriptor_hash does not match effect record descriptor_hash"
                .to_string(),
        ));
    }
    if provenance.descriptor != record.descriptor {
        return Err(EffectError::EffectProvenanceMismatch(
            "effect_provenance descriptor does not match effect record descriptor".to_string(),
        ));
    }

    let expected_group_id = effect_outcome_group_id(&record.cursor);
    if provenance.group_id.as_deref() != Some(expected_group_id.as_str()) {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "effect_provenance group_id does not match deterministic group id `{expected_group_id}`"
        )));
    }
    if provenance.outcome_fact_ordinal.is_some() {
        return Err(EffectError::EffectProvenanceMismatch(
            "framework effect record compatibility facts must not set outcome_fact_ordinal"
                .to_string(),
        ));
    }

    Ok(())
}

/// FLOWIP-120a: decode a committed or recorded effect outcome into the effect's
/// typed output. Replay and the live transactional commit share this one decode
/// path, so a live transactional return value is byte-identical to what a replay
/// reconstructs from the same record.
fn decode_effect_outcome<T>(outcome: &EffectOutcomePayload) -> Result<T, EffectError>
where
    T: DeserializeOwned,
{
    match outcome {
        EffectOutcomePayload::Succeeded { output } => serde_json::from_value(output.clone())
            .map_err(|e| EffectError::Serialization(e.to_string())),
        EffectOutcomePayload::Failed {
            error_type,
            error_message,
            retryable,
        } => Err(EffectError::RecordedFailure {
            error_type: error_type.clone(),
            error_message: error_message.clone(),
            retryable: *retryable,
        }),
    }
}

fn descriptor_for_effect<E>(
    effect: &E,
    stage_logic_version: String,
    effect_type: &'static str,
    schema_version: u32,
) -> Result<EffectDescriptor, EffectError>
where
    E: Effect,
{
    Ok(EffectDescriptor {
        effect_type: effect_type.to_string(),
        label: effect.label().to_string(),
        schema_version,
        stage_logic_version,
        canonical_input_hash: hash_json_value(&effect.canonical_input())?,
    })
}

fn descriptor_hash(descriptor: &EffectDescriptor) -> Result<String, EffectError> {
    hash_json_value(
        &serde_json::to_value(descriptor).map_err(|e| EffectError::Serialization(e.to_string()))?,
    )
}

fn hash_json_value(value: &Value) -> Result<String, EffectError> {
    let canonical = canonicalize_json_value(value.clone());
    let bytes =
        serde_json::to_vec(&canonical).map_err(|e| EffectError::Serialization(e.to_string()))?;
    Ok(hex_digest(digest(&SHA256, &bytes).as_ref()))
}

fn canonicalize_json_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = Map::new();
            for (key, value) in entries {
                out.insert(key, canonicalize_json_value(value));
            }
            Value::Object(out)
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().map(canonicalize_json_value).collect())
        }
        other => other,
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub fn deterministic_event_id(
    recorded_flow_id: &str,
    stage_key: &str,
    input_seq: StageInputPosition,
    output_ordinal: u32,
) -> EventId {
    let material = format!(
        "{recorded_flow_id}:{stage_key}:{}:{output_ordinal}",
        input_seq.0
    );
    let hash = digest(&SHA256, material.as_bytes());
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&hash.as_ref()[..16]);
    EventId::from(obzenflow_core::Ulid(u128::from_be_bytes(id_bytes)))
}

pub fn deterministic_event_time(input_seq: StageInputPosition, output_ordinal: u32) -> u64 {
    input_seq
        .0
        .saturating_mul(1_000)
        .saturating_add(u64::from(output_ordinal))
}

pub fn deterministic_typed_output_event<Out>(
    writer_id: WriterId,
    parent: &ChainEvent,
    output: Out,
    recorded_flow_id: &str,
    stage_key: &str,
    input_seq: StageInputPosition,
    output_ordinal: u32,
) -> Result<ChainEvent, EffectError>
where
    Out: TypedPayload,
{
    let payload =
        serde_json::to_value(output).map_err(|e| EffectError::Serialization(e.to_string()))?;
    let mut event = ChainEventFactory::derived_data_event(
        writer_id,
        parent,
        Out::versioned_event_type(),
        payload,
    );
    event.id = deterministic_event_id(recorded_flow_id, stage_key, input_seq, output_ordinal);
    event.processing_info.event_time = deterministic_event_time(input_seq, output_ordinal);
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::{EventEnvelope, JournalEvent};
    use obzenflow_core::journal::{JournalError, JournalReader};
    use obzenflow_core::{JournalId, JournalOwner, JournalWriterId, TypedPayload};
    use obzenflow_topology::TypeHintInfo;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct MemoryJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        events: Mutex<Vec<EventEnvelope<T>>>,
    }

    impl<T: JournalEvent> MemoryJournal<T> {
        fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                owner: Some(owner),
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<EventEnvelope<T>> {
            self.events.lock().expect("events lock poisoned").clone()
        }
    }

    struct MemoryJournalReader<T: JournalEvent> {
        events: Vec<EventEnvelope<T>>,
        position: usize,
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> JournalReader<T> for MemoryJournalReader<T> {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let next = self.events.get(self.position).cloned();
            if next.is_some() {
                self.position += 1;
            }
            Ok(next)
        }

        async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
            let remaining = self.events.len().saturating_sub(self.position);
            let skipped = remaining.min(n as usize);
            self.position += skipped;
            Ok(skipped as u64)
        }

        fn position(&self) -> u64 {
            self.position as u64
        }
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> Journal<T> for MemoryJournal<T> {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
            self.events
                .lock()
                .expect("events lock poisoned")
                .push(envelope.clone());
            Ok(envelope)
        }

        async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(self.events())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &EventId,
        ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            event_id: &EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(self
                .events()
                .into_iter()
                .find(|envelope| *envelope.event.id() == *event_id))
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(MemoryJournalReader {
                events: self.events(),
                position: 0,
            }))
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(MemoryJournalReader {
                events: self.events(),
                position: position as usize,
            }))
        }

        async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let events = self.events();
            let start = events.len().saturating_sub(count);
            Ok(events[start..].iter().rev().cloned().collect())
        }
    }

    #[derive(Clone, Debug)]
    struct CountingEffect {
        value: u64,
        label: &'static str,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Effect for CountingEffect {
        const EFFECT_TYPE: &'static str = "test.counting";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;

        type Output = u64;

        fn label(&self) -> &str {
            self.label
        }

        fn canonical_input(&self) -> Value {
            json!({ "value": self.value })
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.value + 1)
        }
    }

    #[derive(Clone, Debug)]
    struct TransactionalCountingEffect {
        value: u64,
        normal_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Effect for TransactionalCountingEffect {
        const EFFECT_TYPE: &'static str = "test.transactional_counting";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Transactional;

        type Output = u64;

        fn label(&self) -> &str {
            "transactional"
        }

        fn canonical_input(&self) -> Value {
            json!({ "value": self.value })
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
            self.normal_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.value + 10)
        }
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct FirstOutput {
        value: u64,
    }

    impl TypedPayload for FirstOutput {
        const EVENT_TYPE: &'static str = "test.first_output";
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct SecondOutput {
        value: String,
    }

    impl TypedPayload for SecondOutput {
        const EVENT_TYPE: &'static str = "test.second_output";
    }

    fn output_contract_for<T: TypedPayload>() -> StageOutputContract {
        StageOutputContract::single(crate::feed_plan::PayloadTypeDescriptor {
            type_hint: TypeHintInfo::Exact {
                name: std::any::type_name::<T>().to_string(),
            },
            event_type: Some(T::versioned_event_type()),
            schema_version: Some(T::SCHEMA_VERSION),
            visibility: crate::feed_plan::FactVisibility::Routable,
        })
    }

    #[test]
    fn deterministic_typed_output_events_preserve_ordinals() {
        let writer_id = WriterId::from(StageId::new());
        let parent = ChainEventFactory::data_event(writer_id, "test.input.v1", json!({ "id": 1 }));

        let first = deterministic_typed_output_event(
            writer_id,
            &parent,
            FirstOutput { value: 1 },
            "flow-a",
            "stage-a",
            StageInputPosition(4),
            2,
        )
        .expect("first output event");
        let second = deterministic_typed_output_event(
            writer_id,
            &parent,
            SecondOutput {
                value: "two".to_string(),
            },
            "flow-a",
            "stage-a",
            StageInputPosition(4),
            3,
        )
        .expect("second output event");
        let events = vec![first, second];

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0].content,
            ChainEventContent::Data { event_type, .. } if event_type == "test.first_output.v1"
        ));
        assert!(matches!(
            &events[1].content,
            ChainEventContent::Data { event_type, .. } if event_type == "test.second_output.v1"
        ));
        assert_eq!(
            events[0].id,
            deterministic_event_id("flow-a", "stage-a", StageInputPosition(4), 2)
        );
        assert_eq!(
            events[1].id,
            deterministic_event_id("flow-a", "stage-a", StageInputPosition(4), 3)
        );
        assert_eq!(events[0].processing_info.event_time, 4_002);
        assert_eq!(events[1].processing_info.event_time, 4_003);
    }

    struct TransactionalCountingPort {
        calls: Arc<AtomicUsize>,
        commit: bool,
    }

    #[async_trait]
    impl TransactionalEffectPort<TransactionalCountingEffect> for TransactionalCountingPort {
        async fn execute_and_commit(
            &self,
            effect: TransactionalCountingEffect,
            _ctx: &mut EffectContext,
            commit: EffectCommitHandle<u64>,
        ) -> Result<u64, EffectError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let output = effect.value + 1_000;
            if self.commit {
                commit.commit_success(&output).await?;
            }
            Ok(output)
        }
    }

    /// FLOWIP-120a: a deliberately misbehaving port that commits one value through
    /// the handle but returns a different value, used to prove the runtime derives
    /// the live return from the committed record rather than the port's return value.
    struct DivergentTransactionalPort {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TransactionalEffectPort<TransactionalCountingEffect> for DivergentTransactionalPort {
        async fn execute_and_commit(
            &self,
            effect: TransactionalCountingEffect,
            _ctx: &mut EffectContext,
            commit: EffectCommitHandle<u64>,
        ) -> Result<u64, EffectError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let committed = effect.value + 1_000;
            commit.commit_success(&committed).await?;
            // Return a value that disagrees with what was committed.
            Ok(effect.value + 9_999)
        }
    }

    fn parent_envelope(writer_id: WriterId) -> EventEnvelope<ChainEvent> {
        let event = ChainEventFactory::data_event(writer_id, "test.input", json!({"id": 1}));
        EventEnvelope::new(JournalWriterId::new(), event)
    }

    fn invocation_context(
        journal: Arc<dyn Journal<ChainEvent>>,
        parent: EventEnvelope<ChainEvent>,
        effect_history: Option<Arc<EffectHistory>>,
    ) -> EffectInvocationContext {
        let effect_runtime_mode = if effect_history.is_some() {
            EffectRuntimeMode::ReplayStrict
        } else {
            EffectRuntimeMode::Live
        };
        invocation_context_with_mode(
            journal,
            parent,
            effect_history,
            effect_runtime_mode,
            EffectPortRegistry::new(),
        )
    }

    fn invocation_context_with_mode(
        journal: Arc<dyn Journal<ChainEvent>>,
        parent: EventEnvelope<ChainEvent>,
        effect_history: Option<Arc<EffectHistory>>,
        effect_runtime_mode: EffectRuntimeMode,
        effect_ports: EffectPortRegistry,
    ) -> EffectInvocationContext {
        let stage_id = StageId::new();
        EffectInvocationContext {
            flow_id: FlowId::new(),
            stage_id,
            stage_key: "effect_stage".to_string(),
            writer_id: WriterId::from(stage_id),
            input_seq: StageInputPosition(1),
            stage_logic_version: "test-v1".to_string(),
            data_journal: journal,
            flow_context: None,
            system_journal: None,
            instrumentation: None,
            heartbeat_state: None,
            parent,
            effect_history,
            effect_runtime_mode,
            effect_ports,
            effect_declarations: vec![
                EffectDeclaration::of::<CountingEffect>(),
                EffectDeclaration::transactional_effect::<TransactionalCountingEffect>("tx"),
            ],
            output_contract: StageOutputContract::empty(),
            emit_enabled: false,
            effect_boundary: None,
            boundary_control_events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[tokio::test]
    async fn emit_rejects_contexts_without_output_emission_support() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut effects = Effects::new(invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));

        let err = effects
            .emit(FirstOutput { value: 1 })
            .await
            .expect_err("perform-only contexts must reject emitted outputs");

        assert!(matches!(
            err,
            EffectError::EmitUnsupported { stage_key } if stage_key == "effect_stage"
        ));
        assert!(journal.events().is_empty());
    }

    #[tokio::test]
    async fn emit_rejects_fact_type_outside_stage_output_contract() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut ctx = invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        );
        ctx.emit_enabled = true;
        ctx.output_contract = output_contract_for::<FirstOutput>();
        let mut effects = Effects::new(ctx);

        let err = effects
            .emit(SecondOutput {
                value: "second".to_string(),
            })
            .await
            .expect_err("undeclared output fact types must fail closed");

        assert!(matches!(
            err,
            EffectError::UndeclaredOutput {
                stage_key,
                event_type,
            } if stage_key == "effect_stage" && event_type == SecondOutput::versioned_event_type()
        ));
        assert!(journal.events().is_empty());
    }

    #[tokio::test]
    async fn emit_commits_declared_fact_type_immediately() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut ctx = invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        );
        ctx.emit_enabled = true;
        ctx.output_contract = output_contract_for::<FirstOutput>();
        let mut effects = Effects::new(ctx);

        effects
            .emit(FirstOutput { value: 7 })
            .await
            .expect("declared output fact should be accepted");

        let events = journal.events();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].event.content,
            ChainEventContent::Data { event_type, .. }
                if event_type == &FirstOutput::versioned_event_type()
        ));
    }

    fn effect_records(journal: &MemoryJournal<ChainEvent>) -> Vec<EffectRecord> {
        journal
            .events()
            .into_iter()
            .filter_map(|envelope| {
                effect_record_from_event(&envelope.event).expect("effect record decode")
            })
            .collect()
    }

    #[tokio::test]
    async fn live_perform_records_effect_data_fact() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut effects = Effects::new(invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));

        let output = effects
            .perform(CountingEffect {
                value: 41,
                label: "same",
                calls: calls.clone(),
            })
            .await
            .expect("effect should succeed");

        assert_eq!(output, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = journal.events();
        assert!(matches!(
            events[0].event.content,
            ChainEventContent::Data { .. }
        ));
        let records = effect_records(&journal);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cursor.input_seq, 1);
        assert_eq!(records[0].cursor.effect_ordinal, 0);
        let provenance = events[0]
            .event
            .effect_provenance
            .as_ref()
            .expect("effect data fact should carry provenance");
        assert_eq!(
            provenance.group_id.as_deref(),
            Some(effect_outcome_group_id(&records[0].cursor).as_str())
        );
    }

    #[tokio::test]
    async fn perform_rejects_undeclared_effect_before_execution() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut ctx = invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        );
        ctx.effect_declarations.clear();
        let mut effects = Effects::new(ctx);

        let err = effects
            .perform(CountingEffect {
                value: 41,
                label: "same",
                calls: calls.clone(),
            })
            .await
            .expect_err("undeclared effects must fail before execution");

        match err {
            EffectError::UndeclaredEffect {
                stage_key,
                effect_type,
            } => {
                assert_eq!(stage_key, "effect_stage");
                assert_eq!(effect_type, CountingEffect::EFFECT_TYPE);
            }
            other => panic!("unexpected effect error: {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(effect_records(&journal).is_empty());
    }

    #[tokio::test]
    async fn strict_replay_rejects_undeclared_effect_before_history_lookup() {
        let stage_id = StageId::new();
        let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let live_calls = Arc::new(AtomicUsize::new(0));
        let live_parent = parent_envelope(WriterId::from(stage_id));
        let mut live = Effects::new(invocation_context(live_journal.clone(), live_parent, None));
        live.perform(CountingEffect {
            value: 41,
            label: "same",
            calls: live_calls,
        })
        .await
        .expect("live effect should succeed");

        let live_records = effect_records(&live_journal);
        let history = Arc::new(
            EffectHistory::from_records(
                live_records[0].cursor.recorded_flow_id.clone(),
                live_records,
            )
            .expect("history should index"),
        );
        let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
        let replay_calls = Arc::new(AtomicUsize::new(0));
        let mut ctx = invocation_context(
            replay_journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
        );
        ctx.stage_logic_version = "test-v2".to_string();
        ctx.effect_declarations.clear();
        let mut replay = Effects::new(ctx);

        let err = replay
            .perform(CountingEffect {
                value: 41,
                label: "same",
                calls: replay_calls.clone(),
            })
            .await
            .expect_err("undeclared effects must fail before replay history lookup");

        match err {
            EffectError::UndeclaredEffect {
                stage_key,
                effect_type,
            } => {
                assert_eq!(stage_key, "effect_stage");
                assert_eq!(effect_type, CountingEffect::EFFECT_TYPE);
            }
            other => panic!("unexpected effect error: {other:?}"),
        }
        assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
        assert!(effect_records(&replay_journal).is_empty());
    }

    #[tokio::test]
    async fn capture_is_exempt_from_declared_effect_list() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut ctx = invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        );
        ctx.effect_declarations.clear();
        let mut effects = Effects::new(ctx);

        let captured: u64 = effects
            .capture("side_value", 7)
            .await
            .expect("capture should not require an effect declaration");

        assert_eq!(captured, 7);
        let events = journal.events();
        assert!(matches!(
            &events[0].event.content,
            ChainEventContent::Data { event_type, .. } if event_type == CAPTURE_EVENT_TYPE
        ));
        assert!(events[0].event.effect_provenance.is_some());
        assert_eq!(effect_records(&journal).len(), 1);
    }

    #[tokio::test]
    async fn replay_perform_uses_recorded_output_without_execute() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let live_calls = Arc::new(AtomicUsize::new(0));
        let live_parent = parent_envelope(WriterId::from(stage_id));
        let mut live = Effects::new(invocation_context(journal.clone(), live_parent, None));
        let output = live
            .perform(CountingEffect {
                value: 9,
                label: "same",
                calls: live_calls,
            })
            .await
            .expect("live effect should succeed");
        assert_eq!(output, 10);

        let records = effect_records(&journal);
        let history = Arc::new(
            EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
                .expect("history should index"),
        );
        let replay_calls = Arc::new(AtomicUsize::new(0));
        let mut replay = Effects::new(invocation_context(
            Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
        ));

        let replayed = replay
            .perform(CountingEffect {
                value: 9,
                label: "same",
                calls: replay_calls.clone(),
            })
            .await
            .expect("replay should read recorded output");

        assert_eq!(replayed, 10);
        assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn strict_replay_missing_effect_record_fails_without_execute() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let history = Arc::new(
            EffectHistory::from_records("archived_flow".to_string(), Vec::new())
                .expect("empty history should index"),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut effects = Effects::new(invocation_context_with_mode(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
            EffectRuntimeMode::ReplayStrict,
            EffectPortRegistry::new(),
        ));

        let err = effects
            .perform(CountingEffect {
                value: 9,
                label: "same",
                calls: calls.clone(),
            })
            .await
            .expect_err("strict replay must fail when the cursor is missing");

        assert!(matches!(err, EffectError::MissingRecordedEffect { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(effect_records(&journal).is_empty());
    }

    #[tokio::test]
    async fn resume_incomplete_missing_effect_executes_with_recorded_cursor() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let history = Arc::new(
            EffectHistory::from_records("archived_flow".to_string(), Vec::new())
                .expect("empty history should index"),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut effects = Effects::new(invocation_context_with_mode(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
            EffectRuntimeMode::ResumeIncomplete,
            EffectPortRegistry::new(),
        ));

        let output = effects
            .perform(CountingEffect {
                value: 9,
                label: "same",
                calls: calls.clone(),
            })
            .await
            .expect("resume should execute missing effect live");

        assert_eq!(output, 10);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let records = effect_records(&journal);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cursor.recorded_flow_id, "archived_flow");
        assert_eq!(records[0].cursor.stage_key, "effect_stage");
        assert_eq!(records[0].cursor.input_seq, 1);
        assert_eq!(records[0].cursor.effect_ordinal, 0);
    }

    #[tokio::test]
    async fn resume_incomplete_recorded_effect_suppresses_execution() {
        let stage_id = StageId::new();
        let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let live_calls = Arc::new(AtomicUsize::new(0));
        let mut live = Effects::new(invocation_context(
            live_journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));
        live.perform(CountingEffect {
            value: 9,
            label: "same",
            calls: live_calls,
        })
        .await
        .expect("live effect should record");
        let records = effect_records(&live_journal);
        let history = Arc::new(
            EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
                .expect("history should index"),
        );
        let replay_calls = Arc::new(AtomicUsize::new(0));
        let mut resume = Effects::new(invocation_context_with_mode(
            Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
            EffectRuntimeMode::ResumeIncomplete,
            EffectPortRegistry::new(),
        ));

        let output = resume
            .perform(CountingEffect {
                value: 9,
                label: "same",
                calls: replay_calls.clone(),
            })
            .await
            .expect("resume should use recorded output");

        assert_eq!(output, 10);
        assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn transactional_effect_uses_registered_port_and_commits_once() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let normal_calls = Arc::new(AtomicUsize::new(0));
        let transactional_calls = Arc::new(AtomicUsize::new(0));
        let mut ports = EffectPortRegistry::new();
        ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
            "tx",
            Arc::new(TransactionalCountingPort {
                calls: transactional_calls.clone(),
                commit: true,
            }),
        );
        let mut effects = Effects::new(invocation_context_with_mode(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
            EffectRuntimeMode::Live,
            ports,
        ));

        let output = effects
            .perform(TransactionalCountingEffect {
                value: 7,
                normal_calls: normal_calls.clone(),
            })
            .await
            .expect("transactional port should commit");

        assert_eq!(output, 1_007);
        assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
        assert_eq!(transactional_calls.load(Ordering::SeqCst), 1);
        let records = effect_records(&journal);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cursor.effect_ordinal, 0);
    }

    #[tokio::test]
    async fn transactional_effect_live_return_comes_from_committed_record_not_port_return() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let normal_calls = Arc::new(AtomicUsize::new(0));
        let port_calls = Arc::new(AtomicUsize::new(0));
        let mut ports = EffectPortRegistry::new();
        ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
            "tx",
            Arc::new(DivergentTransactionalPort {
                calls: port_calls.clone(),
            }),
        );
        let mut live = Effects::new(invocation_context_with_mode(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
            EffectRuntimeMode::Live,
            ports,
        ));

        let live_output = live
            .perform(TransactionalCountingEffect {
                value: 7,
                normal_calls: normal_calls.clone(),
            })
            .await
            .expect("transactional effect should commit");

        // The live return is the committed value (7 + 1000), NOT the value the port
        // returned (7 + 9999). Without the structural fix this would be 10_006 live
        // and 1_007 on replay, a divergence.
        assert_eq!(live_output, 1_007);
        assert_eq!(port_calls.load(Ordering::SeqCst), 1);

        let records = effect_records(&journal);
        assert_eq!(records.len(), 1);
        let history = Arc::new(
            EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
                .expect("history should index"),
        );
        let mut replay = Effects::new(invocation_context_with_mode(
            Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
            EffectRuntimeMode::ReplayStrict,
            EffectPortRegistry::new(),
        ));

        let replay_output = replay
            .perform(TransactionalCountingEffect {
                value: 7,
                normal_calls: normal_calls.clone(),
            })
            .await
            .expect("strict replay should reconstruct the committed value");

        assert_eq!(
            replay_output, live_output,
            "live and replay must agree on the committed outcome"
        );
        assert_eq!(
            port_calls.load(Ordering::SeqCst),
            1,
            "replay must not invoke the transactional port"
        );
    }

    #[tokio::test]
    async fn transactional_effect_replay_does_not_require_port_or_execute() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let normal_calls = Arc::new(AtomicUsize::new(0));
        let transactional_calls = Arc::new(AtomicUsize::new(0));
        let mut ports = EffectPortRegistry::new();
        ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
            "tx",
            Arc::new(TransactionalCountingPort {
                calls: transactional_calls,
                commit: true,
            }),
        );
        let mut live = Effects::new(invocation_context_with_mode(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
            EffectRuntimeMode::Live,
            ports,
        ));
        live.perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
        })
        .await
        .expect("live transactional effect should commit");

        let records = effect_records(&journal);
        let history = Arc::new(
            EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
                .expect("history should index"),
        );
        let mut replay = Effects::new(invocation_context_with_mode(
            Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
            EffectRuntimeMode::ReplayStrict,
            EffectPortRegistry::new(),
        ));

        let output = replay
            .perform(TransactionalCountingEffect {
                value: 7,
                normal_calls: normal_calls.clone(),
            })
            .await
            .expect("strict replay should use recorded transactional output");

        assert_eq!(output, 1_007);
        assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn transactional_effect_missing_commit_fails() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let normal_calls = Arc::new(AtomicUsize::new(0));
        let transactional_calls = Arc::new(AtomicUsize::new(0));
        let mut ports = EffectPortRegistry::new();
        ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
            "tx",
            Arc::new(TransactionalCountingPort {
                calls: transactional_calls.clone(),
                commit: false,
            }),
        );
        let mut effects = Effects::new(invocation_context_with_mode(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
            EffectRuntimeMode::Live,
            ports,
        ));

        let err = effects
            .perform(TransactionalCountingEffect {
                value: 7,
                normal_calls: normal_calls.clone(),
            })
            .await
            .expect_err("transactional port returning without commit must fail");

        assert!(matches!(
            err,
            EffectError::TransactionalCommitMissing { .. }
        ));
        assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
        assert_eq!(transactional_calls.load(Ordering::SeqCst), 1);
        assert!(effect_records(&journal).is_empty());
    }

    #[tokio::test]
    async fn transactional_effect_missing_port_fails_before_execute() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let normal_calls = Arc::new(AtomicUsize::new(0));
        let mut effects = Effects::new(invocation_context(
            journal,
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));

        let err = effects
            .perform(TransactionalCountingEffect {
                value: 7,
                normal_calls: normal_calls.clone(),
            })
            .await
            .expect_err("missing transactional port must fail before execution");

        assert!(matches!(err, EffectError::MissingEffectPort { .. }));
        assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn replay_fails_on_descriptor_hash_mismatch() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut live = Effects::new(invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));
        live.perform(CountingEffect {
            value: 1,
            label: "original",
            calls,
        })
        .await
        .expect("live effect should succeed");
        let records = effect_records(&journal);
        let history = Arc::new(
            EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
                .expect("history should index"),
        );
        let mut replay = Effects::new(invocation_context(
            Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
        ));

        let err = replay
            .perform(CountingEffect {
                value: 1,
                label: "changed",
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .await
            .expect_err("descriptor mismatch must fail replay");

        assert!(matches!(err, EffectError::DescriptorMismatch { .. }));
    }

    #[tokio::test]
    async fn effect_history_fails_loud_on_duplicate_cursor() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut live = Effects::new(invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));
        live.perform(CountingEffect {
            value: 1,
            label: "same",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("live effect should succeed");

        let mut records = effect_records(&journal);
        records.push(records[0].clone());

        let err = EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect_err("duplicate cursors must fail loud");

        assert!(matches!(err, EffectError::DuplicateRecordedEffect { .. }));
    }

    #[tokio::test]
    async fn effect_record_decode_rejects_payload_provenance_cursor_mismatch() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut live = Effects::new(invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));
        live.perform(CountingEffect {
            value: 1,
            label: "same",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("live effect should succeed");

        let mut event = journal.events()[0].event.clone();
        event
            .effect_provenance
            .as_mut()
            .expect("effect event should carry provenance")
            .cursor
            .effect_ordinal = 99;

        let err = effect_record_from_event(&event)
            .expect_err("payload/provenance cursor mismatch must fail loud");

        assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
    }

    #[test]
    fn effect_record_decode_rejects_reserved_event_without_provenance() {
        let stage_id = StageId::new();
        let cursor = EffectCursor {
            recorded_flow_id: "flow".to_string(),
            stage_key: "effect_stage".to_string(),
            input_seq: 1,
            effect_ordinal: 0,
        };
        let record = EffectRecord {
            cursor,
            descriptor_hash: "hash".to_string(),
            descriptor: EffectDescriptor {
                effect_type: CountingEffect::EFFECT_TYPE.to_string(),
                label: "same".to_string(),
                schema_version: CountingEffect::SCHEMA_VERSION,
                stage_logic_version: "test-v1".to_string(),
                canonical_input_hash: "input".to_string(),
            },
            outcome: EffectOutcomePayload::Succeeded {
                output: json!(2_u64),
            },
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(stage_id),
            EFFECT_RECORD_EVENT_TYPE,
            serde_json::to_value(record).expect("record should serialize"),
        );

        let err = effect_record_from_event(&event)
            .expect_err("reserved effect record without provenance must fail loud");

        assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
    }

    trait DemoPort: Send + Sync {
        fn value(&self) -> u64;
    }

    struct DemoPortImpl;

    impl DemoPort for DemoPortImpl {
        fn value(&self) -> u64 {
            42
        }
    }

    #[test]
    fn effect_context_resolves_typed_trait_object_ports() {
        let mut ports = EffectPortRegistry::new();
        ports.insert::<dyn DemoPort>("primary", Arc::new(DemoPortImpl));
        let ctx = EffectContext {
            is_replaying: false,
            flow_id: FlowId::new(),
            stage_key: "effect_stage".to_string(),
            input_seq: StageInputPosition(3),
            ports,
        };

        let port = ctx
            .port::<dyn DemoPort>("primary")
            .expect("registered port should resolve");
        assert_eq!(port.value(), 42);
        assert!(matches!(
            ctx.port::<dyn DemoPort>("missing"),
            Err(EffectError::MissingEffectPort { .. })
        ));
    }

    #[tokio::test]
    async fn capture_replays_recorded_value_without_using_live_value() {
        let stage_id = StageId::new();
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut live = Effects::new(invocation_context(
            journal.clone(),
            parent_envelope(WriterId::from(stage_id)),
            None,
        ));

        let captured: u64 = live.capture("side_value", 7).await.expect("capture");
        assert_eq!(captured, 7);

        let records = effect_records(&journal);
        let history = Arc::new(
            EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
                .expect("history should index"),
        );
        let mut replay = Effects::new(invocation_context(
            Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
            parent_envelope(WriterId::from(stage_id)),
            Some(history),
        ));

        let replayed: u64 = replay
            .capture("side_value", 999)
            .await
            .expect("capture should replay");

        assert_eq!(replayed, 7);
    }
}
