// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe user effects.

use async_trait::async_trait;
pub use obzenflow_core::event::payloads::effect_payload::{
    EffectCursor, EffectDescriptor, EffectOutcomePayload, EffectRecord,
};
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::event::{ChainEventContent, ChainEventFactory};
use obzenflow_core::journal::{ArchiveStatus, Journal};
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, FlowId, StageId, WriterId};
use ring::digest::{digest, SHA256};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};
use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

use crate::messaging::upstream_subscription::StageInputPosition;
use crate::replay::{ReplayArchive, ReplayError};

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
}

pub trait EffectBoundaryMiddleware: Send + Sync {
    fn before_effect(&self, event: &ChainEvent) -> EffectBoundaryStart;

    fn after_effect(
        &self,
        context: EffectBoundaryContext,
        event: &ChainEvent,
        outputs: &[ChainEvent],
    );
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

    #[error("non-idempotent effect '{effect_type}' has no idempotency key")]
    MissingIdempotencyKey { effect_type: String },

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
            EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::MissingIdempotencyKey { .. }
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
            EffectError::MissingIdempotencyKey { .. } => "missing_idempotency_key",
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
        Self {
            effect_type: E::EFFECT_TYPE,
            safety: EffectSafety::Transactional,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: vec![EffectPortRequirement::of::<dyn TransactionalEffectPort<E>>(
                executor,
            )],
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

    type Output: Serialize + DeserializeOwned + Send + Sync + 'static;

    fn label(&self) -> Cow<'static, str>;

    fn canonical_input(&self) -> Value;

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError>;

    fn safety(&self) -> EffectSafety {
        EffectSafety::Idempotent
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        None
    }

    fn transactional_executor(&self) -> Option<&'static str> {
        None
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
    committed: Mutex<bool>,
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
                committed: Mutex::new(false),
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
            if *committed {
                return Err(EffectError::Execution(
                    "transactional effect commit handle used more than once".to_string(),
                ));
            }
            *committed = true;
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
            *committed = false;
            return Err(err);
        }

        Ok(())
    }

    fn was_committed(&self) -> bool {
        *self
            .inner
            .committed
            .lock()
            .expect("effect commit handle lock poisoned")
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

#[async_trait]
pub trait EffectResultWriter: Send + Sync {
    async fn append_effect_result(
        &self,
        record: EffectRecord,
        parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<(), EffectError>;
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
            if let ChainEventContent::EffectResult(record) = envelope.event.content {
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
    pub parent: EventEnvelope<ChainEvent>,
    pub effect_history: Option<Arc<EffectHistory>>,
    pub effect_runtime_mode: EffectRuntimeMode,
    pub effect_ports: EffectPortRegistry,
    pub effect_boundary: Option<Arc<dyn EffectBoundaryMiddleware>>,
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

pub struct Effects {
    ctx: EffectInvocationContext,
    next_effect_ordinal: u32,
}

impl Effects {
    pub fn new(ctx: EffectInvocationContext) -> Self {
        Self {
            ctx,
            next_effect_ordinal: 0,
        }
    }

    pub fn is_replaying(&self) -> bool {
        !matches!(self.ctx.effect_runtime_mode, EffectRuntimeMode::Live)
    }

    pub async fn perform<E>(&mut self, effect: E) -> Result<E::Output, EffectError>
    where
        E: Effect,
    {
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

        if matches!(effect.safety(), EffectSafety::Transactional) {
            return self
                .perform_transactional(effect, cursor, descriptor_hash, descriptor)
                .await;
        }

        let boundary_start = if let Some(boundary) = &self.ctx.effect_boundary {
            let start = boundary.before_effect(&self.ctx.parent.event);
            match start.action {
                EffectBoundaryAction::Continue => Some(start.context),
                EffectBoundaryAction::Skip(results) => {
                    let output_event = results.into_iter().next().ok_or_else(|| {
                        EffectError::Execution(
                            "effect boundary skipped without a fallback output".to_string(),
                        )
                    })?;
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

        if matches!(effect.safety(), EffectSafety::NonIdempotentRequiresKey)
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
                    boundary.after_effect(
                        boundary_context,
                        &self.ctx.parent.event,
                        std::slice::from_ref(&output_event),
                    );
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
                    boundary.after_effect(
                        boundary_context,
                        &self.ctx.parent.event,
                        std::slice::from_ref(&error_event),
                    );
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
        cursor: EffectCursor,
        descriptor_hash: String,
        descriptor: EffectDescriptor,
    ) -> Result<E::Output, EffectError>
    where
        E: Effect,
    {
        let executor =
            effect
                .transactional_executor()
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
        let result = port
            .execute_and_commit(effect, &mut effect_ctx, commit)
            .await;

        if !commit_observer.was_committed() {
            return Err(EffectError::TransactionalCommitMissing {
                effect_type: E::EFFECT_TYPE.to_string(),
                executor: executor.to_string(),
            });
        }

        result
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

        match &record.outcome {
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
    let mut event = ChainEventFactory::derived_event(
        writer_id,
        &parent.event,
        ChainEventContent::EffectResult(record),
    );
    if let Some(err) = source_error {
        event.processing_info.status =
            obzenflow_core::event::status::processing_status::ProcessingStatus::error_with_kind(
                err.to_string(),
                Some(obzenflow_core::event::status::processing_status::ErrorKind::Remote),
            );
    }

    data_journal
        .append(event, Some(parent))
        .await
        .map_err(|e| EffectError::Journal(e.to_string()))?;
    Ok(())
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
        label: effect.label().into_owned(),
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
    use obzenflow_core::{JournalId, JournalOwner, JournalWriterId};
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

        type Output = u64;

        fn label(&self) -> Cow<'static, str> {
            Cow::Borrowed(self.label)
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
        executor: Option<&'static str>,
    }

    #[async_trait]
    impl Effect for TransactionalCountingEffect {
        const EFFECT_TYPE: &'static str = "test.transactional_counting";
        const SCHEMA_VERSION: u32 = 1;

        type Output = u64;

        fn label(&self) -> Cow<'static, str> {
            Cow::Borrowed("transactional")
        }

        fn canonical_input(&self) -> Value {
            json!({ "value": self.value })
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
            self.normal_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.value + 10)
        }

        fn safety(&self) -> EffectSafety {
            EffectSafety::Transactional
        }

        fn transactional_executor(&self) -> Option<&'static str> {
            self.executor
        }
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
            parent,
            effect_history,
            effect_runtime_mode,
            effect_ports,
            effect_boundary: None,
        }
    }

    fn effect_records(journal: &MemoryJournal<ChainEvent>) -> Vec<EffectRecord> {
        journal
            .events()
            .into_iter()
            .filter_map(|envelope| match envelope.event.content {
                ChainEventContent::EffectResult(record) => Some(record),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn live_perform_records_effect_result() {
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
        let records: Vec<_> = journal
            .events()
            .into_iter()
            .filter_map(|envelope| match envelope.event.content {
                ChainEventContent::EffectResult(record) => Some(record),
                _ => None,
            })
            .collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cursor.input_seq, 1);
        assert_eq!(records[0].cursor.effect_ordinal, 0);
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

        let records: Vec<_> = journal
            .events()
            .into_iter()
            .filter_map(|envelope| match envelope.event.content {
                ChainEventContent::EffectResult(record) => Some(record),
                _ => None,
            })
            .collect();
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
                executor: Some("tx"),
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
            executor: Some("tx"),
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
                executor: Some("tx"),
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
                executor: Some("tx"),
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
                executor: Some("tx"),
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
        let records: Vec<_> = journal
            .events()
            .into_iter()
            .filter_map(|envelope| match envelope.event.content {
                ChainEventContent::EffectResult(record) => Some(record),
                _ => None,
            })
            .collect();
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

        let mut records: Vec<_> = journal
            .events()
            .into_iter()
            .filter_map(|envelope| match envelope.event.content {
                ChainEventContent::EffectResult(record) => Some(record),
                _ => None,
            })
            .collect();
        records.push(records[0].clone());

        let err = EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect_err("duplicate cursors must fail loud");

        assert!(matches!(err, EffectError::DuplicateRecordedEffect { .. }));
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

        let records: Vec<_> = journal
            .events()
            .into_iter()
            .filter_map(|envelope| match envelope.event.content {
                ChainEventContent::EffectResult(record) => Some(record),
                _ => None,
            })
            .collect();
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
