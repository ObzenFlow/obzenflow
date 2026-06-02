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
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, FlowId, StageId, WriterId};
use ring::digest::{digest, SHA256};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use crate::messaging::upstream_subscription::StageInputPosition;
use crate::replay::{ReplayArchive, ReplayError};

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
            | EffectError::DescriptorMismatch { .. }
            | EffectError::MissingIdempotencyKey { .. } => false,
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
            EffectError::DescriptorMismatch { .. } => "descriptor_mismatch",
            EffectError::RecordedFailure { error_type, .. } => return error_type.clone(),
            EffectError::MissingIdempotencyKey { .. } => "missing_idempotency_key",
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

#[derive(Clone)]
pub struct EffectContext {
    is_replaying: bool,
    flow_id: FlowId,
    stage_key: String,
    input_seq: StageInputPosition,
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
}

#[derive(Clone)]
pub struct EffectHistory {
    recorded_flow_id: String,
    records: Arc<Vec<EffectRecord>>,
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

        Ok(Self {
            recorded_flow_id: archive.archive_flow_id().to_string(),
            records: Arc::new(records),
        })
    }

    fn find(&self, cursor: &EffectCursor) -> Option<&EffectRecord> {
        self.records.iter().find(|record| record.cursor == *cursor)
    }

    pub fn recorded_flow_id(&self) -> &str {
        &self.recorded_flow_id
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
        self.ctx.effect_history.is_some()
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
            return self.replay(effect, history, cursor, descriptor_hash).await;
        }

        if matches!(effect.safety(), EffectSafety::NonIdempotentRequiresKey)
            && effect.idempotency_key().is_none()
        {
            return Err(EffectError::MissingIdempotencyKey {
                effect_type: E::EFFECT_TYPE.to_string(),
            });
        }

        let mut effect_ctx = EffectContext {
            is_replaying: false,
            flow_id: self.ctx.flow_id,
            stage_key: self.ctx.stage_key.clone(),
            input_seq: self.ctx.input_seq,
        };

        match effect.execute(&mut effect_ctx).await {
            Ok(output) => {
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
                Ok(output)
            }
            Err(err) => {
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

    async fn replay<E>(
        &self,
        _effect: E,
        history: &EffectHistory,
        cursor: EffectCursor,
        descriptor_hash: String,
    ) -> Result<E::Output, EffectError>
    where
        E: Effect,
    {
        let record = history
            .find(&cursor)
            .ok_or_else(|| EffectError::MissingRecordedEffect {
                cursor: cursor.clone(),
            })?;

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

    async fn append_record(
        &self,
        record: EffectRecord,
        source_error: Option<&EffectError>,
    ) -> Result<(), EffectError> {
        let mut event = ChainEventFactory::derived_event(
            self.ctx.writer_id,
            &self.ctx.parent.event,
            ChainEventContent::EffectResult(record),
        );
        if let Some(err) = source_error {
            event.processing_info.status =
                obzenflow_core::event::status::processing_status::ProcessingStatus::error_with_kind(
                    err.to_string(),
                    Some(obzenflow_core::event::status::processing_status::ErrorKind::Remote),
                );
        }

        self.ctx
            .data_journal
            .append(event, Some(&self.ctx.parent))
            .await
            .map_err(|e| EffectError::Journal(e.to_string()))?;
        Ok(())
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

    fn parent_envelope(writer_id: WriterId) -> EventEnvelope<ChainEvent> {
        let event = ChainEventFactory::data_event(writer_id, "test.input", json!({"id": 1}));
        EventEnvelope::new(JournalWriterId::new(), event)
    }

    fn invocation_context(
        journal: Arc<dyn Journal<ChainEvent>>,
        parent: EventEnvelope<ChainEvent>,
        effect_history: Option<Arc<EffectHistory>>,
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
        }
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
        let history = Arc::new(EffectHistory {
            recorded_flow_id: records[0].cursor.recorded_flow_id.clone(),
            records: Arc::new(records),
        });
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
        let history = Arc::new(EffectHistory {
            recorded_flow_id: records[0].cursor.recorded_flow_id.clone(),
            records: Arc::new(records),
        });
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
}
