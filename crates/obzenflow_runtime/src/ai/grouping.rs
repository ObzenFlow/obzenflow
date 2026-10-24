// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::stateful::traits::StatefulHandler;
use crate::stages::common::handlers::StatefulTerminationKind;
use crate::stages::stateful::strategies::accumulators::trace::TraceState;
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiMapReduceGroupingEnded, AiMapReduceMapInput, GroupClosedBy, GroupOrdinal, ReduceTrigger,
};
use obzenflow_core::event::schema::TypedFact;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;

/// The function that keys an `ai::map` stage's items.
pub type AiKeyFn<T, K> = Arc<dyn Fn(&T) -> K + Send + Sync + 'static>;

/// How an `ai::map` stage's grouping member keys its items.
pub enum AiKeying<T, K> {
    /// One key for the whole input.
    Unkeyed,
    /// Groups and windows form within each key, and at most `max_open_keys`
    /// keys are open at once.
    Keyed {
        key: AiKeyFn<T, K>,
        max_open_keys: NonZeroU64,
    },
}

impl<T, K> Clone for AiKeying<T, K> {
    fn clone(&self) -> Self {
        match self {
            Self::Unkeyed => Self::Unkeyed,
            Self::Keyed { key, max_open_keys } => Self::Keyed {
                key: Arc::clone(key),
                max_open_keys: *max_open_keys,
            },
        }
    }
}

/// The pure grouping member of an `ai::map` stage. It keys, closes and
/// numbers groups, enforces the open-key limit and, at a natural end of input
/// only, flushes open groups and closes each key's final window.
#[doc(hidden)]
pub struct AiGrouping<T, K> {
    stage: String,
    chunk_size: NonZeroU64,
    trigger: Option<ReduceTrigger>,
    keying: AiKeying<T, K>,
    lineage: obzenflow_core::config::LineagePolicy,
    writer_id: Option<WriterId>,
}

impl<T, K> AiGrouping<T, K> {
    pub fn new(
        stage: impl Into<String>,
        chunk_size: NonZeroU64,
        trigger: Option<ReduceTrigger>,
        keying: AiKeying<T, K>,
    ) -> Self {
        Self {
            stage: stage.into(),
            chunk_size,
            trigger,
            keying,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            writer_id: None,
        }
    }
}

impl<T, K> Clone for AiGrouping<T, K> {
    fn clone(&self) -> Self {
        Self {
            stage: self.stage.clone(),
            chunk_size: self.chunk_size,
            trigger: self.trigger,
            keying: self.keying.clone(),
            lineage: self.lineage,
            writer_id: self.writer_id,
        }
    }
}

impl<T, K> fmt::Debug for AiGrouping<T, K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiGrouping")
            .field("stage", &self.stage)
            .field("chunk_size", &self.chunk_size)
            .field("trigger", &self.trigger)
            .field("keyed", &matches!(self.keying, AiKeying::Keyed { .. }))
            .finish()
    }
}

#[derive(Clone)]
struct KeyGroups<T> {
    open: Vec<T>,
    trace: TraceState,
    first_item: u64,
    items_seen: u64,
    groups: u64,
    window_items: u64,
}

impl<T> KeyGroups<T> {
    fn new() -> Self {
        Self {
            open: Vec::new(),
            trace: TraceState::default(),
            first_item: 0,
            items_seen: 0,
            groups: 0,
            window_items: 0,
        }
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct AiGroupingState<T, K> {
    keys: BTreeMap<Option<K>, KeyGroups<T>>,
    pending: Vec<ChainEvent>,
    failure: Option<HandlerError>,
}

fn protocol_fatal(detail: impl Into<String>) -> HandlerError {
    HandlerError::Fatal(StageFatal::new(
        StageFatalCode::Protocol,
        StageFatalReason::ProtocolInputIntegrity,
        detail,
    ))
}

fn grouped_count(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

fn ordinal(value: u64) -> String {
    let suffix = match (value % 10, value % 100) {
        (1, rem) if rem != 11 => "st",
        (2, rem) if rem != 12 => "nd",
        (3, rem) if rem != 13 => "rd",
        _ => "th",
    };
    format!("{}{suffix}", grouped_count(value))
}

impl<T, K> AiGrouping<T, K>
where
    T: TypedPayload + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    K: Ord + Clone + Serialize + Send + Sync + 'static,
{
    fn window_open(&self, groups: &KeyGroups<T>) -> bool {
        match self.trigger {
            None => false,
            Some(ReduceTrigger::OnEof) => groups.groups > 0,
            Some(ReduceTrigger::EveryNChunks { chunks }) => groups.groups % chunks.get() != 0,
        }
    }

    fn key_open(&self, groups: &KeyGroups<T>) -> bool {
        !groups.open.is_empty() || self.window_open(groups)
    }

    fn key_value(key: &Option<K>) -> Result<Option<serde_json::Value>, HandlerError> {
        key.as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| protocol_fatal(format!("ai group key does not serialize: {error}")))
    }

    fn writer_id(&self) -> Result<WriterId, HandlerError> {
        self.writer_id.ok_or_else(|| {
            protocol_fatal("ai grouping member ran before its writer identity was installed")
        })
    }

    fn fact_event<P: TypedPayload>(
        &self,
        payload: P,
        trace: &TraceState,
    ) -> Result<ChainEvent, HandlerError> {
        let mut event = TypedFact::from_payload(payload)
            .map_err(|error| protocol_fatal(error.to_string()))?
            .into_event(self.writer_id()?);
        trace.apply_to_event(&mut event);
        Ok(event)
    }

    fn close(
        &self,
        key: &Option<K>,
        groups: &mut KeyGroups<T>,
        closed_by: GroupClosedBy,
    ) -> Result<ChainEvent, HandlerError> {
        let group = GroupOrdinal::new(groups.groups);
        let items = std::mem::take(&mut groups.open);
        let first_number = match self.trigger {
            None => 1,
            Some(trigger) => {
                if trigger.place(group).1 == 0 {
                    groups.window_items = 0;
                }
                let first_number = groups.window_items + 1;
                groups.window_items += items.len() as u64;
                first_number
            }
        };
        groups.groups += 1;
        let trace = std::mem::take(&mut groups.trace);
        self.fact_event(
            AiMapReduceMapInput {
                key: Self::key_value(key)?,
                group,
                first_item: groups.first_item,
                first_number,
                closed_by,
                items,
            },
            &trace,
        )
    }

    fn admit(
        &self,
        state: &mut AiGroupingState<T, K>,
        event: ChainEvent,
    ) -> Result<(), HandlerError> {
        let item = T::try_from_event(&event)
            .map_err(|error| protocol_fatal(format!("ai grouping input decode failed: {error}")))?;
        let key = match &self.keying {
            AiKeying::Unkeyed => None,
            AiKeying::Keyed { key, .. } => Some(key(&item)),
        };
        if let AiKeying::Keyed { max_open_keys, .. } = &self.keying {
            let already_open = state
                .keys
                .get(&key)
                .is_some_and(|groups| self.key_open(groups));
            if !already_open {
                let open = state
                    .keys
                    .values()
                    .filter(|groups| self.key_open(groups))
                    .count() as u64;
                if open >= max_open_keys.get() {
                    let shown = Self::key_value(&key)?
                        .map(|value| value.to_string())
                        .unwrap_or_default();
                    return Err(HandlerError::Fatal(StageFatal::new(
                        StageFatalCode::Resource,
                        StageFatalReason::ResourceExhaustion,
                        format!(
                            "stage `{stage}` failed: key {shown} would be its {nth} open key, over the limit of {limit}; \
                             each open key holds its items until its group or window closes; \
                             raise max_open_keys under [runtime.ai.stages.{stage}], or check that the key groups items as intended",
                            stage = self.stage,
                            nth = ordinal(open + 1),
                            limit = grouped_count(max_open_keys.get()),
                        ),
                    )));
                }
            }
        }
        let groups = state.keys.entry(key.clone()).or_insert_with(KeyGroups::new);
        if groups.open.is_empty() {
            groups.first_item = groups.items_seen;
        }
        groups.trace.record_event(&event, self.lineage);
        groups.open.push(item);
        groups.items_seen += 1;
        if groups.open.len() as u64 == self.chunk_size.get() {
            let fact = self.close(&key, groups, GroupClosedBy::ChunkSize)?;
            state.pending.push(fact);
        }
        Ok(())
    }

    fn finish(&self, state: &AiGroupingState<T, K>) -> Result<Vec<ChainEvent>, HandlerError> {
        let mut events = Vec::new();
        let mut unkeyed_seen = false;
        for (key, groups) in &state.keys {
            unkeyed_seen |= key.is_none();
            let mut groups = groups.clone();
            let end_trace = groups.trace.clone();
            if !groups.open.is_empty() {
                events.push(self.close(key, &mut groups, GroupClosedBy::EndOfInput)?);
            }
            if self.window_open(&groups) {
                events.push(self.fact_event(
                    AiMapReduceGroupingEnded {
                        key: Self::key_value(key)?,
                        group_count: groups.groups,
                    },
                    &end_trace,
                )?);
            }
        }
        if self.trigger.is_some() && !unkeyed_seen && matches!(self.keying, AiKeying::Unkeyed) {
            events.push(self.fact_event(
                AiMapReduceGroupingEnded {
                    key: None,
                    group_count: 0,
                },
                &TraceState::default(),
            )?);
        }
        Ok(events)
    }
}

#[async_trait]
impl<T, K> StatefulHandler for AiGrouping<T, K>
where
    T: TypedPayload + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    K: Ord + Clone + Serialize + Send + Sync + 'static,
{
    type State = AiGroupingState<T, K>;

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if state.failure.is_none() {
            state.failure = self.admit(state, event).err();
        }
    }

    fn try_accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
    ) -> Result<(), HandlerError> {
        if let Some(failure) = state.failure.as_ref() {
            return Err(failure.clone());
        }
        self.admit(state, event)
    }

    fn initial_state(&self) -> Self::State {
        AiGroupingState {
            keys: BTreeMap::new(),
            pending: Vec::new(),
            failure: None,
        }
    }

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        state.failure.is_some() || !state.pending.is_empty()
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(failure) = state.failure.take() {
            return Err(failure);
        }
        Ok(std::mem::take(&mut state.pending))
    }

    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(failure) = state.failure.as_ref() {
            return Err(failure.clone());
        }
        self.finish(state)
    }

    fn finalizes_on(&self, kind: StatefulTerminationKind) -> bool {
        matches!(kind, StatefulTerminationKind::NaturalEof)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_counts_read_as_operators_write_them() {
        assert_eq!(grouped_count(1_000), "1,000");
        assert_eq!(grouped_count(999), "999");
        assert_eq!(ordinal(1_001), "1,001st");
        assert_eq!(ordinal(12), "12th");
        assert_eq!(ordinal(22), "22nd");
        assert_eq!(ordinal(113), "113th");
    }
}
