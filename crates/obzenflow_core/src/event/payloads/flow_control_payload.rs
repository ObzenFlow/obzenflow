// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow signal payloads (EOF, watermark, checkpoint, drain, contracts)

use std::fmt;

use crate::event::types::{
    Count, DurationMs, EventType, JournalIndex, JournalPath, RouteKey, SeqNo, ViolationCause,
};
use crate::event::vector_clock::VectorClock;
use crate::StageId;
use crate::WriterId;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Whether an EOF represents normal source exhaustion or a poison/forced close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EofKind {
    Natural,
    Poison,
}

impl EofKind {
    pub const fn from_natural(natural: bool) -> Self {
        if natural {
            Self::Natural
        } else {
            Self::Poison
        }
    }

    pub const fn is_natural(self) -> bool {
        matches!(self, Self::Natural)
    }

    pub const fn is_poison(self) -> bool {
        matches!(self, Self::Poison)
    }
}

impl<'de> Deserialize<'de> for EofKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EofKindVisitor;

        impl Visitor<'_> for EofKindVisitor {
            type Value = EofKind;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("`natural`, `poison`, or legacy EOF natural boolean")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(EofKind::from_natural(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "natural" => Ok(EofKind::Natural),
                    "poison" => Ok(EofKind::Poison),
                    other => Err(E::unknown_variant(other, &["natural", "poison"])),
                }
            }
        }

        deserializer.deserialize_any(EofKindVisitor)
    }
}

/// Source identifier attached to a watermark. This is a render/control label,
/// not the canonical `StageId` newtype.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WatermarkStageId(String);

impl WatermarkStageId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for WatermarkStageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for WatermarkStageId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for WatermarkStageId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Checkpoint identity carried by flow-control checkpoint signals.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointId(String);

impl CheckpointId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for CheckpointId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for CheckpointId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "flow_control_type", rename_all = "snake_case")]
pub enum FlowControlPayload {
    /// End of data stream
    #[serde(rename = "eof")]
    Eof {
        #[serde(alias = "natural")]
        kind: EofKind,
        #[serde(default = "current_timestamp")]
        timestamp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        writer_id: Option<WriterId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        writer_seq: Option<SeqNo>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        writer_seq_by_event_type: BTreeMap<EventType, SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        vector_clock: Option<VectorClock>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_event_id: Option<crate::event::types::EventId>,
    },

    /// Watermark for event-time processing
    #[serde(rename = "watermark")]
    Watermark {
        timestamp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_id: Option<WatermarkStageId>,
    },

    /// Checkpoint for fault tolerance
    #[serde(rename = "checkpoint")]
    Checkpoint {
        id: CheckpointId,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },

    /// Drain request
    #[serde(rename = "drain")]
    Drain,

    /// Pipeline abort signal (fatal gating failure)
    #[serde(rename = "pipeline_abort")]
    PipelineAbort {
        reason: ViolationCause,
        #[serde(skip_serializing_if = "Option::is_none")]
        upstream: Option<StageId>,
    },

    /// Source declares expected production contract
    #[serde(rename = "source_contract")]
    SourceContract {
        expected_count: Option<Count>,
        source_id: StageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        route: Option<RouteKey>,
        journal_path: JournalPath,
        journal_index: JournalIndex,
        #[serde(skip_serializing_if = "Option::is_none")]
        writer_seq: Option<SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        vector_clock: Option<VectorClock>,
    },

    /// Subscriber progress update
    #[serde(rename = "consumption_progress")]
    ConsumptionProgress {
        reader_seq: SeqNo,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_event_id: Option<crate::event::types::EventId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        vector_clock: Option<VectorClock>,
        eof_seen: bool,
        reader_path: JournalPath,
        reader_index: JournalIndex,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_vector_clock: Option<VectorClock>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stalled_since: Option<DurationMs>,
    },

    /// Subscriber detected a gap in upstream sequence
    #[serde(rename = "consumption_gap")]
    ConsumptionGap {
        from_seq: SeqNo,
        to_seq: SeqNo,
        upstream: StageId,
    },

    /// Subscriber finalized consumption for an upstream
    #[serde(rename = "consumption_final")]
    ConsumptionFinal {
        pass: bool,
        consumed_count: Count,
        #[serde(skip_serializing_if = "Option::is_none")]
        expected_count: Option<Count>,
        eof_seen: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_event_id: Option<crate::event::types::EventId>,
        reader_seq: SeqNo,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_vector_clock: Option<VectorClock>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_reason: Option<ViolationCause>,
    },

    /// Subscriber stalled beyond threshold
    #[serde(rename = "reader_stalled")]
    ReaderStalled {
        upstream: StageId,
        stalled_since: DurationMs,
    },

    /// Subscriber detected an at-least-once violation
    #[serde(rename = "at_least_once_violation")]
    AtLeastOnceViolation {
        upstream: StageId,
        reason: ViolationCause,
        reader_seq: SeqNo,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<SeqNo>,
    },
}

impl FlowControlPayload {
    pub fn eof_kind(&self) -> Option<EofKind> {
        match self {
            Self::Eof { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    pub fn is_natural_eof(&self) -> bool {
        self.eof_kind().is_some_and(EofKind::is_natural)
    }

    /// FLOWIP-095d: whether this row is reader telemetry rather than
    /// behavioural flow control.
    ///
    /// The stage journal carries two lanes. Behavioural flow control (EOF,
    /// drain, watermarks, checkpoints, pipeline abort, source contracts)
    /// participates in transport order: it is delivered to downstream
    /// subscriptions and takes per-reader ordinals in the canonical
    /// deterministic merge. Reader telemetry (consumption progress, gaps,
    /// finals, stalls, at-least-once violations) is consumption observability
    /// emitted at wall-clock-gated times, so its journal positions are not a
    /// function of stream content; it is excluded from `TransportOnly`
    /// delivery so per-reader ordinals stay a pure function of the per-input
    /// streams across live, replay, and resume.
    ///
    /// `SourceContract` is behavioural: it is emitted exactly once,
    /// deterministically, at source startup and is consumed by downstream
    /// contract chains.
    ///
    /// Deliberately an exhaustive match with no wildcard arm: adding a new
    /// variant must force a lane decision here.
    pub fn is_reader_telemetry(&self) -> bool {
        match self {
            Self::ConsumptionProgress { .. }
            | Self::ConsumptionGap { .. }
            | Self::ConsumptionFinal { .. }
            | Self::ReaderStalled { .. }
            | Self::AtLeastOnceViolation { .. } => true,
            Self::Eof { .. }
            | Self::Watermark { .. }
            | Self::Checkpoint { .. }
            | Self::Drain
            | Self::PipelineAbort { .. }
            | Self::SourceContract { .. } => false,
        }
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
