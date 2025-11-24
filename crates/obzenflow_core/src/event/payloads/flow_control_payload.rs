//! Flow signal payloads (EOF, watermark, checkpoint, drain, contracts)

use crate::event::types::{
    Count, DurationMs, JournalIndex, JournalPath, RouteKey, SeqNo, ViolationCause,
};
use crate::event::vector_clock::VectorClock;
use crate::StageId;
use crate::WriterId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "flow_control_type", rename_all = "snake_case")]
pub enum FlowControlPayload {
    /// End of data stream
    #[serde(rename = "eof")]
    Eof {
        natural: bool,
        #[serde(default = "current_timestamp")]
        timestamp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        writer_id: Option<WriterId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        writer_seq: Option<SeqNo>,
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
        stage_id: Option<String>,
    },

    /// Checkpoint for fault tolerance
    #[serde(rename = "checkpoint")]
    Checkpoint {
        id: String,
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

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
