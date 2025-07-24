//! Flow signal payloads (EOF, watermark, checkpoint, drain)

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
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}