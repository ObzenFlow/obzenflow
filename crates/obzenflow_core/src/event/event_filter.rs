
use crate::journal::writer_id::WriterId;
use super::event_envelope::EventEnvelope;
use super::event_id::EventId;

/// Filter for creating subscriptions - pure data filtering
#[derive(Clone, Debug)]
pub struct EventFilter {
    /// Filter by event type patterns (e.g., "user.*", "order.created")
    pub event_types: Vec<String>,

    /// Filter by writer IDs
    pub writer_ids: Vec<WriterId>,

    /// Start reading after this event ID (exclusive)
    /// If None, start from the beginning
    pub after_event_id: Option<EventId>,
}

impl EventFilter {
    /// Subscribe to all events
    pub fn all() -> Self {
        Self {
            event_types: vec![],
            writer_ids: vec![],
            after_event_id: None,
        }
    }

    /// Subscribe to specific event types
    pub fn by_event_types(types: Vec<String>) -> Self {
        Self {
            event_types: types,
            writer_ids: vec![],
            after_event_id: None,
        }
    }

    /// Check if an event matches this filter
    pub fn matches(&self, envelope: &EventEnvelope) -> bool {
        // If no filters specified, match all
        if self.event_types.is_empty() && self.writer_ids.is_empty() {
            return true;
        }

        // Check event type patterns
        if !self.event_types.is_empty() {
            let matches_type = self.event_types.iter().any(|pattern| {
                // Simple prefix matching for now
                envelope.event.event_type.starts_with(pattern)
            });
            if !matches_type {
                return false;
            }
        }

        // Check writer IDs
        if !self.writer_ids.is_empty() {
            if !self.writer_ids.contains(&envelope.writer_id) {
                return false;
            }
        }

        true
    }
}
