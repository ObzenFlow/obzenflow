//! System journal subscription for lifecycle and coordination events
//!
//! This module provides a subscription wrapper for system/error journals that
//! implements the SubscriptionPoller trait for consistent polling interface.

use super::subscription_poller::{PollResult, SubscriptionPoller};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, JournalEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::EventEnvelope;
use std::any::Any;

/// Wrapper for system/error journal readers
///
/// Provides consistent PollResult interface for system journal subscriptions,
/// ensuring FSM controls all sleep timing and preventing busy loops.
pub struct SystemSubscription<T>
where
    T: JournalEvent,
{
    reader: Box<dyn JournalReader<T>>,
    eof_received: bool,
    stage_name: String,
}

impl<T> SystemSubscription<T>
where
    T: JournalEvent + 'static,
{
    /// Create a new system subscription wrapper
    pub fn new(reader: Box<dyn JournalReader<T>>, stage_name: String) -> Self {
        Self {
            reader,
            eof_received: false,
            stage_name,
        }
    }

    /// Check if an event represents EOF (only ChainEvent EOF is treated as terminal)
    fn is_eof_event(&self, envelope: &EventEnvelope<T>) -> bool {
        // For ChainEvent, check for explicit EOF flow control
        if let Some(chain_event) = (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>() {
            return matches!(
                &chain_event.content,
                ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
            );
        }
        // System events are not considered terminal here; callers own termination policy.
        false
    }

    /// Get the stage name for debugging
    pub fn stage_name(&self) -> &str {
        &self.stage_name
    }

    /// Check if EOF has been received
    pub fn is_eof(&self) -> bool {
        self.eof_received
    }
}

/// Implement the SubscriptionPoller trait for SystemSubscription
#[async_trait::async_trait]
impl<T> SubscriptionPoller for SystemSubscription<T>
where
    T: JournalEvent + 'static,
{
    type Event = T;

    async fn poll_next(&mut self) -> PollResult<Self::Event> {
        if self.eof_received {
            return PollResult::NoEvents;
        }

        match self.reader.next().await {
            Ok(Some(envelope)) => {
                // Check for EOF in the event
                if self.is_eof_event(&envelope) {
                    self.eof_received = true;
                    tracing::info!("SystemSubscription[{}] received EOF event", self.stage_name);
                }

                tracing::trace!(
                    "SystemSubscription[{}] received event: {}",
                    self.stage_name,
                    envelope.event.id()
                );

                PollResult::Event(envelope)
            }
            Ok(None) => {
                // No events available right now
                tracing::trace!(
                    "SystemSubscription[{}] no events available",
                    self.stage_name
                );
                PollResult::NoEvents
            }
            Err(e) => {
                tracing::error!("SystemSubscription[{}] error: {}", self.stage_name, e);
                PollResult::Error(Box::new(e))
            }
        }
    }

    fn name(&self) -> &str {
        &self.stage_name
    }
}
