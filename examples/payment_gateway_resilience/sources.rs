use super::domain::PaymentCommand;
use super::fixtures;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_runtime_services::stages::common::handlers::FiniteSourceHandler;
use serde_json::json;

/// Finite source that replays a scripted sequence of payment commands.
///
/// This models an upstream order system sending us payment work. The
/// source is intentionally small and deterministic so you can line up
/// logs, journal events, and Prometheus metrics.
#[derive(Clone, Debug)]
pub struct PaymentCommandSource {
    commands: Vec<PaymentCommand>,
    current_index: usize,
    writer_id: WriterId,
}

impl PaymentCommandSource {
    pub fn new() -> Self {
        Self {
            commands: fixtures::scripted_commands(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for PaymentCommandSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError> {
        if self.current_index >= self.commands.len() {
            return Ok(None);
        }

        let cmd = &self.commands[self.current_index];
        self.current_index += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            PaymentCommand::EVENT_TYPE,
            json!(cmd),
        )]))
    }
}
