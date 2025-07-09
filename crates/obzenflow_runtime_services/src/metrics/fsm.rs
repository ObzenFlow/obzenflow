//! FSM implementation for metrics aggregator
//!
//! Implements lifecycle state machine using obzenflow_fsm::StateMachine
//! The FSM only tracks lifecycle states - event processing happens directly

use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant, EventVariant};

use super::states::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState,
};

/// Type alias for the metrics FSM
pub type MetricsAggregatorFsm = StateMachine<
    MetricsAggregatorState,
    MetricsAggregatorEvent,
    MetricsAggregatorContext,
    MetricsAggregatorAction,
>;

/// Build the metrics aggregator FSM with lifecycle transitions only
pub fn build_metrics_aggregator_fsm() -> MetricsAggregatorFsm {
    FsmBuilder::new(MetricsAggregatorState::Initializing)
        // Initializing state transitions
        .when("Initializing")
        .on("StartRunning", |_state, _event, _ctx| async move {
            Ok(Transition {
                next_state: MetricsAggregatorState::Running,
                actions: vec![MetricsAggregatorAction::Initialize],
            })
        })
        .done()
        // Running state transitions
        .when("Running")
        .on("StartDraining", |_state, _event, _ctx| async move {
            Ok(Transition {
                next_state: MetricsAggregatorState::Draining,
                actions: vec![],
            })
        })
        .done()
        // Draining state transitions
        .when("Draining")
        .on("DrainComplete", |_state, event, _ctx| {
            let result = match event {
                MetricsAggregatorEvent::DrainComplete { last_event_id } => {
                    let last_event_id = last_event_id.clone();
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Drained {
                            last_event_id: last_event_id.clone(),
                        },
                        actions: vec![MetricsAggregatorAction::PublishDrainComplete {
                            last_event_id,
                        }],
                    })
                }
                _ => Err("Invalid event for DrainComplete handler".to_string())
            };
            async move { result }
        })
        .done()
        // Drained state (terminal) - no transitions
        .when("Drained")
        .done()
        // Handle unhandled events
        .when_unhandled(|state, event, _ctx| {
            let state_name = state.variant_name().to_string();
            let event_name = event.variant_name().to_string();
            async move {
                tracing::warn!("Unhandled event {} in state {}", event_name, state_name);
                Ok(())
            }
        })
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_flow::reactive_journal::ReactiveJournal;
    use obzenflow_core::EventId;

    #[tokio::test]
    async fn test_initializing_to_running_transition() {
        let journal = Arc::new(ReactiveJournal::new(Arc::new(Default::default())));
        let context = Arc::new(MetricsAggregatorContext::new(journal, None, 60));
        let mut fsm = build_metrics_aggregator_fsm();

        // Start in Initializing state
        assert!(matches!(fsm.state(), MetricsAggregatorState::Initializing));

        // Send StartRunning event
        let actions = fsm
            .handle(MetricsAggregatorEvent::StartRunning, context)
            .await
            .unwrap();

        // Should transition to Running
        assert!(matches!(fsm.state(), MetricsAggregatorState::Running));
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], MetricsAggregatorAction::Initialize));
    }

    #[tokio::test]
    async fn test_running_to_draining_transition() {
        let journal = Arc::new(ReactiveJournal::new(Arc::new(Default::default())));
        let context = Arc::new(MetricsAggregatorContext::new(journal, None, 60));
        let mut fsm = build_metrics_aggregator_fsm();

        // Move to Running state first
        let _ = fsm.handle(MetricsAggregatorEvent::StartRunning, context.clone()).await;

        // Send StartDraining event
        let actions = fsm
            .handle(MetricsAggregatorEvent::StartDraining, context)
            .await
            .unwrap();

        // Should transition to Draining
        assert!(matches!(fsm.state(), MetricsAggregatorState::Draining));
        assert_eq!(actions.len(), 0);
    }

    #[tokio::test]
    async fn test_draining_to_drained_transition() {
        let journal = Arc::new(ReactiveJournal::new(Arc::new(Default::default())));
        let context = Arc::new(MetricsAggregatorContext::new(journal, None, 60));
        let mut fsm = build_metrics_aggregator_fsm();

        // Move to Running then Draining
        let _ = fsm.handle(MetricsAggregatorEvent::StartRunning, context.clone()).await;
        let _ = fsm.handle(MetricsAggregatorEvent::StartDraining, context.clone()).await;

        // Send DrainComplete event
        let last_id = EventId::new();
        let actions = fsm
            .handle(
                MetricsAggregatorEvent::DrainComplete { 
                    last_event_id: Some(last_id.clone()) 
                },
                context,
            )
            .await
            .unwrap();

        // Should transition to Drained
        assert!(matches!(
            fsm.state(),
            MetricsAggregatorState::Drained { last_event_id: Some(_) }
        ));
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0], 
            MetricsAggregatorAction::PublishDrainComplete { last_event_id: Some(_) }
        ));
    }
}

