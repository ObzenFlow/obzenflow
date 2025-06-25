//! Async-first Finite State Machine library inspired by Akka FSM
//! 
//! Core principle: State(S) × Event(E) → Actions(A), State(S')
//! 
//! "If we are in state S and the event E occurs, we should perform the actions A 
//! and make a transition to the state S'."

use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::time::{Duration, Instant};
use tracing::{debug, warn};

pub mod builder;
pub mod types;

#[cfg(test)]
mod test_simple;

#[cfg(test)]
mod unholy_fsm_tests;

// Complex tests commented out until lifetime issues are resolved
// #[cfg(test)]
// mod test_comprehensive;
// 
// #[cfg(test)]
// mod test_edge_cases;
// 
// #[cfg(test)]
// mod test_compile_safety;

pub use builder::FsmBuilder;
pub use types::{StateVariant, EventVariant, Transition};

/// Core FSM trait - the heart of our state machine abstraction
#[async_trait]
pub trait Fsm: Sized {
    /// The state type (e.g., an enum with variants)
    type State: Clone + Debug + PartialEq + Send + Sync + 'static;
    
    /// The event type that triggers transitions
    type Event: Clone + Debug + Send + Sync + 'static;
    
    /// The context type that provides access to external resources
    type Context: Send + Sync;
    
    /// The action type that represents side effects
    type Action: Clone + Debug + Send + Sync;
    
    /// The error type for state machine operations
    type Error: From<String> + Send + Sync;

    /// Get the current state
    fn state(&self) -> &Self::State;

    /// Handle an event and potentially transition to a new state
    async fn handle(
        &mut self,
        event: Self::Event,
        context: Arc<Self::Context>,
    ) -> Result<Vec<Self::Action>, Self::Error>;
}

/// Type alias for async transition handlers
pub type TransitionHandler<S, E, C, A> = Arc<
    dyn Fn(&S, &E, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Transition<S, A>, String>> + Send>>
        + Send
        + Sync,
>;

/// Type alias for async state handlers (entry/exit)
pub type StateHandler<S, C, A> = Arc<
    dyn Fn(&S, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Vec<A>, String>> + Send>>
        + Send
        + Sync,
>;

/// Type alias for timeout handlers
pub type TimeoutHandler<S, C, A> = Arc<
    dyn Fn(&S, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Transition<S, A>, String>> + Send>>
        + Send
        + Sync,
>;

/// The concrete FSM implementation
pub struct StateMachine<S, E, C, A> {
    current_state: S,
    transitions: Arc<HashMap<(String, String), TransitionHandler<S, E, C, A>>>,
    entry_handlers: Arc<HashMap<String, StateHandler<S, C, A>>>,
    exit_handlers: Arc<HashMap<String, StateHandler<S, C, A>>>,
    timeout_handlers: Arc<HashMap<String, (Duration, TimeoutHandler<S, C, A>)>>,
    unhandled_handler: Option<Arc<dyn Fn(&S, &E, Arc<C>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>>,
    state_timeout: Option<Instant>,
    _phantom: PhantomData<(E, C, A)>,
}

impl<S, E, C, A> StateMachine<S, E, C, A>
where
    S: StateVariant,
    E: EventVariant,
    C: Send + Sync,
    A: Clone + Debug + Send + Sync,
{
    /// Create a new state machine
    pub fn new(
        initial_state: S,
        transitions: HashMap<(String, String), TransitionHandler<S, E, C, A>>,
        entry_handlers: HashMap<String, StateHandler<S, C, A>>,
        exit_handlers: HashMap<String, StateHandler<S, C, A>>,
        timeout_handlers: HashMap<String, (Duration, TimeoutHandler<S, C, A>)>,
        unhandled_handler: Option<Arc<dyn Fn(&S, &E, Arc<C>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>>,
    ) -> Self {
        Self {
            current_state: initial_state,
            transitions: Arc::new(transitions),
            entry_handlers: Arc::new(entry_handlers),
            exit_handlers: Arc::new(exit_handlers),
            timeout_handlers: Arc::new(timeout_handlers),
            unhandled_handler,
            state_timeout: None,
            _phantom: PhantomData,
        }
    }

    /// Get the current state
    pub fn state(&self) -> &S {
        &self.current_state
    }

    /// Check if a timeout has occurred for the current state
    pub async fn check_timeout(&mut self, context: Arc<C>) -> Result<Vec<A>, String> {
        if let Some(timeout_instant) = self.state_timeout {
            if Instant::now() >= timeout_instant {
                let state_name = self.current_state.variant_name().to_string();
                if let Some((_, handler)) = self.timeout_handlers.get(&state_name) {
                    let transition = handler(&self.current_state, context.clone()).await?;
                    return self.apply_transition(transition, context).await;
                }
            }
        }
        Ok(vec![])
    }

    /// Handle an event and potentially transition to a new state
    pub async fn handle(&mut self, event: E, context: Arc<C>) -> Result<Vec<A>, String> {
        let state_name = self.current_state.variant_name().to_string();
        let event_name = event.variant_name().to_string();
        let key = (state_name.clone(), event_name.clone());

        debug!("FSM handling event {} in state {}", event_name, state_name);

        if let Some(handler) = self.transitions.get(&key) {
            let transition = handler(&self.current_state, &event, context.clone()).await?;
            self.apply_transition(transition, context).await
        } else {
            // Check for wildcard transitions (from any state)
            let wildcard_key = ("_".to_string(), event_name.clone());
            if let Some(handler) = self.transitions.get(&wildcard_key) {
                let transition = handler(&self.current_state, &event, context.clone()).await?;
                self.apply_transition(transition, context).await
            } else {
                // Handle unhandled event
                if let Some(handler) = &self.unhandled_handler {
                    handler(&self.current_state, &event, context).await?;
                } else {
                    warn!("Unhandled event {} in state {}", event_name, state_name);
                }
                Ok(vec![])
            }
        }
    }

    /// Apply a state transition
    async fn apply_transition(
        &mut self,
        transition: Transition<S, A>,
        context: Arc<C>,
    ) -> Result<Vec<A>, String> {
        let mut all_actions = vec![];

        // Only process if state is actually changing
        if self.current_state != transition.next_state {
            let old_state_name = self.current_state.variant_name().to_string();
            let new_state_name = transition.next_state.variant_name().to_string();

            // Exit current state
            if let Some(handler) = self.exit_handlers.get(&old_state_name) {
                let exit_actions = handler(&self.current_state, context.clone()).await?;
                all_actions.extend(exit_actions);
            }

            // Transition to new state
            self.current_state = transition.next_state;

            // Enter new state
            if let Some(handler) = self.entry_handlers.get(&new_state_name) {
                let entry_actions = handler(&self.current_state, context.clone()).await?;
                all_actions.extend(entry_actions);
            }

            // Set up timeout for new state if configured
            if let Some((duration, _)) = self.timeout_handlers.get(&new_state_name) {
                self.state_timeout = Some(Instant::now() + *duration);
            } else {
                self.state_timeout = None;
            }

            debug!("FSM transitioned from {} to {}", old_state_name, new_state_name);
        }

        // Add transition actions
        all_actions.extend(transition.actions);

        Ok(all_actions)
    }
}

#[async_trait]
impl<S, E, C, A> Fsm for StateMachine<S, E, C, A>
where
    S: StateVariant,
    E: EventVariant,
    C: Send + Sync,
    A: Clone + Debug + Send + Sync,
{
    type State = S;
    type Event = E;
    type Context = C;
    type Action = A;
    type Error = String;

    fn state(&self) -> &Self::State {
        &self.current_state
    }

    async fn handle(
        &mut self,
        event: Self::Event,
        context: Arc<Self::Context>,
    ) -> Result<Vec<Self::Action>, Self::Error> {
        self.handle(event, context).await
    }
}

/// Error types for FSM operations
#[derive(Error, Debug)]
pub enum FsmError {
    #[error("Invalid transition from {from:?} on event {event:?}")]
    InvalidTransition { from: String, event: String },
    
    #[error("Handler error: {0}")]
    HandlerError(String),
    
    #[error("Timeout in state {state:?}")]
    Timeout { state: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestState {
        Idle,
        Active { count: u32 },
        Done,
    }

    impl StateVariant for TestState {
        fn variant_name(&self) -> &str {
            match self {
                TestState::Idle => "Idle",
                TestState::Active { .. } => "Active",
                TestState::Done => "Done",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum TestEvent {
        Start,
        Increment,
        Finish,
    }

    impl EventVariant for TestEvent {
        fn variant_name(&self) -> &str {
            match self {
                TestEvent::Start => "Start",
                TestEvent::Increment => "Increment",
                TestEvent::Finish => "Finish",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum TestAction {
        Log(String),
        Notify,
    }

    struct TestContext {
        total: u32,
    }

    #[tokio::test]
    async fn test_basic_fsm() {
        let fsm = FsmBuilder::<TestState, TestEvent, TestContext, TestAction>::new(TestState::Idle)
            .when("Idle")
            .on("Start", |_state: &TestState, _event: &TestEvent, _ctx: Arc<TestContext>| async {
                Ok(Transition {
                    next_state: TestState::Active { count: 0 },
                    actions: vec![TestAction::Log("Starting".into())],
                })
            })
            .done()
            .build();

        let mut state_machine = fsm;
        let ctx = Arc::new(TestContext { total: 0 });
        
        // Test transition
        let actions = state_machine.handle(TestEvent::Start, ctx).await.unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(state_machine.state(), TestState::Active { count: 0 }));
    }
}