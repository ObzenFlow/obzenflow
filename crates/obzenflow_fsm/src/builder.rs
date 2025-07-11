//! Fluent builder API for creating FSMs (inspired by Akka Classic FSM)

use crate::handlers::{StateHandler, TimeoutHandler, TransitionHandler};
use crate::machine::StateMachine;
use crate::types::{StateVariant, EventVariant, FsmContext, FsmAction, Transition};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::Duration;

/// Main builder for creating FSMs
pub struct FsmBuilder<S, E, C, A> {
    initial_state: S,
    transitions: HashMap<(String, String), TransitionHandler<S, E, C, A>>,
    entry_handlers: HashMap<String, StateHandler<S, C, A>>,
    exit_handlers: HashMap<String, StateHandler<S, C, A>>,
    timeout_handlers: HashMap<String, (Duration, TimeoutHandler<S, C, A>)>,
    unhandled_handler: Option<
        Arc<
            dyn Fn(&S, &E, Arc<C>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
                + Send
                + Sync,
        >,
    >,
    _phantom: PhantomData<(E, C)>,
}

impl<S, E, C, A> FsmBuilder<S, E, C, A>
where
    S: StateVariant,
    E: EventVariant,
    C: FsmContext + 'static,
    A: FsmAction<Context = C> + 'static,
{
    /// Create a new FSM builder with an initial state
    pub fn new(initial_state: S) -> Self {
        Self {
            initial_state,
            transitions: HashMap::new(),
            entry_handlers: HashMap::new(),
            exit_handlers: HashMap::new(),
            timeout_handlers: HashMap::new(),
            unhandled_handler: None,
            _phantom: PhantomData,
        }
    }

    /// Start defining behavior for a specific state
    pub fn when(self, state_name: &str) -> WhenBuilder<S, E, C, A> {
        WhenBuilder {
            builder: self,
            state_name: state_name.to_string(),
        }
    }

    /// Define behavior for any state (wildcard)
    pub fn from_any(self) -> WhenBuilder<S, E, C, A> {
        WhenBuilder {
            builder: self,
            state_name: "_".to_string(),
        }
    }

    /// Define handler for unhandled events
    pub fn when_unhandled<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(&S, &E, Arc<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        self.unhandled_handler = Some(Arc::new(move |s, e, c| Box::pin(handler(s, e, c))));
        self
    }

    /// Define entry handler for a state
    pub fn on_entry<F, Fut>(mut self, state_name: &str, handler: F) -> Self
    where
        F: Fn(&S, Arc<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<A>, String>> + Send + 'static,
    {
        self.entry_handlers.insert(
            state_name.to_string(),
            Arc::new(move |s, c: Arc<C>| Box::pin(handler(s, c))),
        );
        self
    }

    /// Define exit handler for a state
    pub fn on_exit<F, Fut>(mut self, state_name: &str, handler: F) -> Self
    where
        F: Fn(&S, Arc<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<A>, String>> + Send + 'static,
    {
        self.exit_handlers.insert(
            state_name.to_string(),
            Arc::new(move |s, c: Arc<C>| Box::pin(handler(s, c))),
        );
        self
    }

    /// Build the final state machine
    pub fn build(self) -> StateMachine<S, E, C, A> {
        StateMachine::new(
            self.initial_state,
            self.transitions,
            self.entry_handlers,
            self.exit_handlers,
            self.timeout_handlers,
            self.unhandled_handler,
        )
    }
}

/// Builder for defining state-specific behavior
pub struct WhenBuilder<S, E, C, A> {
    builder: FsmBuilder<S, E, C, A>,
    state_name: String,
}

impl<S, E, C, A> WhenBuilder<S, E, C, A>
where
    S: StateVariant,
    E: EventVariant,
    C: FsmContext + 'static,
    A: FsmAction<Context = C> + 'static,
{
    /// Define a transition for a specific event
    pub fn on<F, Fut>(mut self, event_name: &str, handler: F) -> Self
    where
        F: Fn(&S, &E, Arc<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Transition<S, A>, String>> + Send + 'static,
    {
        self.builder.transitions.insert(
            (self.state_name.clone(), event_name.to_string()),
            Arc::new(move |s, e, c: Arc<C>| Box::pin(handler(s, e, c))),
        );
        self
    }

    /// Define a timeout for this state
    pub fn timeout<F, Fut>(
        self,
        duration: Duration,
        handler: F,
    ) -> TimeoutBuilder<S, E, C, A>
    where
        F: Fn(&S, Arc<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Transition<S, A>, String>> + Send + 'static,
    {
        let mut builder = self.builder;
        builder.timeout_handlers.insert(
            self.state_name.clone(),
            (duration, Arc::new(move |s, c: Arc<C>| Box::pin(handler(s, c)))),
        );
        TimeoutBuilder {
            builder,
            state_name: self.state_name,
        }
    }

    /// Continue with the builder (ends this state's configuration)
    pub fn done(self) -> FsmBuilder<S, E, C, A> {
        self.builder
    }
}

/// Builder for states with timeouts
pub struct TimeoutBuilder<S, E, C, A> {
    builder: FsmBuilder<S, E, C, A>,
    state_name: String,
}

impl<S, E, C, A> TimeoutBuilder<S, E, C, A>
where
    S: StateVariant,
    E: EventVariant,
    C: FsmContext + 'static,
    A: FsmAction<Context = C> + 'static,
{
    /// Define a transition for a specific event
    pub fn on<F, Fut>(mut self, event_name: &str, handler: F) -> Self
    where
        F: Fn(&S, &E, Arc<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Transition<S, A>, String>> + Send + 'static,
    {
        self.builder.transitions.insert(
            (self.state_name.clone(), event_name.to_string()),
            Arc::new(move |s, e, c: Arc<C>| Box::pin(handler(s, e, c))),
        );
        self
    }

    /// Return to the main builder
    pub fn done(self) -> FsmBuilder<S, E, C, A> {
        self.builder
    }
}

/// Convenience functions for creating transitions
pub mod transitions {
    use crate::Transition;

    /// Transition to a new state
    pub fn goto<S, A>(state: S) -> Transition<S, A> {
        Transition {
            next_state: state,
            actions: vec![],
        }
    }

    /// Transition to a new state with actions
    pub fn goto_with_actions<S, A>(state: S, actions: Vec<A>) -> Transition<S, A> {
        Transition {
            next_state: state,
            actions,
        }
    }

    /// Stay in the current state
    pub fn stay<S: Clone, A>(current_state: &S) -> Transition<S, A> {
        Transition {
            next_state: current_state.clone(),
            actions: vec![],
        }
    }

    /// Stay in the current state with actions
    pub fn stay_with_actions<S: Clone, A>(current_state: &S, actions: Vec<A>) -> Transition<S, A> {
        Transition {
            next_state: current_state.clone(),
            actions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    enum DoorState {
        Closed,
        Open { since: std::time::Instant },
        Locked,
    }

    impl StateVariant for DoorState {
        fn variant_name(&self) -> &str {
            match self {
                DoorState::Closed => "Closed",
                DoorState::Open { .. } => "Open",
                DoorState::Locked => "Locked",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum DoorEvent {
        Open,
        Close,
        Lock,
        Unlock,
    }

    impl EventVariant for DoorEvent {
        fn variant_name(&self) -> &str {
            match self {
                DoorEvent::Open => "Open",
                DoorEvent::Close => "Close",
                DoorEvent::Lock => "Lock",
                DoorEvent::Unlock => "Unlock",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum DoorAction {
        Ring,
        Log(String),
    }

    struct DoorContext;
    
    impl crate::FsmContext for DoorContext {
        fn describe(&self) -> String {
            "Door FSM context".to_string()
        }
    }
    
    #[async_trait::async_trait]
    impl crate::FsmAction for DoorAction {
        type Context = DoorContext;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            match self {
                DoorAction::Ring => {
                    println!("Ring!");
                    Ok(())
                }
                DoorAction::Log(msg) => {
                    println!("Log: {}", msg);
                    Ok(())
                }
            }
        }
    }

    #[tokio::test]
    async fn test_door_fsm() {
        let fsm = FsmBuilder::<DoorState, DoorEvent, DoorContext, DoorAction>::new(DoorState::Closed)
            .when("Closed")
                .on("Open", |_state: &DoorState, _event: &DoorEvent, _ctx: Arc<DoorContext>| async {
                    Ok(Transition {
                        next_state: DoorState::Open {
                            since: std::time::Instant::now(),
                        },
                        actions: vec![DoorAction::Ring, DoorAction::Log("Door opened".into())],
                    })
                })
                .done()
            .when("Closed")
                .on("Lock", |_state: &DoorState, _event: &DoorEvent, _ctx: Arc<DoorContext>| async {
                    Ok(Transition {
                        next_state: DoorState::Locked,
                        actions: vec![DoorAction::Log("Door locked".into())],
                    })
                })
                .done()
            .when("Open")
                .timeout(Duration::from_secs(5), |_state: &DoorState, _ctx: Arc<DoorContext>| async {
                    Ok(Transition {
                        next_state: DoorState::Closed,
                        actions: vec![DoorAction::Log("Door auto-closed".into())],
                    })
                })
                .on("Close", |_state: &DoorState, _event: &DoorEvent, _ctx: Arc<DoorContext>| async {
                    Ok(Transition {
                        next_state: DoorState::Closed,
                        actions: vec![DoorAction::Log("Door closed".into())],
                    })
                })
                .done()
            .when("Locked")
                .on("Unlock", |_state: &DoorState, _event: &DoorEvent, _ctx: Arc<DoorContext>| async {
                    Ok(Transition {
                        next_state: DoorState::Closed,
                        actions: vec![DoorAction::Log("Door unlocked".into())],
                    })
                })
                .done()
            .on_entry("Open", |_state: &DoorState, _ctx: Arc<DoorContext>| async {
                Ok(vec![DoorAction::Log("Entering Open state".into())])
            })
            .when_unhandled(|state: &DoorState, event: &DoorEvent, _ctx: Arc<DoorContext>| {
                let state = state.clone();
                let event = event.clone();
                async move {
                    tracing::warn!("Unhandled event {:?} in state {:?}", event, state);
                    Ok(())
                }
            })
            .build();

        let mut door = fsm;
        let ctx = Arc::new(DoorContext);

        // Test basic transition
        assert!(matches!(door.state(), DoorState::Closed));

        let actions = door.handle(DoorEvent::Open, ctx.clone()).await.unwrap();
        // Now we should get: entry handler action + transition actions
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], DoorAction::Log(_))); // Entry handler
        assert!(matches!(actions[1], DoorAction::Ring));   // Transition action
        assert!(matches!(actions[2], DoorAction::Log(_))); // Transition action
        assert!(matches!(door.state(), DoorState::Open { .. }));

        // Test another transition - this should work
        let actions = door.handle(DoorEvent::Close, ctx).await.unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], DoorAction::Log(_)));
        assert!(matches!(door.state(), DoorState::Closed));
    }
}
