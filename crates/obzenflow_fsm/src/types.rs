//! Core types for the FSM library

use std::fmt::Debug;

/// Represents a state transition result
#[derive(Clone, Debug)]
pub struct Transition<S, A> {
    /// The next state to transition to
    pub next_state: S,
    /// Actions to perform as part of the transition
    pub actions: Vec<A>,
}

impl<S, A> Transition<S, A> {
    /// Create a transition to a new state with actions
    pub fn to(next_state: S) -> TransitionBuilder<S, A> {
        TransitionBuilder {
            next_state,
            actions: vec![],
        }
    }

    /// Create a transition that stays in the current state
    pub fn stay() -> StayBuilder<S, A> {
        StayBuilder {
            actions: vec![],
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Builder for transitions to a new state
pub struct TransitionBuilder<S, A> {
    next_state: S,
    actions: Vec<A>,
}

impl<S, A> TransitionBuilder<S, A> {
    /// Add actions to perform during the transition
    pub fn with_actions(mut self, actions: Vec<A>) -> Self {
        self.actions = actions;
        self
    }

    /// Add a single action
    pub fn with_action(mut self, action: A) -> Self {
        self.actions.push(action);
        self
    }

    /// Build the transition
    pub fn build(self) -> Transition<S, A> {
        Transition {
            next_state: self.next_state,
            actions: self.actions,
        }
    }
}

/// Builder for transitions that stay in the current state
pub struct StayBuilder<S, A> {
    actions: Vec<A>,
    _phantom: std::marker::PhantomData<S>,
}

impl<S: Clone, A> StayBuilder<S, A> {
    /// Add actions to perform
    pub fn with_actions(mut self, actions: Vec<A>) -> Self {
        self.actions = actions;
        self
    }

    /// Add a single action
    pub fn with_action(mut self, action: A) -> Self {
        self.actions.push(action);
        self
    }

    /// Build the transition (requires current state)
    pub fn in_state(self, current_state: S) -> Transition<S, A> {
        Transition {
            next_state: current_state,
            actions: self.actions,
        }
    }
}

/// Trait for state types that can report their variant name
pub trait StateVariant: Clone + Debug + PartialEq + Send + Sync + 'static {
    /// Get a string representation of the state variant (without data)
    /// For example, State::Running { count: 5 } would return "Running"
    fn variant_name(&self) -> &str;
}

/// Trait for event types that can report their variant name  
pub trait EventVariant: Clone + Debug + Send + Sync + 'static {
    /// Get a string representation of the event variant (without data)
    fn variant_name(&self) -> &str;
}

/// Trait for FSM context types that provide state-specific capabilities
pub trait FsmContext: Send + Sync + 'static {
    /// Get a description of this context for debugging
    fn describe(&self) -> String {
        "FSM Context".to_string()
    }
}

/// Trait for FSM action types that can be executed
#[async_trait::async_trait]
pub trait FsmAction: Clone + Debug + Send + Sync + 'static {
    /// The context type this action operates on
    type Context: FsmContext;
    
    /// Execute this action with the given context
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String>;
    
    /// Get a description of what this action does
    fn describe(&self) -> String {
        format!("{:?}", self)
    }
}

/// A boxed future that is Send
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Result type for FSM operations
pub type FsmResult<T, E = String> = Result<T, E>;