//! Async-first Finite State Machine library inspired by Akka FSM
//! 
//! Core principle: State(S) × Event(E) → Actions(A), State(S')
//! 
//! "If we are in state S and the event E occurs, we should perform the actions A 
//! and make a transition to the state S'."

// Module declarations
pub mod builder;
pub mod error;
pub mod handlers;
pub mod machine;
pub mod types;

// Re-export main types for convenience
pub use builder::FsmBuilder;
pub use error::FsmError;
pub use machine::StateMachine;
pub use types::{StateVariant, EventVariant, FsmContext, FsmAction, Transition};

// Re-export handler types for advanced usage
pub use handlers::{TransitionHandler, StateHandler, TimeoutHandler};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use async_trait::async_trait;

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
    
    impl FsmContext for TestContext {
        fn describe(&self) -> String {
            format!("Test context with total: {}", self.total)
        }
    }
    
    #[async_trait]
    impl FsmAction for TestAction {
        type Context = TestContext;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            match self {
                TestAction::Log(msg) => {
                    println!("Log: {}", msg);
                    Ok(())
                }
                TestAction::Notify => {
                    println!("Notify");
                    Ok(())
                }
            }
        }
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