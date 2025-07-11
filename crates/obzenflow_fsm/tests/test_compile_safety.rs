//! Tests demonstrating compile-time safety of the FSM
//! 
//! Many of these tests are commented out because they SHOULD NOT compile.
//! They demonstrate the type safety of our FSM implementation.

use obzenflow_fsm::{FsmBuilder, StateVariant, EventVariant, FsmContext, FsmAction, Transition};
use std::fmt::Debug;
use std::sync::Arc;

// Valid state and event types for reference
#[derive(Clone, Debug, PartialEq)]
enum ValidState {
    A,
    B,
}

impl StateVariant for ValidState {
    fn variant_name(&self) -> &str {
        match self {
            ValidState::A => "A",
            ValidState::B => "B",
        }
    }
}

#[derive(Clone, Debug)]
enum ValidEvent {
    Go,
}

impl EventVariant for ValidEvent {
    fn variant_name(&self) -> &str {
        "Go"
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ValidAction;

struct ValidContext;

impl FsmContext for ValidContext {}

#[async_trait::async_trait]
impl FsmAction for ValidAction {
    type Context = ValidContext;
    
    async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
        Ok(())
    }
}

/// This SHOULD compile - it's our baseline
#[test]
fn test_valid_fsm_compiles() {
    let _ = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &ValidState, _event: &ValidEvent, _ctx: Arc<ValidContext>| async {
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![ValidAction],
            })
        })
        .done()
        .build();
}

// The following tests demonstrate what SHOULD NOT compile

/*
/// Test 1: Using a type that doesn't implement StateVariant
#[test]
fn test_invalid_state_type() {
    // This should not compile because String doesn't implement StateVariant
    let _ = FsmBuilder::<String, ValidEvent, ValidContext, ValidAction>::new(
        "invalid".to_string()
    );
}
*/

/*
/// Test 2: Using a type that doesn't implement EventVariant
#[test]
fn test_invalid_event_type() {
    // This should not compile because i32 doesn't implement EventVariant
    let _ = FsmBuilder::<ValidState, i32, ValidContext, ValidAction>::new(ValidState::A)
        .when("A")
        .on("42", |_state, _event: &i32, _ctx| async {
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![],
            })
        })
        .build();
}
*/

/*
/// Test 3: Wrong state type in handler
#[test]
fn test_wrong_state_type_in_handler() {
    #[derive(Clone, Debug, PartialEq)]
    enum OtherState {
        X,
        Y,
    }
    
    impl StateVariant for OtherState {
        fn variant_name(&self) -> &str {
            match self {
                OtherState::X => "X",
                OtherState::Y => "Y",
            }
        }
    }
    
    // This should not compile - handler expects ValidState but gets OtherState
    let _ = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &OtherState, _event: &ValidEvent, _ctx: Arc<ValidContext>| async {
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![],
            })
        })
        .build();
}
*/

/*
/// Test 4: Wrong event type in handler
#[test]
fn test_wrong_event_type_in_handler() {
    #[derive(Clone, Debug)]
    enum OtherEvent {
        Stop,
    }
    
    impl EventVariant for OtherEvent {
        fn variant_name(&self) -> &str {
            "Stop"
        }
    }
    
    // This should not compile - handler expects ValidEvent but gets OtherEvent
    let _ = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &ValidState, _event: &OtherEvent, _ctx: Arc<ValidContext>| async {
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![],
            })
        })
        .build();
}
*/

/*
/// Test 5: Wrong context type in handler
#[test]
fn test_wrong_context_type_in_handler() {
    struct OtherContext {
        data: String,
    }
    
    // This should not compile - handler expects ValidContext but gets OtherContext
    let _ = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &ValidState, _event: &ValidEvent, _ctx: Arc<OtherContext>| async {
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![],
            })
        })
        .build();
}
*/

/*
/// Test 6: Returning wrong state type from transition
#[test]
fn test_wrong_return_state_type() {
    #[derive(Clone, Debug, PartialEq)]
    enum WrongState {
        Z,
    }
    
    impl StateVariant for WrongState {
        fn variant_name(&self) -> &str {
            "Z"
        }
    }
    
    // This should not compile - returning WrongState instead of ValidState
    let _ = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &ValidState, _event: &ValidEvent, _ctx: Arc<ValidContext>| async {
            Ok(Transition {
                next_state: WrongState::Z, // Wrong type!
                actions: vec![],
            })
        })
        .build();
}
*/

/*
/// Test 7: Returning wrong action type from transition
#[test]
fn test_wrong_return_action_type() {
    #[derive(Clone, Debug, PartialEq)]
    struct WrongAction;
    
    // This should not compile - returning WrongAction instead of ValidAction
    let _ = FsmBuilder::<ValidState, ValidEvent, ValidContext, ValidAction>::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &ValidState, _event: &ValidEvent, _ctx: Arc<ValidContext>| async {
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![WrongAction], // Wrong type!
            })
        })
        .build();
}
*/

/*
/// Test 8: State without required trait bounds
#[test]
fn test_state_missing_trait_bounds() {
    // Missing Clone
    struct NonCloneState;
    
    impl StateVariant for NonCloneState {
        fn variant_name(&self) -> &str {
            "NonClone"
        }
    }
    
    // This should not compile - NonCloneState doesn't implement Clone
    let _ = FsmBuilder::new(NonCloneState);
}
*/

/*
/// Test 9: Using mutable state incorrectly
#[test]
fn test_cannot_mutate_state_directly() {
    let fsm = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |state: &ValidState, _event: &ValidEvent, _ctx: Arc<ValidContext>| async {
            // This should not compile - state is immutable
            *state = ValidState::B;
            
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![],
            })
        })
        .build();
}
*/

/*
/// Test 10: Lifetime issues with captured values
#[test]
fn test_lifetime_safety() {
    let local_string = String::from("local");
    
    // This should not compile - local_string doesn't live long enough
    let _ = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &ValidState, _event: &ValidEvent, _ctx: Arc<ValidContext>| async {
            let _ = &local_string; // Captured value doesn't live long enough
            
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![],
            })
        })
        .build();
}
*/

/// Test that we can use the FSM trait object safely
#[tokio::test]
async fn test_trait_object_safety() {
    
    let fsm = FsmBuilder::new(ValidState::A)
        .when("A")
        .on("Go", |_state: &ValidState, _event: &ValidEvent, _ctx: Arc<ValidContext>| async {
            Ok(Transition {
                next_state: ValidState::B,
                actions: vec![ValidAction],
            })
        })
        .done()
        .build();
    
    // We can use the FSM through the trait
    let mut machine = fsm;
    let ctx = Arc::new(ValidContext);
    
    // This works because Fsm trait is properly defined
    let actions = machine.handle(ValidEvent::Go, ctx).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(machine.state(), &ValidState::B);
}

/// Test that phantom types provide additional safety
#[test]
fn test_phantom_type_safety() {
    use std::marker::PhantomData;
    
    // Type-level authentication states
    #[derive(Clone, Debug, PartialEq)]
    struct Authenticated;
    #[derive(Clone, Debug, PartialEq)]
    struct Unauthenticated;
    
    #[derive(Clone, Debug, PartialEq)]
    enum AuthState<T: PartialEq + Debug> {
        State { 
            data: String,
            _phantom: PhantomData<T>,
        },
    }
    
    impl<T: Clone + PartialEq + Debug + Send + Sync + 'static> StateVariant for AuthState<T> {
        fn variant_name(&self) -> &str {
            "State"
        }
    }
    
    #[derive(Clone, Debug)]
    enum AuthEvent {
        Login { password: String },
        Logout,
        AccessResource, // Only valid when authenticated
    }
    
    impl EventVariant for AuthEvent {
        fn variant_name(&self) -> &str {
            match self {
                AuthEvent::Login { .. } => "Login",
                AuthEvent::Logout => "Logout",
                AuthEvent::AccessResource => "AccessResource",
            }
        }
    }
    
    #[derive(Clone, Debug, PartialEq)]
    enum AuthAction {
        GrantAccess,
        DenyAccess,
        LogActivity,
    }
    
    struct AuthContext;
    
    impl FsmContext for AuthContext {}
    
    #[async_trait::async_trait]
    impl FsmAction for AuthAction {
        type Context = AuthContext;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            Ok(())
        }
    }
    
    // This demonstrates how phantom types can encode authentication state
    // at the type level, though our FSM doesn't enforce this at compile time
    // for state transitions (that would require type-state pattern)
    
    let _ = FsmBuilder::new(AuthState::<Unauthenticated>::State {
        data: "guest".to_string(),
        _phantom: PhantomData,
    })
    .when("State")
    .on("Login", |state, event: &AuthEvent, _ctx: Arc<AuthContext>| {
        let state = state.clone();
        let event = event.clone();
        async move {
        if let AuthEvent::Login { password } = event {
            if password == "correct" {
                // In a real system, this would upgrade auth level
                // For demonstration, we stay in same type
                Ok(Transition {
                    next_state: state.clone(),
                    actions: vec![AuthAction::GrantAccess],
                })
            } else {
                Ok(Transition {
                    next_state: state.clone(),
                    actions: vec![AuthAction::DenyAccess],
                })
            }
        } else {
            unreachable!()
        }
        }
    })
    .done()
    .build();
}

/// Test that we handle Send + Sync requirements correctly
#[tokio::test]
async fn test_send_sync_requirements() {
    use std::sync::Arc;
    
    #[derive(Clone, Debug, PartialEq)]
    struct SendState {
        // Arc is Send + Sync
        shared_data: Arc<String>,
        // Rc would NOT work here - it's !Send
        // bad_data: Rc<String>, // This would fail to compile
    }
    
    impl StateVariant for SendState {
        fn variant_name(&self) -> &str {
            "SendState"
        }
    }
    
    // Context must be Send + Sync, so we use Arc instead of Rc
    struct ContextWithArc {
        shared_data: Arc<String>,
        other_data: Arc<String>,
    }
    
    impl FsmContext for ContextWithArc {}
    
    // Define a SendAction for this test
    #[derive(Clone, Debug, PartialEq)]
    struct SendAction;
    
    #[async_trait::async_trait]
    impl FsmAction for SendAction {
        type Context = ContextWithArc;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            Ok(())
        }
    }
    
    let fsm = FsmBuilder::<SendState, ValidEvent, ContextWithArc, SendAction>::new(SendState {
        shared_data: Arc::new("initial".to_string()),
    })
    .when("SendState")
    .on("Go", |state: &SendState, _event: &ValidEvent, ctx: Arc<ContextWithArc>| {
        let shared = state.shared_data.clone();
        let ctx_shared = ctx.shared_data.clone();
        
        async move {
            // We can use Arc in async block
            let _ = shared.len();
            let _ = ctx_shared.len();
            
            Ok(Transition {
                next_state: SendState {
                    shared_data: Arc::new("updated".to_string()),
                },
                actions: vec![],
            })
        }
    })
    .done()
    .build();
    
    let mut machine = fsm;
    let ctx = Arc::new(ContextWithArc {
        shared_data: Arc::new("shared".to_string()),
        other_data: Arc::new("other".to_string()),
    });
    
    machine.handle(ValidEvent::Go, ctx).await.unwrap();
}

/// Test const generics with FSM
#[test]
fn test_const_generic_states() {
    #[derive(Clone, Debug, PartialEq)]
    enum SizedState<const N: usize> {
        Array { data: [u8; N] },
        Empty,
    }
    
    impl<const N: usize> StateVariant for SizedState<N> {
        fn variant_name(&self) -> &str {
            match self {
                SizedState::Array { .. } => "Array",
                SizedState::Empty => "Empty",
            }
        }
    }
    
    #[derive(Clone, Debug)]
    enum SizedEvent {
        Fill,
        Clear,
    }
    
    impl EventVariant for SizedEvent {
        fn variant_name(&self) -> &str {
            match self {
                SizedEvent::Fill => "Fill",
                SizedEvent::Clear => "Clear",
            }
        }
    }
    
    #[derive(Clone, Debug, PartialEq)]
    struct NoAction;
    struct NoContext;
    
    impl FsmContext for NoContext {}
    
    #[async_trait::async_trait]
    impl FsmAction for NoAction {
        type Context = NoContext;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            Ok(())
        }
    }
    
    // FSM with compile-time sized arrays
    let _ = FsmBuilder::<SizedState<32>, SizedEvent, NoContext, NoAction>::new(
        SizedState::Empty
    )
    .when("Empty")
    .on("Fill", |_state, _event, _ctx: Arc<NoContext>| async {
        Ok(Transition {
            next_state: SizedState::Array { data: [42u8; 32] },
            actions: vec![],
        })
    })
    .done()
    .when("Array")
    .on("Clear", |_state, _event, _ctx: Arc<NoContext>| async {
        Ok(Transition {
            next_state: SizedState::Empty,
            actions: vec![],
        })
    })
    .done()
    .build();
    
    // Different size - completely different type
    let _ = FsmBuilder::<SizedState<64>, SizedEvent, NoContext, NoAction>::new(
        SizedState::Empty
    );
}