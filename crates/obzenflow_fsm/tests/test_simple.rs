//! Simple tests that demonstrate core FSM functionality

use obzenflow_fsm::{FsmBuilder, StateVariant, EventVariant, FsmContext, FsmAction, Transition};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Clone, Debug, PartialEq)]
enum SimpleState {
    Idle,
    Working { progress: u32 },
    Done,
}

impl StateVariant for SimpleState {
    fn variant_name(&self) -> &str {
        match self {
            SimpleState::Idle => "Idle",
            SimpleState::Working { .. } => "Working",
            SimpleState::Done => "Done",
        }
    }
}

#[derive(Clone, Debug)]
enum SimpleEvent {
    Start,
    Progress,
    Finish,
}

impl EventVariant for SimpleEvent {
    fn variant_name(&self) -> &str {
        match self {
            SimpleEvent::Start => "Start",
            SimpleEvent::Progress => "Progress",
            SimpleEvent::Finish => "Finish",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum SimpleAction {
    Log(String),
    Notify,
}

#[derive(Clone)]
struct SimpleContext {
    log: Arc<RwLock<Vec<String>>>,
}

impl FsmContext for SimpleContext {
    fn describe(&self) -> String {
        "Simple test context".to_string()
    }
}

#[async_trait::async_trait]
impl FsmAction for SimpleAction {
    type Context = SimpleContext;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            SimpleAction::Log(msg) => {
                ctx.log.write().await.push(format!("Action: {}", msg));
                Ok(())
            }
            SimpleAction::Notify => {
                ctx.log.write().await.push("Action: Notify".to_string());
                Ok(())
            }
        }
    }
}

#[tokio::test]
async fn test_simple_transitions() {
    let fsm = FsmBuilder::new(SimpleState::Idle)
        .when("Idle")
            .on("Start", |_state, _event, ctx: Arc<SimpleContext>| async move {
                ctx.log.write().await.push("Starting work".to_string());
                Ok(Transition {
                    next_state: SimpleState::Working { progress: 0 },
                    actions: vec![SimpleAction::Log("Started".to_string())],
                })
            })
            .done()
        .when("Working")
            .on("Progress", |state, _event, ctx: Arc<SimpleContext>| {
                let progress = match state {
                    SimpleState::Working { progress } => *progress + 10,
                    _ => unreachable!(),
                };
                async move {
                    ctx.log.write().await.push(format!("Progress: {}%", progress));
                    
                    if progress >= 100 {
                        Ok(Transition {
                            next_state: SimpleState::Done,
                            actions: vec![SimpleAction::Notify],
                        })
                    } else {
                        Ok(Transition {
                            next_state: SimpleState::Working { progress },
                            actions: vec![],
                        })
                    }
                }
            })
            .on("Finish", |_state, _event, ctx: Arc<SimpleContext>| async move {
                ctx.log.write().await.push("Force finishing".to_string());
                Ok(Transition {
                    next_state: SimpleState::Done,
                    actions: vec![SimpleAction::Log("Finished".to_string())],
                })
            })
            .done()
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(SimpleContext { 
        log: Arc::new(RwLock::new(vec![])) 
    });

    // Start work
    let actions = machine.handle(SimpleEvent::Start, ctx.clone()).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(machine.state(), SimpleState::Working { progress: 0 }));
    
    // Make progress
    for expected in (10..=100).step_by(10) {
        let actions = machine.handle(SimpleEvent::Progress, ctx.clone()).await.unwrap();
        if expected < 100 {
            assert_eq!(actions.len(), 0);
            assert!(matches!(machine.state(), SimpleState::Working { progress } if *progress == expected));
        } else {
            assert_eq!(actions.len(), 1);
            assert!(matches!(machine.state(), SimpleState::Done));
        }
    }
    
    // Verify context log
    let log = ctx.log.read().await;
    assert_eq!(log.len(), 11); // 1 start + 10 progress
    assert_eq!(log[0], "Starting work");
    assert_eq!(log[10], "Progress: 100%");
}

#[tokio::test]
async fn test_entry_exit_handlers() {
    let fsm = FsmBuilder::new(SimpleState::Idle)
        .when("Idle")
            .on("Start", |_state, _event: &SimpleEvent, _ctx: Arc<SimpleContext>| async {
                Ok(Transition {
                    next_state: SimpleState::Working { progress: 0 },
                    actions: vec![],
                })
            })
            .done()
        .on_entry("Working", |_state, ctx: Arc<SimpleContext>| async move {
            ctx.log.write().await.push("Entered Working".to_string());
            Ok(vec![SimpleAction::Log("Entry".to_string())])
        })
        .on_exit("Working", |_state, ctx: Arc<SimpleContext>| async move {
            ctx.log.write().await.push("Exited Working".to_string());
            Ok(vec![SimpleAction::Log("Exit".to_string())])
        })
        .when("Working")
            .on("Finish", |_state, _event: &SimpleEvent, _ctx: Arc<SimpleContext>| async {
                Ok(Transition {
                    next_state: SimpleState::Done,
                    actions: vec![],
                })
            })
            .done()
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(SimpleContext { 
        log: Arc::new(RwLock::new(vec![])) 
    });

    // Transition to Working (triggers entry)
    let actions = machine.handle(SimpleEvent::Start, ctx.clone()).await.unwrap();
    assert_eq!(actions.len(), 1); // Entry action
    
    let log = ctx.log.read().await;
    assert_eq!(*log, vec!["Entered Working"]);
    drop(log); // Release the read lock

    // Transition to Done (triggers exit)
    let actions = machine.handle(SimpleEvent::Finish, ctx.clone()).await.unwrap();
    assert_eq!(actions.len(), 1); // Exit action
    
    let log = ctx.log.read().await;
    assert_eq!(*log, vec!["Entered Working", "Exited Working"]);
}

#[tokio::test]
async fn test_timeout() {
    use tokio::time::sleep;
    
    let fsm = FsmBuilder::new(SimpleState::Idle)
        .when("Idle")
            .on("Start", |_state, _event: &SimpleEvent, _ctx: Arc<SimpleContext>| async {
                Ok(Transition {
                    next_state: SimpleState::Working { progress: 0 },
                    actions: vec![],
                })
            })
            .done()
        .when("Working")
            .timeout(Duration::from_millis(50), |_state, _ctx: Arc<SimpleContext>| async {
                Ok(Transition {
                    next_state: SimpleState::Done,
                    actions: vec![SimpleAction::Log("Timed out".to_string())],
                })
            })
            .done()
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(SimpleContext { 
        log: Arc::new(RwLock::new(vec![])) 
    });

    // Start work
    machine.handle(SimpleEvent::Start, ctx.clone()).await.unwrap();
    assert!(matches!(machine.state(), SimpleState::Working { .. }));

    // Wait for timeout
    sleep(Duration::from_millis(60)).await;
    
    // Check timeout
    let actions = machine.check_timeout(ctx).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(machine.state(), SimpleState::Done));
}

#[tokio::test]
async fn test_unhandled_events() {
    let fsm = FsmBuilder::<SimpleState, SimpleEvent, SimpleContext, SimpleAction>::new(SimpleState::Idle)
        .when("Idle")
            .on("Start", |_state, _event: &SimpleEvent, _ctx: Arc<SimpleContext>| async {
                Ok(Transition {
                    next_state: SimpleState::Working { progress: 0 },
                    actions: vec![],
                })
            })
            .done()
        .when_unhandled(|state, event, ctx: Arc<SimpleContext>| {
            let state_name = state.variant_name().to_string();
            let event_name = event.variant_name().to_string();
            async move {
            ctx.log.write().await.push(format!(
                "Unhandled {:?} in {:?}",
                event_name,
                state_name
            ));
            Ok(())
        }
        })
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(SimpleContext { 
        log: Arc::new(RwLock::new(vec![])) 
    });

    // Send unhandled event
    let actions = machine.handle(SimpleEvent::Finish, ctx.clone()).await.unwrap();
    assert_eq!(actions.len(), 0);
    
    let log = ctx.log.read().await;
    assert_eq!(*log, vec!["Unhandled \"Finish\" in \"Idle\""]);
    
    assert!(matches!(machine.state(), SimpleState::Idle)); // State unchanged
}