//! Simple FSM example with Arc<Context> pattern

use obzenflow_fsm::{FsmBuilder, StateVariant, EventVariant, FsmContext, FsmAction, Transition};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, PartialEq)]
enum DoorState {
    Closed,
    Open,
}

impl StateVariant for DoorState {
    fn variant_name(&self) -> &str {
        match self {
            DoorState::Closed => "Closed",
            DoorState::Open => "Open",
        }
    }
}

#[derive(Clone, Debug)]
enum DoorEvent {
    Open,
    Close,
}

impl EventVariant for DoorEvent {
    fn variant_name(&self) -> &str {
        match self {
            DoorEvent::Open => "Open",
            DoorEvent::Close => "Close",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum DoorAction {
    Ring,
    Log(String),
}

impl FsmContext for DoorContext {}

#[async_trait::async_trait]
impl FsmAction for DoorAction {
    type Context = DoorContext;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            DoorAction::Ring => {
                ctx.log.write().await.push("Ring!".to_string());
                Ok(())
            }
            DoorAction::Log(msg) => {
                ctx.log.write().await.push(msg.clone());
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
struct DoorContext {
    log: Arc<RwLock<Vec<String>>>,
}

#[tokio::main]
async fn main() {
    let fsm = FsmBuilder::new(DoorState::Closed)
        .when("Closed")
            .on("Open", |_state, _event, ctx: Arc<DoorContext>| async move {
                ctx.log.write().await.push("Opening door".to_string());
                Ok(Transition {
                    next_state: DoorState::Open,
                    actions: vec![DoorAction::Ring],
                })
            })
            .done()
        .when("Open")
            .on("Close", |_state, _event, ctx: Arc<DoorContext>| async move {
                ctx.log.write().await.push("Closing door".to_string());
                Ok(Transition {
                    next_state: DoorState::Closed,
                    actions: vec![DoorAction::Log("Door closed".to_string())],
                })
            })
            .done()
        .build();

    let mut door = fsm;
    let ctx = Arc::new(DoorContext {
        log: Arc::new(RwLock::new(vec![])),
    });

    // Open the door
    let actions = door.handle(DoorEvent::Open, ctx.clone()).await.unwrap();
    println!("Actions: {:?}", actions);
    println!("State: {:?}", door.state());

    // Close the door
    let actions = door.handle(DoorEvent::Close, ctx.clone()).await.unwrap();
    println!("Actions: {:?}", actions);
    println!("State: {:?}", door.state());

    // Print log
    let log = ctx.log.read().await;
    println!("Log: {:?}", *log);
}