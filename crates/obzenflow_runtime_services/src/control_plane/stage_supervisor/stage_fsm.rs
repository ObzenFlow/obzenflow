//! Stage FSM using obzenflow_fsm
//!
//! This defines the complete stage state machine with all transitions and actions

use crate::control_plane::stage_supervisor::event_handler::EventHandler;
use crate::data_plane::journal_subscription::ReactiveJournal;
use crate::control_plane::pipeline_supervisor::in_flight_tracker::InFlightTracker;
use crate::message_bus::{FsmMessageBus, StageNotification};
use obzenflow_topology_services::topology::Topology;
use obzenflow_topology_services::stages::StageId;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant, EventVariant};
use std::sync::Arc;
use tokio::sync::{Barrier, RwLock};

/// Stage states - represents the lifecycle of a single stage
#[derive(Clone, Debug, PartialEq)]
pub enum StageState {
    /// Initial state - component created but not initialized
    Created,
    /// Subscriptions created, resources allocated
    Initialized,
    /// Sources only - waiting for the gun to fire
    WaitingForGun,
    /// Normal event processing
    Running,
    /// Graceful shutdown requested
    Draining,
    /// All upstreams complete, draining remaining events
    ReadyToDrain,
    /// No more events to process
    Drained,
    /// Terminal state
    Terminated,
}

impl StateVariant for StageState {
    fn variant_name(&self) -> &str {
        match self {
            StageState::Created => "Created",
            StageState::Initialized => "Initialized",
            StageState::WaitingForGun => "WaitingForGun",
            StageState::Running => "Running",
            StageState::Draining => "Draining",
            StageState::ReadyToDrain => "ReadyToDrain",
            StageState::Drained => "Drained",
            StageState::Terminated => "Terminated",
        }
    }
}

/// Events that drive stage transitions
#[derive(Clone, Debug)]
pub enum StageEvent {
    /// Initialize the stage (create subscriptions, etc.)
    Initialize,
    /// Initialization complete
    Ready,
    /// Start processing (gun fires for sources)
    Start,
    /// Begin graceful shutdown
    BeginDrain,
    /// An upstream stage completed
    UpstreamComplete { stage_id: StageId },
    /// All upstream stages have completed
    AllUpstreamsComplete,
    /// No more events in flight
    InFlightDrained,
    /// Force immediate termination
    ForceShutdown { reason: String },
    /// Unrecoverable error
    Error { message: String },
}

impl EventVariant for StageEvent {
    fn variant_name(&self) -> &str {
        match self {
            StageEvent::Initialize => "Initialize",
            StageEvent::Ready => "Ready",
            StageEvent::Start => "Start",
            StageEvent::BeginDrain => "BeginDrain",
            StageEvent::UpstreamComplete { .. } => "UpstreamComplete",
            StageEvent::AllUpstreamsComplete => "AllUpstreamsComplete",
            StageEvent::InFlightDrained => "InFlightDrained",
            StageEvent::ForceShutdown { .. } => "ForceShutdown",
            StageEvent::Error { .. } => "Error",
        }
    }
}

/// Actions produced by stage transitions
#[derive(Clone, Debug)]
pub enum StageAction {
    /// Create subscriptions to upstream stages
    CreateSubscriptions,
    /// Start the event processing loop
    StartProcessing,
    /// Stop accepting new events
    StopAcceptingEvents,
    /// Signal EOF to downstream stages
    SignalEOF { natural: bool },
    /// Wait for in-flight events to complete
    WaitForInFlight,
    /// Cleanup stage resources
    CleanupResources,
    /// Notify pipeline of state change
    NotifyPipeline(StageNotification),
}

/// Stage context - provides access to resources
pub struct StageContext {
    /// Message bus for communication
    pub bus: Arc<FsmMessageBus>,
    
    /// This stage's ID
    pub stage_id: StageId,
    
    /// Stage name
    pub stage_name: String,
    
    /// Journal for event subscription
    pub journal: Arc<ReactiveJournal>,
    
    /// Event handler for processing
    pub handler: Arc<dyn EventHandler>,
    
    /// Topology for relationships
    pub topology: Arc<Topology>,
    
    /// In-flight tracking
    pub in_flight: Arc<InFlightTracker>,
    
    /// Start barrier (for sources)
    pub start_barrier: Option<Arc<Barrier>>,
    
    /// Completed upstreams
    pub completed_upstreams: Arc<RwLock<Vec<StageId>>>,
    
    /// Writer ID for this stage (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
}

/// Type alias for our stage FSM
pub type StageFsm = StateMachine<StageState, StageEvent, StageContext, StageAction>;

/// Build the stage FSM with all transitions
pub fn build_stage_fsm(is_source: bool) -> StageFsm {
    let mut builder = FsmBuilder::new(StageState::Created);
    
    // Common transitions
    builder = builder
        .when("Created")
            .on("Initialize", |_state: &StageState, _event: &StageEvent, _ctx: Arc<StageContext>| async move {
                Ok(Transition {
                    next_state: StageState::Initialized,
                    actions: vec![
                        StageAction::CreateSubscriptions,
                        StageAction::NotifyPipeline(StageNotification::Initialized),
                    ],
                })
            })
            .done()
        
        .when("Initialized")
            .on("Ready", move |_state: &StageState, _event: &StageEvent, _ctx: Arc<StageContext>| async move {
                Ok(Transition {
                    next_state: if is_source { 
                        StageState::WaitingForGun 
                    } else { 
                        StageState::Running 
                    },
                    actions: if is_source {
                        vec![]
                    } else {
                        vec![
                            StageAction::StartProcessing,
                            StageAction::NotifyPipeline(StageNotification::Started),
                        ]
                    },
                })
            })
            .done();
    
    // Source-specific transition
    if is_source {
        builder = builder
            .when("WaitingForGun")
                .on("Start", |_state: &StageState, _event: &StageEvent, _ctx: Arc<StageContext>| async move {
                    Ok(Transition {
                        next_state: StageState::Running,
                        actions: vec![
                            StageAction::StartProcessing,
                            StageAction::NotifyPipeline(StageNotification::Started),
                        ],
                    })
                })
                .done();
    }
    
    // Running state transitions
    builder = builder
        .when("Running")
            .on("BeginDrain", |_state: &StageState, _event: &StageEvent, _ctx: Arc<StageContext>| async move {
                Ok(Transition {
                    next_state: StageState::Draining,
                    actions: vec![
                        StageAction::StopAcceptingEvents,
                    ],
                })
            })
            .on("Error", |_state: &StageState, event: &StageEvent, _ctx: Arc<StageContext>| {
                let event = event.clone();
                async move {
                    if let StageEvent::Error { message } = event {
                        Ok(Transition {
                            next_state: StageState::Draining,
                            actions: vec![
                                StageAction::StopAcceptingEvents,
                                StageAction::SignalEOF { natural: false },
                                StageAction::NotifyPipeline(StageNotification::Failed { 
                                    error: message
                                }),
                            ],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .done()
        
        // Draining transitions
        .when("Draining")
            .on("AllUpstreamsComplete", |_state: &StageState, _event: &StageEvent, _ctx: Arc<StageContext>| async move {
                Ok(Transition {
                    next_state: StageState::ReadyToDrain,
                    actions: vec![StageAction::WaitForInFlight],
                })
            })
            .on("InFlightDrained", |_state: &StageState, _event: &StageEvent, _ctx: Arc<StageContext>| async move {
                Ok(Transition {
                    next_state: StageState::Drained,
                    actions: vec![
                        StageAction::SignalEOF { natural: true },
                        StageAction::NotifyPipeline(StageNotification::Completed { natural: true }),
                    ],
                })
            })
            .done()
        
        // Ready to drain
        .when("ReadyToDrain")
            .on("InFlightDrained", |_state: &StageState, _event: &StageEvent, _ctx: Arc<StageContext>| async move {
                Ok(Transition {
                    next_state: StageState::Drained,
                    actions: vec![
                        StageAction::SignalEOF { natural: true },
                        StageAction::NotifyPipeline(StageNotification::Completed { natural: true }),
                    ],
                })
            })
            .done()
        
        // Force shutdown from any state
        .from_any()
            .on("ForceShutdown", |state: &StageState, event: &StageEvent, _ctx: Arc<StageContext>| {
                let state_name = state.variant_name().to_string();
                let event = event.clone();
                async move {
                    if let StageEvent::ForceShutdown { reason } = event {
                        // Only cleanup if not already terminated
                        let actions = if matches!(state_name.as_str(), "Terminated" | "Drained") {
                            vec![]
                        } else {
                            vec![
                                StageAction::SignalEOF { natural: false },
                                StageAction::CleanupResources,
                                StageAction::NotifyPipeline(StageNotification::Failed { 
                                    error: reason 
                                }),
                            ]
                        };
                        
                        Ok(Transition {
                            next_state: StageState::Terminated,
                            actions,
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .done();
    
    builder.build()
}

/// Stage handle for external interaction
pub struct StageHandle {
    supervisor: Arc<RwLock<()>>, // Placeholder - will be replaced with actual supervisor reference
}

impl StageHandle {
    pub fn new() -> Self {
        Self {
            supervisor: Arc::new(RwLock::new(())),
        }
    }
}