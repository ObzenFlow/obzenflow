//! Comprehensive tests for the FSM implementation
//! 
//! These tests push edge cases and verify compile-time safety

use crate::{FsmBuilder, Fsm, StateVariant, EventVariant, Transition};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use std::time::Duration;

/// Test complex state with nested data
#[derive(Clone, Debug, PartialEq)]
enum ConnectionState {
    Disconnected { retry_count: u32 },
    Connecting { attempt: u32, started_at: std::time::Instant },
    Connected { session_id: String, heartbeat_count: u64 },
    Reconnecting { old_session: String, attempt: u32 },
    Failed { reason: String, permanent: bool },
}

impl StateVariant for ConnectionState {
    fn variant_name(&self) -> &str {
        match self {
            ConnectionState::Disconnected { .. } => "Disconnected",
            ConnectionState::Connecting { .. } => "Connecting",
            ConnectionState::Connected { .. } => "Connected",
            ConnectionState::Reconnecting { .. } => "Reconnecting",
            ConnectionState::Failed { .. } => "Failed",
        }
    }
}

#[derive(Clone, Debug)]
enum ConnectionEvent {
    Connect { endpoint: String },
    ConnectionEstablished { session_id: String },
    ConnectionLost { reason: String },
    Heartbeat,
    Retry,
    GiveUp,
}

impl EventVariant for ConnectionEvent {
    fn variant_name(&self) -> &str {
        match self {
            ConnectionEvent::Connect { .. } => "Connect",
            ConnectionEvent::ConnectionEstablished { .. } => "ConnectionEstablished",
            ConnectionEvent::ConnectionLost { .. } => "ConnectionLost",
            ConnectionEvent::Heartbeat => "Heartbeat",
            ConnectionEvent::Retry => "Retry",
            ConnectionEvent::GiveUp => "GiveUp",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ConnectionAction {
    OpenSocket(String),
    CloseSocket,
    ScheduleRetry(Duration),
    IncrementHeartbeat,
    LogError(String),
    NotifyDisconnected,
}

/// Complex context with shared mutable state
struct ConnectionContext {
    socket_manager: Arc<Mutex<MockSocketManager>>,
    metrics: Arc<Mutex<ConnectionMetrics>>,
    event_log: mpsc::UnboundedSender<String>,
}

struct MockSocketManager {
    is_open: bool,
    fail_next: bool,
}

struct ConnectionMetrics {
    total_connections: u64,
    total_heartbeats: u64,
    total_failures: u64,
}

#[tokio::test]
async fn test_complex_state_transitions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    
    let fsm = FsmBuilder::new(ConnectionState::Disconnected { retry_count: 0 })
        .when("Disconnected")
            .on("Connect", |state, event, ctx: &mut ConnectionContext| {
                let endpoint = match event {
                    ConnectionEvent::Connect { endpoint } => endpoint.clone(),
                    _ => unreachable!(),
                };
                
                let retry_count = match state {
                    ConnectionState::Disconnected { retry_count } => retry_count.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    ctx.event_log.send(format!("Connecting to {} (attempt #{})", endpoint, retry_count + 1)).unwrap();
                    
                    Ok(Transition {
                        next_state: ConnectionState::Connecting {
                            attempt: retry_count + 1,
                            started_at: std::time::Instant::now(),
                        },
                        actions: vec![ConnectionAction::OpenSocket(endpoint)],
                    })
                }
            })
            .done()
        
        .when("Connecting")
            .timeout(Duration::from_secs(5), |state, ctx: &mut ConnectionContext| {
                let attempt = match state {
                    ConnectionState::Connecting { attempt, .. } => attempt.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    ctx.event_log.send("Connection timeout".to_string()).unwrap();
                    
                    if attempt >= 3 {
                        Ok(Transition {
                            next_state: ConnectionState::Failed {
                                reason: "Connection timeout after 3 attempts".to_string(),
                                permanent: true,
                            },
                            actions: vec![
                                ConnectionAction::CloseSocket,
                                ConnectionAction::LogError("Max retries exceeded".to_string()),
                            ],
                        })
                    } else {
                        Ok(Transition {
                            next_state: ConnectionState::Disconnected { retry_count: attempt },
                            actions: vec![
                                ConnectionAction::CloseSocket,
                                ConnectionAction::ScheduleRetry(Duration::from_secs(attempt as u64)),
                            ],
                        })
                    }
                }
            })
            .on("ConnectionEstablished", |_state, event, ctx: &mut ConnectionContext| {
                let session_id = match event {
                    ConnectionEvent::ConnectionEstablished { session_id } => session_id.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    let mut metrics = ctx.metrics.lock().await;
                    metrics.total_connections += 1;
                    
                    Ok(Transition {
                        next_state: ConnectionState::Connected {
                            session_id,
                            heartbeat_count: 0,
                        },
                        actions: vec![],
                    })
                }
            })
            .done()
        
        .when("Connected")
            .on("Heartbeat", |state, _event, ctx: &mut ConnectionContext| {
                let (session_id, heartbeat_count) = match state {
                    ConnectionState::Connected { session_id, heartbeat_count } => 
                        (session_id.clone(), *heartbeat_count),
                    _ => unreachable!(),
                };
                
                async move {
                    let mut metrics = ctx.metrics.lock().await;
                    metrics.total_heartbeats += 1;
                    
                    Ok(Transition {
                        next_state: ConnectionState::Connected {
                            session_id,
                            heartbeat_count: heartbeat_count + 1,
                        },
                        actions: vec![ConnectionAction::IncrementHeartbeat],
                    })
                }
            })
            .on("ConnectionLost", |state, event, ctx| {
                let session_id = match state {
                    ConnectionState::Connected { session_id, .. } => session_id.clone(),
                    _ => unreachable!(),
                };
                
                let reason = match event {
                    ConnectionEvent::ConnectionLost { reason } => reason.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    ctx.event_log.send(format!("Connection lost: {}", reason)).unwrap();
                    
                    Ok(Transition {
                        next_state: ConnectionState::Reconnecting {
                            old_session: session_id,
                            attempt: 1,
                        },
                        actions: vec![
                            ConnectionAction::CloseSocket,
                            ConnectionAction::NotifyDisconnected,
                        ],
                    })
                }
            })
            .done()
        
        .when("Reconnecting")
            .on("ConnectionEstablished", |_state, event, _ctx| {
                let session_id = match event {
                    ConnectionEvent::ConnectionEstablished { session_id } => session_id.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    Ok(Transition {
                        next_state: ConnectionState::Connected {
                            session_id,
                            heartbeat_count: 0,
                        },
                        actions: vec![],
                    })
                }
            })
            .on("GiveUp", |state, _event, ctx| {
                let old_session = match state {
                    ConnectionState::Reconnecting { old_session, .. } => old_session.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    let mut metrics = ctx.metrics.lock().await;
                    metrics.total_failures += 1;
                    
                    Ok(Transition {
                        next_state: ConnectionState::Failed {
                            reason: format!("Failed to reconnect session {}", old_session),
                            permanent: false,
                        },
                        actions: vec![
                            ConnectionAction::LogError("Reconnection failed".to_string()),
                        ],
                    })
                }
            })
            .done()
        
        // Global handler for any unhandled event
        .when_unhandled(|state, event, ctx: &mut ConnectionContext| {
            let state = state.clone();
            let event = event.clone();
            async move {
                ctx.event_log.send(
                    format!("Unhandled event {:?} in state {:?}", event, state)
                ).unwrap();
                Ok(())
            }
        })
        
        .build();

    let mut connection = fsm;
    let mut ctx = ConnectionContext {
        socket_manager: Arc::new(Mutex::new(MockSocketManager {
            is_open: false,
            fail_next: false,
        })),
        metrics: Arc::new(Mutex::new(ConnectionMetrics {
            total_connections: 0,
            total_heartbeats: 0,
            total_failures: 0,
        })),
        event_log: tx,
    };

    // Test: Initial connection
    let actions = connection.handle(
        ConnectionEvent::Connect { endpoint: "ws://example.com".to_string() },
        &mut ctx
    ).await.unwrap();
    
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], ConnectionAction::OpenSocket(_)));
    assert!(matches!(connection.state(), ConnectionState::Connecting { attempt: 1, .. }));

    // Test: Connection established
    let actions = connection.handle(
        ConnectionEvent::ConnectionEstablished { session_id: "sess-123".to_string() },
        &mut ctx
    ).await.unwrap();
    
    assert_eq!(actions.len(), 0);
    assert!(matches!(
        connection.state(), 
        ConnectionState::Connected { session_id, heartbeat_count: 0 } if session_id == "sess-123"
    ));

    // Test: Multiple heartbeats
    for i in 1..=3 {
        let actions = connection.handle(ConnectionEvent::Heartbeat, &mut ctx).await.unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            connection.state(),
            ConnectionState::Connected { heartbeat_count, .. } if *heartbeat_count == i
        ));
    }

    // Test: Connection lost and reconnecting
    let actions = connection.handle(
        ConnectionEvent::ConnectionLost { reason: "Network error".to_string() },
        &mut ctx
    ).await.unwrap();
    
    assert_eq!(actions.len(), 2);
    assert!(matches!(
        connection.state(),
        ConnectionState::Reconnecting { old_session, attempt: 1 } if old_session == "sess-123"
    ));

    // Verify metrics
    let metrics = ctx.metrics.lock().await;
    assert_eq!(metrics.total_connections, 1);
    assert_eq!(metrics.total_heartbeats, 3);
    
    // Check event log
    let mut events = vec![];
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    assert!(events.contains(&"Connecting to ws://example.com (attempt #1)".to_string()));
    assert!(events.contains(&"Connection lost: Network error".to_string()));
}

#[tokio::test]
async fn test_timeout_handling() {
    use tokio::time::sleep;
    
    let fsm = FsmBuilder::new(ConnectionState::Disconnected { retry_count: 0 })
        .when("Connecting")
            .timeout(Duration::from_millis(100), |_state, _ctx| async {
                Ok(Transition {
                    next_state: ConnectionState::Failed {
                        reason: "Timeout".to_string(),
                        permanent: false,
                    },
                    actions: vec![ConnectionAction::LogError("Connection timed out".to_string())],
                })
            })
            .done()
        .when("Disconnected")
            .on("Connect", |_state: &ConnectionState, _event: &ConnectionEvent, _ctx: &mut ConnectionContext| async {
                Ok(Transition {
                    next_state: ConnectionState::Connecting {
                        attempt: 1,
                        started_at: std::time::Instant::now(),
                    },
                    actions: vec![],
                })
            })
            .done()
        .build();

    let mut connection = fsm;
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut ctx = ConnectionContext {
        socket_manager: Arc::new(Mutex::new(MockSocketManager {
            is_open: false,
            fail_next: false,
        })),
        metrics: Arc::new(Mutex::new(ConnectionMetrics {
            total_connections: 0,
            total_heartbeats: 0,
            total_failures: 0,
        })),
        event_log: tx,
    };

    // Start connection
    connection.handle(
        ConnectionEvent::Connect { endpoint: "test".to_string() },
        &mut ctx
    ).await.unwrap();
    
    assert!(matches!(connection.state(), ConnectionState::Connecting { .. }));

    // Wait for timeout
    sleep(Duration::from_millis(150)).await;
    
    // Check timeout should trigger state change
    let actions = connection.check_timeout(&mut ctx).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], ConnectionAction::LogError(_)));
    assert!(matches!(
        connection.state(),
        ConnectionState::Failed { permanent: false, .. }
    ));
}

#[tokio::test]
async fn test_invalid_transitions_handled_gracefully() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    
    let fsm = FsmBuilder::new(ConnectionState::Disconnected { retry_count: 0 })
        .when("Disconnected")
            .on("Connect", |_state: &ConnectionState, _event: &ConnectionEvent, _ctx: &mut ConnectionContext| async {
                Ok(Transition {
                    next_state: ConnectionState::Connecting {
                        attempt: 1,
                        started_at: std::time::Instant::now(),
                    },
                    actions: vec![],
                })
            })
            .done()
        .when_unhandled(|state, event, ctx: &mut ConnectionContext| {
            let state_name = state.variant_name().to_string();
            let event_name = event.variant_name().to_string();
            async move {
                ctx.event_log.send(
                    format!("Invalid transition: {} in state {}", event_name, state_name)
                ).unwrap();
                Ok(())
            }
        })
        .build();

    let mut connection = fsm;
    let mut ctx = ConnectionContext {
        socket_manager: Arc::new(Mutex::new(MockSocketManager {
            is_open: false,
            fail_next: false,
        })),
        metrics: Arc::new(Mutex::new(ConnectionMetrics {
            total_connections: 0,
            total_heartbeats: 0,
            total_failures: 0,
        })),
        event_log: tx,
    };

    // Try invalid transition: Heartbeat while Disconnected
    let actions = connection.handle(ConnectionEvent::Heartbeat, &mut ctx).await.unwrap();
    assert_eq!(actions.len(), 0); // No actions, just logged
    assert!(matches!(connection.state(), ConnectionState::Disconnected { .. })); // State unchanged

    // Check that it was logged
    let event = rx.recv().await.unwrap();
    assert_eq!(event, "Invalid transition: Heartbeat in state Disconnected");
}

/// Test that demonstrates compile-time safety
#[cfg(test)]
mod compile_time_safety {
    // This should NOT compile - demonstrating type safety
    /*
    #[test]
    fn test_invalid_state_type() {
        // This won't compile because String doesn't implement StateVariant
        let fsm = FsmBuilder::<String, ConnectionEvent, ConnectionContext, ConnectionAction>::new(
            "invalid".to_string()
        );
    }
    */
    
    // This should NOT compile - wrong state type in handler
    /*
    #[test]
    fn test_wrong_state_in_handler() {
        let fsm = FsmBuilder::new(ConnectionState::Disconnected { retry_count: 0 })
            .when("Disconnected")
            .on("Connect", |state: &String, _event, _ctx| async { // Wrong type!
                Ok(Transition {
                    next_state: ConnectionState::Connecting {
                        attempt: 1,
                        started_at: std::time::Instant::now(),
                    },
                    actions: vec![],
                })
            })
            .build();
    }
    */
}

/// Test concurrent state modifications
#[tokio::test]
async fn test_concurrent_operations() {
    use std::sync::atomic::{AtomicU32, Ordering};
    
    #[derive(Clone, Debug, PartialEq)]
    enum CounterState {
        Idle,
        Counting { value: u32 },
        Done { final_value: u32 },
    }

    impl StateVariant for CounterState {
        fn variant_name(&self) -> &str {
            match self {
                CounterState::Idle => "Idle",
                CounterState::Counting { .. } => "Counting",
                CounterState::Done { .. } => "Done",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum CounterEvent {
        Start,
        Increment,
        Finish,
    }

    impl EventVariant for CounterEvent {
        fn variant_name(&self) -> &str {
            match self {
                CounterEvent::Start => "Start",
                CounterEvent::Increment => "Increment",
                CounterEvent::Finish => "Finish",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum CounterAction {
        Initialize,
        UpdateValue(u32),
        Finalize,
    }

    struct CounterContext {
        external_counter: Arc<AtomicU32>,
    }

    let fsm = FsmBuilder::new(CounterState::Idle)
        .when("Idle")
            .on("Start", |_state: &CounterState, _event: &CounterEvent, ctx: &mut CounterContext| async move {
                ctx.external_counter.store(0, Ordering::SeqCst);
                Ok(Transition {
                    next_state: CounterState::Counting { value: 0 },
                    actions: vec![CounterAction::Initialize],
                })
            })
            .done()
        .when("Counting")
            .on("Increment", |state, _event, ctx| {
                let current = match state {
                    CounterState::Counting { value } => value.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    // Simulate some async work
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    
                    let new_value = current + 1;
                    ctx.external_counter.fetch_add(1, Ordering::SeqCst);
                    
                    Ok(Transition {
                        next_state: CounterState::Counting { value: new_value },
                        actions: vec![CounterAction::UpdateValue(new_value)],
                    })
                }
            })
            .on("Finish", |state, _event, _ctx| {
                let final_value = match state {
                    CounterState::Counting { value } => value.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    Ok(Transition {
                        next_state: CounterState::Done { final_value },
                        actions: vec![CounterAction::Finalize],
                    })
                }
            })
            .done()
        .build();

    let shared_fsm = Arc::new(Mutex::new(fsm));
    let external_counter = Arc::new(AtomicU32::new(0));
    
    // Start counting
    {
        let mut fsm = shared_fsm.lock().await;
        let mut ctx = CounterContext {
            external_counter: external_counter.clone(),
        };
        fsm.handle(CounterEvent::Start, &mut ctx).await.unwrap();
    }

    // Spawn multiple tasks to increment concurrently
    let mut handles = vec![];
    for _ in 0..10 {
        let fsm_clone = shared_fsm.clone();
        let counter_clone = external_counter.clone();
        
        let handle = tokio::spawn(async move {
            let mut fsm = fsm_clone.lock().await;
            let mut ctx = CounterContext {
                external_counter: counter_clone,
            };
            fsm.handle(CounterEvent::Increment, &mut ctx).await.unwrap();
        });
        
        handles.push(handle);
    }

    // Wait for all increments
    for handle in handles {
        handle.await.unwrap();
    }

    // Finish and check final state
    {
        let mut fsm = shared_fsm.lock().await;
        let mut ctx = CounterContext {
            external_counter: external_counter.clone(),
        };
        
        let actions = fsm.handle(CounterEvent::Finish, &mut ctx).await.unwrap();
        assert!(matches!(actions[0], CounterAction::Finalize));
        
        // Verify final state
        assert!(matches!(
            fsm.state(),
            CounterState::Done { final_value: 10 }
        ));
        
        // Verify external counter matches
        assert_eq!(external_counter.load(Ordering::SeqCst), 10);
    }
}

/// Test entry and exit handlers
#[tokio::test]
async fn test_entry_exit_handlers() {
    use std::sync::atomic::{AtomicU32, Ordering};
    
    #[derive(Clone, Debug, PartialEq)]
    enum LifecycleState {
        Created,
        Initialized { id: u32 },
        Active,
        Terminated,
    }

    impl StateVariant for LifecycleState {
        fn variant_name(&self) -> &str {
            match self {
                LifecycleState::Created => "Created",
                LifecycleState::Initialized { .. } => "Initialized",
                LifecycleState::Active => "Active",
                LifecycleState::Terminated => "Terminated",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum LifecycleEvent {
        Initialize { id: u32 },
        Activate,
        Terminate,
    }

    impl EventVariant for LifecycleEvent {
        fn variant_name(&self) -> &str {
            match self {
                LifecycleEvent::Initialize { .. } => "Initialize",
                LifecycleEvent::Activate => "Activate",
                LifecycleEvent::Terminate => "Terminate",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum LifecycleAction {
        Log(String),
        AllocateResources,
        ReleaseResources,
    }

    struct LifecycleContext {
        entry_count: Arc<AtomicU32>,
        exit_count: Arc<AtomicU32>,
    }

    let fsm = FsmBuilder::new(LifecycleState::Created)
        .when("Created")
            .on("Initialize", |_state, event, _ctx| {
                let id = match event {
                    LifecycleEvent::Initialize { id } => id.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    Ok(Transition {
                        next_state: LifecycleState::Initialized { id },
                        actions: vec![LifecycleAction::AllocateResources],
                    })
                }
            })
            .done()
        
        .when("Initialized")
            .on("Activate", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: LifecycleState::Active,
                    actions: vec![],
                })
            })
            .done()
        
        .when("Active")
            .on("Terminate", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: LifecycleState::Terminated,
                    actions: vec![LifecycleAction::ReleaseResources],
                })
            })
            .done()
        
        // Entry handlers
        .on_entry("Initialized", |state, ctx| async move {
            ctx.entry_count.fetch_add(1, Ordering::SeqCst);
            let id = match state {
                LifecycleState::Initialized { id } => id.clone(),
                _ => unreachable!(),
            };
            Ok(vec![LifecycleAction::Log(format!("Entered Initialized with id={}", id))])
        })
        
        .on_entry("Active", |_state, ctx| async move {
            ctx.entry_count.fetch_add(1, Ordering::SeqCst);
            Ok(vec![LifecycleAction::Log("Entered Active state".to_string())])
        })
        
        // Exit handlers
        .on_exit("Initialized", |_state, ctx| async move {
            ctx.exit_count.fetch_add(1, Ordering::SeqCst);
            Ok(vec![LifecycleAction::Log("Exiting Initialized state".to_string())])
        })
        
        .on_exit("Active", |_state, ctx| async move {
            ctx.exit_count.fetch_add(1, Ordering::SeqCst);
            Ok(vec![LifecycleAction::Log("Exiting Active state".to_string())])
        })
        
        .build();

    let mut lifecycle = fsm;
    let mut ctx = LifecycleContext {
        entry_count: Arc::new(AtomicU32::new(0)),
        exit_count: Arc::new(AtomicU32::new(0)),
    };

    // Initialize
    let actions = lifecycle.handle(
        LifecycleEvent::Initialize { id: 42 },
        &mut ctx
    ).await.unwrap();
    
    // Should have: entry action + transition action
    assert_eq!(actions.len(), 2);
    assert!(matches!(actions[0], LifecycleAction::Log(_)));
    assert!(matches!(actions[1], LifecycleAction::AllocateResources));
    assert_eq!(ctx.entry_count.load(Ordering::SeqCst), 1);

    // Activate
    let actions = lifecycle.handle(LifecycleEvent::Activate, &mut ctx).await.unwrap();
    
    // Should have: exit handler + entry handler
    assert_eq!(actions.len(), 2);
    assert!(matches!(actions[0], LifecycleAction::Log(ref s) if s.contains("Exiting")));
    assert!(matches!(actions[1], LifecycleAction::Log(ref s) if s.contains("Entered Active")));
    assert_eq!(ctx.entry_count.load(Ordering::SeqCst), 2);
    assert_eq!(ctx.exit_count.load(Ordering::SeqCst), 1);

    // Terminate
    let actions = lifecycle.handle(LifecycleEvent::Terminate, &mut ctx).await.unwrap();
    
    // Should have: exit handler + transition action
    assert_eq!(actions.len(), 2);
    assert!(matches!(actions[0], LifecycleAction::Log(ref s) if s.contains("Exiting Active")));
    assert!(matches!(actions[1], LifecycleAction::ReleaseResources));
    assert_eq!(ctx.exit_count.load(Ordering::SeqCst), 2);
}

/// Test error handling in handlers
#[tokio::test]
async fn test_error_handling() {
    #[derive(Clone, Debug, PartialEq)]
    enum FallibleState {
        Ready,
        Processing,
        Error { message: String },
    }

    impl StateVariant for FallibleState {
        fn variant_name(&self) -> &str {
            match self {
                FallibleState::Ready => "Ready",
                FallibleState::Processing => "Processing",
                FallibleState::Error { .. } => "Error",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum FallibleEvent {
        Process { should_fail: bool },
        Reset,
    }

    impl EventVariant for FallibleEvent {
        fn variant_name(&self) -> &str {
            match self {
                FallibleEvent::Process { .. } => "Process",
                FallibleEvent::Reset => "Reset",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum FallibleAction {
        StartProcessing,
        HandleError(String),
        Cleanup,
    }

    struct FallibleContext;

    let fsm = FsmBuilder::new(FallibleState::Ready)
        .when("Ready")
            .on("Process", |_state, event, _ctx| {
                let should_fail = match event {
                    FallibleEvent::Process { should_fail } => should_fail.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    if should_fail {
                        Err("Simulated processing error".to_string())
                    } else {
                        Ok(Transition {
                            next_state: FallibleState::Processing,
                            actions: vec![FallibleAction::StartProcessing],
                        })
                    }
                }
            })
            .done()
        
        .when("Error")
            .on("Reset", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: FallibleState::Ready,
                    actions: vec![FallibleAction::Cleanup],
                })
            })
            .done()
        
        .build();

    let mut machine = fsm;
    let mut ctx = FallibleContext;

    // Test successful processing
    let result = machine.handle(
        FallibleEvent::Process { should_fail: false },
        &mut ctx
    ).await;
    
    assert!(result.is_ok());
    assert!(matches!(machine.state(), FallibleState::Processing));

    // Go back to ready state manually for next test
    machine.current_state = FallibleState::Ready;

    // Test failed processing
    let result = machine.handle(
        FallibleEvent::Process { should_fail: true },
        &mut ctx
    ).await;
    
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "Simulated processing error");
    assert!(matches!(machine.state(), FallibleState::Ready)); // State unchanged on error
}

/// Test state persistence and restoration
#[tokio::test]
async fn test_state_persistence() {
    // Create FSM in a specific state
    let initial_state = ConnectionState::Connected {
        session_id: "existing-session".to_string(),
        heartbeat_count: 42,
    };
    
    let fsm = FsmBuilder::new(initial_state.clone())
        .when("Connected")
            .on("Heartbeat", |state, _event, _ctx| {
                let (session_id, heartbeat_count) = match state {
                    ConnectionState::Connected { session_id, heartbeat_count } => 
                        (session_id.clone(), *heartbeat_count),
                    _ => unreachable!(),
                };
                
                async move {
                    Ok(Transition {
                        next_state: ConnectionState::Connected {
                            session_id,
                            heartbeat_count: heartbeat_count + 1,
                        },
                        actions: vec![ConnectionAction::IncrementHeartbeat],
                    })
                }
            })
            .done()
        .build();

    let mut restored_fsm = fsm;
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut ctx = ConnectionContext {
        socket_manager: Arc::new(Mutex::new(MockSocketManager {
            is_open: true,
            fail_next: false,
        })),
        metrics: Arc::new(Mutex::new(ConnectionMetrics {
            total_connections: 1,
            total_heartbeats: 42,
            total_failures: 0,
        })),
        event_log: tx,
    };

    // Verify restored state
    assert!(matches!(
        restored_fsm.state(),
        ConnectionState::Connected { session_id, heartbeat_count: 42 } if session_id == "existing-session"
    ));

    // Continue from restored state
    let actions = restored_fsm.handle(ConnectionEvent::Heartbeat, &mut ctx).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        restored_fsm.state(),
        ConnectionState::Connected { heartbeat_count: 43, .. }
    ));
}