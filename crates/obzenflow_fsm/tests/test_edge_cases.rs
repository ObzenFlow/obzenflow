//! Edge case tests for the FSM implementation
//! 
//! These tests push the FSM to its limits with extreme scenarios

use obzenflow_fsm::{FsmBuilder, StateVariant, EventVariant, FsmContext, FsmAction, Transition};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Test deeply nested state data
#[derive(Clone, Debug, PartialEq)]
enum NestedState {
    Level1 {
        data: HashMap<String, Vec<Option<Box<InnerData>>>>,
    },
    Level2 {
        matrix: Vec<Vec<Vec<u32>>>,
    },
}

#[derive(Clone, Debug, PartialEq)]
struct InnerData {
    values: Vec<f64>,
    nested: Option<Box<InnerData>>,
}

impl StateVariant for NestedState {
    fn variant_name(&self) -> &str {
        match self {
            NestedState::Level1 { .. } => "Level1",
            NestedState::Level2 { .. } => "Level2",
        }
    }
}

#[derive(Clone, Debug)]
enum NestedEvent {
    Transform,
    Mutate { key: String, value: String },
}

impl EventVariant for NestedEvent {
    fn variant_name(&self) -> &str {
        match self {
            NestedEvent::Transform => "Transform",
            NestedEvent::Mutate { .. } => "Mutate",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct NoAction;

struct EmptyContext;

impl FsmContext for EmptyContext {}

#[async_trait::async_trait]
impl FsmAction for NoAction {
    type Context = EmptyContext;
    
    async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn test_deeply_nested_state() {
    let initial_data = {
        let mut map = HashMap::new();
        let inner = InnerData {
            values: vec![1.0, 2.0, 3.0],
            nested: Some(Box::new(InnerData {
                values: vec![4.0, 5.0],
                nested: None,
            })),
        };
        map.insert("key1".to_string(), vec![Some(Box::new(inner))]);
        map
    };

    let fsm = FsmBuilder::<NestedState, NestedEvent, EmptyContext, NoAction>::new(NestedState::Level1 { data: initial_data })
        .when("Level1")
            .on("Transform", |state, _event, _ctx| {
                let data = match state {
                    NestedState::Level1 { data } => data.clone(),
                    _ => unreachable!(),
                };
                
                async move {
                    // Transform nested data into matrix
                    let mut matrix = vec![];
                    for (_key, values) in data {
                        for opt in values {
                            if let Some(inner) = opt {
                                // Convert f64 values to u32
                                let u32_values: Vec<u32> = inner.values.iter()
                                    .map(|&v| v as u32)
                                    .collect();
                                matrix.push(vec![u32_values]);
                            }
                        }
                    }
                    
                    Ok(Transition {
                        next_state: NestedState::Level2 {
                            matrix,
                        },
                        actions: vec![],
                    })
                }
            })
            .done()
        .when("Level2")
            .on("Mutate", |state, _event, _ctx| {
                let next_state = state.clone();
                // Since we removed metadata, this is now a no-op
                async move {
                    Ok(Transition {
                        next_state,
                        actions: vec![],
                    })
                }
            })
            .done()
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(EmptyContext);

    // Transform to Level2
    machine.handle(NestedEvent::Transform, ctx.clone()).await.unwrap();
    
    // Verify transformation
    if let NestedState::Level2 { matrix } = machine.state() {
        assert_eq!(matrix.len(), 1);
        assert_eq!(matrix[0].len(), 1);
        assert_eq!(matrix[0][0], vec![1, 2, 3]); // Now u32 values
        
        // Mutate doesn't apply to Level2 anymore since we removed metadata
        machine.handle(
            NestedEvent::Mutate {
                key: "test".to_string(),
                value: "value".to_string(),
            },
            ctx.clone()
        ).await.unwrap();
    } else {
        panic!("Expected Level2 state");
    }
}

/// Test zero-sized types (ZSTs)
#[tokio::test]
async fn test_zero_sized_types() {
    #[derive(Clone, Debug, PartialEq)]
    enum ZstState {
        Empty,
        AlsoEmpty,
        WithPhantom(std::marker::PhantomData<String>),
    }

    impl StateVariant for ZstState {
        fn variant_name(&self) -> &str {
            match self {
                ZstState::Empty => "Empty",
                ZstState::AlsoEmpty => "AlsoEmpty",
                ZstState::WithPhantom(_) => "WithPhantom",
            }
        }
    }

    #[derive(Clone, Debug)]
    struct ZstEvent;

    impl EventVariant for ZstEvent {
        fn variant_name(&self) -> &str {
            "ZstEvent"
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct ZstAction;

    struct ZstContext;
    
    impl FsmContext for ZstContext {}
    
    #[async_trait::async_trait]
    impl FsmAction for ZstAction {
        type Context = ZstContext;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            Ok(())
        }
    }

    let fsm = FsmBuilder::new(ZstState::Empty)
        .when("Empty")
            .on("ZstEvent", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: ZstState::AlsoEmpty,
                    actions: vec![ZstAction],
                })
            })
            .done()
        .when("AlsoEmpty")
            .on("ZstEvent", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: ZstState::WithPhantom(std::marker::PhantomData),
                    actions: vec![],
                })
            })
            .done()
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(ZstContext);

    // Test transitions with ZSTs
    let actions = machine.handle(ZstEvent, ctx.clone()).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(machine.state(), ZstState::AlsoEmpty));

    machine.handle(ZstEvent, ctx).await.unwrap();
    assert!(matches!(machine.state(), ZstState::WithPhantom(_)));
}

/// Test maximum handler complexity
#[tokio::test]
async fn test_complex_async_handlers() {
    use tokio::time::{sleep, timeout};
    use std::time::Duration;
    
    #[derive(Clone, Debug, PartialEq)]
    enum AsyncState {
        Idle,
        Working {
            tasks_completed: u32,
            parallel_tasks: Vec<String>,
        },
        Completed {
            total_tasks: u32,
            duration_ms: u64,
        },
    }

    impl StateVariant for AsyncState {
        fn variant_name(&self) -> &str {
            match self {
                AsyncState::Idle => "Idle",
                AsyncState::Working { .. } => "Working",
                AsyncState::Completed { .. } => "Completed",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum AsyncEvent {
        StartWork { task_count: u32 },
        ProcessBatch { batch_size: u32 },
        FinishWork,
    }

    impl EventVariant for AsyncEvent {
        fn variant_name(&self) -> &str {
            match self {
                AsyncEvent::StartWork { .. } => "StartWork",
                AsyncEvent::ProcessBatch { .. } => "ProcessBatch",
                AsyncEvent::FinishWork => "FinishWork",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum AsyncAction {
        SpawnTasks(u32),
        UpdateProgress(f32),
        GenerateReport(String),
    }

    struct AsyncContext {
        start_time: std::time::Instant,
        external_service: Arc<RwLock<Vec<String>>>,
    }
    
    impl FsmContext for AsyncContext {}
    
    #[async_trait::async_trait]
    impl FsmAction for AsyncAction {
        type Context = AsyncContext;
        
        async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
            match self {
                AsyncAction::SpawnTasks(count) => {
                    ctx.external_service.write().await.push(format!("Spawning {} tasks", count));
                    Ok(())
                }
                AsyncAction::UpdateProgress(progress) => {
                    ctx.external_service.write().await.push(format!("Progress: {}%", progress));
                    Ok(())
                }
                AsyncAction::GenerateReport(report) => {
                    ctx.external_service.write().await.push(format!("Report: {}", report));
                    Ok(())
                }
            }
        }
    }

    let fsm = FsmBuilder::new(AsyncState::Idle)
        .when("Idle")
            .on("StartWork", |_state, event, ctx: Arc<AsyncContext>| {
                let task_count = match event {
                    AsyncEvent::StartWork { task_count } => task_count.clone(),
                    _ => unreachable!(),
                };
                
                let external_service = ctx.external_service.clone();
                
                async move {
                    // Simulate complex async operations
                    let handles: Vec<_> = (0..3).map(|i| {
                        let service = external_service.clone();
                        tokio::spawn(async move {
                            sleep(Duration::from_millis(10)).await;
                            service.write().await.push(format!("Task {} initialized", i));
                        })
                    }).collect();
                    
                    // Wait for all initialization tasks
                    for handle in handles {
                        handle.await.map_err(|e| e.to_string())?;
                    }
                    
                    // Perform some CPU-bound work
                    let parallel_tasks: Vec<String> = (0..task_count)
                        .map(|i| format!("Task-{}", i))
                        .collect();
                    
                    Ok(Transition {
                        next_state: AsyncState::Working {
                            tasks_completed: 0,
                            parallel_tasks,
                        },
                        actions: vec![AsyncAction::SpawnTasks(task_count)],
                    })
                }
            })
            .done()
        
        .when("Working")
            .on("ProcessBatch", |state, event, ctx| {
                let (tasks_completed, mut parallel_tasks) = match state {
                    AsyncState::Working { tasks_completed, parallel_tasks } => 
                        (*tasks_completed, parallel_tasks.clone()),
                    _ => unreachable!(),
                };
                
                let batch_size = match event {
                    AsyncEvent::ProcessBatch { batch_size } => batch_size.clone() as usize,
                    _ => unreachable!(),
                };
                
                let external_service = ctx.external_service.clone();
                
                async move {
                    // Process batch with timeout
                    let result = timeout(Duration::from_secs(1), async {
                        let batch: Vec<_> = parallel_tasks
                            .drain(..batch_size.min(parallel_tasks.len()))
                            .collect();
                        
                        // Simulate processing each item
                        for task in &batch {
                            sleep(Duration::from_millis(5)).await;
                            external_service.write().await.push(format!("Processed: {}", task));
                        }
                        
                        batch.len() as u32
                    }).await;
                    
                    match result {
                        Ok(processed) => {
                            let new_completed = tasks_completed + processed;
                            let progress = if parallel_tasks.is_empty() {
                                100.0
                            } else {
                                (new_completed as f32 / (new_completed + parallel_tasks.len() as u32) as f32) * 100.0
                            };
                            
                            Ok(Transition {
                                next_state: AsyncState::Working {
                                    tasks_completed: new_completed,
                                    parallel_tasks,
                                },
                                actions: vec![AsyncAction::UpdateProgress(progress)],
                            })
                        }
                        Err(_) => Err("Processing timeout".to_string()),
                    }
                }
            })
            .on("FinishWork", |state, _event, ctx| {
                let tasks_completed = match state {
                    AsyncState::Working { tasks_completed, .. } => tasks_completed.clone(),
                    _ => unreachable!(),
                };
                
                let duration_ms = ctx.start_time.elapsed().as_millis() as u64;
                
                async move {
                    // Generate final report
                    let report = format!(
                        "Completed {} tasks in {}ms",
                        tasks_completed,
                        duration_ms
                    );
                    
                    Ok(Transition {
                        next_state: AsyncState::Completed {
                            total_tasks: tasks_completed,
                            duration_ms,
                        },
                        actions: vec![AsyncAction::GenerateReport(report)],
                    })
                }
            })
            .done()
        
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(AsyncContext {
        start_time: std::time::Instant::now(),
        external_service: Arc::new(RwLock::new(Vec::new())),
    });

    // Start work
    machine.handle(
        AsyncEvent::StartWork { task_count: 10 },
        ctx.clone()
    ).await.unwrap();

    // Process in batches
    for _ in 0..3 {
        let actions = machine.handle(
            AsyncEvent::ProcessBatch { batch_size: 3 },
            ctx.clone()
        ).await.unwrap();
        
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], AsyncAction::UpdateProgress(_)));
    }

    // Finish
    let actions = machine.handle(AsyncEvent::FinishWork, ctx.clone()).await.unwrap();
    assert!(matches!(actions[0], AsyncAction::GenerateReport(_)));
    
    // Verify external service was called
    let service_logs = ctx.external_service.read().await;
    assert!(service_logs.len() > 10); // Init tasks + processed tasks
}

/// Test state equality with complex data
#[tokio::test]
async fn test_state_equality_edge_cases() {
    use std::f64::NAN;
    
    #[derive(Clone, Debug)]
    enum FloatState {
        WithNan { value: f64 },
        WithInfinity { value: f64 },
        Normal { value: f64 },
    }

    // Custom PartialEq to handle NaN
    impl PartialEq for FloatState {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (FloatState::WithNan { value: v1 }, FloatState::WithNan { value: v2 }) => {
                    v1.is_nan() && v2.is_nan() || v1 == v2
                }
                (FloatState::WithInfinity { value: v1 }, FloatState::WithInfinity { value: v2 }) => v1 == v2,
                (FloatState::Normal { value: v1 }, FloatState::Normal { value: v2 }) => v1 == v2,
                _ => false,
            }
        }
    }

    impl StateVariant for FloatState {
        fn variant_name(&self) -> &str {
            match self {
                FloatState::WithNan { .. } => "WithNan",
                FloatState::WithInfinity { .. } => "WithInfinity",
                FloatState::Normal { .. } => "Normal",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum FloatEvent {
        MakeNan,
        MakeInfinity,
        Normalize,
    }

    impl EventVariant for FloatEvent {
        fn variant_name(&self) -> &str {
            match self {
                FloatEvent::MakeNan => "MakeNan",
                FloatEvent::MakeInfinity => "MakeInfinity",
                FloatEvent::Normalize => "Normalize",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct FloatAction;

    struct FloatContext;
    
    impl FsmContext for FloatContext {}
    
    #[async_trait::async_trait]
    impl FsmAction for FloatAction {
        type Context = FloatContext;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            Ok(())
        }
    }

    let fsm = FsmBuilder::new(FloatState::Normal { value: 1.0 })
        .when("Normal")
            .on("MakeNan", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: FloatState::WithNan { value: NAN },
                    actions: vec![],
                })
            })
            .on("MakeInfinity", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: FloatState::WithInfinity { value: f64::INFINITY },
                    actions: vec![],
                })
            })
            .done()
        .from_any()
            .on("Normalize", |_state, _event, _ctx| async {
                Ok(Transition {
                    next_state: FloatState::Normal { value: 0.0 },
                    actions: vec![FloatAction],
                })
            })
            .done()
        .build();

    let mut machine = fsm;
    let ctx = Arc::new(FloatContext);

    // Test NaN handling
    machine.handle(FloatEvent::MakeNan, ctx.clone()).await.unwrap();
    if let FloatState::WithNan { value } = machine.state() {
        assert!(value.is_nan());
    }

    // Test normalizing from NaN state
    let actions = machine.handle(FloatEvent::Normalize, ctx).await.unwrap();
    assert_eq!(actions.len(), 1); // Actions still execute even if state doesn't change
}

/// Test with extremely large numbers of states
#[tokio::test]
async fn test_many_states() {
    #[derive(Clone, Debug, PartialEq)]
    enum ManyStates {
        State0,
        State1,
        State2,
        State3,
        State4,
        State5,
        State6,
        State7,
        State8,
        State9,
        // ... imagine 100+ more states
    }

    impl StateVariant for ManyStates {
        fn variant_name(&self) -> &str {
            match self {
                ManyStates::State0 => "State0",
                ManyStates::State1 => "State1",
                ManyStates::State2 => "State2",
                ManyStates::State3 => "State3",
                ManyStates::State4 => "State4",
                ManyStates::State5 => "State5",
                ManyStates::State6 => "State6",
                ManyStates::State7 => "State7",
                ManyStates::State8 => "State8",
                ManyStates::State9 => "State9",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum ManyEvents {
        Next,
        JumpTo(u8),
        Reset,
    }

    impl EventVariant for ManyEvents {
        fn variant_name(&self) -> &str {
            match self {
                ManyEvents::Next => "Next",
                ManyEvents::JumpTo(_) => "JumpTo",
                ManyEvents::Reset => "Reset",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct Transition_;

    struct EmptyCtx;
    
    impl FsmContext for EmptyCtx {}
    
    #[async_trait::async_trait]
    impl FsmAction for Transition_ {
        type Context = EmptyCtx;
        
        async fn execute(&self, _ctx: &Self::Context) -> Result<(), String> {
            Ok(())
        }
    }

    let mut builder = FsmBuilder::<ManyStates, ManyEvents, EmptyCtx, Transition_>::new(ManyStates::State0);
    
    // Add transitions for each state
    for i in 0..9 {
        let state_name = format!("State{}", i);
        let next_state = match i {
            0 => ManyStates::State1,
            1 => ManyStates::State2,
            2 => ManyStates::State3,
            3 => ManyStates::State4,
            4 => ManyStates::State5,
            5 => ManyStates::State6,
            6 => ManyStates::State7,
            7 => ManyStates::State8,
            8 => ManyStates::State9,
            _ => ManyStates::State0,
        };
        
        builder = builder
            .when(&state_name)
            .on("Next", move |_state, _event, _ctx| {
                let next = next_state.clone();
                async move {
                    Ok(Transition {
                        next_state: next,
                        actions: vec![],
                    })
                }
            })
            .done();
    }
    
    // Add reset from any state
    builder = builder
        .from_any()
        .on("Reset", |_state, _event, _ctx| async {
            Ok(Transition {
                next_state: ManyStates::State0,
                actions: vec![],
            })
        })
        .done();
    
    let mut machine = builder.build();
    let ctx = Arc::new(EmptyCtx);

    // Test sequential transitions
    for expected in 1..=9 {
        machine.handle(ManyEvents::Next, ctx.clone()).await.unwrap();
        let state_num = match machine.state() {
            ManyStates::State1 => 1,
            ManyStates::State2 => 2,
            ManyStates::State3 => 3,
            ManyStates::State4 => 4,
            ManyStates::State5 => 5,
            ManyStates::State6 => 6,
            ManyStates::State7 => 7,
            ManyStates::State8 => 8,
            ManyStates::State9 => 9,
            _ => 0,
        };
        assert_eq!(state_num, expected);
    }

    // Test reset from any state
    machine.handle(ManyEvents::Reset, ctx).await.unwrap();
    assert!(matches!(machine.state(), ManyStates::State0));
}

// Note: Phantom type safety is demonstrated in test_compile_safety.rs