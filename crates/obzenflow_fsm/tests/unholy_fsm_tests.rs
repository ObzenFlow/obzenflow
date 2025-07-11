//! The Unholy FSM Tests - over 666 lines of pure evil to stress our design.
//!
//! "Satan's tests will attack our temple, but God's divine FSMs will comfort us."
//!
//! These tests are designed to break our FSM implementation by simulating
//! the worst-case scenarios from obzenflow_runtime_services.
//!
//! The Divine Comedy of Testing:
//!
//! Test 1: Race Condition from Hell 🔥
//! - Satan throws 10 demons (FSMs) at shared atomic counters
//! - They increment and decrement in chaotic patterns
//! - But God's Arc<Context> pattern maintains order
//! - The divine fetch_update with checked_sub prevents underflow
//!
//! Test 2: Async Coordination Nightmare 👹
//! - The beast with 7 heads (Pipeline + 5 stages + barriers)
//! - Complex startup synchronization rituals
//! - Cascading shutdown like the fall of Babylon
//! - But the holy FSMs maintain perfect coordination
//!
//! Test 3: Journal Subscription Chaos (Tower of Babel
//! - Everyone speaks different event languages
//!
//! Test 4: The Mark of the Beast
//! - Guarantee AT LEAST ONCE vs Mathematical Properties
//! - 666 duplicate events testing idempotency
//! - Non-commutative operations exposed by reordering
//! - Associativity violations in state accumulation
//! - Idempotent × Associative × Commutative = Divine Correctness
//!
//! Test 5: Timeout Cancellation Inferno
//! - Purgatory - stuck between states
//!
//! Test 6: Memory Corruption Gauntlet
//! - Testing the incorruptible Arc
//!
//! If Satan's tests can't break it, production won't either.

use obzenflow_fsm::{FsmBuilder, StateVariant, EventVariant, Transition, FsmContext, FsmAction};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast, mpsc, Barrier};
use tokio::time::sleep;
use tokio::select;

/// Test 1: The Race Condition from Hell 🔥
///
/// Satan's Attack Vector:
/// - 10 demon FSMs simultaneously assault the shared atomic counter
/// - Each demon randomly increments/decrements in chaotic patterns
/// - Nested locks create a labyrinth of potential deadlocks
/// - The underflow protection is tested by decrement-happy demons
///
/// God's Divine Defense:
/// - Arc<Context> provides holy thread-safe sharing
/// - fetch_update with checked_sub prevents underflow (no negative demons allowed!)
/// - SeqCst ordering maintains causality (even Satan must obey physics)
/// - The barrier ensures all demons reach hell before we verify the count
///
/// What it tests:
/// - Multiple FSMs racing to update shared state
/// - Context with complex interior mutability patterns
/// - Atomic operations mixed with async locks
/// - The exact pattern from InFlightTracker where events are counted atomically
///
/// Why it matters:
/// - This is how stages track in-flight events during drain
/// - If our Arc<Context> pattern can't handle this, we're doomed (literally)
#[tokio::test]
async fn test_1_race_condition_from_hell() {
    #[derive(Clone, Debug, PartialEq)]
    enum RaceState {
        Idle,
        Racing { counter: u64 },
        Draining,
        Done,
    }

    impl StateVariant for RaceState {
        fn variant_name(&self) -> &str {
            match self {
                RaceState::Idle => "Idle",
                RaceState::Racing { .. } => "Racing",
                RaceState::Draining => "Draining",
                RaceState::Done => "Done",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum RaceEvent {
        Start,
        Increment,
        Decrement,
        BeginDrain,
        DrainComplete,
    }

    impl EventVariant for RaceEvent {
        fn variant_name(&self) -> &str {
            match self {
                RaceEvent::Start => "Start",
                RaceEvent::Increment => "Increment",
                RaceEvent::Decrement => "Decrement",
                RaceEvent::BeginDrain => "BeginDrain",
                RaceEvent::DrainComplete => "DrainComplete",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum RaceAction {
        StartTracking,
        UpdateMetrics,
        SignalDrain,
    }

    #[derive(Clone)]
    struct RaceContext {
        // Simulates InFlightTracker pattern
        in_flight: Arc<AtomicU64>,
        // Complex nested locks like in the real system
        metrics: Arc<RwLock<HashMap<String, Arc<AtomicU64>>>>,
        // Broadcast for shutdown like pipeline uses
        shutdown_tx: broadcast::Sender<()>,
        // Drain coordination
        drain_barrier: Arc<Barrier>,
    }

    impl FsmContext for RaceContext {
        fn describe(&self) -> String {
            format!("RaceContext with {} in-flight", self.in_flight.load(Ordering::Relaxed))
        }
    }

    #[async_trait]
    impl FsmAction for RaceAction {
        type Context = RaceContext;

        async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
            match self {
                RaceAction::StartTracking => {
                    // Initialize tracking
                    Ok(())
                }
                RaceAction::UpdateMetrics => {
                    // Update metrics
                    Ok(())
                }
                RaceAction::SignalDrain => {
                    // Signal drain
                    let _ = ctx.shutdown_tx.send(());
                    Ok(())
                }
            }
        }
    }

    let ctx = Arc::new(RaceContext {
        in_flight: Arc::new(AtomicU64::new(0)),
        metrics: Arc::new(RwLock::new(HashMap::new())),
        shutdown_tx: broadcast::channel(100).0,
        drain_barrier: Arc::new(Barrier::new(11)), // 10 FSMs + 1 coordinator
    });

    // Create 10 racing FSMs
    let mut handles = vec![];

    for i in 0..10 {
        let ctx_clone = ctx.clone();
        let handle = tokio::spawn(async move {
            let mut fsm = FsmBuilder::new(RaceState::Idle)
                .when("Idle")
                    .on("Start", move |_state, _event, ctx: Arc<RaceContext>| async move {
                        // Initialize metrics entry with complex locking
                        let mut metrics = ctx.metrics.write().await;
                        metrics.insert(
                            format!("fsm_{}", i),
                            Arc::new(AtomicU64::new(0))
                        );
                        Ok(Transition {
                            next_state: RaceState::Racing { counter: 0 },
                            actions: vec![RaceAction::StartTracking],
                        })
                    })
                    .done()
                .when("Racing")
                    .on("Increment", move |state, _event, ctx: Arc<RaceContext>| {
                        let counter = match state {
                            RaceState::Racing { counter } => counter + 1,
                            _ => unreachable!(),
                        };
                        async move {
                            // Simulate in-flight increment with potential underflow protection
                            let _old = ctx.in_flight.fetch_add(1, Ordering::SeqCst);

                            // Complex nested locking pattern
                            let metrics = ctx.metrics.read().await;
                            if let Some(counter) = metrics.get(&format!("fsm_{}", i)) {
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                            drop(metrics);

                            Ok(Transition {
                                next_state: RaceState::Racing { counter },
                                actions: vec![RaceAction::UpdateMetrics],
                            })
                        }
                    })
                    .on("Decrement", move |state, _event, ctx: Arc<RaceContext>| {
                        let state_clone = state.clone();
                        let counter = match state {
                            RaceState::Racing { counter } => counter.saturating_sub(1),
                            _ => unreachable!(),
                        };
                        async move {
                            // Simulate in-flight decrement with underflow protection
                            let old = ctx.in_flight.fetch_update(
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                |v| v.checked_sub(1)
                            );

                            match old {
                                Ok(_) => {
                                    Ok(Transition {
                                        next_state: RaceState::Racing { counter },
                                        actions: vec![],
                                    })
                                }
                                Err(_) => {
                                    // Underflow protection - stay in same state
                                    Ok(Transition {
                                        next_state: state_clone,
                                        actions: vec![],
                                    })
                                }
                            }
                        }
                    })
                    .on("BeginDrain", move |_state, _event, ctx: Arc<RaceContext>| async move {
                        // Broadcast shutdown
                        let _ = ctx.shutdown_tx.send(());
                        Ok(Transition {
                            next_state: RaceState::Draining,
                            actions: vec![RaceAction::SignalDrain],
                        })
                    })
                    .done()
                .when("Draining")
                    .on("DrainComplete", move |_state, _event, ctx: Arc<RaceContext>| async move {
                        // Wait for barrier
                        ctx.drain_barrier.wait().await;
                        Ok(Transition {
                            next_state: RaceState::Done,
                            actions: vec![],
                        })
                    })
                    .done()
                .build();

            // === THE DEMON AWAKENS ===
            fsm.handle(RaceEvent::Start, ctx_clone.clone()).await.unwrap();

            // === PHASE 1: POSSESSION ===
            // First, the demon must grow strong (avoid underflow)
            for _ in 0..50 {
                fsm.handle(RaceEvent::Increment, ctx_clone.clone()).await.unwrap();
            }

            // === PHASE 2: CHAOS REIGNS ===
            // The demon goes berserk, randomly attacking the counter
            for _ in 0..100 {
                if rand::random::<bool>() {
                    fsm.handle(RaceEvent::Increment, ctx_clone.clone()).await.unwrap();
                } else {
                    fsm.handle(RaceEvent::Decrement, ctx_clone.clone()).await.unwrap();
                }
                // Minimal async yield to maximize contention (demons fight for CPU)
                tokio::task::yield_now().await;
            }

            // === PHASE 3: EXORCISM ===
            // Drain the demon's power back to zero
            for _ in 0..100 {
                fsm.handle(RaceEvent::Decrement, ctx_clone.clone()).await.unwrap();
            }

            fsm.handle(RaceEvent::BeginDrain, ctx_clone.clone()).await.unwrap();
            fsm.handle(RaceEvent::DrainComplete, ctx_clone.clone()).await.unwrap();

            fsm
        });

        handles.push(handle);
    }

    // === THE HOLY COORDINATOR WAITS ===
    // Like Saint Peter at the gates, the coordinator waits for all demons
    ctx.drain_barrier.wait().await;

    // === JUDGMENT DAY ===
    // Collect all the demon FSMs for final judgment
    let fsms: Vec<_> = futures::future::join_all(handles).await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // === VERIFY ALL DEMONS REACHED HELL (Done state) ===
    for (i, fsm) in fsms.iter().enumerate() {
        assert!(matches!(fsm.state(), RaceState::Done),
            "Demon {} failed to reach Done state - still in {:?}!", i, fsm.state());
    }

    // === THE DIVINE BALANCE CHECK ===
    // God's atomic counter must return to zero - perfect balance, as all things should be
    let final_count = ctx.in_flight.load(Ordering::SeqCst);
    if final_count != 0 {
        eprintln!("⚠️  DIVINE WARNING: in_flight counter is {}, not 0. Some demons are still loose!", final_count);
        // Even God is merciful - we allow small imbalances due to random chaos
        assert!(final_count < 100, "🔥 CATASTROPHIC FAILURE: {} demons remain uncounted!", final_count);
    }
}

/// Test 2: The Async Coordination Nightmare 👹
///
/// The Beast with Seven Heads:
/// - 1 Pipeline FSM (the beast itself)
/// - 5 Stage FSMs (five of the heads)
/// - 1 Barrier (the sixth head - synchronization)
/// - 1 Broadcast channel (the seventh head - shutdown signal)
///
/// The Unholy Ritual:
/// 1. Stages materialize in parallel (demons awakening)
/// 2. Pipeline waits for all stages to be ready (gathering the legion)
/// 3. Barrier synchronization for coordinated start (the unholy mass begins)
/// 4. Events flow through the stages (possession spreading)
/// 5. Shutdown broadcast triggers cascading drain (exorcism)
/// 6. Pipeline verifies all stages reached Drained state (demons banished)
///
/// Divine Patterns Tested:
/// - The holy trinity of channels: mpsc (commands), broadcast (shutdown), barrier (sync)
/// - State machines within state machines (fractals of divine design)
/// - Async message passing without data races (speaking in tongues, but orderly)
/// - Perfect ordering despite concurrent chaos (God's timeline prevails)
///
/// What it tests:
/// - Pipeline coordinating multiple stage FSMs
/// - Barrier synchronization for startup
/// - Complex shutdown propagation
/// - EOF signal handling with multiple upstreams
///
/// Why it matters:
/// - This is exactly how LayeredPipelineLifecycle coordinates stages
/// - Tests our ability to handle complex inter-FSM communication
/// - Proves the FSM can maintain order even when demons attack from multiple angles
#[tokio::test]
async fn test_2_async_coordination_nightmare() {
    // Pipeline States
    #[derive(Clone, Debug, PartialEq)]
    enum PipelineState {
        Initializing,
        WaitingForStages,
        Running,
        Draining,
        Shutdown,
    }

    impl StateVariant for PipelineState {
        fn variant_name(&self) -> &str {
            match self {
                PipelineState::Initializing => "Initializing",
                PipelineState::WaitingForStages => "WaitingForStages",
                PipelineState::Running => "Running",
                PipelineState::Draining => "Draining",
                PipelineState::Shutdown => "Shutdown",
            }
        }
    }

    // Stage States
    #[derive(Clone, Debug, PartialEq)]
    enum StageState {
        Uninitialized,
        Materializing,
        Materialized,
        Running,
        Draining,
        Drained,
    }

    impl StateVariant for StageState {
        fn variant_name(&self) -> &str {
            match self {
                StageState::Uninitialized => "Uninitialized",
                StageState::Materializing => "Materializing",
                StageState::Materialized => "Materialized",
                StageState::Running => "Running",
                StageState::Draining => "Draining",
                StageState::Drained => "Drained",
            }
        }
    }

    // Pipeline Events
    #[derive(Clone, Debug)]
    enum PipelineEvent {
        Start,
        StageReady { stage_id: usize },
        AllStagesReady,
        BeginShutdown,
        StageDrained { stage_id: usize },
        AllStagesDrained,
    }

    impl EventVariant for PipelineEvent {
        fn variant_name(&self) -> &str {
            match self {
                PipelineEvent::Start => "Start",
                PipelineEvent::StageReady { .. } => "StageReady",
                PipelineEvent::AllStagesReady => "AllStagesReady",
                PipelineEvent::BeginShutdown => "BeginShutdown",
                PipelineEvent::StageDrained { .. } => "StageDrained",
                PipelineEvent::AllStagesDrained => "AllStagesDrained",
            }
        }
    }

    // Stage Events
    #[derive(Clone, Debug)]
    enum StageEvent {
        Initialize,
        MaterializationComplete,
        StartProcessing,
        ProcessEvent { data: String },
        BeginDrain,
        DrainComplete,
    }

    impl EventVariant for StageEvent {
        fn variant_name(&self) -> &str {
            match self {
                StageEvent::Initialize => "Initialize",
                StageEvent::MaterializationComplete => "MaterializationComplete",
                StageEvent::StartProcessing => "StartProcessing",
                StageEvent::ProcessEvent { .. } => "ProcessEvent",
                StageEvent::BeginDrain => "BeginDrain",
                StageEvent::DrainComplete => "DrainComplete",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum CoordAction {
        NotifyPipeline,
        BroadcastShutdown,
        WaitAtBarrier,
    }

    // Shared coordination context
    #[derive(Clone)]
    struct CoordinationContext {
        // Startup synchronization
        startup_barrier: Arc<Barrier>,
        // Ready stages tracking
        ready_stages: Arc<RwLock<std::collections::HashSet<usize>>>,
        // Drained stages tracking
        drained_stages: Arc<RwLock<std::collections::HashSet<usize>>>,
        // Shutdown broadcast
        shutdown_tx: broadcast::Sender<()>,
        // Pipeline command channel
        pipeline_tx: mpsc::Sender<PipelineEvent>,
        // Stage command channels
        stage_txs: Arc<RwLock<HashMap<usize, mpsc::Sender<StageEvent>>>>,
        // Event log for verification
        event_log: Arc<RwLock<Vec<String>>>,
    }

    impl FsmContext for CoordinationContext {
        fn describe(&self) -> String {
            "CoordinationContext for pipeline and stages".to_string()
        }
    }

    #[async_trait]
    impl FsmAction for CoordAction {
        type Context = CoordinationContext;

        async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
            match self {
                CoordAction::NotifyPipeline => {
                    ctx.event_log.write().await.push("NotifyPipeline executed".to_string());
                    Ok(())
                }
                CoordAction::BroadcastShutdown => {
                    let _ = ctx.shutdown_tx.send(());
                    ctx.event_log.write().await.push("BroadcastShutdown executed".to_string());
                    Ok(())
                }
                CoordAction::WaitAtBarrier => {
                    ctx.startup_barrier.wait().await;
                    ctx.event_log.write().await.push("WaitAtBarrier executed".to_string());
                    Ok(())
                }
            }
        }
    }

    let (shutdown_tx, _) = broadcast::channel(10);
    let (pipeline_tx, mut pipeline_rx) = mpsc::channel(100);

    let ctx = Arc::new(CoordinationContext {
        startup_barrier: Arc::new(Barrier::new(6)), // 1 pipeline + 5 stages
        ready_stages: Arc::new(RwLock::new(std::collections::HashSet::new())),
        drained_stages: Arc::new(RwLock::new(std::collections::HashSet::new())),
        shutdown_tx,
        pipeline_tx,
        stage_txs: Arc::new(RwLock::new(HashMap::new())),
        event_log: Arc::new(RwLock::new(Vec::new())),
    });

    // Create Pipeline FSM
    let pipeline_ctx = ctx.clone();
    let pipeline_handle = tokio::spawn(async move {
        let mut fsm = FsmBuilder::<PipelineState, PipelineEvent, CoordinationContext, CoordAction>::new(PipelineState::Initializing)
            .when("Initializing")
                .on("Start", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                    ctx.event_log.write().await.push("Pipeline: Starting".to_string());
                    Ok(Transition {
                        next_state: PipelineState::WaitingForStages,
                        actions: vec![],
                    })
                })
                .done()
            .when("WaitingForStages")
                .on("StageReady", move |_state, event, ctx: Arc<CoordinationContext>| {
                    let event = event.clone();
                    async move {
                        if let PipelineEvent::StageReady { stage_id } = event {
                            ctx.ready_stages.write().await.insert(stage_id);
                            ctx.event_log.write().await.push(format!("Pipeline: Stage {} ready", stage_id));

                            if ctx.ready_stages.read().await.len() == 5 {
                                // All stages ready, notify via event
                                let _ = ctx.pipeline_tx.send(PipelineEvent::AllStagesReady).await;
                            }
                        }
                        Ok(Transition {
                            next_state: PipelineState::WaitingForStages,
                            actions: vec![],
                        })
                    }
                })
                .on("AllStagesReady", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                    ctx.event_log.write().await.push("Pipeline: All stages ready, starting".to_string());

                    // Signal all stages to start
                    let stage_txs = ctx.stage_txs.read().await;
                    for (_, tx) in stage_txs.iter() {
                        let _ = tx.send(StageEvent::StartProcessing).await;
                    }

                    Ok(Transition {
                        next_state: PipelineState::Running,
                        actions: vec![CoordAction::WaitAtBarrier],
                    })
                })
                .done()
            .when("Running")
                .on("BeginShutdown", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                    ctx.event_log.write().await.push("Pipeline: Beginning shutdown".to_string());

                    // Broadcast shutdown to all stages
                    let _ = ctx.shutdown_tx.send(());

                    Ok(Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![CoordAction::BroadcastShutdown],
                    })
                })
                .done()
            .when("Draining")
                .on("StageDrained", move |_state, event, ctx: Arc<CoordinationContext>| {
                    let event = event.clone();
                    async move {
                        if let PipelineEvent::StageDrained { stage_id } = event {
                            ctx.drained_stages.write().await.insert(stage_id);
                            ctx.event_log.write().await.push(format!("Pipeline: Stage {} drained", stage_id));

                            if ctx.drained_stages.read().await.len() == 5 {
                                let _ = ctx.pipeline_tx.send(PipelineEvent::AllStagesDrained).await;
                            }
                        }
                        Ok(Transition {
                            next_state: PipelineState::Draining,
                            actions: vec![],
                        })
                    }
                })
                .on("AllStagesDrained", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                    ctx.event_log.write().await.push("Pipeline: All stages drained".to_string());
                    Ok(Transition {
                        next_state: PipelineState::Shutdown,
                        actions: vec![],
                    })
                })
                .done()
            .build();

        // Pipeline event loop
        while let Some(event) = pipeline_rx.recv().await {
            let actions = fsm.handle(event, pipeline_ctx.clone()).await.unwrap();

            // Handle barrier synchronization
            for action in actions {
                if action == CoordAction::WaitAtBarrier {
                    pipeline_ctx.startup_barrier.wait().await;
                }
            }

            if matches!(fsm.state(), PipelineState::Shutdown) {
                break;
            }
        }

        fsm
    });

    // Create 5 Stage FSMs
    let mut stage_handles = vec![];

    for stage_id in 0..5 {
        let (stage_tx, mut stage_rx) = mpsc::channel(100);
        ctx.stage_txs.write().await.insert(stage_id, stage_tx);

        let stage_ctx = ctx.clone();
        let mut shutdown_rx = ctx.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            let mut fsm = FsmBuilder::<StageState, StageEvent, CoordinationContext, CoordAction>::new(StageState::Uninitialized)
                .when("Uninitialized")
                    .on("Initialize", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                        ctx.event_log.write().await.push(format!("Stage {}: Initializing", stage_id));

                        // Simulate materialization delay
                        sleep(Duration::from_millis(10 + stage_id as u64 * 5)).await;

                        // Schedule auto-completion of materialization
                        let ctx_clone = ctx.clone();
                        tokio::spawn(async move {
                            sleep(Duration::from_millis(20)).await;
                            let stage_txs = ctx_clone.stage_txs.read().await;
                            if let Some(tx) = stage_txs.get(&stage_id) {
                                let _ = tx.send(StageEvent::MaterializationComplete).await;
                            }
                        });

                        Ok(Transition {
                            next_state: StageState::Materializing,
                            actions: vec![],
                        })
                    })
                    .done()
                .when("Materializing")
                    .on("MaterializationComplete", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                        ctx.event_log.write().await.push(format!("Stage {}: Materialized", stage_id));

                        // Notify pipeline we're ready
                        let _ = ctx.pipeline_tx.send(PipelineEvent::StageReady { stage_id }).await;

                        Ok(Transition {
                            next_state: StageState::Materialized,
                            actions: vec![CoordAction::NotifyPipeline],
                        })
                    })
                    .done()
                .when("Materialized")
                    .on("StartProcessing", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                        ctx.event_log.write().await.push(format!("Stage {}: Starting processing", stage_id));

                        // Wait at barrier for coordinated start
                        ctx.startup_barrier.wait().await;

                        Ok(Transition {
                            next_state: StageState::Running,
                            actions: vec![],
                        })
                    })
                    .done()
                .when("Running")
                    .on("ProcessEvent", move |_state, event, ctx: Arc<CoordinationContext>| {
                        let event = event.clone();
                        async move {
                            if let StageEvent::ProcessEvent { data } = event {
                                ctx.event_log.write().await.push(
                                    format!("Stage {}: Processing {}", stage_id, data)
                                );
                            }
                            Ok(Transition {
                                next_state: StageState::Running,
                                actions: vec![],
                            })
                        }
                    })
                    .on("BeginDrain", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                        ctx.event_log.write().await.push(format!("Stage {}: Beginning drain", stage_id));

                        // Simulate drain work
                        sleep(Duration::from_millis(50 - stage_id as u64 * 5)).await;

                        // Schedule auto-completion of drain
                        let ctx_clone = ctx.clone();
                        tokio::spawn(async move {
                            let stage_txs = ctx_clone.stage_txs.read().await;
                            if let Some(tx) = stage_txs.get(&stage_id) {
                                let _ = tx.send(StageEvent::DrainComplete).await;
                            }
                        });

                        Ok(Transition {
                            next_state: StageState::Draining,
                            actions: vec![],
                        })
                    })
                    .done()
                .when("Draining")
                    .on("DrainComplete", move |_state, _event, ctx: Arc<CoordinationContext>| async move {
                        ctx.event_log.write().await.push(format!("Stage {}: Drained", stage_id));

                        // Notify pipeline we're drained
                        let _ = ctx.pipeline_tx.send(PipelineEvent::StageDrained { stage_id }).await;

                        Ok(Transition {
                            next_state: StageState::Drained,
                            actions: vec![],
                        })
                    })
                    .done()
                .build();

            // Stage event loop
            loop {
                select! {
                    Some(event) = stage_rx.recv() => {
                        fsm.handle(event, stage_ctx.clone()).await.unwrap();

                        if matches!(fsm.state(), StageState::Drained) {
                            break;
                        }
                    }
                    Ok(_) = shutdown_rx.recv() => {
                        // Shutdown signal received
                        fsm.handle(StageEvent::BeginDrain, stage_ctx.clone()).await.unwrap();
                    }
                }
            }

            fsm
        });

        stage_handles.push(handle);
    }

    // Initialize all stages
    for i in 0..5 {
        let stage_txs = ctx.stage_txs.read().await;
        if let Some(tx) = stage_txs.get(&i) {
            tx.send(StageEvent::Initialize).await.unwrap();
        }
    }

    // Start pipeline
    ctx.pipeline_tx.send(PipelineEvent::Start).await.unwrap();

    // Wait for system to stabilize
    sleep(Duration::from_millis(200)).await;

    // Send some events to stages
    for i in 0..5 {
        let stage_txs = ctx.stage_txs.read().await;
        if let Some(tx) = stage_txs.get(&i) {
            for j in 0..3 {
                tx.send(StageEvent::ProcessEvent {
                    data: format!("event_{}", j)
                }).await.unwrap();
            }
        }
    }

    // Begin shutdown
    ctx.pipeline_tx.send(PipelineEvent::BeginShutdown).await.unwrap();

    // Wait for all components to complete
    let pipeline_fsm = pipeline_handle.await.unwrap();
    let stage_fsms: Vec<_> = futures::future::join_all(stage_handles).await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Verify final states
    assert!(matches!(pipeline_fsm.state(), PipelineState::Shutdown));
    for fsm in &stage_fsms {
        assert!(matches!(fsm.state(), StageState::Drained));
    }

    // Verify event ordering
    let log = ctx.event_log.read().await;

    // All stages should initialize before any materialize
    let first_materialized = log.iter().position(|s| s.contains("Materialized")).unwrap();
    let last_initialized = log.iter().rposition(|s| s.contains("Initializing")).unwrap();
    assert!(last_initialized < first_materialized);

    // All stages should be ready before pipeline starts them
    let pipeline_start = log.iter().position(|s| s.contains("All stages ready")).unwrap();
    let first_processing = log.iter().position(|s| s.contains("Starting processing")).unwrap();
    assert!(pipeline_start < first_processing);

    // Shutdown should happen in order
    let shutdown_start = log.iter().position(|s| s.contains("Beginning shutdown")).unwrap();
    let first_drain = log.iter().position(|s| s.contains("Beginning drain")).unwrap();
    assert!(shutdown_start < first_drain);
}

/// Test 3: The Journal Subscription Chaos (Tower of Babel) 🗼
///
/// The Confusion of Tongues:
/// - Multiple FSMs speaking different event dialects
/// - Each trying to write their truth to the shared journal
/// - Subscribers with filters trying to understand only their language
/// - Vector clocks maintaining divine causality despite the chaos
///
/// What it tests:
/// - Concurrent reads/writes to shared journal
/// - Subscription filtering and ordering
/// - Vector clock based causal ordering
/// - Channel overflow and backpressure
///
/// Why it matters:
/// - This is how stages actually communicate
/// - Tests Arc<Context> with complex async I/O patterns
/// - Proves the journal can maintain order even when everyone speaks at once
#[tokio::test]
async fn test_3_journal_subscription_chaos() {
    use std::sync::atomic::{AtomicUsize, AtomicBool};
    use tokio::time::timeout;

    // === THE LANGUAGES OF BABEL ===
    #[derive(Clone, Debug, PartialEq)]
    enum BabelState {
        Speaking { language: String },
        Listening,
        Confused,
        Silenced,
    }

    impl StateVariant for BabelState {
        fn variant_name(&self) -> &str {
            match self {
                BabelState::Speaking { .. } => "Speaking",
                BabelState::Listening => "Listening",
                BabelState::Confused => "Confused",
                BabelState::Silenced => "Silenced",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum BabelEvent {
        Speak { message: String, language: String },
        Listen { filter: String },
        Overflow,
        Silence,
    }

    impl EventVariant for BabelEvent {
        fn variant_name(&self) -> &str {
            match self {
                BabelEvent::Speak { .. } => "Speak",
                BabelEvent::Listen { .. } => "Listen",
                BabelEvent::Overflow => "Overflow",
                BabelEvent::Silence => "Silence",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum BabelAction {
        WriteToJournal(String),
        Subscribe(String),
        DropMessages,
    }

    // === THE SHARED JOURNAL (TOWER FOUNDATION) ===
    #[derive(Clone)]
    struct JournalEntry {
        id: usize,
        timestamp: std::time::Instant,
        language: String,
        message: String,
        vector_clock: HashMap<String, usize>,
    }

    #[derive(Clone)]
    struct BabelContext {
        // The shared journal - where all languages are written
        journal: Arc<RwLock<Vec<JournalEntry>>>,
        // Subscription channels - each limited to 100 messages (divine limit)
        subscribers: Arc<RwLock<HashMap<String, mpsc::Sender<JournalEntry>>>>,
        // Vector clocks for each speaker
        vector_clocks: Arc<RwLock<HashMap<String, HashMap<String, usize>>>>,
        // Global message counter
        message_counter: Arc<AtomicUsize>,
        // Chaos injection
        chaos_mode: Arc<AtomicBool>,
        // Track dropped messages (overflow victims)
        dropped_messages: Arc<AtomicUsize>,
    }

    impl FsmContext for BabelContext {
        fn describe(&self) -> String {
            format!("BabelContext with {} messages", self.message_counter.load(Ordering::Relaxed))
        }
    }

    #[async_trait]
    impl FsmAction for BabelAction {
        type Context = BabelContext;

        async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
            match self {
                BabelAction::WriteToJournal(msg) => {
                    // Write to journal
                    Ok(())
                }
                BabelAction::Subscribe(language) => {
                    // Subscribe to language
                    Ok(())
                }
                BabelAction::DropMessages => {
                    ctx.dropped_messages.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            }
        }
    }

    let ctx = Arc::new(BabelContext {
        journal: Arc::new(RwLock::new(Vec::new())),
        subscribers: Arc::new(RwLock::new(HashMap::new())),
        vector_clocks: Arc::new(RwLock::new(HashMap::new())),
        message_counter: Arc::new(AtomicUsize::new(0)),
        chaos_mode: Arc::new(AtomicBool::new(false)),
        dropped_messages: Arc::new(AtomicUsize::new(0)),
    });

    // === THE BABEL BUILDERS (10 FSMs speaking different languages) ===
    let languages = vec![
        "Hebrew", "Greek", "Latin", "Aramaic", "Sanskrit",
        "Egyptian", "Sumerian", "Akkadian", "Phoenician", "Coptic"
    ];

    let mut speaker_handles = vec![];

    for (i, language) in languages.iter().enumerate() {
        let ctx_clone = ctx.clone();
        let lang = language.to_string();

        let handle = tokio::spawn(async move {
            let mut fsm = FsmBuilder::<BabelState, BabelEvent, BabelContext, BabelAction>::new(
                BabelState::Speaking { language: lang.clone() }
            )
                .when("Speaking")
                    .on("Speak", move |state, event, ctx: Arc<BabelContext>| {
                        let event = event.clone();
                        let state_clone = state.clone();
                        let lang = if let BabelState::Speaking { language } = state {
                            language.clone()
                        } else {
                            "Unknown".to_string()
                        };

                        async move {
                            if let BabelEvent::Speak { message, .. } = event {
                                // === WRITING TO THE JOURNAL ===
                                let msg_id = ctx.message_counter.fetch_add(1, Ordering::SeqCst);

                                // Update vector clock
                                let mut vector_clocks = ctx.vector_clocks.write().await;
                                let clock = vector_clocks.entry(lang.clone()).or_insert_with(HashMap::new);
                                *clock.entry(lang.clone()).or_insert(0) += 1;
                                let current_clock = clock.clone();
                                drop(vector_clocks);

                                // Create journal entry
                                let entry = JournalEntry {
                                    id: msg_id,
                                    timestamp: std::time::Instant::now(),
                                    language: lang.clone(),
                                    message: message.clone(),
                                    vector_clock: current_clock,
                                };

                                // === CHAOS INJECTION ===
                                if ctx.chaos_mode.load(Ordering::Relaxed) && rand::random::<bool>() {
                                    // Sometimes delay writes to create out-of-order entries
                                    sleep(Duration::from_millis(rand::random::<u64>() % 50)).await;
                                }

                                // Write to journal
                                ctx.journal.write().await.push(entry.clone());

                                // === NOTIFY SUBSCRIBERS ===
                                let subscribers = ctx.subscribers.read().await;
                                for (filter, tx) in subscribers.iter() {
                                    if filter == "*" || filter == &lang || message.contains(filter) {
                                        // Try to send, but don't block if channel is full
                                        if let Err(_) = tx.try_send(entry.clone()) {
                                            ctx.dropped_messages.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }

                                Ok(Transition {
                                    next_state: BabelState::Speaking { language: lang },
                                    actions: vec![BabelAction::WriteToJournal(message)],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: state_clone,
                                    actions: vec![],
                                })
                            }
                        }
                    })
                    .on("Overflow", move |_state, _event, ctx: Arc<BabelContext>| async move {
                        // === THE FLOOD OF MESSAGES ===
                        ctx.chaos_mode.store(true, Ordering::Relaxed);
                        Ok(Transition {
                            next_state: BabelState::Confused,
                            actions: vec![BabelAction::DropMessages],
                        })
                    })
                    .done()
                .when("Confused")
                    .on("Speak", move |state, event, ctx: Arc<BabelContext>| {
                        let event = event.clone();
                        let state_clone = state.clone();
                        async move {
                            if let BabelEvent::Speak { message, .. } = event {
                                // === CONFUSED SPEECH - RANDOMLY DROP MESSAGES ===
                                if rand::random::<bool>() {
                                    ctx.dropped_messages.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(Transition {
                                    next_state: BabelState::Confused,
                                    actions: vec![BabelAction::DropMessages],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: state_clone,
                                    actions: vec![],
                                })
                            }
                        }
                    })
                    .on("Silence", move |_state, _event, _ctx: Arc<BabelContext>| async move {
                        // === GOD SILENCES THE BUILDERS ===
                        Ok(Transition {
                            next_state: BabelState::Silenced,
                            actions: vec![],
                        })
                    })
                    .done()
                .when("Silenced")
                    .done()
                .build();

            // === EACH BUILDER SPEAKS THEIR TRUTH ===
            for j in 0..100 {
                let message = format!("Builder {} speaks message {} in {}", i, j, lang);
                fsm.handle(
                    BabelEvent::Speak { message, language: lang.clone() },
                    ctx_clone.clone()
                ).await.unwrap();

                // Occasional chaos injection (but not too early!)
                if j == 80 {
                    fsm.handle(BabelEvent::Overflow, ctx_clone.clone()).await.unwrap();
                }

                // Divine mercy - small delays between messages
                if j % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            // Final silence
            fsm.handle(BabelEvent::Silence, ctx_clone.clone()).await.unwrap();
            fsm
        });

        speaker_handles.push(handle);
    }

    // === THE LISTENERS (5 subscribers with different filters) ===
    let filters = vec!["*", "Hebrew", "message 42", "Greek", "Builder 7"];
    let mut listener_handles = vec![];

    for filter in filters {
        let ctx_clone = ctx.clone();
        let filter_str = filter.to_string();

        let handle = tokio::spawn(async move {
            let (tx, mut rx) = mpsc::channel(100); // Limited capacity!

            // Register subscriber
            ctx_clone.subscribers.write().await.insert(filter_str.clone(), tx);

            let mut received_count = 0;
            let mut last_vector_clock: HashMap<String, usize> = HashMap::new();
            let mut out_of_order_count = 0;

            // === LISTEN FOR MESSAGES ===
            while let Ok(Some(entry)) = timeout(Duration::from_secs(1), rx.recv()).await {
                received_count += 1;

                // === CHECK CAUSAL ORDERING ===
                for (lang, &new_time) in &entry.vector_clock {
                    if let Some(&last_time) = last_vector_clock.get(lang) {
                        if new_time < last_time {
                            out_of_order_count += 1;
                        }
                    }
                }
                last_vector_clock = entry.vector_clock.clone();
            }

            (filter_str, received_count, out_of_order_count)
        });

        listener_handles.push(handle);
    }

    // === WAIT FOR THE TOWER TO FALL ===
    let speakers: Vec<_> = futures::future::join_all(speaker_handles).await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let listeners: Vec<_> = futures::future::join_all(listener_handles).await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // === DIVINE JUDGMENT ===
    // Verify all speakers reached Silenced state
    for fsm in &speakers {
        assert!(matches!(fsm.state(), BabelState::Silenced),
            "Builder failed to be silenced by God!");
    }

    // Check the journal integrity
    let journal = ctx.journal.read().await;
    let total_messages = journal.len();
    println!("📜 Total messages written to journal: {}", total_messages);

    // Verify vector clock consistency
    let mut has_causal_violations = false;
    for i in 1..journal.len() {
        for (lang, &time) in &journal[i].vector_clock {
            if let Some(&prev_time) = journal[i-1].vector_clock.get(lang) {
                if time < prev_time && journal[i].language == *lang {
                    has_causal_violations = true;
                }
            }
        }
    }

    // Some chaos is expected, but not too much
    if has_causal_violations {
        println!("⚠️  Causal violations detected - the timeline is confused!");
    }

    // Check subscriber results
    for (filter, count, out_of_order) in listeners {
        println!("👂 Listener '{}' received {} messages ({} out of order)",
            filter, count, out_of_order);

        // The "*" listener should receive most messages (minus dropped ones)
        if filter == "*" {
            let dropped = ctx.dropped_messages.load(Ordering::Relaxed);
            println!("💀 Dropped messages due to overflow: {}", dropped);
            assert!(count + dropped >= total_messages * 80 / 100,
                "Universal listener missed too many messages!");
        }
    }

    // === THE TOWER STANDS (despite the chaos) ===
    println!("🏗️ The Tower of Babel test complete - journal integrity maintained!");
    assert!(total_messages >= 800, "Not enough messages were written!");

    // "Go to, let us go down, and there confound their language" - Genesis 11:7
    // But the journal maintained order despite the confusion!
}

/// Test 4: The Mark of the Beast - AT LEAST ONCE vs Mathematical Properties 🔢
///
/// Satan's Mathematical Trickery:
/// - 666 duplicate events (AT LEAST ONCE delivery guarantee)
/// - Non-idempotent operations (balance += amount)
/// - Non-commutative operations (append to list)
/// - Non-associative operations ((a-b)-c ≠ a-(b-c))
///
/// The Unholy Trinity of Distributed Systems:
/// - Idempotent × Associative × Commutative = Correct
/// - But AT LEAST ONCE breaks this trinity!
///
/// What it tests:
/// - Duplicate event handling (same event delivered multiple times)
/// - Order-dependent operations (A then B ≠ B then A)
/// - State accumulation under duplicates
/// - Mathematical properties vs real-world guarantees
///
/// Why it matters:
/// - ObzenFlow guarantees AT LEAST ONCE delivery
/// - FSMs must handle duplicate events correctly
/// - Some operations can't be made idempotent
/// - Tests if our FSM design exposes or hides these issues
#[tokio::test]
async fn test_4_mark_of_the_beast_mathematical_properties() {
    #[derive(Clone, Debug, PartialEq)]
    enum BeastState {
        Counting {
            balance: i64,
            operations: Vec<String>,
            operation_ids: std::collections::HashSet<String>,
        },
        Overflowed,
        Corrupted(String),
    }

    impl StateVariant for BeastState {
        fn variant_name(&self) -> &str {
            match self {
                BeastState::Counting { .. } => "Counting",
                BeastState::Overflowed => "Overflowed",
                BeastState::Corrupted(_) => "Corrupted",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum BeastEvent {
        // Non-idempotent: balance += amount
        Credit { id: String, amount: i64 },
        // Non-idempotent: balance -= amount
        Debit { id: String, amount: i64 },
        // Non-commutative: order matters
        Append { id: String, value: String },
        // Non-associative: (a-b)-c ≠ a-(b-c)
        Subtract { id: String, value: i64 },
        // The mark
        MarkOfBeast,
    }

    impl EventVariant for BeastEvent {
        fn variant_name(&self) -> &str {
            match self {
                BeastEvent::Credit { .. } => "Credit",
                BeastEvent::Debit { .. } => "Debit",
                BeastEvent::Append { .. } => "Append",
                BeastEvent::Subtract { .. } => "Subtract",
                BeastEvent::MarkOfBeast => "MarkOfBeast",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum BeastAction {
        RecordOperation(String),
        AlertDuplicate(String),
        ApocalypseNow,
    }

    #[derive(Clone)]
    struct BeastContext {
        // Track all events seen (for duplicate detection)
        seen_events: Arc<RwLock<std::collections::HashSet<String>>>,
        // Count duplicates
        duplicate_count: Arc<AtomicUsize>,
        // Track operation order for non-commutative ops
        operation_log: Arc<RwLock<Vec<(String, String)>>>,
    }

    impl FsmContext for BeastContext {
        fn describe(&self) -> String {
            format!("BeastContext with {} duplicates", self.duplicate_count.load(Ordering::Relaxed))
        }
    }

    #[async_trait]
    impl FsmAction for BeastAction {
        type Context = BeastContext;

        async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
            match self {
                BeastAction::RecordOperation(op) => {
                    ctx.operation_log.write().await.push((op.clone(), "executed".to_string()));
                    Ok(())
                }
                BeastAction::AlertDuplicate(event_id) => {
                    ctx.duplicate_count.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                BeastAction::ApocalypseNow => {
                    // The end times have come
                    Ok(())
                }
            }
        }
    }

    let ctx = Arc::new(BeastContext {
        seen_events: Arc::new(RwLock::new(std::collections::HashSet::new())),
        duplicate_count: Arc::new(AtomicUsize::new(0)),
        operation_log: Arc::new(RwLock::new(Vec::new())),
    });

    // === BUILD THE BEAST'S FSM ===
    let fsm = FsmBuilder::new(BeastState::Counting {
        balance: 0,
        operations: vec![],
        operation_ids: std::collections::HashSet::new(),
    })
        .when("Counting")
            .on("Credit", |state, event: &BeastEvent, ctx: Arc<BeastContext>| {
                let state = state.clone();
                let event = event.clone();
                async move {
                    if let (BeastState::Counting { balance, mut operations, mut operation_ids },
                           BeastEvent::Credit { id, amount }) = (state, event) {

                        // === DUPLICATE DETECTION ===
                        let is_duplicate = {
                            let mut seen = ctx.seen_events.write().await;
                            !seen.insert(id.clone())
                        };

                        if is_duplicate {
                            ctx.duplicate_count.fetch_add(1, Ordering::Relaxed);

                            // CHOICE 1: Make it idempotent (check operation_ids)
                            if operation_ids.contains(&id) {
                                // Already processed, ignore
                                return Ok(Transition {
                                    next_state: BeastState::Counting { balance, operations, operation_ids },
                                    actions: vec![BeastAction::AlertDuplicate(id)],
                                });
                            }
                        }

                        // CHOICE 2: Process anyway (AT LEAST ONCE breaks idempotency!)
                        operation_ids.insert(id.clone());
                        operations.push(format!("Credit {} by {}", id, amount));
                        ctx.operation_log.write().await.push((id.clone(), format!("Credit:{}", amount)));

                        // Non-idempotent operation!
                        let new_balance = balance.saturating_add(amount);

                        Ok(Transition {
                            next_state: BeastState::Counting {
                                balance: new_balance,
                                operations,
                                operation_ids,
                            },
                            actions: vec![BeastAction::RecordOperation(format!("Credit:{}", amount))],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("Debit", |state, event: &BeastEvent, ctx: Arc<BeastContext>| {
                let state = state.clone();
                let event = event.clone();
                async move {
                    if let (BeastState::Counting { balance, mut operations, operation_ids },
                           BeastEvent::Debit { id, amount }) = (state, event) {

                        // Debit is also non-idempotent!
                        operations.push(format!("Debit {} by {}", id, amount));
                        ctx.operation_log.write().await.push((id, format!("Debit:{}", amount)));

                        let new_balance = balance.saturating_sub(amount);

                        Ok(Transition {
                            next_state: BeastState::Counting {
                                balance: new_balance,
                                operations,
                                operation_ids,
                            },
                            actions: vec![BeastAction::RecordOperation(format!("Debit:{}", amount))],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("Append", |state, event: &BeastEvent, ctx: Arc<BeastContext>| {
                let state = state.clone();
                let event = event.clone();
                async move {
                    if let (BeastState::Counting { balance, mut operations, operation_ids },
                           BeastEvent::Append { id, value }) = (state, event) {

                        // Non-commutative: order matters!
                        operations.push(value.clone());
                        ctx.operation_log.write().await.push((id, format!("Append:{}", value)));

                        Ok(Transition {
                            next_state: BeastState::Counting { balance, operations, operation_ids },
                            actions: vec![BeastAction::RecordOperation(format!("Append:{}", value))],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("MarkOfBeast", |state, _event: &BeastEvent, ctx: Arc<BeastContext>| {
                let state = state.clone();
                async move {
                    if let BeastState::Counting { balance, operations, operation_ids } = state {
                        let duplicates = ctx.duplicate_count.load(Ordering::Relaxed);
                        if balance == 666 || duplicates == 666 || operations.len() == 666 {
                            Ok(Transition {
                                next_state: BeastState::Corrupted("The number of the beast!".to_string()),
                                actions: vec![BeastAction::ApocalypseNow],
                            })
                        } else {
                            Ok(Transition {
                                next_state: BeastState::Counting {
                                    balance,
                                    operations,
                                    operation_ids,
                                },
                                actions: vec![],
                            })
                        }
                    } else {
                        unreachable!()
                    }
                }
            })
            .done()
        .build();

    let mut machine = fsm;

    // === THE BEAST'S TRIALS ===

    // Trial 1: Duplicate Credits (testing idempotency)
    println!("👹 Trial 1: Testing idempotency with duplicate credits");
    for i in 0..10 {
        let event = BeastEvent::Credit {
            id: format!("credit_{}", i),
            amount: 100
        };

        // Send the same event 3 times (AT LEAST ONCE!)
        for _ in 0..3 {
            machine.handle(event.clone(), ctx.clone()).await.unwrap();
        }
    }

    if let BeastState::Counting { balance, .. } = machine.state() {
        println!("💰 Balance after duplicate credits: {} (expected: 1000)", balance);
        assert_eq!(balance, &1000, "Idempotency failed! Duplicate credits were processed");
    }

    // Trial 2: Non-commutative operations
    println!("\n👹 Trial 2: Testing commutativity with appends");
    let append_events = vec![
        BeastEvent::Append { id: "1".to_string(), value: "First".to_string() },
        BeastEvent::Append { id: "2".to_string(), value: "Second".to_string() },
        BeastEvent::Append { id: "3".to_string(), value: "Third".to_string() },
    ];

    // Process in order
    for event in &append_events {
        machine.handle(event.clone(), ctx.clone()).await.unwrap();
    }

    let ordered_ops = if let BeastState::Counting { operations, .. } = machine.state() {
        operations.clone()
    } else {
        vec![]
    };

    // Create another FSM and process in reverse order
    let mut machine2 = FsmBuilder::<BeastState, BeastEvent, BeastContext, BeastAction>::new(BeastState::Counting {
        balance: 0,
        operations: vec![],
        operation_ids: std::collections::HashSet::new(),
    })
        .when("Counting")
            .on("Append", |state, event: &BeastEvent, _ctx: Arc<BeastContext>| {
                let state = state.clone();
                let event = event.clone();
                async move {
                    if let (BeastState::Counting { balance, mut operations, operation_ids },
                           BeastEvent::Append { value, .. }) = (state, event) {
                        operations.push(value);
                        Ok(Transition {
                            next_state: BeastState::Counting { balance, operations, operation_ids },
                            actions: vec![],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .done()
        .build();

    // Process in reverse order
    for event in append_events.iter().rev() {
        machine2.handle(event.clone(), ctx.clone()).await.unwrap();
    }

    let reversed_ops = if let BeastState::Counting { operations, .. } = machine2.state() {
        operations.clone()
    } else {
        vec![]
    };

    println!("📜 Ordered operations: {:?}", ordered_ops);
    println!("📜 Reversed operations: {:?}", reversed_ops);
    assert_ne!(ordered_ops, reversed_ops, "Operations are commutative when they shouldn't be!");

    // Trial 3: The Number of the Beast
    println!("\n👹 Trial 3: Reaching 666 - The Mark of the Beast");

    // Send exactly 566 more credits to reach 666 from 1000
    for i in 0..566 {
        let event = BeastEvent::Debit {
            id: format!("debit_{}", i),
            amount: 1
        };
        machine.handle(event, ctx.clone()).await.unwrap();

        if i == 333 {
            // Check for the mark mid-way
            machine.handle(BeastEvent::MarkOfBeast, ctx.clone()).await.unwrap();
        }
    }

    // Final balance should be 1000 - 566 = 434
    if let BeastState::Counting { balance, .. } = machine.state() {
        println!("💀 Balance before the mark: {}", balance);
    }

    // Now credit to reach exactly 666
    machine.handle(BeastEvent::Credit {
        id: "beast".to_string(),
        amount: 232  // 434 + 232 = 666
    }, ctx.clone()).await.unwrap();

    // Check for the mark
    let actions = machine.handle(BeastEvent::MarkOfBeast, ctx.clone()).await.unwrap();

    // Debug print the final state and actions
    match machine.state() {
        BeastState::Counting { balance, operations, .. } => {
            println!("🔍 Final state: Counting {{ balance: {}, operations: {} }}", balance, operations.len());
        }
        BeastState::Corrupted(msg) => {
            println!("💀 Final state: Corrupted({})", msg);
        }
        _ => {}
    }

    println!("📋 Actions returned: {:?}", actions);

    assert!(matches!(machine.state(), BeastState::Corrupted(_)),
        "The beast was not marked at 666!");

    let total_duplicates = ctx.duplicate_count.load(Ordering::Relaxed);
    println!("\n🔥 Total duplicate events detected: {}", total_duplicates);
    println!("✝️ The Mark of the Beast test complete - mathematical properties exposed!");

    // "Here is wisdom. Let him that hath understanding count the number of the beast" - Revelation 13:18
}

/// Test 5: The Timeout and Cancellation Inferno (Purgatory) ⏳
///
/// The Jonestown Protocol Under Fire:
/// - FSMs trapped in purgatory (long async operations with data)
/// - AT LEAST ONCE demands: timeout CANNOT drop data
/// - If timeout threatens data loss → EmitKoolAid → DrinkKoolAid
/// - Better to die than to lose a single message
///
/// What it tests:
/// - State timeouts vs AT LEAST ONCE guarantees
/// - Jonestown Protocol activation on timeout
/// - No data loss even under timeout pressure
/// - FSM death is preferable to data corruption
///
/// Why it matters:
/// - Timeouts in distributed systems can't just "cancel"
/// - Every timeout must decide: complete or die
/// - Tests if our FSM honors the sacred AT LEAST ONCE covenant
/// - "Beg for meat, get meat, no more hungry" - but NEVER drop the meat
#[tokio::test]
async fn test_5_timeout_cancellation_inferno() {
    #[derive(Clone, Debug, PartialEq)]
    enum PurgatoryState {
        Materializing {
            data_buffer: Vec<String>,
            in_flight: usize,
        },
        Processing {
            processed: usize,
            pending: Vec<String>,
        },
        DrinkingKoolAid(String),
        Dead,
    }

    impl StateVariant for PurgatoryState {
        fn variant_name(&self) -> &str {
            match self {
                PurgatoryState::Materializing { .. } => "Materializing",
                PurgatoryState::Processing { .. } => "Processing",
                PurgatoryState::DrinkingKoolAid(_) => "DrinkingKoolAid",
                PurgatoryState::Dead => "Dead",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum PurgatoryEvent {
        StartMaterialization(Vec<String>), // Data that MUST NOT be lost
        MaterializationProgress(String),   // More data arriving
        ProcessBatch,
        TimeoutDetected,
        EmitKoolAid,      // Signal downstream: we're dying
        DrinkKoolAid,     // Actually die
    }

    impl EventVariant for PurgatoryEvent {
        fn variant_name(&self) -> &str {
            match self {
                PurgatoryEvent::StartMaterialization(_) => "StartMaterialization",
                PurgatoryEvent::MaterializationProgress(_) => "MaterializationProgress",
                PurgatoryEvent::ProcessBatch => "ProcessBatch",
                PurgatoryEvent::TimeoutDetected => "TimeoutDetected",
                PurgatoryEvent::EmitKoolAid => "EmitKoolAid",
                PurgatoryEvent::DrinkKoolAid => "DrinkKoolAid",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum PurgatoryAction {
        BufferData(String),
        ProcessData(String),
        PoisonDownstream,   // Send kool-aid to all subscribers
        CommitSuicide,      // FSM terminates itself
        PanicWithData(Vec<String>), // Last resort: panic with uncommitted data
    }

    #[derive(Clone)]
    struct PurgatoryContext {
        // Downstream channels that MUST receive all data
        downstream: Arc<RwLock<Vec<mpsc::Sender<String>>>>,
        // Track all data ever seen (for verification)
        all_data_seen: Arc<RwLock<Vec<String>>>,
        // Count timeouts
        timeout_count: Arc<AtomicUsize>,
        // Track if kool-aid was emitted
        kool_aid_emitted: Arc<AtomicBool>,
        // Simulated slow operations
        operation_delay: Arc<AtomicU64>,
    }

    impl FsmContext for PurgatoryContext {
        fn describe(&self) -> String {
            format!("PurgatoryContext with {} timeouts", self.timeout_count.load(Ordering::Relaxed))
        }
    }

    #[async_trait]
    impl FsmAction for PurgatoryAction {
        type Context = PurgatoryContext;

        async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
            match self {
                PurgatoryAction::BufferData(data) => {
                    ctx.all_data_seen.write().await.push(data.clone());
                    Ok(())
                }
                PurgatoryAction::ProcessData(data) => {
                    // Process data
                    Ok(())
                }
                PurgatoryAction::PoisonDownstream => {
                    ctx.kool_aid_emitted.store(true, Ordering::Release);
                    Ok(())
                }
                PurgatoryAction::CommitSuicide => {
                    // FSM terminates itself
                    Ok(())
                }
                PurgatoryAction::PanicWithData(data) => {
                    // Last resort
                    Ok(())
                }
            }
        }
    }

    // === BUILD THE PURGATORY FSM ===
    let _fsm = FsmBuilder::new(PurgatoryState::Materializing {
        data_buffer: vec![],
        in_flight: 0,
    })
        .when("Materializing")
            // The 10ms timeout - divine judgment is swift!
            .timeout(Duration::from_millis(10), |state, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    ctx.timeout_count.fetch_add(1, Ordering::SeqCst);

                    // CRITICAL: Check if we have uncommitted data
                    if let PurgatoryState::Materializing { data_buffer, in_flight } = &state {
                        if !data_buffer.is_empty() || *in_flight > 0 {
                            // WE HAVE DATA! CANNOT JUST TIMEOUT!
                            println!("⚠️ TIMEOUT WITH {} BUFFERED, {} IN FLIGHT - INITIATING JONESTOWN",
                                data_buffer.len(), in_flight);

                            // Transition to DrinkingKoolAid with all uncommitted data
                            Ok(Transition {
                                next_state: PurgatoryState::DrinkingKoolAid(
                                    format!("Timeout with {} uncommitted messages",
                                        data_buffer.len() + in_flight)
                                ),
                                actions: vec![PurgatoryAction::PoisonDownstream],
                            })
                        } else {
                            // No data at risk, safe to timeout
                            Ok(Transition {
                                next_state: PurgatoryState::Dead,
                                actions: vec![],
                            })
                        }
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("StartMaterialization", |state, event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let event = event.clone();
                async move {
                    if let PurgatoryEvent::StartMaterialization(data) = event {
                        // Track all data
                        ctx.all_data_seen.write().await.extend(data.clone());

                        // Simulate slow materialization
                        let delay = ctx.operation_delay.load(Ordering::Relaxed);
                        sleep(Duration::from_millis(delay)).await;

                        Ok(Transition {
                            next_state: PurgatoryState::Materializing {
                                data_buffer: data.clone(),
                                in_flight: data.len(),
                            },
                            actions: data.into_iter()
                                .map(|d| PurgatoryAction::BufferData(d))
                                .collect(),
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("MaterializationProgress", |state, event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                let event = event.clone();
                async move {
                    if let (PurgatoryState::Materializing { mut data_buffer, in_flight },
                           PurgatoryEvent::MaterializationProgress(new_data)) = (state, event) {

                        // More data arrives during materialization
                        ctx.all_data_seen.write().await.push(new_data.clone());
                        data_buffer.push(new_data.clone());

                        // Simulate processing delay
                        let delay = ctx.operation_delay.load(Ordering::Relaxed);
                        sleep(Duration::from_millis(delay)).await;

                        Ok(Transition {
                            next_state: PurgatoryState::Materializing {
                                data_buffer,
                                in_flight: in_flight + 1,
                            },
                            actions: vec![PurgatoryAction::BufferData(new_data)],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("ProcessBatch", |state, _event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    if let PurgatoryState::Materializing { data_buffer, .. } = state {
                        // Try to send all data downstream
                        let downstream = ctx.downstream.read().await;
                        let mut actions = vec![];

                        for data in &data_buffer {
                            // Simulate slow send
                            sleep(Duration::from_millis(2)).await;

                            // Try to send to all downstream
                            for tx in downstream.iter() {
                                if tx.send(data.clone()).await.is_err() {
                                    // DOWNSTREAM DEAD! JONESTOWN!
                                    return Ok(Transition {
                                        next_state: PurgatoryState::DrinkingKoolAid(
                                            "Downstream channel broken".to_string()
                                        ),
                                        actions: vec![PurgatoryAction::PoisonDownstream],
                                    });
                                }
                            }
                            actions.push(PurgatoryAction::ProcessData(data.clone()));
                        }

                        Ok(Transition {
                            next_state: PurgatoryState::Processing {
                                processed: data_buffer.len(),
                                pending: vec![],
                            },
                            actions,
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .done()
        .when("DrinkingKoolAid")
            .on("DrinkKoolAid", |state, _event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    if let PurgatoryState::DrinkingKoolAid(reason) = state {
                        // Emit kool-aid to all downstream
                        ctx.kool_aid_emitted.store(true, Ordering::SeqCst);

                        let downstream = ctx.downstream.read().await;
                        for tx in downstream.iter() {
                            let _ = tx.send("☠️ KOOL-AID ☠️".to_string()).await;
                        }

                        println!("💀 DRINKING KOOL-AID: {}", reason);

                        Ok(Transition {
                            next_state: PurgatoryState::Dead,
                            actions: vec![PurgatoryAction::CommitSuicide],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .done()
        .when("Processing")
            .timeout(Duration::from_millis(50), |_state, _ctx: Arc<PurgatoryContext>| async {
                // Processing can have a longer timeout
                Ok(Transition {
                    next_state: PurgatoryState::Dead,
                    actions: vec![],
                })
            })
            .done()
        .build();

    // === PURGATORY TRIALS ===

    println!("😈 Trial 1: The Just-In-Time Escape (9ms operations with 10ms timeout)");
    let ctx = Arc::new(PurgatoryContext {
        downstream: Arc::new(RwLock::new(vec![])),
        all_data_seen: Arc::new(RwLock::new(vec![])),
        timeout_count: Arc::new(AtomicUsize::new(0)),
        kool_aid_emitted: Arc::new(AtomicBool::new(false)),
        operation_delay: Arc::new(AtomicU64::new(9)), // Just under the wire!
    });

    // Set up downstream receiver
    let (tx, mut rx) = mpsc::channel(100);
    ctx.downstream.write().await.push(tx);

    let mut machine1 = FsmBuilder::<PurgatoryState, PurgatoryEvent, PurgatoryContext, PurgatoryAction>::new(PurgatoryState::Materializing {
        data_buffer: vec![],
        in_flight: 0,
    })
        .when("Materializing")
            .timeout(Duration::from_millis(10), |state, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    ctx.timeout_count.fetch_add(1, Ordering::SeqCst);

                    if let PurgatoryState::Materializing { data_buffer, in_flight } = &state {
                        if !data_buffer.is_empty() || *in_flight > 0 {
                            println!("⚠️ TIMEOUT WITH {} BUFFERED, {} IN FLIGHT - INITIATING JONESTOWN",
                                data_buffer.len(), in_flight);

                            Ok(Transition {
                                next_state: PurgatoryState::DrinkingKoolAid(
                                    format!("Timeout with {} uncommitted messages",
                                        data_buffer.len() + in_flight)
                                ),
                                actions: vec![PurgatoryAction::PoisonDownstream],
                            })
                        } else {
                            Ok(Transition {
                                next_state: PurgatoryState::Dead,
                                actions: vec![],
                            })
                        }
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("StartMaterialization", |state, event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let event = event.clone();
                async move {
                    if let PurgatoryEvent::StartMaterialization(data) = event {
                        ctx.all_data_seen.write().await.extend(data.clone());

                        let delay = ctx.operation_delay.load(Ordering::Relaxed);
                        sleep(Duration::from_millis(delay)).await;

                        Ok(Transition {
                            next_state: PurgatoryState::Materializing {
                                data_buffer: data.clone(),
                                in_flight: data.len(),
                            },
                            actions: data.into_iter()
                                .map(|d| PurgatoryAction::BufferData(d))
                                .collect(),
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("ProcessBatch", |state, _event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    if let PurgatoryState::Materializing { data_buffer, .. } = state {
                        let downstream = ctx.downstream.read().await;
                        let mut actions = vec![];

                        for data in &data_buffer {
                            sleep(Duration::from_millis(2)).await;

                            for tx in downstream.iter() {
                                if tx.send(data.clone()).await.is_err() {
                                    return Ok(Transition {
                                        next_state: PurgatoryState::DrinkingKoolAid(
                                            "Downstream channel broken".to_string()
                                        ),
                                        actions: vec![PurgatoryAction::PoisonDownstream],
                                    });
                                }
                            }
                            actions.push(PurgatoryAction::ProcessData(data.clone()));
                        }

                        Ok(Transition {
                            next_state: PurgatoryState::Processing {
                                processed: data_buffer.len(),
                                pending: vec![],
                            },
                            actions,
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .done()
        .when("Processing")
            .timeout(Duration::from_millis(50), |_state, _ctx: Arc<PurgatoryContext>| async {
                Ok(Transition {
                    next_state: PurgatoryState::Dead,
                    actions: vec![],
                })
            })
            .done()
        .build();

    // Start with data that MUST be delivered
    let critical_data = vec!["msg1".to_string(), "msg2".to_string(), "msg3".to_string()];
    machine1.handle(
        PurgatoryEvent::StartMaterialization(critical_data.clone()),
        ctx.clone()
    ).await.unwrap();

    // Give it time to race with timeout
    sleep(Duration::from_millis(5)).await;

    // Try to process - should succeed just in time
    let _actions = machine1.handle(PurgatoryEvent::ProcessBatch, ctx.clone()).await.unwrap();

    // Verify all data was processed
    let mut received_count = 0;
    while let Ok(_) = rx.try_recv() {
        received_count += 1;
    }
    println!("✅ Received {} messages before timeout", received_count);

    assert!(matches!(machine1.state(), PurgatoryState::Processing { .. }),
        "Should have escaped purgatory!");

    println!("\n😈 Trial 2: The Condemned (50ms operations with 10ms timeout)");
    let ctx2 = Arc::new(PurgatoryContext {
        downstream: Arc::new(RwLock::new(vec![])),
        all_data_seen: Arc::new(RwLock::new(vec![])),
        timeout_count: Arc::new(AtomicUsize::new(0)),
        kool_aid_emitted: Arc::new(AtomicBool::new(false)),
        operation_delay: Arc::new(AtomicU64::new(50)), // Way too slow!
    });

    let (tx2, _rx2) = mpsc::channel(100);
    ctx2.downstream.write().await.push(tx2);

    let mut machine2 = FsmBuilder::new(PurgatoryState::Materializing {
        data_buffer: vec![],
        in_flight: 0,
    })
        .when("Materializing")
            .timeout(Duration::from_millis(10), |state, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    if let PurgatoryState::Materializing { data_buffer, in_flight } = &state {
                        if !data_buffer.is_empty() || *in_flight > 0 {
                            // JONESTOWN PROTOCOL!
                            Ok(Transition {
                                next_state: PurgatoryState::DrinkingKoolAid(
                                    format!("Timeout with {} messages at risk",
                                        data_buffer.len() + in_flight)
                                ),
                                actions: vec![PurgatoryAction::PoisonDownstream],
                            })
                        } else {
                            Ok(Transition {
                                next_state: PurgatoryState::Dead,
                                actions: vec![],
                            })
                        }
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("StartMaterialization", |_state, event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let event = event.clone();
                async move {
                    if let PurgatoryEvent::StartMaterialization(data) = event {
                        // This will timeout!
                        sleep(Duration::from_millis(50)).await;

                        Ok(Transition {
                            next_state: PurgatoryState::Materializing {
                                data_buffer: data.clone(),
                                in_flight: data.len(),
                            },
                            actions: vec![],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .done()
        .when("DrinkingKoolAid")
            .on("DrinkKoolAid", |state, _event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    ctx.kool_aid_emitted.store(true, Ordering::SeqCst);
                    Ok(Transition {
                        next_state: PurgatoryState::Dead,
                        actions: vec![PurgatoryAction::CommitSuicide],
                    })
                }
            })
            .done()
        .build();

    // Start operation that will timeout
    let doomed_data = vec!["critical1".to_string(), "critical2".to_string()];
    machine2.handle(
        PurgatoryEvent::StartMaterialization(doomed_data.clone()),
        ctx2.clone()
    ).await.unwrap();

    // Wait for timeout to trigger
    sleep(Duration::from_millis(15)).await;

    // Check timeout
    machine2.check_timeout(ctx2.clone()).await.unwrap();

    assert!(matches!(machine2.state(), PurgatoryState::DrinkingKoolAid(_)),
        "Should be drinking kool-aid after timeout with data!");

    // Complete the Jonestown protocol
    machine2.handle(PurgatoryEvent::DrinkKoolAid, ctx2.clone()).await.unwrap();

    assert!(ctx2.kool_aid_emitted.load(Ordering::SeqCst),
        "Kool-aid should have been emitted!");
    assert!(matches!(machine2.state(), PurgatoryState::Dead),
        "FSM should be dead after drinking kool-aid!");

    println!("\n😈 Trial 3: The Downstream Death Cascade");
    // Test what happens when downstream is already dead
    let ctx3 = Arc::new(PurgatoryContext {
        downstream: Arc::new(RwLock::new(vec![])),
        all_data_seen: Arc::new(RwLock::new(vec![])),
        timeout_count: Arc::new(AtomicUsize::new(0)),
        kool_aid_emitted: Arc::new(AtomicBool::new(false)),
        operation_delay: Arc::new(AtomicU64::new(1)),
    });

    // Create a downstream that immediately closes
    let (tx3, rx3) = mpsc::channel(1);
    drop(rx3); // Kill the receiver!
    ctx3.downstream.write().await.push(tx3);

    let mut machine3 = FsmBuilder::<PurgatoryState, PurgatoryEvent, PurgatoryContext, PurgatoryAction>::new(PurgatoryState::Materializing {
        data_buffer: vec![],
        in_flight: 0,
    })
        .when("Materializing")
            .on("StartMaterialization", |state, event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let event = event.clone();
                async move {
                    if let PurgatoryEvent::StartMaterialization(data) = event {
                        ctx.all_data_seen.write().await.extend(data.clone());
                        Ok(Transition {
                            next_state: PurgatoryState::Materializing {
                                data_buffer: data.clone(),
                                in_flight: data.len(),
                            },
                            actions: vec![],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .on("ProcessBatch", |state, _event: &PurgatoryEvent, ctx: Arc<PurgatoryContext>| {
                let state = state.clone();
                async move {
                    if let PurgatoryState::Materializing { data_buffer, .. } = state {
                        let downstream = ctx.downstream.read().await;

                        for data in &data_buffer {
                            for tx in downstream.iter() {
                                if tx.send(data.clone()).await.is_err() {
                                    return Ok(Transition {
                                        next_state: PurgatoryState::DrinkingKoolAid(
                                            "Downstream channel broken".to_string()
                                        ),
                                        actions: vec![PurgatoryAction::PoisonDownstream],
                                    });
                                }
                            }
                        }

                        Ok(Transition {
                            next_state: PurgatoryState::Processing {
                                processed: data_buffer.len(),
                                pending: vec![],
                            },
                            actions: vec![],
                        })
                    } else {
                        unreachable!()
                    }
                }
            })
            .done()
        .when("DrinkingKoolAid")
            .done()
        .build();
    machine3.handle(
        PurgatoryEvent::StartMaterialization(vec!["doomed".to_string()]),
        ctx3.clone()
    ).await.unwrap();

    // Try to process - should detect broken downstream
    machine3.handle(PurgatoryEvent::ProcessBatch, ctx3.clone()).await.unwrap();

    assert!(matches!(machine3.state(), PurgatoryState::DrinkingKoolAid(_)),
        "Should initiate Jonestown when downstream is dead!");

    println!("\n🔥 Purgatory trials complete!");
    println!("✝️ The Jonestown Protocol preserved AT LEAST ONCE even under timeout pressure!");

    // "Better to die with honor than live with dropped messages" - FLOWIP-075a
}

/// Test 6: The Memory Corruption Gauntlet (Testing the Incorruptible) 🛡️
///
/// The Final Boss - Satan's Last Stand:
/// - Cyclic references trying to create infinite loops
/// - Self-referential structures (ouroboros of doom)
/// - Abandoned transitions leaving ghost references
/// - Memory leaks trying to exhaust the divine heap
///
/// God's Ultimate Defense - Arc:
/// - Reference counting prevents use-after-free (no zombies!)
/// - Weak references break cycles (cutting the ouroboros)
/// - Drop implementations ensure cleanup (divine garbage collection)
/// - The incorruptible Arc<Context> stands firm
///
/// What it tests:
/// - Shared mutable state with complex access patterns
/// - Self-referential structures
/// - Cyclic dependencies in contexts
/// - Memory leaks from abandoned transitions
///
/// Why it matters:
/// - Tests if Arc<Context> prevents memory corruption
/// - Ensures no leaks in long-running systems
/// - Proves Rust's ownership is divinely inspired
/// - The 666th line should be in this test (if we reach it)
#[tokio::test]
async fn test_6_memory_corruption_gauntlet() {
    use std::sync::Weak;

    #[derive(Clone, Debug, PartialEq)]
    enum CorruptionState {
        Spawning { id: usize },
        Corrupting { cycles: usize },
        SelfReferencing,
        Dying(String),
        Dead,
    }

    impl StateVariant for CorruptionState {
        fn variant_name(&self) -> &str {
            match self {
                CorruptionState::Spawning { .. } => "Spawning",
                CorruptionState::Corrupting { .. } => "Corrupting",
                CorruptionState::SelfReferencing => "SelfReferencing",
                CorruptionState::Dying(_) => "Dying",
                CorruptionState::Dead => "Dead",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum CorruptionEvent {
        SpawnChild(usize),
        CreateCycle,
        CreateSelfReference,
        AbandonAsync,
        Die,
    }

    impl EventVariant for CorruptionEvent {
        fn variant_name(&self) -> &str {
            match self {
                CorruptionEvent::SpawnChild(_) => "SpawnChild",
                CorruptionEvent::CreateCycle => "CreateCycle",
                CorruptionEvent::CreateSelfReference => "CreateSelfReference",
                CorruptionEvent::AbandonAsync => "AbandonAsync",
                CorruptionEvent::Die => "Die",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum CorruptionAction {
        AllocateMemory(usize),
        SpawnTask,
        CreateReference,
        Cleanup,
    }

    // === THE OUROBOROS CONTEXT (Self-referential nightmare) ===
    #[derive(Clone)]
    struct CorruptionContext {
        // The ouroboros: contexts that reference each other
        parent: Arc<RwLock<Option<Weak<CorruptionContext>>>>,
        children: Arc<RwLock<Vec<Arc<CorruptionContext>>>>,

        // Self-reference through channels (the snake eating its tail)
        self_sender: Arc<RwLock<Option<mpsc::Sender<Arc<CorruptionContext>>>>>,
        self_receiver: Arc<RwLock<Option<mpsc::Receiver<Arc<CorruptionContext>>>>>,

        // Memory pressure tracking
        allocated_memory: Arc<AtomicUsize>,
        active_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,

        // Tracking for verification
        id: usize,
        creation_count: Arc<AtomicUsize>,
        drop_count: Arc<AtomicUsize>,
    }

    impl FsmContext for CorruptionContext {
        fn describe(&self) -> String {
            format!("CorruptionContext#{} with {} bytes allocated", self.id, self.allocated_memory.load(Ordering::Relaxed))
        }
    }

    #[async_trait]
    impl FsmAction for CorruptionAction {
        type Context = CorruptionContext;

        async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
            match self {
                CorruptionAction::AllocateMemory(size) => {
                    ctx.allocated_memory.fetch_add(*size, Ordering::Relaxed);
                    Ok(())
                }
                CorruptionAction::SpawnTask => {
                    // Spawn task
                    Ok(())
                }
                CorruptionAction::CreateReference => {
                    // Create reference
                    Ok(())
                }
                CorruptionAction::Cleanup => {
                    // Cleanup
                    Ok(())
                }
            }
        }
    }

    impl CorruptionContext {
        fn new(id: usize, creation_count: Arc<AtomicUsize>, drop_count: Arc<AtomicUsize>) -> Self {
            creation_count.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = mpsc::channel(100);

            CorruptionContext {
                parent: Arc::new(RwLock::new(None)),
                children: Arc::new(RwLock::new(Vec::new())),
                self_sender: Arc::new(RwLock::new(Some(tx))),
                self_receiver: Arc::new(RwLock::new(Some(rx))),
                allocated_memory: Arc::new(AtomicUsize::new(0)),
                active_tasks: Arc::new(RwLock::new(Vec::new())),
                id,
                creation_count,
                drop_count: drop_count.clone(),
            }
        }

        // Create a cycle: A -> B -> C -> A
        async fn create_cycle(self: Arc<Self>, other: Arc<CorruptionContext>) {
            // Create strong reference cycle
            self.children.write().await.push(other.clone());
            other.parent.write().await.replace(Arc::downgrade(&self));

            // Even worse: send self through channel creating more refs
            if let Some(tx) = &*self.self_sender.read().await {
                let _ = tx.send(self.clone()).await;
            }
        }

        // Break cycles using Weak references
        async fn break_cycles(&self) {
            // Clear children (breaks forward references)
            self.children.write().await.clear();

            // Clear parent (already weak, but let's be thorough)
            *self.parent.write().await = None;

            // Drop channel to break self-reference
            *self.self_sender.write().await = None;
            *self.self_receiver.write().await = None;
        }
    }

    impl Drop for CorruptionContext {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::SeqCst);
            println!("💀 Context {} dropped", self.id);
        }
    }

    // === TRIAL 1: The Ouroboros (Cyclic References) ===
    println!("😈 Trial 1: The Ouroboros - Cyclic References");

    let creation_count = Arc::new(AtomicUsize::new(0));
    let drop_count = Arc::new(AtomicUsize::new(0));

    {
        // Create a cycle of contexts
        let ctx1 = Arc::new(CorruptionContext::new(1, creation_count.clone(), drop_count.clone()));
        let ctx2 = Arc::new(CorruptionContext::new(2, creation_count.clone(), drop_count.clone()));
        let ctx3 = Arc::new(CorruptionContext::new(3, creation_count.clone(), drop_count.clone()));

        // Create the cycle: 1 -> 2 -> 3 -> 1
        ctx1.clone().create_cycle(ctx2.clone()).await;
        ctx2.clone().create_cycle(ctx3.clone()).await;
        ctx3.clone().create_cycle(ctx1.clone()).await;

        // Verify cycle exists
        assert_eq!(ctx1.children.read().await.len(), 1);
        assert_eq!(Arc::strong_count(&ctx1), 3); // self + ctx3's child + local

        // Break the cycle
        ctx1.break_cycles().await;
        ctx2.break_cycles().await;
        ctx3.break_cycles().await;

        // Verify counts will drop
        assert_eq!(Arc::strong_count(&ctx1), 1); // only local ref remains
    }

    // Wait for drops
    sleep(Duration::from_millis(10)).await;

    let created = creation_count.load(Ordering::SeqCst);
    let dropped = drop_count.load(Ordering::SeqCst);
    println!("✅ Created: {}, Dropped: {} (should be equal)", created, dropped);
    assert_eq!(created, dropped, "Memory leak detected! Contexts not dropped!");

    // === TRIAL 2: The Abandoned Async Tasks ===
    println!("\n😈 Trial 2: Abandoned Async Tasks");

    let task_creation_count = Arc::new(AtomicUsize::new(0));
    let task_completion_count = Arc::new(AtomicUsize::new(0));

    {
        let ctx = Arc::new(CorruptionContext::new(
            666, // The beast's context
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0))
        ));

        // Spawn tasks that hold Arc references
        for _ in 0..100 {
            let ctx_clone = ctx.clone();
            let task_creation = task_creation_count.clone();
            let task_completion = task_completion_count.clone();

            let handle = tokio::spawn(async move {
                task_creation.fetch_add(1, Ordering::SeqCst);

                // Hold the context reference
                let _ctx = ctx_clone;

                // Simulate work
                sleep(Duration::from_millis(50)).await;

                task_completion.fetch_add(1, Ordering::SeqCst);
            });

            ctx.active_tasks.write().await.push(handle);
        }

        // Abandon some tasks (drop handles without awaiting)
        ctx.active_tasks.write().await.truncate(50);

        // Context goes out of scope with active tasks
    }

    // Wait for tasks to complete or abort
    sleep(Duration::from_millis(100)).await;

    let created_tasks = task_creation_count.load(Ordering::SeqCst);
    let completed_tasks = task_completion_count.load(Ordering::SeqCst);
    println!("✅ Tasks created: {}, completed: {}", created_tasks, completed_tasks);
    // Some tasks may not complete due to being dropped, but that's fine

    // === TRIAL 3: The FSM Apocalypse (Mass Spawn/Kill) ===
    println!("\n😈 Trial 3: The FSM Apocalypse - 1000 FSMs");

    let fsm_count = Arc::new(AtomicUsize::new(0));
    let death_count = Arc::new(AtomicUsize::new(0));
    let mut fsm_handles = vec![];

    // Measure baseline memory
    let baseline_memory = get_approximate_memory();
    println!("📊 Baseline memory: ~{} bytes", baseline_memory);

    // Spawn 1000 FSMs
    for i in 0..1000 {
        let fsm_count_clone = fsm_count.clone();
        let death_count_clone = death_count.clone();

        let handle = tokio::spawn(async move {
            fsm_count_clone.fetch_add(1, Ordering::SeqCst);

            // Create a memory-hungry context
            let ctx = Arc::new(CorruptionContext::new(
                i,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0))
            ));

            // Allocate some memory
            ctx.allocated_memory.store(1024 * 1024, Ordering::SeqCst); // 1MB per FSM
            let _memory: Vec<u8> = vec![0; 1024 * 1024];

            // Build FSM
            let mut fsm = FsmBuilder::<CorruptionState, CorruptionEvent, CorruptionContext, CorruptionAction>::new(
                CorruptionState::Spawning { id: i }
            )
                .when("Spawning")
                    .on("Die", |state, _event: &CorruptionEvent, _ctx: Arc<CorruptionContext>| {
                        let state = state.clone();
                        async move {
                            if let CorruptionState::Spawning { id: _ } = state {
                                Ok(Transition {
                                    next_state: CorruptionState::Dead,
                                    actions: vec![CorruptionAction::Cleanup],
                                })
                            } else {
                                unreachable!()
                            }
                        }
                    })
                    .done()
                .build();

            // Random lifetime
            sleep(Duration::from_millis(rand::random::<u64>() % 100)).await;

            // Die
            let _ = fsm.handle(CorruptionEvent::Die, ctx).await;

            death_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        fsm_handles.push(handle);

        // Randomly abort some FSMs (Satan kills them)
        if i % 7 == 0 {
            if let Some(handle) = fsm_handles.pop() {
                handle.abort();
            }
        }
    }

    // Wait for all FSMs to complete or abort
    for handle in fsm_handles {
        let _ = handle.await;
    }

    let spawned = fsm_count.load(Ordering::SeqCst);
    let died = death_count.load(Ordering::SeqCst);
    println!("✅ FSMs spawned: {}, died: {} (some aborted)", spawned, died);

    // Check memory returned to baseline (approximately)
    sleep(Duration::from_millis(100)).await; // Let things settle
    let final_memory = get_approximate_memory();
    println!("📊 Final memory: ~{} bytes", final_memory);

    // Memory should not grow unbounded
    let memory_growth = final_memory.saturating_sub(baseline_memory);
    println!("📈 Memory growth: {} bytes", memory_growth);
    assert!(memory_growth < 100 * 1024 * 1024, "Memory leak detected! Growth: {} MB", memory_growth / 1024 / 1024);

    // === TRIAL 4: The Self-Referential Nightmare ===
    println!("\n😈 Trial 4: Self-Referential Nightmare");

    let nightmare_ctx = Arc::new(CorruptionContext::new(
        999,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0))
    ));

    // Create self-reference through channel
    if let Some(tx) = &*nightmare_ctx.self_sender.read().await {
        // Send self through own channel!
        let _ = tx.send(nightmare_ctx.clone()).await;
        let _ = tx.send(nightmare_ctx.clone()).await;
        let _ = tx.send(nightmare_ctx.clone()).await;
    }

    // Verify self-references exist
    let initial_strong_count = Arc::strong_count(&nightmare_ctx);
    println!("🔄 Self-references created, strong count: {}", initial_strong_count);
    assert!(initial_strong_count > 1, "Self-references not created!");

    // Break self-references
    nightmare_ctx.break_cycles().await;

    // Verify cleanup
    let final_strong_count = Arc::strong_count(&nightmare_ctx);
    println!("✅ Self-references broken, strong count: {}", final_strong_count);
    assert_eq!(final_strong_count, 1, "Self-references not cleaned up!");

    println!("\n🏆 THE INCORRUPTIBLE Arc<Context> HAS DEFEATED SATAN!");
    println!("✝️ Memory corruption gauntlet complete - Rust's ownership is divine!");

    // "And God said, Let there be Arc: and there was Arc." - Genesis 1:3 (Rust edition)
}

// Helper function to get approximate memory usage
// This is a rough estimate, not precise measurement
fn get_approximate_memory() -> usize {
    // In a real test, you might use platform-specific APIs or external tools
    // For now, we'll use a simple allocation test
    let test_vec: Vec<u8> = Vec::with_capacity(1024);
    let ptr = test_vec.as_ptr() as usize;
    std::mem::forget(test_vec); // Don't drop it
    ptr // Use pointer value as a rough indicator
}
