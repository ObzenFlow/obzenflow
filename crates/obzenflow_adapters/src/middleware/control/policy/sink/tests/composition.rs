// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[tokio::test]
async fn real_limiter_and_breaker_retry_onion_preserves_both_declared_orders() {
    for limiter_first in [true, false] {
        let stage_id = StageId::new();
        let config = sink_stage_config(stage_id, "ordered_sink");
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let limiter_factory = RateLimiterFactory::new(100_000.0).with_burst(3.0);
        let breaker_factory = CircuitBreakerBuilder::new(10)
            .with_retry_fixed(Duration::ZERO, 3)
            .build();
        let limiter = materialize_sink_policy(&limiter_factory, &config, &control, 0);
        let breaker = materialize_sink_policy(breaker_factory.as_ref(), &config, &control, 1);
        let trace = Arc::new(Mutex::new(Vec::new()));
        let limiter = traced_sink_policy(limiter, &trace);
        let breaker = traced_sink_policy(breaker, &trace);
        let policies = if limiter_first {
            vec![limiter, breaker]
        } else {
            vec![breaker, limiter]
        };
        let boundary = PerSinkDeliveryPolicyBoundary::new(policies);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut executor = SequenceSinkExecutor {
            parent_event_id: EventId::new(),
            outcomes: VecDeque::from([
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                    "first transient failure".to_string(),
                ))),
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                    "second transient failure".to_string(),
                ))),
                SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                    DeliveryPayload::success(DeliveryMethod::Noop, None),
                )))),
            ]),
            calls: calls.clone(),
        };

        let report = boundary
            .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let expected = if limiter_first {
            concat!(
                "admit:rate_limiter|admit:circuit_breaker|",
                "commit:rate_limiter|commit:circuit_breaker|",
                "observe:circuit_breaker:failed|observe:rate_limiter:failed|",
                "admit:rate_limiter|admit:circuit_breaker|",
                "commit:rate_limiter|commit:circuit_breaker|",
                "observe:circuit_breaker:failed|observe:rate_limiter:failed|",
                "admit:rate_limiter|admit:circuit_breaker|",
                "commit:rate_limiter|commit:circuit_breaker|",
                "observe:circuit_breaker:delivered|observe:rate_limiter:delivered"
            )
        } else {
            concat!(
                "admit:circuit_breaker|admit:rate_limiter|",
                "commit:circuit_breaker|commit:rate_limiter|",
                "observe:rate_limiter:failed|observe:circuit_breaker:failed|",
                "admit:circuit_breaker|admit:rate_limiter|",
                "commit:circuit_breaker|commit:rate_limiter|",
                "observe:rate_limiter:failed|observe:circuit_breaker:failed|",
                "admit:circuit_breaker|admit:rate_limiter|",
                "commit:circuit_breaker|commit:rate_limiter|",
                "observe:rate_limiter:delivered|observe:circuit_breaker:delivered"
            )
        };
        assert_eq!(trace.lock().unwrap().join("|"), expected);

        let limiter_snapshotter = control
            .rate_limiter_snapshotter(&stage_id)
            .expect("real limiter registers a metrics snapshotter");
        let limiter_metrics = limiter_snapshotter();
        assert_eq!(limiter_metrics.events_total, 3);
        assert_eq!(limiter_metrics.tokens_consumed_total, 3.0);

        let breaker_snapshotter = control
            .circuit_breaker_snapshotter(&stage_id)
            .expect("real breaker registers a metrics snapshotter");
        let breaker_metrics = breaker_snapshotter();
        assert_eq!(breaker_metrics.requests_total, 3);
        assert_eq!(breaker_metrics.failures_total, 2);
        assert_eq!(breaker_metrics.successes_total, 1);
    }
}

#[tokio::test]
async fn real_open_breaker_rejection_refunds_earlier_limiter_reservation() {
    let stage_id = StageId::new();
    let config = sink_stage_config(stage_id, "refund_sink");
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let breaker_factory = CircuitBreakerBuilder::new(1)
        .with_retry_fixed(Duration::ZERO, 2)
        .build();
    let breaker = materialize_sink_policy(breaker_factory.as_ref(), &config, &control, 1);

    // One real physical failure opens the real breaker. Settlement then
    // suppresses recovery, so the scripted executor needs no second item.
    let warm_calls = Arc::new(AtomicUsize::new(0));
    let mut warm_executor = SequenceSinkExecutor {
        parent_event_id: EventId::new(),
        outcomes: VecDeque::from([SinkDeliveryAttemptOutcome::Delivered(Err(
            HandlerError::Remote("open the breaker".to_string()),
        ))]),
        calls: warm_calls.clone(),
    };
    let warm_boundary = PerSinkDeliveryPolicyBoundary::new(vec![breaker.clone()]);
    let warm_report = warm_boundary
        .around_retryable_sink_delivery(&mut warm_executor, BoundaryStopReceiver::default())
        .await;
    assert!(matches!(
        warm_report.outcome,
        SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Err(
            HandlerError::Remote(_)
        )))
    ));
    assert_eq!(warm_calls.load(Ordering::SeqCst), 1);

    let limiter_factory = RateLimiterFactory::new(100_000.0).with_burst(1.0);
    let limiter = materialize_sink_policy(&limiter_factory, &config, &control, 0);
    let trace = Arc::new(Mutex::new(Vec::new()));
    let boundary = PerSinkDeliveryPolicyBoundary::new(vec![
        traced_sink_policy(limiter, &trace),
        traced_sink_policy(breaker, &trace),
    ]);
    let mut executor = PanicExecutor;

    let report = boundary
        .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::Rejected(ref rejection)
            if rejection.policy == "circuit_breaker"
                && rejection.reason == "circuit breaker open"
    ));
    assert_eq!(
        trace.lock().unwrap().join("|"),
        concat!(
            "admit:rate_limiter|admit:circuit_breaker|",
            "observe:rate_limiter:not_executed"
        )
    );

    let limiter_snapshotter = control
        .rate_limiter_snapshotter(&stage_id)
        .expect("real limiter registers a metrics snapshotter");
    let limiter_metrics = limiter_snapshotter();
    assert_eq!(limiter_metrics.events_total, 0);
    assert_eq!(limiter_metrics.tokens_consumed_total, 0.0);
    assert_eq!(limiter_metrics.bucket_tokens, 1.0);

    let breaker_snapshotter = control
        .circuit_breaker_snapshotter(&stage_id)
        .expect("real breaker registers a metrics snapshotter");
    let breaker_metrics = breaker_snapshotter();
    assert_eq!(breaker_metrics.requests_total, 1);
    assert_eq!(breaker_metrics.failures_total, 1);
    assert!(breaker_metrics.state.is_open());
}

#[tokio::test]
async fn third_party_factory_uses_the_sink_carrier_and_rejects() {
    // FLOWIP-115b: the carrier is a general abstraction, not breaker-special.
    // A non-breaker factory (control_role None) routes through the same
    // `materialize` path purely by declaring a control surface, and its
    // policy composes in the same onion the breaker uses, short-circuiting
    // delivery on rejection.
    let factory = ThirdPartySinkFactory;
    assert!(factory.declaration().is_control());
    assert!(factory
        .declaration()
        .supports(MiddlewareSurfaceKind::SinkDelivery));
    assert_eq!(factory.control_role(), ControlMiddlewareRole::None);

    let config = StageConfig {
        stage_id: StageId::new(),
        name: "third_party".to_string(),
        flow_name: "test".to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        resolved_policies: Default::default(),
    };
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let surface = MiddlewareSurface::SinkDelivery(crate::middleware::SinkDeliverySurface {
        stage_id: config.stage_id,
        configured_target: None,
    });
    let unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SinkDelivery(crate::middleware::SinkDeliveryUnitId {
            target: crate::middleware::SinkDeliveryTarget::Stage,
        }),
    };
    let origin = MiddlewareOrigin::Stage;
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &unit,
        origin: &origin,
        declaration_index: crate::middleware::MiddlewareDeclarationIndex::resolved(0),
    };
    let materialization = MiddlewareMaterializationContext {
        config: &config,
        control_middleware: &control,
        stage_type: obzenflow_core::event::context::StageType::Sink,
    };
    let attachment = factory
        .materialize(request, &materialization)
        .expect("third-party factory should materialize a sink policy");
    let policy = match attachment {
        MiddlewareSurfaceAttachment::SinkDelivery(policy) => policy,
        _ => panic!("expected a SinkDelivery attachment from the third-party factory"),
    };

    let boundary = PerSinkDeliveryPolicyBoundary::new(vec![policy]);
    let mut executor = PanicExecutor;
    let report = boundary.around_sink_delivery(&mut executor).await;

    match report.outcome {
        SinkDeliveryBoundaryOutcome::Rejected(rejection) => {
            assert_eq!(rejection.policy, "third_party_reject");
            assert_eq!(rejection.reason, "third party policy");
        }
        _ => panic!("expected the third-party policy to reject delivery"),
    }
}
