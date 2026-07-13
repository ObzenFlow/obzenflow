// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Compile proof for FLOWIP-115h's approved public authoring surface.

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::circuit_breaker::RetryLimits;
use obzenflow_adapters::middleware::{CircuitBreakerBuilder, MiddlewareFactory};
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_dsl::{async_infinite_source, effectful_transform, sink};
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, EffectfulTransformHandler,
};
use obzenflow_runtime::stages::{SourceError, SourcePollRetryOwnership, SourcePollRetrySafety};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PublicPayload;

impl TypedPayload for PublicPayload {
    const EVENT_TYPE: &'static str = "flowip_115h.public_payload";
}

#[derive(Clone, Debug)]
struct RetrySafeFeed;

#[async_trait]
impl AsyncFiniteSourceHandler for RetrySafeFeed {
    fn poll_retry_ownership(&self) -> SourcePollRetryOwnership {
        SourcePollRetryOwnership::NoNestedRetry
    }

    fn poll_retry_safety(&self) -> Option<SourcePollRetrySafety> {
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation)
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
struct RetrySafeInfiniteFeed;

#[async_trait]
impl AsyncInfiniteSourceHandler for RetrySafeInfiniteFeed {
    fn poll_retry_ownership(&self) -> SourcePollRetryOwnership {
        SourcePollRetryOwnership::NoNestedRetry
    }

    fn poll_retry_safety(&self) -> Option<SourcePollRetrySafety> {
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation)
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
struct PublicEffect;

#[async_trait]
impl Effect for PublicEffect {
    const EFFECT_TYPE: &'static str = "flowip_115h.public_effect";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = PublicPayload;

    fn label(&self) -> &str {
        "public_effect"
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(PublicPayload)
    }
}

#[derive(Clone, Debug)]
struct PublicEffectfulTransform;

#[async_trait]
impl EffectfulTransformHandler for PublicEffectfulTransform {
    type Input = PublicPayload;

    async fn process(&self, _input: PublicPayload, _fx: &mut Effects) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn retry_breaker() -> Box<dyn MiddlewareFactory> {
    CircuitBreakerBuilder::new(3)
        .with_retry_fixed(Duration::from_millis(250), 3)
        .with_retry_limits(RetryLimits {
            max_single_delay: Duration::from_secs(5),
            max_total_wall_time: Duration::from_secs(20),
        })
        .build()
}

#[test]
fn approved_builder_spellings_compile() {
    let _fixed = CircuitBreakerBuilder::new(3)
        .with_retry_fixed(Duration::from_millis(250), 3)
        .with_retry_limits(RetryLimits {
            max_single_delay: Duration::from_secs(5),
            max_total_wall_time: Duration::from_secs(20),
        })
        .build();

    let _exponential = CircuitBreakerBuilder::new(3)
        .with_retry_exponential(3)
        .with_retry_limits(RetryLimits {
            max_single_delay: Duration::from_secs(5),
            max_total_wall_time: Duration::from_secs(20),
        })
        .build();
}

#[test]
fn source_retry_declarations_are_available_from_runtime_and_facade_paths() {
    let feed = RetrySafeFeed;
    assert_eq!(
        feed.poll_retry_safety(),
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation)
    );

    let _: obzenflow_runtime::prelude::SourcePollRetrySafety =
        obzenflow_runtime::prelude::SourcePollRetrySafety::NonRetrySafe;
    let _: obzenflow_runtime::prelude::SourcePollRetryOwnership =
        obzenflow_runtime::prelude::SourcePollRetryOwnership::NoNestedRetry;

    let native_retry = obzenflow::sources::HttpRetryConfig::disabled();
    assert!(native_retry.is_disabled());
    let _: obzenflow::sources::SourcePollRetrySafety =
        obzenflow::sources::SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation;
}

#[test]
fn approved_retry_attachment_spellings_compile() {
    let infinite_feed = RetrySafeInfiniteFeed;
    assert_eq!(
        infinite_feed.poll_retry_safety(),
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation)
    );
    assert_eq!(
        infinite_feed.poll_retry_ownership(),
        SourcePollRetryOwnership::NoNestedRetry
    );

    let _infinite_source =
        async_infinite_source!(PublicPayload => infinite_feed, [retry_breaker()]);

    let _effectful = effectful_transform!(
        PublicPayload -> PublicPayload => PublicEffectfulTransform,
        effects: [PublicEffect with [retry_breaker()]],
        middleware: []
    );

    let _sink = sink!(
        PublicPayload => |_payload, _delivery| {},
        delivery: idempotent,
        middleware: [retry_breaker()]
    );
}

#[test]
fn typed_live_failure_variants_are_constructible_without_message_parsing() {
    let source = SourceError::RateLimited {
        message: "quota".to_string(),
        retry_after: Some(Duration::from_secs(1)),
    };
    assert!(matches!(source, SourceError::RateLimited { .. }));

    let effect = EffectError::TransientExecution("temporary".to_string());
    assert!(matches!(effect, EffectError::TransientExecution(_)));
}
