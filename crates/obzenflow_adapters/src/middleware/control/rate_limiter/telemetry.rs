// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: rate-limiter lifecycle event construction.
//!
//! The admission core ([`super::admission_core`]) is `ChainEvent`-free, so these
//! helpers live in the shell and turn the core's plain-data outputs into durable
//! observability `ChainEvent` facts. The shared inherent helpers on
//! `RateLimiterMiddleware` and the surface adapters call into here.

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
};
use obzenflow_core::WriterId;

use super::admission_core::RateLimitDelayEvent;

/// Build the durable observability `ChainEvent` for one rate-limiter lifecycle
/// fact.
pub(super) fn rate_limiter_event(writer_id: WriterId, event: RateLimiterEvent) -> ChainEvent {
    ChainEventFactory::observability_event(
        writer_id,
        ObservabilityPayload::Middleware(MiddlewareLifecycle::RateLimiter(event)),
    )
}

/// Build the one-shot `Delayed` lifecycle fact emitted on the first wait of an
/// acquire (FLOWIP-114o).
pub(super) fn delayed_event(writer_id: WriterId, info: RateLimitDelayEvent) -> ChainEvent {
    rate_limiter_event(
        writer_id,
        RateLimiterEvent::Delayed {
            delay_ms: info.delay_ms,
            current_rate: info.current_rate,
            limit_rate: info.limit_rate,
        },
    )
}
