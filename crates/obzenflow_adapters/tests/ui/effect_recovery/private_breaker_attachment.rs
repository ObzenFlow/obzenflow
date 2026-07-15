// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::control::policy::EffectPolicyAttachment;
use obzenflow_adapters::middleware::control::CircuitBreakerMiddleware;
use std::sync::Arc;

fn forbidden(policy: Arc<CircuitBreakerMiddleware>) {
    let _ = EffectPolicyAttachment::circuit_breaker(policy);
}

fn main() {}
