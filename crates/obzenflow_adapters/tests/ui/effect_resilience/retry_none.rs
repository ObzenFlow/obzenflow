// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::{CircuitBreaker, EffectResilience};

fn main() {
    let breaker = CircuitBreaker::builder()
        .consecutive_failures(3)
        .build()
        .unwrap();

    let _ = EffectResilience::with_breaker(breaker).retry(None);
}
