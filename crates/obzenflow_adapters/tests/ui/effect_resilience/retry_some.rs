// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::{CircuitBreaker, EffectResilience, Retry};
use std::time::Duration;

fn main() {
    let breaker = CircuitBreaker::builder()
        .consecutive_failures(3)
        .build()
        .unwrap();
    let retry = Retry::fixed(Duration::from_millis(1));

    let _ = EffectResilience::with_breaker(breaker).retry(Some(retry));
}
