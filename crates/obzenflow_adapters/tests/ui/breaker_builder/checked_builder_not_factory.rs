// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::control::CircuitBreaker;
use obzenflow_adapters::middleware::MiddlewareFactory;

fn needs_factory(_factory: &dyn MiddlewareFactory) {}

fn main() {
    let unchecked = CircuitBreaker::builder().consecutive_failures(1);
    needs_factory(&unchecked);
}
