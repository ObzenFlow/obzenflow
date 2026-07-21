// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::MiddlewareMaterializationContext;

fn steal_registration_authority(context: &MiddlewareMaterializationContext<'_>) {
    let _aggregator = &context.control_middleware;
}

fn main() {}
