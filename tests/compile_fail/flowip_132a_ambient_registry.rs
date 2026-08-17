// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::{
    EffectContext, EffectPortRegistry, EffectPortResolutionError,
};
use std::sync::Arc;

fn raw_context_lookup(context: &EffectContext) {
    let _ = context.port::<usize>("client");
}

fn main() {
    let registry = EffectPortRegistry::new();
    let _ = registry.get::<usize>("client");
    let _ = registry.with_deferred::<usize>(
        "client",
        Arc::new(|| Ok::<Arc<usize>, EffectPortResolutionError>(Arc::new(1))),
    );
}
