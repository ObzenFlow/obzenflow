// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::{
    EffectPortResolutionError, EffectPortSlot, EffectRegistrationBuilder, NamedEffect,
};
use std::sync::Arc;

fn bind_async<E, P>(builder: EffectRegistrationBuilder<E>, slot: EffectPortSlot<P>)
where
    E: NamedEffect,
    P: ?Sized + Send + Sync + 'static,
{
    let resolver = Arc::new(|| async {
        Err::<Arc<P>, EffectPortResolutionError>(
            EffectPortResolutionError::ClientConstructionFailed,
        )
    });
    let _ = builder.bind_deferred(slot, resolver);
}

fn main() {}
