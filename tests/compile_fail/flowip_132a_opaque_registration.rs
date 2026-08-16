// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_runtime::effects::{EffectRegistration, NamedEffect};

fn inspect<E: NamedEffect>(registration: EffectRegistration<E>) {
    let _ = registration.resolver();
    let _ = registration.entries;
}

fn main() {}
