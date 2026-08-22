// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::{EffectRegistration, NamedEffect};

fn inspect<E: NamedEffect>(registration: EffectRegistration<E>) {
    let _ = registration.resolver();
    let _ = registration.entries;
}

fn main() {}
