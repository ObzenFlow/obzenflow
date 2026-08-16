// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "support/typed_effectful.rs"]
mod support;

use obzenflow_runtime::effects::{BindingAuthorityFault, EffectDeclaration, EffectError};

fn inspect_fault(fault: BindingAuthorityFault) {
    let _ = fault.kind;
}

fn main() {
    let _ = EffectError::EffectTargetInvariantViolation { slot: "client" };
    let mut declaration = EffectDeclaration::of::<support::FirstEffect>();
    declaration.effect_type = "https://credential-canary.example";
}
