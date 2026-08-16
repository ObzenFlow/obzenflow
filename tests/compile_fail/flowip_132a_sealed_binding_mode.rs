// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_runtime::effects::{Effect, EffectBindingMode, EffectInvocationBinding};

struct AmbientRegistryMode;

impl<E: Effect> EffectBindingMode<E> for AmbientRegistryMode {
    fn invocation_binding(_effect: &E) -> EffectInvocationBinding {
        unimplemented!()
    }
}

fn main() {}
