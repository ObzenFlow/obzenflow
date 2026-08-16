// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_adapters::ai::{ChatCompletion, CHAT_CLIENT};
use obzenflow_core::ai::ChatTarget;
use obzenflow_runtime::effects::{EffectRegistrationBuilder, ResolvedEffectPort};
use std::sync::Arc;

fn bind_wrong_type(builder: EffectRegistrationBuilder<ChatCompletion>) {
    let _ = builder.bind_eager_with_metadata(
        CHAT_CLIENT,
        ResolvedEffectPort::new(
            Arc::new(()),
            Arc::new(ChatTarget::new("fixture", "model")),
        ),
    );
}

fn main() {}
