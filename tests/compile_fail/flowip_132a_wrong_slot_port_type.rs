// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_adapters::ai::{ChatCompletion, CHAT_CLIENT};
use obzenflow_runtime::effects::EffectRegistrationBuilder;
use std::sync::Arc;

fn bind_wrong_type(builder: EffectRegistrationBuilder<ChatCompletion>) {
    let _ = builder.bind_eager(CHAT_CLIENT, Arc::new(()));
}

fn main() {}
