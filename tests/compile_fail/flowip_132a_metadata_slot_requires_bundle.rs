// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_adapters::ai::{ChatCompletion, CHAT_CLIENT};
use obzenflow_core::ai::ChatClient;
use obzenflow_runtime::effects::EffectRegistrationBuilder;
use std::sync::Arc;

fn bind_without_metadata(
    builder: EffectRegistrationBuilder<ChatCompletion>,
    client: Arc<dyn ChatClient>,
) {
    let _ = builder.bind_eager(CHAT_CLIENT, client);
}

fn main() {}
