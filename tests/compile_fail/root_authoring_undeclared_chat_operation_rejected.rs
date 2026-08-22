// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::ai::ChatEffects;
use obzenflow_core::ai::{ChatParams, ChatRequestSpec};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::Effects;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Output;

impl TypedPayload for Output {
    const EVENT_TYPE: &'static str = "compile_contract.undeclared_chat.output";
}

async fn undeclared(fx: &mut Effects<Output, obzenflow_runtime::effect_set![]>) {
    let request = ChatRequestSpec {
        messages: Vec::new(),
        params: ChatParams::default(),
        tools: Vec::new(),
        response_format: None,
    };
    let _ = fx.chat_completion("undeclared", request).await;
}

fn main() {}
