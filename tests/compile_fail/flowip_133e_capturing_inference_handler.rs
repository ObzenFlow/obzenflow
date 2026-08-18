// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::typed::ai;
use obzenflow_core::ai::{
    AiRoleLogicFailure, ChatCompletionReply, ChatParams, ChatRequestSpec,
};

struct Input;
struct Output;

fn interpret(
    _input: Input,
    _request: ChatRequestSpec,
    _reply: ChatCompletionReply,
) -> Result<Output, AiRoleLogicFailure> {
    Ok(Output)
}

fn main() {
    let prompt = String::from("captured prompt");
    let _handler = ai::inference_handler(
        move |_input: &Input| {
            let _ = prompt.len();
            Ok(ChatRequestSpec {
                messages: Vec::new(),
                params: ChatParams::default(),
                tools: Vec::new(),
                response_format: None,
            })
        },
        interpret,
    );
}
