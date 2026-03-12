// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Chat request budgeting helpers.
//!
//! `TokenEstimator::estimate_chat_request` provides a low-level primitive for estimating the
//! total input size of a fully materialised chat request. Real AI applications typically need
//! something slightly higher-level: compute how many tokens remain for a variable user-content
//! slot after accounting for:
//! - system and static messages
//! - tool definitions
//! - response format overhead
//! - reserved output tokens
//! - a caller-defined safety margin
//!
//! FLOWIP-086z promotes this into a first-class helper so chunk planners can target the real
//! "available input" budget rather than a hand-tuned magic number.

use super::{AiProvider, ChatMessage, ChatParams, ChatRequest, ChatResponseFormat, ToolDefinition};
use super::{TokenCount, TokenEstimator};

/// A single message position in a budgeting template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatBudgetMessage {
    /// A fully-specified chat message.
    Static(ChatMessage),
    /// A single variable user slot surrounded by a prompt scaffold.
    ///
    /// The eventual user message content is assumed to be:
    /// `prefix + <variable content> + suffix`.
    VariableUserSlot { prefix: String, suffix: String },
}

/// Template describing the static portions of a chat request for budgeting.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChatBudgetTemplate {
    pub messages: Vec<ChatBudgetMessage>,
    pub params: ChatParams,
    pub tools: Vec<ToolDefinition>,
    pub response_format: Option<ChatResponseFormat>,
}

/// Budget parameters for planning variable input capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatBudgetSpec {
    pub context_window: TokenCount,
    pub reserved_output: TokenCount,
    pub safety_margin: TokenCount,
}

/// Result of planning a chat input budget against a template.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatBudgetPlan {
    /// Estimated tokens consumed by the request excluding the variable slot scaffold.
    pub request_overhead: TokenCount,
    /// Estimated tokens consumed by the variable slot scaffold (`prefix + suffix`).
    pub variable_slot_overhead: TokenCount,
    /// Remaining tokens available for the variable content itself.
    pub available_input: TokenCount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatBudgetError {
    MissingVariableSlot,
    MultipleVariableSlots { count: usize },
    InvalidSpec { message: String },
}

impl std::fmt::Display for ChatBudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingVariableSlot => {
                f.write_str("chat budget template requires one variable user slot")
            }
            Self::MultipleVariableSlots { count } => write!(
                f,
                "chat budget template supports exactly one variable user slot (found {count})"
            ),
            Self::InvalidSpec { message } => write!(f, "invalid chat budget spec: {message}"),
        }
    }
}

impl std::error::Error for ChatBudgetError {}

/// Plan how many tokens remain for a variable user slot in a chat template.
///
/// The template must contain exactly one [`ChatBudgetMessage::VariableUserSlot`].
pub fn plan_chat_input_budget(
    estimator: &dyn TokenEstimator,
    template: &ChatBudgetTemplate,
    spec: ChatBudgetSpec,
) -> Result<ChatBudgetPlan, ChatBudgetError> {
    if spec.context_window == TokenCount::ZERO {
        return Err(ChatBudgetError::InvalidSpec {
            message: "context_window must be greater than zero".to_string(),
        });
    }

    let reserved_total = spec.reserved_output.saturating_add(spec.safety_margin);
    if reserved_total >= spec.context_window {
        return Err(ChatBudgetError::InvalidSpec {
            message: "reserved_output + safety_margin must be less than context_window".to_string(),
        });
    }

    let mut variable_slots: Vec<(&str, &str)> = Vec::new();
    let mut materialised_messages: Vec<ChatMessage> = Vec::with_capacity(template.messages.len());

    for msg in &template.messages {
        match msg {
            ChatBudgetMessage::Static(m) => materialised_messages.push(m.clone()),
            ChatBudgetMessage::VariableUserSlot { prefix, suffix } => {
                variable_slots.push((prefix.as_str(), suffix.as_str()));
                materialised_messages.push(ChatMessage::user(""));
            }
        }
    }

    match variable_slots.len() {
        0 => return Err(ChatBudgetError::MissingVariableSlot),
        1 => {}
        n => {
            return Err(ChatBudgetError::MultipleVariableSlots { count: n });
        }
    }

    let (prefix, suffix) = variable_slots[0];
    let variable_slot_overhead = estimator.estimate_text(&format!("{prefix}{suffix}")).tokens;

    // Best-effort budgeting does not require a "real" provider/model label, since the estimator
    // is already selected by the caller (for example via resolve_estimator_for_model()).
    let req = ChatRequest {
        provider: AiProvider::new("unknown"),
        model: "unknown".to_string(),
        messages: materialised_messages,
        params: template.params.clone(),
        tools: template.tools.clone(),
        response_format: template.response_format.clone(),
    };

    let request_overhead = estimator.estimate_chat_request(&req).tokens;

    let overhead_total = request_overhead.saturating_add(variable_slot_overhead);
    let available = spec
        .context_window
        .saturating_sub(spec.reserved_output)
        .saturating_sub(spec.safety_margin)
        .saturating_sub(overhead_total);

    if available == TokenCount::ZERO {
        return Err(ChatBudgetError::InvalidSpec {
            message: "available_input resolves to zero after overhead subtraction".to_string(),
        });
    }

    Ok(ChatBudgetPlan {
        request_overhead,
        variable_slot_overhead,
        available_input: available,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::{ChatResponseFormat, HeuristicTokenEstimator};
    use serde_json::json;

    fn est() -> HeuristicTokenEstimator {
        HeuristicTokenEstimator::new(1.0, 0, 0, 0).expect("valid")
    }

    #[test]
    fn rejects_missing_variable_slot() {
        let template = ChatBudgetTemplate {
            messages: vec![ChatBudgetMessage::Static(ChatMessage::system("sys"))],
            ..Default::default()
        };

        let err = plan_chat_input_budget(
            &est(),
            &template,
            ChatBudgetSpec {
                context_window: TokenCount::new(100),
                reserved_output: TokenCount::new(10),
                safety_margin: TokenCount::new(5),
            },
        )
        .expect_err("should fail");

        assert_eq!(err, ChatBudgetError::MissingVariableSlot);
    }

    #[test]
    fn rejects_multiple_variable_slots() {
        let template = ChatBudgetTemplate {
            messages: vec![
                ChatBudgetMessage::VariableUserSlot {
                    prefix: "a".to_string(),
                    suffix: "b".to_string(),
                },
                ChatBudgetMessage::VariableUserSlot {
                    prefix: "c".to_string(),
                    suffix: "d".to_string(),
                },
            ],
            ..Default::default()
        };

        let err = plan_chat_input_budget(
            &est(),
            &template,
            ChatBudgetSpec {
                context_window: TokenCount::new(100),
                reserved_output: TokenCount::new(10),
                safety_margin: TokenCount::new(5),
            },
        )
        .expect_err("should fail");

        assert!(matches!(
            err,
            ChatBudgetError::MultipleVariableSlots { count: 2 }
        ));
    }

    #[test]
    fn computes_available_input_from_overheads() {
        let template = ChatBudgetTemplate {
            messages: vec![
                ChatBudgetMessage::Static(ChatMessage::system("AAA")),
                ChatBudgetMessage::VariableUserSlot {
                    prefix: "BBB".to_string(),
                    suffix: "CCC".to_string(),
                },
            ],
            ..Default::default()
        };

        let plan = plan_chat_input_budget(
            &est(),
            &template,
            ChatBudgetSpec {
                context_window: TokenCount::new(100),
                reserved_output: TokenCount::new(10),
                safety_margin: TokenCount::new(5),
            },
        )
        .expect("should succeed");

        // bytes_per_token=1.0 and overheads are zero, so tokens==bytes.
        assert_eq!(plan.request_overhead, TokenCount::new(3));
        assert_eq!(plan.variable_slot_overhead, TokenCount::new(6));
        assert_eq!(plan.available_input, TokenCount::new(76));
    }

    #[test]
    fn request_overhead_accounts_for_tools_and_response_format() {
        let tool = ToolDefinition {
            name: "t".to_string(),
            description: None,
            parameters_schema: Some(json!({"type":"object","properties":{"x":{"type":"number"}}})),
        };

        let template_base = ChatBudgetTemplate {
            messages: vec![
                ChatBudgetMessage::Static(ChatMessage::system("sys")),
                ChatBudgetMessage::VariableUserSlot {
                    prefix: "pref".to_string(),
                    suffix: "suf".to_string(),
                },
            ],
            ..Default::default()
        };

        let template_tools = ChatBudgetTemplate {
            tools: vec![tool],
            ..template_base.clone()
        };
        let template_schema = ChatBudgetTemplate {
            response_format: Some(ChatResponseFormat::JsonSchema {
                schema: json!({"type":"object","properties":{"x":{"type":"string"}}}),
            }),
            ..template_base.clone()
        };

        let spec = ChatBudgetSpec {
            context_window: TokenCount::new(1000),
            reserved_output: TokenCount::new(0),
            safety_margin: TokenCount::new(0),
        };

        let base = plan_chat_input_budget(&est(), &template_base, spec)
            .expect("base should succeed")
            .request_overhead;
        let with_tools = plan_chat_input_budget(&est(), &template_tools, spec)
            .expect("tools should succeed")
            .request_overhead;
        let with_schema = plan_chat_input_budget(&est(), &template_schema, spec)
            .expect("schema should succeed")
            .request_overhead;

        assert!(with_tools > base);
        assert!(with_schema > base);
    }
}
