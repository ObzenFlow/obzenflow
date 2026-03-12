// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Best-effort model metadata used for context-window-aware planning.
//!
//! The framework treats context-window information as a best-effort hint. When it is not
//! available, callers must provide an explicit context window before performing request-aware
//! budgeting.

use super::{ResolvedTokenEstimator, TokenCount};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextWindowSource {
    BuiltIn,
    Override,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ChatModelProfile {
    pub model: String,
    pub estimator: ResolvedTokenEstimator,
    pub context_window: Option<TokenCount>,
    pub context_window_source: ContextWindowSource,
}

impl ChatModelProfile {
    pub fn with_context_window_override(mut self, context_window: TokenCount) -> Self {
        self.context_window = Some(context_window);
        self.context_window_source = ContextWindowSource::Override;
        self
    }
}
