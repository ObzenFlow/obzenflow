// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event intent types
//!
//! Supports the CHAIN maturity model by making event intent explicit.

use serde::{Deserialize, Serialize};

/// The intent behind an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntentContext {
    /// A command to perform an action
    Command { action: String, target: String },

    /// A query for information
    Query { question: String },

    /// A fact that occurred
    Event { fact: String },

    /// A document or content
    Document { content: String },
}

impl IntentContext {
    /// Check if this is a command intent
    pub fn is_command(&self) -> bool {
        matches!(self, IntentContext::Command { .. })
    }

    /// Check if this is a query intent
    pub fn is_query(&self) -> bool {
        matches!(self, IntentContext::Query { .. })
    }

    /// Check if this is an event intent
    pub fn is_event(&self) -> bool {
        matches!(self, IntentContext::Event { .. })
    }

    /// Check if this is a document intent
    pub fn is_document(&self) -> bool {
        matches!(self, IntentContext::Document { .. })
    }
}
