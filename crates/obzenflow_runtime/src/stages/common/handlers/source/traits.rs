// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::execution_payload::{
    SourcePollContinuation, SourcePollErrorKind,
};
use obzenflow_core::event::SourceDiagnostic;
use std::fmt;

/// `SourceError` is the error that a source read returns when it fails.
///
/// Every variant carries a typed diagnostic and no input text.
/// Policies classify through [`SourceError::kind`], so a terminal error keeps
/// its category's health meaning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    /// The underlying transport or dependency timed out.
    Timeout(SourceDiagnostic),
    /// The underlying transport or dependency failed (e.g. network error).
    Transport(SourceDiagnostic),
    /// The source could not decode a response or input as a whole.
    Deserialization(SourceDiagnostic),
    /// A record-local refusal after the input was read. This is not
    /// dependency-health evidence.
    Validation(SourceDiagnostic),
    /// Catch-all for other source-specific failures.
    Other(SourceDiagnostic),
    /// The current reader cannot safely continue. A report, not a lifecycle
    /// decision: the source supervisor alone chooses the stage outcome.
    Terminal {
        kind: SourcePollErrorKind,
        diagnostic: SourceDiagnostic,
    },
}

impl SourceError {
    pub fn kind(&self) -> SourcePollErrorKind {
        match self {
            Self::Timeout(_) => SourcePollErrorKind::Timeout,
            Self::Transport(_) => SourcePollErrorKind::Transport,
            Self::Deserialization(_) => SourcePollErrorKind::Deserialization,
            Self::Validation(_) => SourcePollErrorKind::Validation,
            Self::Other(_) => SourcePollErrorKind::Other,
            Self::Terminal { kind, .. } => *kind,
        }
    }

    pub fn diagnostic(&self) -> &SourceDiagnostic {
        match self {
            Self::Timeout(diagnostic)
            | Self::Transport(diagnostic)
            | Self::Deserialization(diagnostic)
            | Self::Validation(diagnostic)
            | Self::Other(diagnostic)
            | Self::Terminal { diagnostic, .. } => diagnostic,
        }
    }

    pub fn continuation(&self) -> SourcePollContinuation {
        match self {
            Self::Terminal { .. } => SourcePollContinuation::Terminal,
            _ => SourcePollContinuation::Recoverable,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.continuation() == SourcePollContinuation::Terminal
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_terminal() {
            f.write_str("terminal ")?;
        }
        write!(
            f,
            "source {} error: {}",
            self.kind().label(),
            self.diagnostic()
        )
    }
}

impl std::error::Error for SourceError {}
