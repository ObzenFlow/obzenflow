// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Verdicts and the exit-code contract (FLOWIP-095j).
//!
//! `0` fully certified match, and only here does [`MATCHED_LINE`] print;
//! `1` divergence in the certified region;
//! `2` certified region matched but uncertified stages exist;
//! `3` refused (lineage, incomplete-archive replay, stage sets, status gate,
//! manifest version, unavailable archive).

use std::path::PathBuf;

use super::error::RefusalReason;
use super::report::VerificationReport;

/// The tutorial's closing line. Printed by the runtime, never typeset by the
/// marketing site, and only on a fully certified match.
pub const MATCHED_LINE: &str = "output matched the original run, 0 differences";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    CertifiedMatch,
    Diverged,
    UncertifiedRemainder,
    Refused,
}

impl Verdict {
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::CertifiedMatch => 0,
            Self::Diverged => 1,
            Self::UncertifiedRemainder => 2,
            Self::Refused => 3,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CertifiedMatch => "certified_match",
            Self::Diverged => "diverged",
            Self::UncertifiedRemainder => "uncertified_remainder",
            Self::Refused => "refused",
        }
    }
}

/// The result of a verification attempt.
#[derive(Debug)]
pub enum VerifyOutcome {
    /// The comparison ran to completion (verdict may still be divergence).
    Completed {
        report: Box<VerificationReport>,
        /// Where the report was written, when it was.
        report_path: Option<PathBuf>,
    },
    /// The comparison was declined for a stated reason (exit code 3).
    Refused(RefusalReason),
}

impl VerifyOutcome {
    pub fn verdict(&self) -> Verdict {
        match self {
            Self::Completed { report, .. } => match report.verdict.as_str() {
                "certified_match" => Verdict::CertifiedMatch,
                "diverged" => Verdict::Diverged,
                _ => Verdict::UncertifiedRemainder,
            },
            Self::Refused(_) => Verdict::Refused,
        }
    }

    pub fn exit_code(&self) -> u8 {
        self.verdict().exit_code()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_follow_the_contract() {
        assert_eq!(Verdict::CertifiedMatch.exit_code(), 0);
        assert_eq!(Verdict::Diverged.exit_code(), 1);
        assert_eq!(Verdict::UncertifiedRemainder.exit_code(), 2);
        assert_eq!(Verdict::Refused.exit_code(), 3);
    }
}
