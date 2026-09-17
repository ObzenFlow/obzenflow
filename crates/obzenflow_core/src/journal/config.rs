// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration shared by handles of the same journal.

use std::time::Duration;

/// Configure a journal before publication starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JournalConfig {
    pub observability: ObservabilityPolicy,
}

/// Optional attachments are dense unless explicitly throttled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ObservabilityPolicy {
    #[default]
    EveryRecord,
    Periodic {
        interval: Duration,
    },
}
