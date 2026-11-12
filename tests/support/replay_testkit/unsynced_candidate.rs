// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::config::{ConfigScope, ConfigSource};
use obzenflow_runtime::runtime_config::{CandidateSet, ConfigValue, ScopedCandidate};

pub fn admit_unsynced(candidates: &mut CandidateSet) {
    candidates
        .admit(ScopedCandidate::unqualified(
            "journal.sync",
            ConfigScope::Global,
            ConfigSource::File,
            ConfigValue::Text("off".to_owned()),
        ))
        .expect("journal.sync admits off");
}
