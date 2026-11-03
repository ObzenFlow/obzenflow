// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::manifest::archive_manifest;
use obzenflow_core::id::StageId;
use obzenflow_core::WriterId;
use std::path::Path;

pub fn stage_writer(run_dir: &Path, stage_key: &str) -> WriterId {
    let stage_id: StageId = archive_manifest(run_dir)["stages"][stage_key]["stage_id"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain the stage id for '{stage_key}'"))
        .parse()
        .unwrap_or_else(|error| panic!("the stage id for '{stage_key}' parses: {error}"));
    WriterId::from(stage_id)
}
