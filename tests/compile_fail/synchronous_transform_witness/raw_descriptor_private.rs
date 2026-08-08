// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::dsl::stage_descriptor::TransformDescriptor;

fn main() {
    let _ = std::mem::size_of::<TransformDescriptor<()>>();
}
