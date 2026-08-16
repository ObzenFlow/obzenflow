// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::ai::CHAT_CLIENT;
use obzenflow_runtime::effects::EffectPortMetadataContext;

fn bypass_boundary(context: &EffectPortMetadataContext) {
    let _ = context.port(CHAT_CLIENT);
}

fn main() {}
