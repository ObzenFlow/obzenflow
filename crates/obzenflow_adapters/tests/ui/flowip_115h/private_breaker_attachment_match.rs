// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::control::policy::EffectPolicyAttachment;

fn forbidden(attachment: EffectPolicyAttachment) {
    let EffectPolicyAttachment { kind: _ } = attachment;
}

fn main() {}
