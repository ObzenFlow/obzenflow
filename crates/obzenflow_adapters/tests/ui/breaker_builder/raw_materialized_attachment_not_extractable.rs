// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::MiddlewareSurfaceAttachment;

fn launder_payload(attachment: MiddlewareSurfaceAttachment) {
    let _policy = attachment.into_effect();
}

fn main() {}
