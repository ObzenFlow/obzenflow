// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::MiddlewareSurfaceAttachment;

fn forge_claim(attachment: MiddlewareSurfaceAttachment) {
    let MiddlewareSurfaceAttachment { claim: _, .. } = attachment;
}

fn main() {}
