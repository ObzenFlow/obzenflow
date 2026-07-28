// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::MiddlewareDeclaration;

fn main() {
    let declaration = MiddlewareDeclaration::legacy_shell("legacy", "legacy");
    let _ = declaration.is_legacy_shell();
}
