// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::testing::sink::SinkConformanceFailure;

fn main() {
    let _ = SinkConformanceFailure {
        suite: "forged",
        case: "forged".into(),
        detail: "forged".into(),
    };
}
