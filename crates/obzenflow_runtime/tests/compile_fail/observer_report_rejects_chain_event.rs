// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::stages::observer::ObserverReport;

fn main() {
    let event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "forbidden.data.v1",
        serde_json::json!({}),
    );
    let _ = ObserverReport::empty().with_diagnostic(event);
}
