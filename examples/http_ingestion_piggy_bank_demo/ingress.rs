// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared HTTP-ingress configuration for the piggy bank demo.

use obzenflow_infra::web::endpoints::event_ingestion::IngestionConfig;

pub const ACCOUNTS_BASE_PATH: &str = "/api/bank/accounts";
pub const TX_BASE_PATH: &str = "/api/bank/tx";

pub fn config(base_path: impl Into<String>) -> IngestionConfig {
    IngestionConfig {
        base_path: base_path.into(),
        ..Default::default()
    }
}
