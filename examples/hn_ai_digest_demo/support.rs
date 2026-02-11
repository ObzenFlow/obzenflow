// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Reuse the existing HN ingestion demo building blocks; this demo only adds AI stages.
#[path = "../hn_ingestion_demo/decoder.rs"]
pub mod decoder;

#[path = "../hn_ingestion_demo/domain.rs"]
pub mod domain;

#[path = "flow.rs"]
pub mod flow;

#[path = "../hn_ingestion_demo/mock_server.rs"]
pub mod mock_server;

#[path = "../hn_ingestion_demo/util.rs"]
pub mod util;
