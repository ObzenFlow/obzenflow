// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Test-facing aggregator mirroring `payment_gateway_resilience/support.rs`:
//! integration tests include this one file instead of each sibling module,
//! keeping example items on a public path.

#[path = "allocation.rs"]
pub mod allocation;

#[path = "desk.rs"]
pub mod desk;

#[path = "domain.rs"]
pub mod domain;

#[path = "flow.rs"]
pub mod flow;

#[path = "warehouse.rs"]
pub mod warehouse;
