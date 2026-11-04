// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! User-owned facts for the flash-sale allocation process.

use obzenflow::schema::TypedPayload;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderId(String);

impl From<&str> for OrderId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for OrderId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sku(String);

impl From<&str> for Sku {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HoldId(String);

impl From<String> for HoldId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// A fact that an order entered the limited-stock allocation process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderPlaced {
    pub order_id: OrderId,
    pub sku: Sku,
}

/// A fact that an order was cancelled after entering the process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCancelled {
    pub order_id: OrderId,
}

/// The domain input sum consumed by the one serial allocation authority.
///
/// This is the real source and handler input type, not a transport DTO. Its
/// variants are facts that already happened upstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllocationInput {
    OrderPlaced(OrderPlaced),
    OrderCancelled(OrderCancelled),
}

impl TypedPayload for AllocationInput {
    const EVENT_TYPE: &'static str = "allocation.input";
    const SCHEMA_VERSION: u32 = 1;
}

/// The warehouse accepted a hold already authorised by the allocator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StockReserved {
    pub order_id: OrderId,
    pub sku: Sku,
    pub hold_id: HoldId,
}

impl TypedPayload for StockReserved {
    const EVENT_TYPE: &'static str = "allocation.stock_reserved";
    const SCHEMA_VERSION: u32 = 1;
}

/// The warehouse released a previously folded hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StockReleased {
    pub order_id: OrderId,
    pub sku: Sku,
    pub hold_id: HoldId,
}

impl TypedPayload for StockReleased {
    const EVENT_TYPE: &'static str = "allocation.stock_released";
    const SCHEMA_VERSION: u32 = 1;
}

/// The allocator could not reach the reserve path while it was degraded.
///
/// This payload intentionally contains domain identity only. Breaker state,
/// policy names, and framework error codes remain in the separate effect
/// failure record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationFailed {
    pub order_id: OrderId,
    pub sku: Sku,
}

impl TypedPayload for ReservationFailed {
    const EVENT_TYPE: &'static str = "allocation.reservation_failed";
    const SCHEMA_VERSION: u32 = 1;
}

/// The allocator's own configured capacity was already allocated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoldOut {
    pub order_id: OrderId,
    pub sku: Sku,
}

impl TypedPayload for SoldOut {
    const EVENT_TYPE: &'static str = "allocation.sold_out";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationConfirmed {
    pub order_id: OrderId,
    pub sku: Sku,
    pub confirmation: u64,
}

impl TypedPayload for ReservationConfirmed {
    const EVENT_TYPE: &'static str = "allocation.reservation_confirmed";
    const SCHEMA_VERSION: u32 = 1;
}

/// A cancellation had no currently folded warehouse hold to release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelIgnored {
    pub order_id: OrderId,
}

impl TypedPayload for CancelIgnored {
    const EVENT_TYPE: &'static str = "allocation.cancel_ignored";
    const SCHEMA_VERSION: u32 = 1;
}
