// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CarrierDetails {
    pub carrier_code: String,
    pub carrier_name: String,
    pub country: String,
    pub fleet_size: u32,
}

impl TypedPayload for CarrierDetails {
    const EVENT_TYPE: &'static str = "carrier.details";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightRecord {
    pub carrier: String,
    pub date: String,
    pub origin: String,
    pub destination: String,
    pub scheduled_duration: u32,
    pub delay_minutes: u32,
    pub flight_number: String,
    pub delay_category: Option<String>,
}

impl TypedPayload for FlightRecord {
    const EVENT_TYPE: &'static str = "flight.record";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedFlight {
    pub carrier_code: String,
    pub carrier_name: String,
    pub carrier_country: String,
    pub date: String,
    pub origin: String,
    pub destination: String,
    pub scheduled_duration: u32,
    pub delay_minutes: u32,
    pub flight_number: String,
    pub delay_category: Option<String>,
}

impl TypedPayload for EnrichedFlight {
    const EVENT_TYPE: &'static str = "flight.enriched";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CarrierStatistics {
    pub carrier: String,
    pub total_delay: u64,
    pub flight_count: u64,
    pub average_delay: f64,
}

impl TypedPayload for CarrierStatistics {
    const EVENT_TYPE: &'static str = "carrier.statistics";
    const SCHEMA_VERSION: u32 = 1;
}
