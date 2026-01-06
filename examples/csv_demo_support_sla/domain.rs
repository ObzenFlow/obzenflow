use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Customer {
    pub customer_id: String,
    pub plan: String,
    pub region: String,
}

impl TypedPayload for Customer {
    const EVENT_TYPE: &'static str = "support.customer";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticket {
    pub ticket_id: String,
    pub customer_id: String,
    pub created_at: String,
    pub priority: String,
    pub category: String,
}

impl TypedPayload for Ticket {
    const EVENT_TYPE: &'static str = "support.ticket";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriagedTicket {
    pub ticket_id: String,
    pub customer_id: String,
    pub created_at: String,
    pub priority: String,
    pub category: String,
    pub priority_sla_hours: u32,
}

impl TypedPayload for TriagedTicket {
    const EVENT_TYPE: &'static str = "support.ticket.triaged";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedTicket {
    pub ticket_id: String,
    pub customer_id: String,
    pub plan: String,
    pub region: String,
    pub created_at: String,
    pub priority: String,
    pub category: String,
    pub priority_sla_hours: u32,
    pub effective_sla_hours: u32,
    pub due_bucket: String,
}

impl TypedPayload for EnrichedTicket {
    const EVENT_TYPE: &'static str = "support.ticket.enriched";
    const SCHEMA_VERSION: u32 = 1;
}

pub fn priority_sla_hours(priority: &str) -> u32 {
    match priority.to_ascii_uppercase().as_str() {
        "P0" => 4,
        "P1" => 24,
        "P2" => 72,
        "P3" => 168,
        _ => 168,
    }
}

pub fn plan_sla_cap_hours(plan: &str) -> u32 {
    match plan.to_ascii_lowercase().as_str() {
        "enterprise" => 8,
        "pro" => 24,
        "free" => 72,
        _ => 72,
    }
}

pub fn due_bucket(effective_sla_hours: u32) -> &'static str {
    if effective_sla_hours <= 4 {
        "paged"
    } else if effective_sla_hours <= 24 {
        "same_day"
    } else {
        "backlog"
    }
}

