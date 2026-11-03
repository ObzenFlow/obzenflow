// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::schema::TypedPayload;
use serde::{Deserialize, Serialize};

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

pub fn priority_sla_hours(priority: &str) -> u32 {
    match priority.to_ascii_uppercase().as_str() {
        "P0" => 4,
        "P1" => 24,
        "P2" => 72,
        "P3" => 168,
        _ => 168,
    }
}

/// Deterministic domain projection shared by the example and its replay oracle.
pub fn triage_ticket(ticket: Ticket) -> TriagedTicket {
    let priority_sla_hours = priority_sla_hours(&ticket.priority);
    TriagedTicket {
        ticket_id: ticket.ticket_id,
        customer_id: ticket.customer_id,
        created_at: ticket.created_at,
        priority: ticket.priority,
        category: ticket.category,
        priority_sla_hours,
    }
}
