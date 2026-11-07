// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::schema::TypedPayload;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParcelStatus {
    PickedUp,
    InTransit,
    OutForDelivery,
    Delivered,
}

impl ParcelStatus {
    const JOURNEY: [Self; 4] = [
        Self::PickedUp,
        Self::InTransit,
        Self::OutForDelivery,
        Self::Delivered,
    ];
}

impl fmt::Display for ParcelStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PickedUp => "picked up",
            Self::InTransit => "in transit",
            Self::OutForDelivery => "out for delivery",
            Self::Delivered => "delivered",
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParcelUpdate {
    pub parcel_id: String,
    pub customer: String,
    pub postal_code: String,
    pub status: ParcelStatus,
}

impl TypedPayload for ParcelUpdate {
    const EVENT_TYPE: &'static str = "parcel.update";
}

impl ParcelUpdate {
    pub fn located(&self, depot: &str) -> LocatedParcel {
        LocatedParcel {
            parcel_id: self.parcel_id.clone(),
            customer: self.customer.clone(),
            status: self.status,
            depot: depot.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocatedParcel {
    pub parcel_id: String,
    pub customer: String,
    pub status: ParcelStatus,
    pub depot: String,
}

impl TypedPayload for LocatedParcel {
    const EVENT_TYPE: &'static str = "parcel.located";
}

impl LocatedParcel {
    pub fn emailed(&self, message: String) -> EmailSent {
        EmailSent {
            parcel_id: self.parcel_id.clone(),
            customer: self.customer.clone(),
            message,
        }
    }

    pub fn texted(&self, message: String) -> SmsSent {
        SmsSent {
            parcel_id: self.parcel_id.clone(),
            customer: self.customer.clone(),
            message,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmailSent {
    pub parcel_id: String,
    pub customer: String,
    pub message: String,
}

impl TypedPayload for EmailSent {
    const EVENT_TYPE: &'static str = "parcel.email_sent";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SmsSent {
    pub parcel_id: String,
    pub customer: String,
    pub message: String,
}

impl TypedPayload for SmsSent {
    const EVENT_TYPE: &'static str = "parcel.sms_sent";
}

const PARCELS: [(&str, &str, &str); 8] = [
    ("P-1001", "ana", "M5V 3L9"),
    ("P-1002", "ben", "H2X 1Y4"),
    ("P-1003", "chloe", "V6B 2W9"),
    ("P-1004", "dev", "T2P 1J9"),
    ("P-1005", "emma", "K1P 5J2"),
    ("P-1006", "finn", "M4C 1B5"),
    ("P-1007", "grace", "H3B 4W8"),
    ("P-1008", "hugo", "V5K 0A1"),
];

pub fn carrier_updates() -> Vec<ParcelUpdate> {
    ParcelStatus::JOURNEY
        .into_iter()
        .flat_map(|status| {
            PARCELS
                .into_iter()
                .map(move |(parcel_id, customer, postal_code)| ParcelUpdate {
                    parcel_id: parcel_id.to_owned(),
                    customer: customer.to_owned(),
                    postal_code: postal_code.to_owned(),
                    status,
                })
        })
        .collect()
}

pub fn format_email(sent: &EmailSent) -> String {
    format!("[email] {}: {}", sent.customer, sent.message)
}

pub fn format_sms(sent: &SmsSent) -> String {
    format!("[sms]   {}: {}", sent.customer, sent.message)
}
