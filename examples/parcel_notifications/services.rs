// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{DepotFound, EmailSent, ParcelUpdate, SmsSent};
use async_trait::async_trait;
use obzenflow::effects::EffectError;
use serde::Serialize;
use std::time::Duration;

#[obzenflow::port]
#[async_trait]
pub trait DepotDirectory {
    #[effect(type = "depot.lookup", exactly_once)]
    async fn lookup(&self, request: &DepotLookup) -> Result<DepotFound, EffectError>;
}

#[obzenflow::port]
#[async_trait]
pub trait CustomerMessaging {
    #[effect(type = "notify.email", exactly_once)]
    async fn email(&self, notice: &EmailNotice) -> Result<EmailSent, EffectError>;

    #[effect(type = "notify.sms", exactly_once)]
    async fn sms(&self, notice: &SmsNotice) -> Result<SmsSent, EffectError>;
}

#[derive(Clone, Debug, Serialize)]
pub struct DepotLookup {
    update: ParcelUpdate,
}

impl DepotLookup {
    pub fn for_update(update: &ParcelUpdate) -> Self {
        Self {
            update: update.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EmailNotice {
    parcel: DepotFound,
}

impl EmailNotice {
    pub fn for_parcel(parcel: &DepotFound) -> Self {
        Self {
            parcel: parcel.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SmsNotice {
    parcel: DepotFound,
}

impl SmsNotice {
    pub fn for_parcel(parcel: &DepotFound) -> Self {
        Self {
            parcel: parcel.clone(),
        }
    }
}

#[derive(Debug)]
pub struct SimulatedDepots;

#[async_trait]
impl DepotDirectory for SimulatedDepots {
    async fn lookup(&self, request: &DepotLookup) -> Result<DepotFound, EffectError> {
        tokio::time::sleep(Duration::from_millis(40)).await;
        let depot = match request.update.postal_code.chars().next() {
            Some('M') => "Toronto",
            Some('H') => "Montréal",
            Some('V') => "Vancouver",
            Some('T') => "Calgary",
            _ => "Ottawa",
        };
        Ok(request.update.depot_found(depot))
    }
}

#[derive(Debug)]
pub struct SimulatedMessaging;

#[async_trait]
impl CustomerMessaging for SimulatedMessaging {
    async fn email(&self, notice: &EmailNotice) -> Result<EmailSent, EffectError> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let parcel = &notice.parcel;
        let message = format!(
            "parcel {} is {}, {} depot",
            parcel.parcel_id, parcel.status, parcel.depot
        );
        Ok(parcel.emailed(message))
    }

    async fn sms(&self, notice: &SmsNotice) -> Result<SmsSent, EffectError> {
        tokio::time::sleep(Duration::from_millis(60)).await;
        let parcel = &notice.parcel;
        let message = format!("parcel {} arrives today", parcel.parcel_id);
        Ok(parcel.texted(message))
    }
}
