// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{EmailSent, LocatedParcel, ParcelUpdate, SmsSent};
use async_trait::async_trait;
use obzenflow::effects::EffectError;
use serde::Serialize;
use std::time::Duration;

#[obzenflow::port]
#[async_trait]
pub trait DepotDirectory: Send + Sync {
    #[effect(type = "depot.lookup", exactly_once)]
    async fn lookup(&self, request: &DepotLookup) -> Result<LocatedParcel, EffectError>;
}

#[obzenflow::port]
#[async_trait]
pub trait CustomerMessaging: Send + Sync {
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
    parcel: LocatedParcel,
}

impl EmailNotice {
    pub fn for_parcel(parcel: &LocatedParcel) -> Self {
        Self {
            parcel: parcel.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SmsNotice {
    parcel: LocatedParcel,
}

impl SmsNotice {
    pub fn for_parcel(parcel: &LocatedParcel) -> Self {
        Self {
            parcel: parcel.clone(),
        }
    }
}

#[derive(Debug)]
pub struct SimulatedDepots;

#[obzenflow::system("depot-directory")]
#[async_trait]
impl DepotDirectory for SimulatedDepots {
    async fn lookup(&self, request: &DepotLookup) -> Result<LocatedParcel, EffectError> {
        tokio::time::sleep(Duration::from_millis(40)).await;
        let depot = match request.update.postal_code.chars().next() {
            Some('M') => "Toronto",
            Some('H') => "Montréal",
            Some('V') => "Vancouver",
            Some('T') => "Calgary",
            _ => "Ottawa",
        };
        Ok(request.update.located(depot))
    }
}

#[derive(Debug)]
pub struct SimulatedMessaging;

#[obzenflow::system("messaging-provider")]
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
