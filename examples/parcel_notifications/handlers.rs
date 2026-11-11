// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{EmailSent, LocatedParcel, ParcelStatus, SmsSent};
use super::services::{CustomerMessaging, EmailNotice, SmsNotice};
use async_trait::async_trait;
use obzenflow::effects::{Effects, Port, StageCompletion};
use obzenflow::error::HandlerError;
use obzenflow::stages::transforms::EffectfulTransformHandler;

#[obzenflow::command]
#[derive(Clone, Debug)]
pub struct NotifyCustomer {
    #[port]
    messaging: Port<dyn CustomerMessaging>,
}

impl NotifyCustomer {
    pub fn new(messaging: Port<dyn CustomerMessaging>) -> Self {
        Self { messaging }
    }
}

#[async_trait]
impl EffectfulTransformHandler for NotifyCustomer {
    type Input = LocatedParcel;
    type Output = obzenflow::schema::stage_fact_set![EmailSent, SmsSent];

    async fn process(
        &self,
        parcel: LocatedParcel,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.call(EmailNotice::for_parcel(&parcel)).await?;
        if parcel.status == ParcelStatus::OutForDelivery {
            fx.call(SmsNotice::for_parcel(&parcel)).await?;
        }
        Ok(fx.complete()?)
    }
}
