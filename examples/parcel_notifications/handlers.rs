// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{DepotFound, EmailSent, ParcelStatus, SmsSent};
use super::services::{CustomerMessaging, EmailNotice, SmsNotice};
use async_trait::async_trait;
use obzenflow::effects::{Effects, StageCompletion};
use obzenflow::error::HandlerError;
use obzenflow::schema::outcome;
use obzenflow::stages::transforms::EffectfulTransformHandler;

#[obzenflow::command(through = CustomerMessaging)]
#[derive(Clone, Debug)]
pub struct NotifyCustomer;

#[async_trait]
impl EffectfulTransformHandler for NotifyCustomer {
    type Input = DepotFound;
    type Output = outcome![EmailSent | SmsSent];

    async fn process(
        &self,
        parcel: DepotFound,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.call(EmailNotice::for_parcel(&parcel)).await?;
        if parcel.status == ParcelStatus::OutForDelivery {
            fx.call(SmsNotice::for_parcel(&parcel)).await?;
        }
        Ok(fx.complete()?)
    }
}
