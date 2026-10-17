// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::schema::TypedPayload;
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TxnId(pub u64);

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AccountKey {
    pub bank: String,
    pub account: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PaymentFormat {
    #[serde(rename = "ACH")]
    Ach,
    Bitcoin,
    Cash,
    Cheque,
    #[serde(rename = "Credit Card")]
    CreditCard,
    Reinvestment,
    Wire,
}

impl FromStr for PaymentFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ACH" => Ok(Self::Ach),
            "Bitcoin" => Ok(Self::Bitcoin),
            "Cash" => Ok(Self::Cash),
            "Cheque" => Ok(Self::Cheque),
            "Credit Card" => Ok(Self::CreditCard),
            "Reinvestment" => Ok(Self::Reinvestment),
            "Wire" => Ok(Self::Wire),
            other => Err(format!("unknown payment format '{other}'")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountHistory {
    pub bank: String,
    pub account: String,
    pub payments: u64,
    pub counterparties: u64,
    #[serde(deserialize_with = "payment_formats")]
    pub formats: Vec<PaymentFormat>,
}

impl AccountHistory {
    pub fn key(&self) -> AccountKey {
        AccountKey {
            bank: self.bank.clone(),
            account: self.account.clone(),
        }
    }
}

impl TypedPayload for AccountHistory {
    const EVENT_TYPE: &'static str = "aml.account_history";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    pub txn_id: TxnId,
    pub timestamp: String,
    pub from_bank: String,
    pub from_account: String,
    pub to_bank: String,
    pub to_account: String,
    pub amount_received: f64,
    pub receiving_currency: String,
    pub amount_paid: f64,
    pub payment_currency: String,
    pub payment_format: PaymentFormat,
    #[serde(deserialize_with = "laundering_label")]
    pub is_laundering: bool,
}

impl Transaction {
    pub fn payer(&self) -> AccountKey {
        AccountKey {
            bank: self.from_bank.clone(),
            account: self.from_account.clone(),
        }
    }

    pub fn payee(&self) -> AccountKey {
        AccountKey {
            bank: self.to_bank.clone(),
            account: self.to_account.clone(),
        }
    }
}

impl TypedPayload for Transaction {
    const EVENT_TYPE: &'static str = "aml.transaction";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfiledTransaction {
    pub transaction: Transaction,
    pub history: Option<AccountHistory>,
}

impl TypedPayload for ProfiledTransaction {
    const EVENT_TYPE: &'static str = "aml.transaction_profiled";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReasonCode(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RulesVersion(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reasons {
    pub first: ReasonCode,
    pub rest: Vec<ReasonCode>,
}

impl Reasons {
    pub fn from_fired(fired: Vec<ReasonCode>) -> Option<Self> {
        let mut fired = fired.into_iter();
        let first = fired.next()?;
        Some(Self {
            first,
            rest: fired.collect(),
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &ReasonCode> {
        std::iter::once(&self.first).chain(self.rest.iter())
    }
}

impl Serialize for Reasons {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(1 + self.rest.len()))?;
        for reason in self.iter() {
            seq.serialize_element(reason)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Reasons {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fired = Vec::<ReasonCode>::deserialize(deserializer)?;
        Self::from_fired(fired).ok_or_else(|| de::Error::custom("a flag needs at least one reason"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CaseNumber(pub u64);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionCleared {
    pub txn_id: TxnId,
    pub rules_version: RulesVersion,
}

impl TypedPayload for TransactionCleared {
    const EVENT_TYPE: &'static str = "aml.transaction_cleared";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertRaised {
    pub txn_id: TxnId,
    pub account: AccountKey,
    pub reasons: Reasons,
    pub rules_version: RulesVersion,
}

impl TypedPayload for AlertRaised {
    const EVENT_TYPE: &'static str = "aml.alert_raised";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScoreUnavailable {
    pub txn_id: TxnId,
    pub account: AccountKey,
    pub reason: String,
}

impl TypedPayload for ScoreUnavailable {
    const EVENT_TYPE: &'static str = "aml.score_unavailable";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CaseOpened {
    pub case_number: CaseNumber,
    pub txn_id: TxnId,
    pub account: AccountKey,
    pub reasons: Reasons,
    pub rules_version: RulesVersion,
}

impl TypedPayload for CaseOpened {
    const EVENT_TYPE: &'static str = "aml.case_opened";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CaseUnavailable {
    pub txn_id: TxnId,
    pub account: AccountKey,
    pub reason: String,
}

impl TypedPayload for CaseUnavailable {
    const EVENT_TYPE: &'static str = "aml.case_unavailable";
    const SCHEMA_VERSION: u32 = 1;
}

fn laundering_label<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    struct Label;

    impl<'de> Visitor<'de> for Label {
        type Value = bool;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("0, 1, true or false")
        }

        fn visit_bool<E: de::Error>(self, value: bool) -> Result<bool, E> {
            Ok(value)
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<bool, E> {
            match value {
                0 => Ok(false),
                1 => Ok(true),
                other => Err(E::custom(format!("laundering label {other} is not 0 or 1"))),
            }
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<bool, E> {
            u64::try_from(value)
                .map_err(|_| E::custom(format!("laundering label {value} is not 0 or 1")))
                .and_then(|value| self.visit_u64(value))
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<bool, E> {
            match value {
                "0" | "false" => Ok(false),
                "1" | "true" => Ok(true),
                other => Err(E::custom(format!(
                    "laundering label '{other}' is not 0 or 1"
                ))),
            }
        }
    }

    deserializer.deserialize_any(Label)
}

fn payment_formats<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<PaymentFormat>, D::Error> {
    struct Formats;

    impl<'de> Visitor<'de> for Formats {
        type Value = Vec<PaymentFormat>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("payment formats separated by ';' or a list of them")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            value
                .split(';')
                .filter(|format| !format.is_empty())
                .map(|format| format.parse().map_err(E::custom))
                .collect()
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut formats = Vec::new();
            while let Some(format) = seq.next_element()? {
                formats.push(format);
            }
            Ok(formats)
        }
    }

    deserializer.deserialize_any(Formats)
}
