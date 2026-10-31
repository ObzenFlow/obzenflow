// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use super::handlers::{AccountLedger, Assess};
use obzenflow::flow::{
    async_infinite_source, flow, join, merge, sink, source, stateful, transform, FlowBuildError,
    FlowDefinition,
};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::rate_limit;
use obzenflow::stages::sinks::SnapshotTableFormatter;
use obzenflow::stages::sources::{
    CsvDecodeError, CsvDecoder, CsvRecord, CsvSource, HostedIngressSource, IngressDecodeError,
    IngressDecoder, IngressRecord, YamlDecodeError, YamlDecoder, YamlRecord, YamlSelection,
    YamlSource,
};
use obzenflow::stages::stateful::emit;
use obzenflow::stages::{joins, merges, sinks, stateful, transforms};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const DEMO_JOURNAL_ROOT: &str = "target/http-ingestion-piggy-bank-demo-logs";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiggyBankSettings {
    accounts_file: PathBuf,
    transactions_file: PathBuf,
    ach_debits_file: PathBuf,
    #[serde(default)]
    fee_rule: FeeRule,
}

/// Decodes one entry of `accounts.yaml`; a negative opening balance rejects
/// only that account.
#[derive(Clone, Debug)]
pub(crate) struct AccountYaml;

impl YamlDecoder for AccountYaml {
    type Output = AccountOpened;

    fn decode(&self, record: YamlRecord<'_>) -> Result<AccountOpened, YamlDecodeError> {
        let account: AccountOpened = record.deserialize()?;
        if account.initial_balance_cents < 0 {
            return Err(YamlDecodeError::invalid_value("initial_balance_cents"));
        }
        Ok(account)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerRow {
    account_id: String,
    kind: EntryKind,
    amount_cents: u64,
    #[serde(default)]
    note: Option<String>,
}

impl LedgerRow {
    fn into_entry(self) -> LedgerEntry {
        LedgerEntry {
            account_id: self.account_id,
            kind: self.kind,
            amount_cents: self.amount_cents,
            note: self.note,
            presentment: None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LedgerCsv;

impl CsvDecoder for LedgerCsv {
    type Output = LedgerEntry;

    fn decode(&self, record: CsvRecord<'_>) -> Result<LedgerEntry, CsvDecodeError> {
        let row: LedgerRow = record.deserialize()?;
        if row.amount_cents == 0 {
            return Err(CsvDecodeError::invalid_value("amount_cents"));
        }
        Ok(row.into_entry())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AchDebitCsv;

impl CsvDecoder for AchDebitCsv {
    type Output = AchDebit;

    fn decode(&self, record: CsvRecord<'_>) -> Result<AchDebit, CsvDecodeError> {
        let debit: AchDebit = record.deserialize()?;
        if debit.amount_cents == 0 {
            return Err(CsvDecodeError::invalid_value("amount_cents"));
        }
        Ok(debit)
    }
}

#[derive(Clone, Debug)]
pub struct AccountIngress;

impl IngressDecoder for AccountIngress {
    type Output = AccountOpened;

    fn decode(&self, record: IngressRecord<'_>) -> Result<AccountOpened, IngressDecodeError> {
        let account: AccountOpened = record.deserialize()?;
        if account.initial_balance_cents < 0 {
            return Err(IngressDecodeError::invalid_value("initial_balance_cents"));
        }
        Ok(account)
    }
}

/// Decodes one posted ledger entry; a zero amount rejects only that entry.
#[derive(Clone, Debug)]
pub struct LedgerIngress;

impl IngressDecoder for LedgerIngress {
    type Output = LedgerEntry;

    fn decode(&self, record: IngressRecord<'_>) -> Result<LedgerEntry, IngressDecodeError> {
        let row: LedgerRow = record.deserialize()?;
        if row.amount_cents == 0 {
            return Err(IngressDecodeError::invalid_value("amount_cents"));
        }
        Ok(row.into_entry())
    }
}

#[derive(Clone, Debug)]
pub struct AchIngress;

impl IngressDecoder for AchIngress {
    type Output = AchDebit;

    fn decode(&self, record: IngressRecord<'_>) -> Result<AchDebit, IngressDecodeError> {
        let debit: AchDebit = record.deserialize()?;
        if debit.amount_cents == 0 {
            return Err(IngressDecodeError::invalid_value("amount_cents"));
        }
        Ok(debit)
    }
}

fn present(debit: AchDebit) -> LedgerEntry {
    LedgerEntry {
        account_id: debit.account_id,
        kind: EntryKind::Debit,
        amount_cents: debit.amount_cents,
        note: None,
        presentment: Some(Presentment {
            company_name: debit.company_name,
            company_id: debit.company_id,
            entry_description: debit.entry_description,
        }),
    }
}

fn post(account: AccountOpened, entry: LedgerEntry) -> PostedEntry {
    PostedEntry {
        account_id: entry.account_id,
        owner: account.owner,
        kind: entry.kind,
        amount_cents: entry.amount_cents,
        initial_balance_cents: account.initial_balance_cents,
        note: entry.note,
        presentment: entry.presentment,
    }
}

fn return_line(
    account_id: &str,
    company_name: &str,
    amount_cents: u64,
    presentment: u32,
    fee: &str,
    fee_rule: &str,
) -> String {
    format!(
        "{account_id:<9}  {company_name:<16}  {:>8}  presentment {presentment}  returned, {fee} ({fee_rule})",
        format_unsigned_cents(amount_cents),
    )
}

fn fee_charged_line(charged: &FeeCharged) -> String {
    return_line(
        &charged.account_id,
        &charged.company_name,
        charged.amount_cents,
        charged.presentment,
        &format!("fee {}", format_unsigned_cents(charged.fee_cents)),
        &charged.fee_rule,
    )
}

fn retry_returned_line(returned: &RetryReturned) -> String {
    return_line(
        &returned.account_id,
        &returned.company_name,
        returned.amount_cents,
        returned.presentment,
        "no fee",
        &returned.fee_rule,
    )
}

pub fn build_flow(
    new_accounts_source: HostedIngressSource<AccountIngress>,
    tx_source: HostedIngressSource<LedgerIngress>,
    ach_source: HostedIngressSource<AchIngress>,
) -> FlowDefinition {
    assemble_flow(
        PathBuf::from(DEMO_JOURNAL_ROOT),
        new_accounts_source,
        tx_source,
        ach_source,
    )
}

pub fn assemble_flow(
    journal_root: PathBuf,
    new_accounts_source: HostedIngressSource<AccountIngress>,
    tx_source: HostedIngressSource<LedgerIngress>,
    ach_source: HostedIngressSource<AchIngress>,
) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: PiggyBankSettings| {
        let accounts_source = YamlSource::builder(AccountYaml)
            .path(settings.accounts_file)
            .selection(YamlSelection::SequenceAt("/accounts".into()))
            .build()
            .map_err(|error| FlowBuildError::StageResourcesFailed(error.to_string()))?;
        let ledger_source = CsvSource::builder(LedgerCsv)
            .path(settings.transactions_file)
            .chunk_size(1)
            .build()
            .map_err(|error| FlowBuildError::StageResourcesFailed(error.to_string()))?;
        let ach_debits_source = CsvSource::builder(AchDebitCsv)
            .path(settings.ach_debits_file)
            .chunk_size(1)
            .build()
            .map_err(|error| FlowBuildError::StageResourcesFailed(error.to_string()))?;
        let batch_presentments = transforms::map(present);
        let live_presentments = transforms::map(present);
        let assess_handler = Assess::new(settings.fee_rule);
        let post_entry = joins::inner_then_live(
            |account: &AccountOpened| account.account_id.clone(),
            |entry: &LedgerEntry| entry.account_id.clone(),
            post,
        );
        let checkbook_handler = stateful::group_by(
            |entry: &BalanceMoved| entry.account_id.clone(),
            AccountLedger::record,
            |account_id: &String, ledger: &AccountLedger| ledger.snapshot(account_id),
        )
        .emit(emit::always)
        .updated_groups();
        let printer_sink = sinks::ConsoleSink::<CheckbookSnapshot, _>::new(
            SnapshotTableFormatter::new(
                &["#", "Kind", "Amount", "Credit", "Debit", "Balance", "Note"],
                |snapshot: &CheckbookSnapshot| {
                    snapshot
                        .transactions
                        .iter()
                        .map(|entry| {
                            let amount = format_unsigned_cents(entry.amount_cents);
                            let (credit, debit) = match entry.kind {
                                EntryKind::Credit => (amount.clone(), String::new()),
                                EntryKind::Debit => (String::new(), amount.clone()),
                            };

                            let kind_label = match entry.kind {
                                EntryKind::Credit => "Credit",
                                EntryKind::Debit => "Debit",
                            };

                            vec![
                                entry.index.to_string(),
                                kind_label.to_string(),
                                amount,
                                credit,
                                debit,
                                format_cents(entry.balance_cents),
                                entry.note.as_deref().unwrap_or("").to_string(),
                            ]
                        })
                        .collect()
                },
            )
            .with_header(|snapshot: &CheckbookSnapshot| {
                vec![
                    format!("Account: {} ({})", snapshot.account_id, snapshot.owner,),
                    format!(
                        "Current: {} | Available: {}",
                        format_cents(snapshot.current_balance_cents),
                        format_cents(snapshot.available_balance_cents),
                    ),
                ]
            })
            .with_footer(|snapshot: &CheckbookSnapshot| {
                vec![format!(
                    "Credits: {} | Debits: {} | Tx: {}",
                    format_cents(snapshot.total_credits_cents),
                    format_cents(snapshot.total_debits_cents),
                    snapshot.transactions.len(),
                )]
            }),
        );

        let fee_report_sink = sinks::ConsoleSink::new(fee_charged_line);
        let free_retry_sink = sinks::ConsoleSink::new(retry_returned_line);

        Ok(flow! {
            name: "http_ingestion_piggy_bank_demo",
            journals: disk_journals(journal_root.clone()),

            stages: {
                // Ingestion
                batch_accounts = source!(AccountOpened => accounts_source);
                batch_tx = source!(LedgerEntry with { rate_limit(2.0) } => ledger_source);
                batch_ach = source!(AchDebit with { rate_limit(2.0) } => ach_debits_source);
                live_accounts = async_infinite_source!(AccountOpened => new_accounts_source);
                live_tx = async_infinite_source!(
                    LedgerEntry with { rate_limit(50.0) } => tx_source
                );
                live_ach = async_infinite_source!(AchDebit => ach_source);

                // Processing
                batch_presentments = transform!(AchDebit -> LedgerEntry => batch_presentments);
                live_presentments = transform!(AchDebit -> LedgerEntry => live_presentments);
                accounts = merge!(AccountOpened => merges::then_live);
                transactions = merge!(LedgerEntry => merges::then_live);
                posted = join!(
                    catalog accounts: AccountOpened,
                    LedgerEntry -> PostedEntry => post_entry
                );
                assess = stateful!(
                    PostedEntry -> { BalanceMoved, FeeCharged, RetryReturned } => assess_handler
                );
                checkbook = stateful!(
                    BalanceMoved -> CheckbookSnapshot => checkbook_handler
                );

                // Delivery
                printer = sink!(
                    CheckbookSnapshot => printer_sink
                );
                fee_report = sink!(FeeCharged => fee_report_sink);
                free_retries = sink!(RetryReturned => free_retry_sink);
            },

            topology: {
                batch_accounts |> accounts;
                live_accounts |> accounts;
                batch_ach |> batch_presentments;
                live_ach |> live_presentments;
                batch_tx |> transactions;
                batch_presentments |> transactions;
                live_tx |> transactions;
                live_presentments |> transactions;
                (accounts, transactions) |> posted;
                posted |> assess;
                assess |> checkbook;
                assess |> fee_report;
                assess |> free_retries;
                checkbook |> printer;
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn only_the_ach_path_takes_nacha_fields_and_it_takes_them_all() {
        let credit_with_retry_fields = serde_json::json!({
            "account_id": "chk-58213",
            "kind": "Credit",
            "amount_cents": 4999,
            "company_name": "NORTHSIDE FIT",
            "company_id": "1834577921",
            "entry_description": "RETRY PYMT"
        });
        assert!(LedgerIngress
            .decode(IngressRecord::new(&credit_with_retry_fields))
            .is_err());

        let partial_debit = serde_json::json!({
            "account_id": "chk-58213",
            "company_name": "NORTHSIDE FIT",
            "amount_cents": 4999
        });
        assert!(AchIngress
            .decode(IngressRecord::new(&partial_debit))
            .is_err());

        let retry = serde_json::json!({
            "account_id": "chk-58213",
            "company_name": "NORTHSIDE FIT",
            "company_id": "1834577921",
            "entry_description": "RETRY PYMT",
            "amount_cents": 4999
        });
        let entry = present(
            AchIngress
                .decode(IngressRecord::new(&retry))
                .expect("a whole ACH debit decodes"),
        );
        assert_eq!(entry.kind, EntryKind::Debit);
        assert!(entry
            .presentment
            .is_some_and(|presentment| presentment.is_retry()));
    }

    fn shipped_app(file: &str) -> toml::Table {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/http_ingestion_piggy_bank_demo")
            .join(file);
        let mut config: toml::Table = std::fs::read_to_string(path)
            .expect("the shipped configuration is readable")
            .parse()
            .expect("the shipped configuration parses");
        match config.remove("app") {
            Some(toml::Value::Table(app)) => app,
            other => panic!("{file} has no [app] table: {other:?}"),
        }
    }

    fn settings(fee_rule: &str) -> Result<PiggyBankSettings, toml::de::Error> {
        toml::from_str(&format!(
            "accounts_file = \"a.yaml\"\n\
             transactions_file = \"t.csv\"\n\
             ach_debits_file = \"d.csv\"\n\
             {fee_rule}"
        ))
    }

    #[test]
    fn fee_rule_takes_either_spelling_and_defaults_to_fee_per_return() {
        assert_eq!(settings("").unwrap().fee_rule, FeeRule::FeePerReturn);
        assert_eq!(
            settings("fee_rule = \"fee_per_return\"").unwrap().fee_rule,
            FeeRule::FeePerReturn
        );
        assert_eq!(
            settings("fee_rule = \"one_fee_per_payment\"")
                .unwrap()
                .fee_rule,
            FeeRule::OneFeePerPayment
        );
        let misspelled = settings("fee_rule = \"one_fee_per_paymnet\"")
            .unwrap_err()
            .to_string();
        assert!(
            misspelled.contains("`fee_per_return`") && misspelled.contains("`one_fee_per_payment`"),
            "{misspelled}"
        );
        let unknown = settings("fee_rul = \"one_fee_per_payment\"")
            .unwrap_err()
            .to_string();
        assert!(unknown.contains("`fee_rule`"), "{unknown}");
    }

    #[test]
    fn every_shipped_configuration_reads_the_default_settings_except_the_v2_fee_rule() {
        let default = shipped_app("obzenflow.toml");
        toml::Value::Table(default.clone())
            .try_into::<PiggyBankSettings>()
            .expect("obzenflow.toml");
        for file in [
            "obzenflow.auth.toml",
            "obzenflow.studio.toml",
            "obzenflow.fee-rule-v2.toml",
        ] {
            let mut app = shipped_app(file);
            toml::Value::Table(app.clone())
                .try_into::<PiggyBankSettings>()
                .expect(file);
            if file == "obzenflow.fee-rule-v2.toml" {
                assert_eq!(
                    app.remove("fee_rule"),
                    Some(toml::Value::String("one_fee_per_payment".to_owned()))
                );
            }
            assert_eq!(app, default, "{file}");
        }
    }
}
