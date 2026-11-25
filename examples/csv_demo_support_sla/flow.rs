// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{
    due_bucket, plan_sla_cap_hours, triage_ticket, Customer, EnrichedTicket, Ticket, TriagedTicket,
};
use super::scripted::TicketFixture;
use anyhow::Result;
use obzenflow::application::{FlowApplication, LogLevel, Presentation};
use obzenflow::error::HandlerError;
use obzenflow::flow::{flow, join, sink, source, transform, FlowBuildError, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::stages::joins::{JoinReferenceView, TypedJoinHandler};
use obzenflow::stages::sinks::{CsvProjection, CsvSink};
use obzenflow::stages::sources::{CsvDecoder, CsvFile, CsvSource};
use obzenflow::stages::transforms::TypedTransformHandler;
use obzenflow::systems::file::CsvFileSystem;
use obzenflow::systems::{Port, SystemConfig};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::PathBuf;

pub const DEMO_JOURNAL_ROOT: &str = "target/csv-demo-support-sla/logs";
const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/csv_demo_support_sla/obzenflow.toml"
);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportSlaSettings {
    pub customers_csv: PathBuf,
    pub tickets: TicketInput,
    pub output_csv: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TicketInput {
    File(PathBuf),
    Fixture(TicketFixture),
}

#[derive(Clone, Debug)]
struct CustomerCsv;

impl CsvDecoder for CustomerCsv {
    type Output = Customer;
}

#[derive(Clone, Debug)]
struct TicketCsv;

impl CsvDecoder for TicketCsv {
    type Output = Ticket;
}

#[derive(Clone, Debug)]
struct TicketTriage;

impl TicketTriage {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for TicketTriage {
    type Input = Ticket;
    type Output = TriagedTicket;

    fn process(&self, ticket: Ticket) -> Result<TriagedTicket, HandlerError> {
        Ok(triage_ticket(ticket))
    }
}

#[derive(Clone, Debug)]
struct SupportSlaJoin;

impl TypedJoinHandler for SupportSlaJoin {
    type State = ();
    type ReferenceKey = String;
    type Reference = Customer;
    type Stream = TriagedTicket;
    type Output = EnrichedTicket;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(
        &self,
        customer: &Self::Reference,
    ) -> Result<Self::ReferenceKey, HandlerError> {
        Ok(customer.customer_id.clone())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
        ticket: Self::Stream,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        let Some(customer) = references.select(&ticket.customer_id) else {
            return Ok(Vec::new());
        };
        let cap_hours = plan_sla_cap_hours(&customer.plan);
        let effective_sla_hours = ticket.priority_sla_hours.min(cap_hours);
        let due_bucket = due_bucket(effective_sla_hours).to_string();

        Ok(vec![EnrichedTicket {
            ticket_id: ticket.ticket_id,
            customer_id: ticket.customer_id,
            plan: customer.plan,
            region: customer.region,
            created_at: ticket.created_at,
            priority: ticket.priority,
            category: ticket.category,
            priority_sla_hours: ticket.priority_sla_hours,
            effective_sla_hours,
            due_bucket,
        }])
    }
}

#[derive(Clone, Debug)]
struct EnrichedTicketCsv;

impl CsvProjection for EnrichedTicketCsv {
    type Input = EnrichedTicket;
    type Row = EnrichedTicket;

    fn project(&self, ticket: Self::Input) -> Result<Self::Row, HandlerError> {
        Ok(ticket)
    }
}

fn resources_failed(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(error.to_string())
}

fn build_flow() -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: SupportSlaSettings| {
        let customers = CsvSource::builder(CustomerCsv)
            .path(&settings.customers_csv)
            .build()
            .map_err(resources_failed)?;
        let ticket_store: Port<dyn CsvFile> = match &settings.tickets {
            TicketInput::File(path) => CsvFileSystem::at(path).port(),
            TicketInput::Fixture(config) => config.clone().port(),
        };
        let tickets = CsvSource::builder(TicketCsv)
            .through(ticket_store)
            .chunk_size(25)
            .build()
            .map_err(resources_failed)?;

        if let Some(outputs) = settings
            .output_csv
            .parent()
            .filter(|outputs| !outputs.as_os_str().is_empty())
        {
            std::fs::create_dir_all(outputs).map_err(|error| {
                resources_failed(format!("create outputs dir {}: {error}", outputs.display()))
            })?;
        }
        let output_sink = CsvSink::builder(EnrichedTicketCsv)
            .path(&settings.output_csv)
            .columns([
                "ticket_id",
                "customer_id",
                "plan",
                "region",
                "created_at",
                "priority",
                "category",
                "priority_sla_hours",
                "effective_sla_hours",
                "due_bucket",
            ])
            .headers([
                "Ticket ID",
                "Customer ID",
                "Plan",
                "Region",
                "Created At",
                "Priority",
                "Category",
                "Priority SLA (h)",
                "Effective SLA (h)",
                "Due Bucket",
            ])
            .build()
            .map_err(resources_failed)?;

        let join_handler = SupportSlaJoin;
        let triage_handler = TicketTriage::new();

        Ok(flow! {
            name: "csv_demo_support_sla",
            journals: disk_journals(PathBuf::from(DEMO_JOURNAL_ROOT)),

            stages: {
                customers = source!(Customer => customers);
                tickets = source!(Ticket => tickets);

                triage = transform!(Ticket -> TriagedTicket => triage_handler);

                enrich = join!(catalog customers: Customer, TriagedTicket -> EnrichedTicket => join_handler);

                csv_out = sink!(EnrichedTicket => output_sink);
            },

            topology: {
                tickets |> triage;
                (customers, triage) |> enrich;
                enrich |> csv_out;
            }
        })
    })
}

pub fn run_example(
    presentation: Presentation,
    cli_args: impl IntoIterator<Item = OsString>,
) -> Result<()> {
    FlowApplication::builder()
        .with_config_file(CONFIG_FILE)
        .with_cli_args(cli_args)
        .with_presentation(presentation)
        .with_log_level(LogLevel::Info)
        .run_blocking(build_flow())?;

    Ok(())
}
