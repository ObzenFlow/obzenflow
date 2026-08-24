// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{
    due_bucket, plan_sla_cap_hours, triage_ticket, Customer, EnrichedTicket, Ticket, TriagedTicket,
};
use super::fixtures;
use anyhow::{Context, Result};
use obzenflow::sinks::{CsvProjection, CsvSink};
use obzenflow::sources::{CsvDecoder, CsvSource};
use obzenflow_dsl::{flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::{FlowApplication, LogLevel, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TypedTransformHandler;
use obzenflow_runtime::stages::{JoinReferenceView, TypedJoinHandler};
use std::path::PathBuf;

pub struct DemoPaths {
    pub customers_csv: PathBuf,
    pub tickets_csv: PathBuf,
    pub output_csv: PathBuf,
    pub journals_dir: PathBuf,
}

impl DemoPaths {
    pub fn resolve() -> Result<Self> {
        let out_root = PathBuf::from("target/csv-demo-support-sla");
        let journals_dir = out_root.join("logs");
        let outputs_dir = out_root.join("outputs");
        std::fs::create_dir_all(&outputs_dir)
            .with_context(|| format!("create outputs dir {}", outputs_dir.display()))?;

        let fixture_paths = fixtures::paths()?;

        Ok(Self {
            customers_csv: fixture_paths.customers_csv,
            tickets_csv: fixture_paths.tickets_csv,
            output_csv: outputs_dir.join("enriched_tickets.csv"),
            journals_dir,
        })
    }
}

/// A source decoder owns the domain type it emits, just as a transform handler
/// owns its input and output types.
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

/// An authored join witness: these associated types are what `join!` proves
/// against its catalog, stream, and output declaration.
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

/// A CSV projection owns its accepted domain type in the same way as the
/// transform and join handlers above.
#[derive(Clone, Debug)]
struct EnrichedTicketCsv;

impl CsvProjection for EnrichedTicketCsv {
    type Input = EnrichedTicket;
    type Row = EnrichedTicket;

    fn project(&self, ticket: Self::Input) -> Result<Self::Row, HandlerError> {
        Ok(ticket)
    }
}

fn build_flow(
    customers: CsvSource<CustomerCsv>,
    tickets: CsvSource<TicketCsv>,
    output_sink: CsvSink<EnrichedTicketCsv>,
    journals_dir: PathBuf,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let join_handler = SupportSlaJoin;
        let triage_handler = TicketTriage::new();

        Ok(flow! {
            name: "csv_demo_support_sla",
            journals: disk_journals(journals_dir),

            stages: {
                customers = source!(Customer => customers);
                tickets = source!(Ticket => tickets);

                triage = transform!(Ticket -> TriagedTicket => triage_handler);

                enrich = join!(catalog customers: Customer, TriagedTicket -> EnrichedTicket => join_handler);

                csv_out = sink!(EnrichedTicket => output_sink);
            },

            topology: {
                tickets |> triage;
                triage |> enrich;
                enrich |> csv_out;
            }
        })
    })
}

pub fn run_example(paths: DemoPaths, presentation: Presentation) -> Result<()> {
    let customers = CsvSource::builder(CustomerCsv)
        .path(&paths.customers_csv)
        .build()?;
    let tickets = CsvSource::builder(TicketCsv)
        .path(&paths.tickets_csv)
        .chunk_size(25)
        .build()?;

    let output_sink = CsvSink::builder(EnrichedTicketCsv)
        .path(&paths.output_csv)
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
        .auto_flush(true)
        .build()?;

    FlowApplication::builder()
        .with_presentation(presentation)
        .with_log_level(LogLevel::Info)
        .run_blocking(build_flow(
            customers,
            tickets,
            output_sink,
            paths.journals_dir,
        ))?;

    Ok(())
}
