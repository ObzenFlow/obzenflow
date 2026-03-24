// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{
    due_bucket, plan_sla_cap_hours, priority_sla_hours, Customer, EnrichedTicket, Ticket,
    TriagedTicket,
};
use super::fixtures;
use anyhow::{Context, Result};
use async_trait::async_trait;
use obzenflow::sinks::CsvSink;
use obzenflow::sources::CsvSource;
use obzenflow::typed::joins;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, join, sink, source, transform};
use obzenflow_infra::application::{FlowApplication, LogLevel, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TransformHandler;
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

#[derive(Clone, Debug)]
struct TicketTriage;

impl TicketTriage {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransformHandler for TicketTriage {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        if !Ticket::event_type_matches(&event.event_type()) {
            return Ok(vec![event]);
        }

        let Some(ticket) = Ticket::from_event(&event) else {
            return Ok(vec![event.mark_as_validation_error(
                "ticket_triage_failed: could not deserialize Ticket payload",
            )]);
        };

        let priority_sla_hours = priority_sla_hours(&ticket.priority);
        let triaged = TriagedTicket {
            ticket_id: ticket.ticket_id,
            customer_id: ticket.customer_id,
            created_at: ticket.created_at,
            priority: ticket.priority,
            category: ticket.category,
            priority_sla_hours,
        };

        let payload =
            serde_json::to_value(&triaged).map_err(|e| HandlerError::Other(e.to_string()))?;

        Ok(vec![ChainEventFactory::derived_data_event(
            event.writer_id,
            &event,
            TriagedTicket::versioned_event_type(),
            payload,
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn build_flow(
    customers: CsvSource<Customer>,
    tickets: CsvSource<Ticket>,
    output_sink: CsvSink,
    journals_dir: PathBuf,
) -> obzenflow_dsl::FlowDefinition {
    let join_handler = joins::inner(
        |c: &Customer| c.customer_id.clone(),
        |t: &TriagedTicket| t.customer_id.clone(),
        |customer: Customer, ticket: TriagedTicket| {
            let cap_hours = plan_sla_cap_hours(&customer.plan);
            let effective_sla_hours = ticket.priority_sla_hours.min(cap_hours);
            let due_bucket = due_bucket(effective_sla_hours).to_string();

            EnrichedTicket {
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
            }
        },
    );

    flow! {
        name: "csv_demo_support_sla",
        journals: disk_journals(journals_dir),
        middleware: [],

        stages: {
            customers = source!(Customer => customers);
            tickets = source!(Ticket => tickets);

            triage = transform!(TicketTriage::new());

            enrich = join!(catalog customers: Customer, TriagedTicket -> EnrichedTicket => join_handler);

            csv_out = sink!(output_sink);
        },

        topology: {
            tickets |> triage;
            triage |> enrich;
            enrich |> csv_out;
        }
    }
}

pub fn run_example(paths: DemoPaths, presentation: Presentation) -> Result<()> {
    let customers = CsvSource::typed_from_file::<Customer>(&paths.customers_csv)?;
    let tickets = CsvSource::typed_builder::<Ticket>()
        .path(&paths.tickets_csv)
        .chunk_size(25)
        .build()?;

    let output_sink = CsvSink::builder()
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
