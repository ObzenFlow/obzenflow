// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::system_payload::ContractName;
use obzenflow_core::{
    Contract, ContractContext, ContractEventScope, ContractReadContext, ContractResult,
    ContractWriteContext,
};

/// A chain of contracts for a single logical edge between stages.
///
/// Each contract in the chain has its own write/read context so that
/// implementations can maintain independent state.
pub struct ContractChain {
    contracts: Vec<Box<dyn Contract>>,
    write_contexts: Vec<ContractWriteContext>,
    read_contexts: Vec<ContractReadContext>,
}

impl Default for ContractChain {
    fn default() -> Self {
        Self::new()
    }
}

impl ContractChain {
    /// Create an empty contract chain.
    pub fn new() -> Self {
        Self {
            contracts: Vec::new(),
            write_contexts: Vec::new(),
            read_contexts: Vec::new(),
        }
    }

    /// Add a contract to the chain.
    ///
    /// Contexts are initialized with dummy stage IDs; the caller is expected
    /// to set `writer_stage`, `reader_stage`, and `upstream_stage` on each
    /// call to `on_write` / `on_read`.
    pub fn with_contract<C>(mut self, contract: C) -> Self
    where
        C: Contract + 'static,
    {
        // Use placeholder stage IDs; they will be overwritten on use.
        let placeholder_stage = obzenflow_core::StageId::new();
        self.write_contexts
            .push(ContractWriteContext::new(placeholder_stage));
        self.read_contexts.push(ContractReadContext::new(
            placeholder_stage,
            placeholder_stage,
        ));
        self.contracts.push(Box::new(contract));
        self
    }

    /// Whether the chain has any contracts configured.
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Invoke all contracts' `on_write` hooks for a written event.
    pub fn on_write(
        &mut self,
        event: &obzenflow_core::ChainEvent,
        writer_stage: obzenflow_core::StageId,
        writer_seq: obzenflow_core::event::types::SeqNo,
    ) {
        for ctx in &mut self.write_contexts {
            ctx.writer_stage = writer_stage;
            ctx.writer_seq = writer_seq;
        }

        for (contract, ctx) in self.contracts.iter().zip(self.write_contexts.iter_mut()) {
            contract.on_write(event, ctx);
        }
    }

    /// Invoke all contracts' `on_read` hooks for a read event.
    pub fn on_read(
        &mut self,
        event: &obzenflow_core::ChainEvent,
        reader_stage: obzenflow_core::StageId,
        reader_seq: obzenflow_core::event::types::SeqNo,
        upstream_stage: obzenflow_core::StageId,
    ) {
        for ctx in &mut self.read_contexts {
            ctx.reader_stage = reader_stage;
            ctx.reader_seq = reader_seq;
            ctx.upstream_stage = upstream_stage;
        }

        for (contract, ctx) in self.contracts.iter().zip(self.read_contexts.iter_mut()) {
            contract.on_read(event, ctx);
        }
    }

    /// Feed one physical edge delivery to each contract according to its
    /// declared evidence population.
    ///
    /// Authored-prefix contracts ignore forwarded rows. Physical-edge
    /// diagnostics still observe those rows because their subject is the
    /// delivery itself rather than the journal owner's committed frontier.
    pub fn on_edge_delivery(
        &mut self,
        event: &obzenflow_core::ChainEvent,
        producer_event: &obzenflow_core::ChainEvent,
        reader_stage: obzenflow_core::StageId,
        reader_seq: obzenflow_core::event::types::SeqNo,
        upstream_stage: obzenflow_core::StageId,
        authored_by_upstream: bool,
    ) {
        for ctx in &mut self.write_contexts {
            ctx.writer_stage = upstream_stage;
            ctx.writer_seq = obzenflow_core::event::types::SeqNo(0);
        }
        for ctx in &mut self.read_contexts {
            ctx.reader_stage = reader_stage;
            ctx.reader_seq = reader_seq;
            ctx.upstream_stage = upstream_stage;
        }

        for ((contract, write_ctx), read_ctx) in self
            .contracts
            .iter()
            .zip(self.write_contexts.iter_mut())
            .zip(self.read_contexts.iter_mut())
        {
            if authored_by_upstream || contract.event_scope() == ContractEventScope::PhysicalEdge {
                let event = if contract.event_scope() == ContractEventScope::SourceProduction {
                    producer_event
                } else {
                    event
                };
                contract.on_read(event, read_ctx);
                contract.on_write(event, write_ctx);
            }
        }
    }

    /// Verify all contracts in the chain and collect their results.
    pub fn verify_all(
        &self,
        upstream_stage: obzenflow_core::StageId,
        downstream_stage: obzenflow_core::StageId,
    ) -> Vec<(ContractName, ContractResult)> {
        self.contracts
            .iter()
            .zip(self.write_contexts.iter())
            .zip(self.read_contexts.iter())
            .map(|((contract, write_ctx), read_ctx)| {
                let ctx = ContractContext {
                    upstream_stage,
                    downstream_stage,
                    write_state: &write_ctx.state,
                    read_state: &read_ctx.state,
                };
                (contract.contract_name(), contract.verify(&ctx))
            })
            .collect()
    }

    /// Check all contracts' `check_progress` hooks and collect any failures.
    ///
    /// Contracts that do not report incremental status return `ContractResult::Pending`.
    pub fn check_progress_all(
        &self,
        upstream_stage: obzenflow_core::StageId,
        downstream_stage: obzenflow_core::StageId,
    ) -> Vec<(ContractName, ContractResult)> {
        self.contracts
            .iter()
            .zip(self.write_contexts.iter())
            .zip(self.read_contexts.iter())
            .map(|((contract, write_ctx), read_ctx)| {
                let ctx = ContractContext {
                    upstream_stage,
                    downstream_stage,
                    write_state: &write_ctx.state,
                    read_state: &read_ctx.state,
                };

                let result = match contract.check_progress(&ctx) {
                    Some(v) => ContractResult::Failed(v),
                    None => ContractResult::pending(
                        contract.contract_name(),
                        &ctx,
                        obzenflow_core::contracts::PendingReason::ProgressOnly,
                    ),
                };
                (contract.contract_name(), result)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{ChainEvent, StageId, WriterId};
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn selected_transport_never_replaces_the_original_source_population() {
        use obzenflow_core::contracts::ContractEvidenceDetails;
        use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
        use obzenflow_core::event::types::{Count, JournalIndex, JournalPath, SeqNo};
        let upstream = StageId::new();
        let reader = StageId::new();
        let mut chain = ContractChain::new()
            .with_contract(obzenflow_core::TransportContract::new())
            .with_contract(obzenflow_core::SourceContract::new());
        let declaration = ChainEventFactory::source_contract_event(
            upstream.into(),
            obzenflow_core::event::SourceContractEventParams {
                expected_count: Some(Count(10)),
                source_id: upstream,
                route: None,
                journal_path: JournalPath("source".into()),
                journal_index: JournalIndex(0),
                writer_seq: None,
                vector_clock: None,
            },
        );
        chain.on_edge_delivery(&declaration, &declaration, reader, SeqNo(0), upstream, true);
        for seq in 1..=2 {
            let data = ChainEventFactory::data_event(
                upstream.into(),
                "selected",
                std::num::NonZeroU32::MIN,
                json!({}),
            );
            chain.on_edge_delivery(&data, &data, reader, SeqNo(seq), upstream, true);
        }
        let mut producer = ChainEventFactory::eof_event(upstream.into(), true);
        if let obzenflow_core::event::ChainPayload::FlowControl(FlowControlPayload::Eof {
            writer_seq,
            ..
        }) = &mut producer.payload
        {
            *writer_seq = Some(SeqNo(10));
        }
        let mut selected = producer.clone();
        if let obzenflow_core::event::ChainPayload::FlowControl(FlowControlPayload::Eof {
            writer_seq,
            ..
        }) = &mut selected.payload
        {
            *writer_seq = Some(SeqNo(2));
        }
        chain.on_edge_delivery(&selected, &producer, reader, SeqNo(2), upstream, true);
        let results = chain.verify_all(upstream, reader);
        assert!(
            matches!(&results[0].1, ContractResult::Passed(e) if matches!(e.details, ContractEvidenceDetails::Transport { advertised_writer_seq: Some(SeqNo(2)), consumed_count: Count(2) }))
        );
        assert!(
            matches!(&results[1].1, ContractResult::Passed(e) if matches!(e.details, ContractEvidenceDetails::Source { expected_count: Some(Count(10)), produced_count: Some(Count(10)), .. }))
        );
    }

    struct CountingContract {
        scope: ContractEventScope,
        reads: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
    }

    impl Contract for CountingContract {
        fn name(&self) -> &str {
            "CountingContract"
        }

        fn event_scope(&self) -> ContractEventScope {
            self.scope
        }

        fn on_write(&self, _event: &ChainEvent, _ctx: &mut ContractWriteContext) {
            self.writes.fetch_add(1, Ordering::SeqCst);
        }

        fn on_read(&self, _event: &ChainEvent, _ctx: &mut ContractReadContext) {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }

        fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult {
            ContractResult::pending(
                self.contract_name(),
                ctx,
                obzenflow_core::contracts::PendingReason::EvaluationUnavailable,
            )
        }
    }

    #[test]
    fn edge_delivery_keeps_authored_prefix_and_physical_populations_distinct() {
        let authored_reads = Arc::new(AtomicUsize::new(0));
        let authored_writes = Arc::new(AtomicUsize::new(0));
        let physical_reads = Arc::new(AtomicUsize::new(0));
        let physical_writes = Arc::new(AtomicUsize::new(0));
        let mut chain = ContractChain::new()
            .with_contract(CountingContract {
                scope: ContractEventScope::UpstreamAuthored,
                reads: authored_reads.clone(),
                writes: authored_writes.clone(),
            })
            .with_contract(CountingContract {
                scope: ContractEventScope::PhysicalEdge,
                reads: physical_reads.clone(),
                writes: physical_writes.clone(),
            });
        let upstream = StageId::new();
        let reader = StageId::new();
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.forwarded",
            std::num::NonZeroU32::MIN,
            json!({"value": 1}),
        );

        chain.on_edge_delivery(
            &event,
            &event,
            reader,
            obzenflow_core::event::types::SeqNo(0),
            upstream,
            false,
        );
        assert_eq!(authored_reads.load(Ordering::SeqCst), 0);
        assert_eq!(authored_writes.load(Ordering::SeqCst), 0);
        assert_eq!(physical_reads.load(Ordering::SeqCst), 1);
        assert_eq!(physical_writes.load(Ordering::SeqCst), 1);

        chain.on_edge_delivery(
            &event,
            &event,
            reader,
            obzenflow_core::event::types::SeqNo(1),
            upstream,
            true,
        );
        assert_eq!(authored_reads.load(Ordering::SeqCst), 1);
        assert_eq!(authored_writes.load(Ordering::SeqCst), 1);
        assert_eq!(physical_reads.load(Ordering::SeqCst), 2);
        assert_eq!(physical_writes.load(Ordering::SeqCst), 2);
    }
}
