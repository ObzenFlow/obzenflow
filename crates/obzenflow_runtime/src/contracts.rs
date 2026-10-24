// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::system_payload::ContractName;
use obzenflow_core::{
    Contract, ContractContext, ContractReadContext, ContractResult, ContractState,
    ContractWriteContext, DivergenceContract,
};

/// A chain of contracts for a single logical edge between stages.
///
/// Each contract in the chain has its own write/read context so that
/// implementations can maintain independent state. An edge inside a cycle
/// also holds a divergence monitor, which reports only during the run.
pub struct ContractChain {
    contracts: Vec<Box<dyn Contract>>,
    write_contexts: Vec<ContractWriteContext>,
    read_contexts: Vec<ContractReadContext>,
    divergence: Option<DivergenceContract>,
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
            divergence: None,
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

    /// Hold a divergence monitor for an edge inside a cycle. It observes every
    /// delivered row and is asked only during the run, never at the edge's end.
    pub fn with_divergence(mut self, monitor: DivergenceContract) -> Self {
        self.divergence = Some(monitor);
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

    /// Feed one physical edge delivery to the chain.
    ///
    /// Contracts observe only rows the edge's upstream authored. The divergence
    /// monitor observes every delivered row, forwarded control signals included.
    pub fn on_edge_delivery(
        &mut self,
        event: &obzenflow_core::ChainEvent,
        reader_stage: obzenflow_core::StageId,
        reader_seq: obzenflow_core::event::types::SeqNo,
        upstream_stage: obzenflow_core::StageId,
        authored_by_upstream: bool,
    ) {
        if let Some(divergence) = &self.divergence {
            divergence.observe(event);
        }
        if !authored_by_upstream {
            return;
        }

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
            contract.on_read(event, read_ctx);
            contract.on_write(event, write_ctx);
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

    /// Check all contracts' `check_progress` hooks and the divergence monitor,
    /// and collect any failures.
    ///
    /// A check that reports no failure returns `ContractResult::Pending`.
    pub fn check_progress_all(
        &self,
        upstream_stage: obzenflow_core::StageId,
        downstream_stage: obzenflow_core::StageId,
    ) -> Vec<(ContractName, ContractResult)> {
        let mut results: Vec<_> = self
            .contracts
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
            .collect();
        if let Some(divergence) = &self.divergence {
            let name = ContractName::from(DivergenceContract::NAME);
            let result = match divergence.check(upstream_stage, downstream_stage) {
                Some(v) => ContractResult::Failed(v),
                None => ContractResult::pending(
                    name.clone(),
                    &ContractContext {
                        upstream_stage,
                        downstream_stage,
                        write_state: &ContractState::default(),
                        read_state: &ContractState::default(),
                    },
                    obzenflow_core::contracts::PendingReason::ProgressOnly,
                ),
            };
            results.push((name, result));
        }
        results
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

    fn eager_divergence() -> DivergenceContract {
        DivergenceContract::with_thresholds(
            obzenflow_core::SccId::from(obzenflow_core::Ulid::new()),
            obzenflow_core::DivergenceThresholds {
                max_signals_when_no_data: 0,
                ..Default::default()
            },
        )
    }

    fn is_signals_when_no_data(result: &ContractResult, signals: u64) -> bool {
        use obzenflow_core::contracts::{ContractEvidenceDetails, DivergencePredicate};
        matches!(result, ContractResult::Failed(v) if matches!(
            &v.details,
            ContractEvidenceDetails::Divergence { evaluated_predicates, flow_control_signals, .. }
                if *evaluated_predicates == vec![DivergencePredicate::SignalsWhenNoData]
                    && *flow_control_signals == signals
        ))
    }

    #[test]
    fn selected_transport_counts_the_selected_rows() {
        use obzenflow_core::contracts::ContractEvidenceDetails;
        use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
        use obzenflow_core::event::types::{Count, SeqNo};
        let upstream = StageId::new();
        let reader = StageId::new();
        let mut chain =
            ContractChain::new().with_contract(obzenflow_core::TransportContract::new());
        for seq in 1..=2 {
            let data = ChainEventFactory::data_event(
                upstream.into(),
                "selected",
                std::num::NonZeroU32::MIN,
                json!({}),
            );
            chain.on_edge_delivery(&data, reader, SeqNo(seq), upstream, true);
        }
        let mut selected = ChainEventFactory::eof_event(upstream.into(), true);
        if let obzenflow_core::event::ChainPayload::FlowControl(FlowControlPayload::Eof {
            writer_seq,
            ..
        }) = &mut selected.payload
        {
            *writer_seq = Some(SeqNo(2));
        }
        chain.on_edge_delivery(&selected, reader, SeqNo(2), upstream, true);
        let results = chain.verify_all(upstream, reader);
        assert_eq!(results.len(), 1);
        assert!(
            matches!(&results[0].1, ContractResult::Passed(e) if matches!(e.details, ContractEvidenceDetails::Transport { advertised_writer_seq: Some(SeqNo(2)), consumed_count: Count(2) }))
        );
    }

    #[test]
    fn verify_all_gives_no_divergence_result_while_check_progress_all_does() {
        let upstream = StageId::new();
        let reader = StageId::new();
        let mut chain = ContractChain::new()
            .with_contract(obzenflow_core::TransportContract::new())
            .with_divergence(eager_divergence());
        chain.on_edge_delivery(
            &ChainEventFactory::drain_event(upstream.into()),
            reader,
            obzenflow_core::event::types::SeqNo(0),
            upstream,
            true,
        );

        let names = |results: &[(ContractName, ContractResult)]| -> Vec<String> {
            results.iter().map(|(name, _)| name.to_string()).collect()
        };
        let finals = chain.verify_all(upstream, reader);
        assert_eq!(names(&finals), [obzenflow_core::TransportContract::NAME]);

        let progress = chain.check_progress_all(upstream, reader);
        assert_eq!(
            names(&progress),
            [
                obzenflow_core::TransportContract::NAME,
                DivergenceContract::NAME
            ]
        );
        assert!(is_signals_when_no_data(&progress[1].1, 1));
    }

    struct CountingContract {
        reads: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
    }

    impl Contract for CountingContract {
        fn name(&self) -> &str {
            "CountingContract"
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
                obzenflow_core::contracts::PendingReason::ProgressOnly,
            )
        }
    }

    #[test]
    fn contracts_observe_authored_rows_and_the_monitor_observes_every_row() {
        let reads = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        let mut chain = ContractChain::new()
            .with_contract(CountingContract {
                reads: reads.clone(),
                writes: writes.clone(),
            })
            .with_divergence(eager_divergence());
        let upstream = StageId::new();
        let reader = StageId::new();
        let forwarded = ChainEventFactory::drain_event(WriterId::from(StageId::new()));

        chain.on_edge_delivery(
            &forwarded,
            reader,
            obzenflow_core::event::types::SeqNo(0),
            upstream,
            false,
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(writes.load(Ordering::SeqCst), 0);
        assert!(is_signals_when_no_data(
            &chain.check_progress_all(upstream, reader)[1].1,
            1
        ));

        chain.on_edge_delivery(
            &forwarded,
            reader,
            obzenflow_core::event::types::SeqNo(1),
            upstream,
            true,
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert!(is_signals_when_no_data(
            &chain.check_progress_all(upstream, reader)[1].1,
            2
        ));
    }
}
