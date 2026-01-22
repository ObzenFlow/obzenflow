use obzenflow_core::{
    Contract, ContractContext, ContractReadContext, ContractResult, ContractWriteContext,
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

    /// Verify all contracts in the chain and collect their results.
    pub fn verify_all(
        &self,
        upstream_stage: obzenflow_core::StageId,
        downstream_stage: obzenflow_core::StageId,
    ) -> Vec<(String, ContractResult)> {
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
                (contract.name().to_string(), contract.verify(&ctx))
            })
            .collect()
    }
}
