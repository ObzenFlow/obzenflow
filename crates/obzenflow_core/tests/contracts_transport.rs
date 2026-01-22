use obzenflow_core::{
    contracts::{ContractContext, ContractReadContext, ContractWriteContext},
    event::types::SeqNo,
    event::{
        payloads::flow_control_payload::FlowControlPayload, ChainEvent, ChainEventContent,
        ChainEventFactory,
    },
    Contract, StageId, TransportContract, WriterId,
};

fn make_data_event(writer: WriterId) -> ChainEvent {
    ChainEventFactory::data_event(writer, "test.event", serde_json::json!({ "k": 1 }))
}

fn make_eof_with_seq(writer: WriterId, seq: u64) -> ChainEvent {
    let mut eof = ChainEventFactory::eof_event(writer, true);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        writer_id,
        writer_seq,
        ..
    }) = &mut eof.content
    {
        *writer_id = Some(writer);
        *writer_seq = Some(SeqNo(seq));
    }
    eof
}

#[test]
fn transport_contract_passes_when_counts_match() {
    let contract = TransportContract::new();
    let writer_stage = StageId::new();
    let reader_stage = StageId::new();

    let writer_id = WriterId::from(writer_stage);

    let mut write_ctx = ContractWriteContext::new(writer_stage);
    let mut read_ctx = ContractReadContext::new(reader_stage, writer_stage);

    // Simulate three data events written and read.
    //
    // Writer-side count is derived from EOF writer_seq, not per-event writes.
    for _ in 0..3 {
        let event = make_data_event(writer_id);
        contract.on_read(&event, &mut read_ctx);
    }

    // Simulate EOF from writer advertising 3 events written.
    let eof = make_eof_with_seq(writer_id, 3);
    contract.on_write(&eof, &mut write_ctx);

    let ctx = ContractContext {
        upstream_stage: writer_stage,
        downstream_stage: reader_stage,
        write_state: &write_ctx.state,
        read_state: &read_ctx.state,
    };

    let result = contract.verify(&ctx);
    match result {
        obzenflow_core::contracts::ContractResult::Passed(evidence) => {
            assert_eq!(evidence.contract_name, "TransportContract");
            assert_eq!(evidence.upstream_stage, writer_stage);
            assert_eq!(evidence.downstream_stage, reader_stage);
        }
        other => panic!("expected Passed, got {other:?}"),
    }
}

#[test]
fn transport_contract_fails_when_counts_diverge() {
    let contract = TransportContract::new();
    let writer_stage = StageId::new();
    let reader_stage = StageId::new();
    let writer_id = WriterId::from(writer_stage);

    let mut write_ctx = ContractWriteContext::new(writer_stage);
    let mut read_ctx = ContractReadContext::new(reader_stage, writer_stage);

    // Writer advertises two events written via EOF.
    let eof = make_eof_with_seq(writer_id, 2);
    contract.on_write(&eof, &mut write_ctx);

    // Downstream only reads one data event.
    let event = make_data_event(writer_id);
    contract.on_read(&event, &mut read_ctx);

    let ctx = ContractContext {
        upstream_stage: writer_stage,
        downstream_stage: reader_stage,
        write_state: &write_ctx.state,
        read_state: &read_ctx.state,
    };

    let result = contract.verify(&ctx);
    match result {
        obzenflow_core::contracts::ContractResult::Failed(violation) => {
            assert_eq!(violation.contract_name, "TransportContract");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}
