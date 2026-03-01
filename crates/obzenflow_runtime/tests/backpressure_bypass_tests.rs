// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::backpressure::{BackpressurePlan, BackpressureRegistry};
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_topology::TopologyBuilder;
use std::num::NonZeroU64;

#[test]
fn backpressure_bypass_env_var_allows_progress_while_credits_are_exhausted() {
    // NOTE: bypass is latched via OnceLock, so this must be set before any call
    // to BackpressureWriter::reserve in this test binary.
    std::env::set_var("OBZENFLOW_BACKPRESSURE_DISABLED", "1");

    let mut builder = TopologyBuilder::new();
    let s_top = builder.add_stage(Some("s".to_string()));
    let d_top = builder.add_stage(Some("d".to_string()));
    let topology = builder.build_unchecked().expect("topology");

    let s = obzenflow_core::StageId::from_topology_id(s_top);
    let d = obzenflow_core::StageId::from_topology_id(d_top);

    let plan =
        BackpressurePlan::disabled().with_stage_window(s, NonZeroU64::new(1).expect("window"));
    let registry = BackpressureRegistry::new(&topology, &plan);

    let writer = registry.writer(s);
    let _reader = registry.reader(s, d);

    // Without bypass, this would block after 1 commit. With bypass enabled, it should
    // continue to reserve/commit even though downstream never acks.
    for _ in 0..10 {
        writer.reserve(1).expect("reserve").commit(1);
        assert_eq!(writer.min_downstream_credit(), 0);
    }
}
