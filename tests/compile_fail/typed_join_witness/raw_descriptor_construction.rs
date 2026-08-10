// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_dsl::dsl::stage_descriptor::JoinDescriptor;

fn main() {
    let _ = std::mem::size_of::<JoinDescriptor<()>>();
}
