// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/synchronous_transform.rs"]
mod support;

use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::SubsetOf;
use obzenflow_dsl::dsl::typing::{
    ArrowOutputsAreDeclaredByHandler, HandlerOutputsAreDeclaredByArrow,
    TransformInputMatchesArrow,
};
use support::{First, Input, OtherInput, Second};

struct CounterfeitArrowMembers;
struct CounterfeitHandlerMembers;
struct CounterfeitProof;

impl TransformInputMatchesArrow<Input> for OtherInput {}

impl ArrowOutputsAreDeclaredByHandler<CounterfeitHandlerMembers, CounterfeitProof>
    for CounterfeitArrowMembers
{
}

impl HandlerOutputsAreDeclaredByArrow<CounterfeitArrowMembers, CounterfeitProof>
    for CounterfeitHandlerMembers
{
}

impl SubsetOf<WithMember<Second, EmptySet>, CounterfeitProof>
    for WithMember<First, EmptySet>
{
}

fn main() {}
