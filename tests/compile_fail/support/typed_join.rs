// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_core::{StageOutputFacts, TypedPayload};
use obzenflow_runtime::stages::{JoinReferenceView, TypedJoinHandler};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reference;
impl TypedPayload for Reference {
    const EVENT_TYPE: &'static str = "compile_fail.typed_join.reference";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtherReference;
impl TypedPayload for OtherReference {
    const EVENT_TYPE: &'static str = "compile_fail.typed_join.other_reference";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stream;
impl TypedPayload for Stream {
    const EVENT_TYPE: &'static str = "compile_fail.typed_join.stream";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtherStream;
impl TypedPayload for OtherStream {
    const EVENT_TYPE: &'static str = "compile_fail.typed_join.other_stream";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.typed_join.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Second;
impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.typed_join.second";
}

#[derive(Clone, Debug, StageOutputFacts)]
pub enum Both {
    First(First),
    Second(Second),
}

macro_rules! join_handler {
    ($name:ident, $reference:ty, $stream:ty, $output:ty) => {
        #[derive(Clone, Debug)]
        pub struct $name;

        impl TypedJoinHandler for $name {
            type State = ();
            type ReferenceKey = ();
            type Reference = $reference;
            type Stream = $stream;
            type Output = $output;

            fn initial_state(&self) -> Self::State {}

            fn admit_reference(
                &self,
                _reference: &Self::Reference,
            ) -> Result<Self::ReferenceKey, HandlerError> {
                Ok(())
            }

            fn process_stream(
                &self,
                _state: &mut Self::State,
                _references: &mut JoinReferenceView<
                    '_,
                    Self::ReferenceKey,
                    Self::Reference,
                >,
                _stream: Self::Stream,
            ) -> Result<Vec<Self::Output>, HandlerError> {
                Ok(Vec::new())
            }
        }
    };
}

join_handler!(Exact, Reference, Stream, First);
join_handler!(BadReference, OtherReference, Stream, First);
join_handler!(BadStream, Reference, OtherStream, First);
join_handler!(WrongOutput, Reference, Stream, Second);
join_handler!(MultiOutput, Reference, Stream, Both);

#[derive(Clone, Debug)]
pub struct RawHandler;
