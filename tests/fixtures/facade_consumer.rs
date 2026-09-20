// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! This module disables the extern prelude, so workspace dependencies cannot
//! accidentally satisfy a macro's caller-site implementation-crate imports.
//! Only the renamed facade and honest third-party application dependencies are
//! explicitly available. Packaging/registry qualification remains a release gate.
#![no_implicit_prelude]

extern crate async_trait;
extern crate core;
extern crate obzenflow as of;
extern crate serde;
extern crate std;

use self::async_trait::async_trait;
use self::of::effects::{Effects, StageCompletion};
use self::of::prelude::*;
use self::of::schema::{StageFactSet, TypedFactSet};
use self::of::stages::{joins, sinks, sources, stateful, transforms};
use self::serde::{Deserialize, Serialize};
use self::std::prelude::rust_2021::*;
use self::std::vec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Fact(u64);

impl TypedPayload for Fact {
    const EVENT_TYPE: &'static str = "facade.fact";
}

#[derive(Debug, Clone, StageOutputFacts)]
#[stage_output(schema = of::schema)]
enum Output {
    Emitted(Fact),
    #[stage_output(empty)]
    Filtered,
}

#[derive(Debug, Clone, EffectOutcomeFacts)]
#[effect_outcome(schema = of::schema)]
struct Outcome {
    fact: Fact,
}

#[derive(Debug, Clone, EffectOutcomeFacts)]
#[effect_outcome(schema = of::schema)]
enum EffectSum {
    Success(Fact),
}

type Facts = of::schema::stage_fact_set![Fact];
type NoEffects = of::effects::effect_set![];

#[derive(Clone, Debug)]
struct Pure;

impl sources::TypedFiniteSourceHandler for Pure {
    type Output = Fact;
    fn next(&mut self) -> Result<Option<Vec<Fact>>, sources::SourceError> {
        Ok(None)
    }
}

#[async_trait]
impl sources::TypedAsyncFiniteSourceHandler for Pure {
    type Output = Fact;
    async fn next(&mut self) -> Result<Option<Vec<Fact>>, sources::SourceError> {
        Ok(None)
    }
}

impl sources::TypedInfiniteSourceHandler for Pure {
    type Output = Fact;
    fn next(&mut self) -> Result<Vec<Fact>, sources::SourceError> {
        Ok(vec![Fact(1)])
    }
}

#[async_trait]
impl sources::TypedAsyncInfiniteSourceHandler for Pure {
    type Output = Fact;
    async fn next(&mut self) -> Result<Vec<Fact>, sources::SourceError> {
        Ok(vec![Fact(1)])
    }
}

impl transforms::TypedTransformHandler for Pure {
    type Input = Fact;
    type Output = Output;
    fn process(&self, input: Fact) -> Result<Output, HandlerError> {
        Ok(Output::Emitted(input))
    }
}

impl stateful::TypedStatefulHandler for Pure {
    type State = u64;
    type Input = Fact;
    type Output = Fact;
    fn initial_state(&self) -> u64 {
        0
    }
    fn accumulate(&self, state: &mut u64, input: Fact) {
        *state += input.0;
    }
    fn emit(&self, state: &u64) -> Result<stateful::StatefulEmission<u64, Fact>, HandlerError> {
        Ok(stateful::StatefulEmission::RetainEpoch {
            next_state: *state,
            outputs: vec![Fact(*state)],
        })
    }
}

impl joins::TypedJoinHandler for Pure {
    type State = ();
    type ReferenceKey = u64;
    type Reference = Fact;
    type Stream = Fact;
    type Output = Fact;
    fn initial_state(&self) {}
    fn reference_mode(&self) -> joins::JoinReferenceMode {
        joins::JoinReferenceMode::FiniteEof
    }
    fn admit_reference(&self, reference: &Fact) -> Result<u64, HandlerError> {
        Ok(reference.0)
    }
    fn process_stream(
        &self,
        _state: &mut (),
        _references: &mut joins::JoinReferenceView<'_, u64, Fact>,
        stream: Fact,
    ) -> Result<Vec<Fact>, HandlerError> {
        Ok(vec![stream])
    }
}

#[async_trait]
impl sinks::InlineSink for Pure {
    type Input = Fact;
    async fn write(
        &mut self,
        _input: Fact,
        _context: sinks::SinkWriteContext,
    ) -> sinks::SinkWriteResult {
        Ok(sinks::SinkWriteReport::terminal(
            sinks::SinkTerminalOutcome::success_via(sinks::DeliveryMethod::Noop, None),
        ))
    }
}

#[async_trait]
impl transforms::EffectfulTransformHandler for Pure {
    type Input = Fact;
    type Output = Fact;
    type AllowedEffects = NoEffects;

    async fn process(
        &self,
        input: Fact,
        fx: &mut Effects<Fact, NoEffects>,
    ) -> Result<StageCompletion<Fact>, HandlerError> {
        fx.emit(input).await?;
        Ok(fx.complete()?)
    }
}

#[async_trait]
impl stateful::EffectfulStatefulHandler for Pure {
    type State = u64;
    type Input = Fact;
    type Output = Fact;
    type AllowedEffects = NoEffects;

    fn initial_state(&self) -> u64 {
        0
    }

    async fn decide(
        &mut self,
        state: &u64,
        input: &Fact,
        fx: &mut Effects<Fact, NoEffects>,
    ) -> Result<StageCompletion<Fact>, HandlerError> {
        fx.emit(Fact(state + input.0)).await?;
        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut u64, fact: Fact) -> Result<(), HandlerError> {
        *state = fact.0;
        Ok(())
    }
}

#[test]
fn renamed_facade_derives_preserve_flat_facts() {
    let sum_facts = EffectSum::Success(Fact(3)).into_facts().unwrap();
    std::assert!(std::matches!(
        EffectSum::try_from_facts(&sum_facts).unwrap(),
        EffectSum::Success(Fact(3))
    ));
    let facts = Output::Emitted(Fact(7)).into_facts().unwrap();
    std::assert_eq!(facts.len(), 1);
    std::assert!(std::matches!(
        Output::try_from_facts(&facts).unwrap(),
        Output::Emitted(Fact(7))
    ));
    std::assert!(Output::Filtered.into_facts().unwrap().is_empty());
    let outcome = Outcome { fact: Fact(9) };
    std::assert_eq!(
        Outcome::try_from_facts(&outcome.into_facts().unwrap())
            .unwrap()
            .fact,
        Fact(9)
    );
    std::assert_eq!(Facts::member_fact_types().len(), 1);
    fn no_effects<S: of::effects::EffectSet>() {}
    no_effects::<NoEffects>();
}

#[test]
fn renamed_facade_macros_and_constructors_compile() {
    let _flow = FlowDefinition::materialize(|_config| {
        let input = sources::finite([Fact(1)]);
        let mapped = transforms::map(|fact: Fact| fact);
        let folded =
            stateful::reduce(Fact(0), |sum: &mut Fact, fact: &Fact| sum.0 += fact.0).emit_on_eof();
        let output = sinks::SinkTyped::new(|_: Fact| async {});
        Ok(of::flow::flow! {
            name: "facade",
            journals: of::journal::memory_journals(),
            stages: {
                input = of::flow::source!(Fact => input);
                mapped = transform!(Fact -> Fact => mapped);
                folded = stateful!(Fact -> Fact => folded);
                output = sink!(Fact => output);
            },
            topology: {
                input |> mapped;
                mapped |> folded;
                folded |> output;
            }
        })
    });

    // Placeholder-first authoring works through both prelude and qualified paths.
    let _ = source!(Fact => placeholder!());
    let _ = of::flow::async_source!(Fact => placeholder!());
    let _ = infinite_source!(Fact => placeholder!());
    let _ = of::flow::async_infinite_source!(Fact => placeholder!());
    let _ = effectful_transform!(Fact -> Fact => Pure, observers: []);
    let _ = of::flow::effectful_stateful!(Fact -> Fact => Pure, observers: []);
    let _ = of::flow::transform!(Fact -> Fact => placeholder!());
    let _ = of::flow::stateful!(Fact -> Fact => placeholder!());
    let _ = of::flow::sink!(Fact => placeholder!());
    let joined = joins::inner(
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |_reference: Fact, fact: Fact| fact,
    );
    let _ = of::flow::join!(catalog input: Fact, Fact -> Fact => joined);
    let _ = source!(Fact => Pure);
    let _ = async_source!(Fact => Pure);
    let _ = infinite_source!(Fact => Pure);
    let _ = async_infinite_source!(Fact => Pure);
    let _ = transform!(Fact -> Fact => Pure);
    let _ = stateful!(Fact -> Fact => Pure);
    let _ = join!(catalog input: Fact, Fact -> Fact => Pure);
    let _ = sink!(Fact => Pure);

    let _selected = FlowDefinition::materialize(|_config| {
        let input = sources::once(Fact(1));
        let first = sinks::SinkTyped::new(|_: Fact| async {});
        let second = sinks::debug::<Fact>();
        Ok(flow! {
            journals: of::journal::memory_journals(),
            stages: {
                input = source!(Fact => input);
                output = of::flow::sink!(Fact => handler_set!(first, second))?;
            },
            topology: { input |> output; }
        })
    });

    // Checking the function value compiles every AI macro path without opening
    // a provider or requiring credentials.
    let _ = ai_stages;
}

use self::of::ai::{
    AiFinaliseRole, AiMapRole, AiRoleLogicFailure, ChatCompletion, ChatCompletionReply,
    ChatMessage, ChatParams, ChatRequestSpec, ChunkInfo, InferenceHandler, Many, TokenCount,
};

fn request() -> ChatRequestSpec {
    ChatRequestSpec {
        messages: vec![ChatMessage::user("Return a number.")],
        params: ChatParams::default(),
        tools: Vec::new(),
        response_format: None,
    }
}

impl InferenceHandler for Pure {
    type Input = Fact;
    type Output = Fact;
    fn prepare(&self, _input: &Fact) -> Result<ChatRequestSpec, HandlerError> {
        Ok(request())
    }
    fn interpret(
        &self,
        input: Fact,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Fact, HandlerError> {
        Ok(input)
    }
}

impl AiMapRole<u64, Fact> for Pure {
    fn prepare(
        &self,
        _items: &[u64],
        _chunk: &ChunkInfo,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(request())
    }
    fn interpret(
        &self,
        items: Vec<u64>,
        _chunk: ChunkInfo,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Fact, AiRoleLogicFailure> {
        Ok(Fact(items.into_iter().sum()))
    }
}

impl AiFinaliseRole<Fact, Many<Fact>, Fact> for Pure {
    fn prepare(
        &self,
        _seed: &Fact,
        _collected: &Many<Fact>,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(request())
    }
    fn interpret(
        &self,
        seed: Fact,
        _collected: Many<Fact>,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Fact, AiRoleLogicFailure> {
        Ok(seed)
    }
}

fn ai_stages(chat: of::effects::EffectBinding<ChatCompletion>) {
    let handler = Pure;
    let _ = of::flow::inference!(
        Fact -> Fact uses at_least_once(ChatCompletion)
            via chat with of::middleware::ai_resilience() => handler
    );
    let map_role = Pure;
    let finalise_role = Pure;
    let _ = of::flow::ai_map_reduce!(
        Fact -> Fact => {
            map: [u64] -> Fact uses at_least_once(ChatCompletion)
                via chat with of::middleware::ai_resilience() => map_role,
            reduce: (Fact, [Fact]) -> Fact uses at_least_once(ChatCompletion)
                via chat with of::middleware::ai_resilience() => finalise_role,
        },
        chunking: by_budget {
            items: |seed: &Fact| vec![seed.0],
            render: |item: &u64, _ctx| item.to_string(),
            budget: TokenCount::new(100),
            max_items: Some(1),
            oversize: error,
        }
    );
}
