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
use self::of::stages::{joins, merges, sinks, sources, stateful, transforms};
use self::serde::{Deserialize, Serialize};
use self::std::prelude::rust_2021::*;
use self::std::vec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Fact(u64);

impl TypedPayload for Fact {
    const EVENT_TYPE: &'static str = "facade.fact";
}

#[derive(Clone, Debug)]
struct FactIngress;

impl sources::IngressDecoder for FactIngress {
    type Output = Fact;
}

#[test]
fn renamed_facade_ingress_builders_compile() {
    use self::of::application::ingress::{ingress_source, IngestionConfig};

    let mut app = of::application::FlowApplication::builder();
    let http_source = app.http_ingress(FactIngress, IngestionConfig::default());
    let inbox = ingress_source(
        FactIngress,
        IngestionConfig {
            ingress_key: Some("direct".into()),
            ..IngestionConfig::default()
        },
    );
    let _sender = inbox.handle();
    let direct_source = app.ingress(inbox);
    let _flow = FlowDefinition::materialize(move |_| {
        let output = sinks::ConsoleSink::<Fact, _>::new(sinks::DebugFormatter);
        Ok(flow! {
            name: "facade_ingress",
            journals: of::journal::memory_journals(),
            stages: {
                http = async_infinite_source!(Fact => http_source);
                direct = async_infinite_source!(Fact => direct_source);
                output = sink!(Fact => output);
            },
            topology: { http |> output; direct |> output; }
        })
    });
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
type NoEffects = <Pure as of::effects::EffectHandler>::AllowedEffects;

#[derive(Clone, Debug, of::effects::EffectHandler)]
#[effect_handler(effects = of::effects)]
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
    fn emit(&self, state: &u64) -> stateful::StatefulEmission<Fact> {
        stateful::StatefulEmission::RetainEpoch {
            outputs: vec![Fact(*state)],
        }
    }
    fn post_emit(&self, _: &mut u64) {}
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

#[derive(Clone, Debug)]
struct PointInTime;

impl joins::TypedAsOfJoinHandler for PointInTime {
    type State = ();
    type ReferenceKey = u64;
    type EffectiveTime = u64;
    type Reference = Fact;
    type Stream = Fact;
    type Output = Fact;
    fn initial_state(&self) {}
    fn admit_reference(&self, reference: &Fact) -> Result<(u64, u64), HandlerError> {
        Ok((reference.0, reference.0))
    }
    fn process_stream(
        &self,
        _state: &mut (),
        references: &mut joins::AsOfReferenceView<'_, u64, u64, Fact>,
        stream: Fact,
    ) -> Result<Vec<Fact>, HandlerError> {
        match references.select_as_of(&stream.0, &stream.0) {
            joins::AsOf::Found(version) => Ok(vec![version]),
            joins::AsOf::BeforeFirstVersion | joins::AsOf::NoVersions => Ok(vec![stream]),
        }
    }
}

#[async_trait]
impl sinks::InlineSink for Pure {
    type Input = Fact;
    fn describe(&self) -> sinks::SinkDescription {
        sinks::SinkDescription::method(sinks::DeliveryMethod::Noop)
    }

    async fn write(&mut self, _input: Fact) -> Result<(), sinks::SinkWriteFailure> {
        Ok(())
    }
}

#[async_trait]
impl transforms::EffectfulTransformHandler for Pure {
    type Input = Fact;
    type Output = Fact;

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
        let input = sources::ValuesSource::new([Fact(1)]);
        let mapped = transforms::map(|fact: Fact| fact);
        let folded = stateful::reduce(Fact(0), |sum: &mut Fact, fact: &Fact| sum.0 += fact.0)
            .emit(stateful::emit::on_eof);
        let output = Pure;
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
    let _ = effectful_transform!(Fact -> Fact => Pure);
    let _ = of::flow::effectful_stateful!(Fact -> Fact => Pure);
    let _ = of::flow::transform!(Fact -> Fact => placeholder!());
    let _ = of::flow::stateful!(Fact -> Fact => placeholder!());
    let _ = of::flow::sink!(Fact => placeholder!());
    let joined = joins::inner(
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |_reference: Fact, fact: Fact| fact,
    );
    let _ = of::flow::join!(catalog input: Fact, Fact -> Fact => joined);
    let _ = merge!(Fact => merges::then_live);
    let _ = of::flow::merge!(Fact => merges::then_live);
    let _ = merge!(first input: Fact => merges::concat);
    let _ = of::flow::merge!(first input: Fact => merges::concat);
    let then_live = joins::inner_then_live(
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |_reference: Fact, fact: Fact| fact,
    );
    let _ = join!(catalog input: Fact, Fact -> Fact => then_live);
    let _ = joins::left_then_live(
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |_reference: Option<Fact>, fact: Fact| fact,
    );
    let _ = joins::strict_then_live(
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |_reference: Fact, fact: Fact| fact,
    );
    let _ = joins::JoinReferenceMode::FiniteThenLive;
    let priced = joins::left_as_of(
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |fact: &Fact| fact.0,
        |_version: joins::AsOf<Fact>, fact: Fact| fact,
    );
    let _ = join!(catalog input: Fact, Fact -> Fact => priced);
    let point_in_time = joins::AsOfJoin::new(PointInTime);
    let _ = of::flow::join!(catalog input: Fact, Fact -> Fact => point_in_time);
    let _ = source!(Fact => Pure);
    let _ = async_source!(Fact => Pure);
    let _ = infinite_source!(Fact => Pure);
    let _ = async_infinite_source!(Fact => Pure);
    let _ = transform!(Fact -> Fact => Pure);
    let _ = stateful!(Fact -> Fact => Pure);
    let _ = join!(catalog input: Fact, Fact -> Fact => Pure);
    let _ = sink!(Fact => Pure);

    let _selected = FlowDefinition::materialize(|_config| {
        let input = sources::ValuesSource::new([Fact(1)]);
        let first = Pure;
        let second = sinks::ConsoleSink::<Fact, _>::new(sinks::DebugFormatter);
        Ok(flow! {
            journals: of::journal::memory_journals(),
            stages: {
                input = source!(Fact => input);
                output = of::flow::sink!(Fact => handler_set!(first, second))?;
            },
            topology: { input |> output; }
        })
    });
}

use self::of::ai::{
    AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec, Chunk, Partials,
};

fn request() -> ChatRequestSpec {
    ChatRequestSpec::new("Return numbers only.", "Return a number.")
}

impl AiRole<Fact> for Pure {
    type Effect = ChatCompletion;
    type Out = Fact;

    fn prepare(&self, _input: &Fact) -> Result<ChatRequestSpec, HandlerError> {
        Ok(request())
    }

    fn interpret(
        &self,
        input: Fact,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<Fact, HandlerError> {
        outcome.replied()?;
        Ok(input)
    }
}

impl AiRole<Chunk<Fact>> for Pure {
    type Effect = ChatCompletion;
    type Out = Fact;

    fn prepare(&self, _: &Chunk<Fact>) -> Result<ChatRequestSpec, HandlerError> {
        Ok(request())
    }

    fn interpret(
        &self,
        chunk: Chunk<Fact>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<Fact, HandlerError> {
        outcome.replied()?;
        Ok(Fact(chunk.iter().map(|fact| fact.0).sum()))
    }
}

impl AiRole<Partials<Fact, Fact>> for Pure {
    type Effect = ChatCompletion;
    type Out = Fact;

    fn prepare(&self, _: &Partials<Fact, Fact>) -> Result<ChatRequestSpec, HandlerError> {
        Ok(request())
    }

    fn interpret(
        &self,
        input: Partials<Fact, Fact>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<Fact, HandlerError> {
        outcome.replied()?;
        Ok(Fact(input.partials().iter().map(|fact| fact.0).sum()))
    }
}

#[test]
fn renamed_facade_ai_rows_compile() {
    let breaker = of::middleware::circuit_breaker().consecutive_failures(5);
    let infer = of::ai::infer(Pure);
    ai!(Fact -> Fact with breaker => infer);
    let tag = of::ai::map(Pure).chunks(1);
    ai!(Fact -> Fact => tag);
    let digest = of::ai::map(Pure).chunks(1).reduce(Pure).on_eof();
    ai!(Fact -> Fact => digest);
}
