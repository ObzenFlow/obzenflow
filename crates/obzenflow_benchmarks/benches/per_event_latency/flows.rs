// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Fixed linear topologies: a source, `depth - 1` passthrough transforms and a
//! sink. The DSL takes literal stages, so each supported depth is spelled out.

use super::harness::{Event, Passthrough, Sink, Source};
use obzenflow_core::journal::{factory::FlowJournalFactory, JournalError};
use obzenflow_core::FlowId;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_runtime::pipeline::FlowHandle;

pub async fn build<P, J>(
    journals: P,
    depth: usize,
    source: Source,
    sink: Sink,
) -> anyhow::Result<FlowHandle>
where
    P: Fn(FlowId) -> Result<J, JournalError> + Send + Sync + 'static,
    J: FlowJournalFactory + 'static,
{
    anyhow::ensure!(
        matches!(depth, 1 | 2 | 3 | 4 | 5 | 20 | 100),
        "Unsupported depth: {depth}"
    );
    FlowDefinition::materialize(move |_runtime_config| {
        Ok(match depth {
            1 => flow! {
                journals: journals,
                stages: {
                    src = source!(Event => source);
                    snk = sink!(Event => sink);
                },
                topology: {
                    src |> snk;
                }
            },
            2 => flow! {
                journals: journals,
                stages: {
                    src = source!(Event => source);
                    s1 = transform!(Event -> Event => Passthrough);
                    snk = sink!(Event => sink);
                },
                topology: {
                    src |> s1;
                    s1 |> snk;
                }
            },
            3 => flow! {
                journals: journals,
                stages: {
                    src = source!(Event => source);
                    s1 = transform!(Event -> Event => Passthrough);
                    s2 = transform!(Event -> Event => Passthrough);
                    snk = sink!(Event => sink);
                },
                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> snk;
                }
            },
            4 => flow! {
                journals: journals,
                stages: {
                    src = source!(Event => source);
                    s1 = transform!(Event -> Event => Passthrough);
                    s2 = transform!(Event -> Event => Passthrough);
                    s3 = transform!(Event -> Event => Passthrough);
                    snk = sink!(Event => sink);
                },
                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> s3;
                    s3 |> snk;
                }
            },
            5 => flow! {
                journals: journals,
                stages: {
                    src = source!(Event => source);
                    s1 = transform!(Event -> Event => Passthrough);
                    s2 = transform!(Event -> Event => Passthrough);
                    s3 = transform!(Event -> Event => Passthrough);
                    s4 = transform!(Event -> Event => Passthrough);
                    snk = sink!(Event => sink);
                },
                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> s3;
                    s3 |> s4;
                    s4 |> snk;
                }
            },
            20 => flow! {
                journals: journals,
                stages: {
                    src = source!(Event => source);
                    s1 = transform!(Event -> Event => Passthrough);
                    s2 = transform!(Event -> Event => Passthrough);
                    s3 = transform!(Event -> Event => Passthrough);
                    s4 = transform!(Event -> Event => Passthrough);
                    s5 = transform!(Event -> Event => Passthrough);
                    s6 = transform!(Event -> Event => Passthrough);
                    s7 = transform!(Event -> Event => Passthrough);
                    s8 = transform!(Event -> Event => Passthrough);
                    s9 = transform!(Event -> Event => Passthrough);
                    s10 = transform!(Event -> Event => Passthrough);
                    s11 = transform!(Event -> Event => Passthrough);
                    s12 = transform!(Event -> Event => Passthrough);
                    s13 = transform!(Event -> Event => Passthrough);
                    s14 = transform!(Event -> Event => Passthrough);
                    s15 = transform!(Event -> Event => Passthrough);
                    s16 = transform!(Event -> Event => Passthrough);
                    s17 = transform!(Event -> Event => Passthrough);
                    s18 = transform!(Event -> Event => Passthrough);
                    s19 = transform!(Event -> Event => Passthrough);
                    snk = sink!(Event => sink);
                },
                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> s3;
                    s3 |> s4;
                    s4 |> s5;
                    s5 |> s6;
                    s6 |> s7;
                    s7 |> s8;
                    s8 |> s9;
                    s9 |> s10;
                    s10 |> s11;
                    s11 |> s12;
                    s12 |> s13;
                    s13 |> s14;
                    s14 |> s15;
                    s15 |> s16;
                    s16 |> s17;
                    s17 |> s18;
                    s18 |> s19;
                    s19 |> snk;
                }
            },
            100 => flow! {
                journals: journals,
                stages: {
                    src = source!(Event => source);
                    s1 = transform!(Event -> Event => Passthrough);
                    s2 = transform!(Event -> Event => Passthrough);
                    s3 = transform!(Event -> Event => Passthrough);
                    s4 = transform!(Event -> Event => Passthrough);
                    s5 = transform!(Event -> Event => Passthrough);
                    s6 = transform!(Event -> Event => Passthrough);
                    s7 = transform!(Event -> Event => Passthrough);
                    s8 = transform!(Event -> Event => Passthrough);
                    s9 = transform!(Event -> Event => Passthrough);
                    s10 = transform!(Event -> Event => Passthrough);
                    s11 = transform!(Event -> Event => Passthrough);
                    s12 = transform!(Event -> Event => Passthrough);
                    s13 = transform!(Event -> Event => Passthrough);
                    s14 = transform!(Event -> Event => Passthrough);
                    s15 = transform!(Event -> Event => Passthrough);
                    s16 = transform!(Event -> Event => Passthrough);
                    s17 = transform!(Event -> Event => Passthrough);
                    s18 = transform!(Event -> Event => Passthrough);
                    s19 = transform!(Event -> Event => Passthrough);
                    s20 = transform!(Event -> Event => Passthrough);
                    s21 = transform!(Event -> Event => Passthrough);
                    s22 = transform!(Event -> Event => Passthrough);
                    s23 = transform!(Event -> Event => Passthrough);
                    s24 = transform!(Event -> Event => Passthrough);
                    s25 = transform!(Event -> Event => Passthrough);
                    s26 = transform!(Event -> Event => Passthrough);
                    s27 = transform!(Event -> Event => Passthrough);
                    s28 = transform!(Event -> Event => Passthrough);
                    s29 = transform!(Event -> Event => Passthrough);
                    s30 = transform!(Event -> Event => Passthrough);
                    s31 = transform!(Event -> Event => Passthrough);
                    s32 = transform!(Event -> Event => Passthrough);
                    s33 = transform!(Event -> Event => Passthrough);
                    s34 = transform!(Event -> Event => Passthrough);
                    s35 = transform!(Event -> Event => Passthrough);
                    s36 = transform!(Event -> Event => Passthrough);
                    s37 = transform!(Event -> Event => Passthrough);
                    s38 = transform!(Event -> Event => Passthrough);
                    s39 = transform!(Event -> Event => Passthrough);
                    s40 = transform!(Event -> Event => Passthrough);
                    s41 = transform!(Event -> Event => Passthrough);
                    s42 = transform!(Event -> Event => Passthrough);
                    s43 = transform!(Event -> Event => Passthrough);
                    s44 = transform!(Event -> Event => Passthrough);
                    s45 = transform!(Event -> Event => Passthrough);
                    s46 = transform!(Event -> Event => Passthrough);
                    s47 = transform!(Event -> Event => Passthrough);
                    s48 = transform!(Event -> Event => Passthrough);
                    s49 = transform!(Event -> Event => Passthrough);
                    s50 = transform!(Event -> Event => Passthrough);
                    s51 = transform!(Event -> Event => Passthrough);
                    s52 = transform!(Event -> Event => Passthrough);
                    s53 = transform!(Event -> Event => Passthrough);
                    s54 = transform!(Event -> Event => Passthrough);
                    s55 = transform!(Event -> Event => Passthrough);
                    s56 = transform!(Event -> Event => Passthrough);
                    s57 = transform!(Event -> Event => Passthrough);
                    s58 = transform!(Event -> Event => Passthrough);
                    s59 = transform!(Event -> Event => Passthrough);
                    s60 = transform!(Event -> Event => Passthrough);
                    s61 = transform!(Event -> Event => Passthrough);
                    s62 = transform!(Event -> Event => Passthrough);
                    s63 = transform!(Event -> Event => Passthrough);
                    s64 = transform!(Event -> Event => Passthrough);
                    s65 = transform!(Event -> Event => Passthrough);
                    s66 = transform!(Event -> Event => Passthrough);
                    s67 = transform!(Event -> Event => Passthrough);
                    s68 = transform!(Event -> Event => Passthrough);
                    s69 = transform!(Event -> Event => Passthrough);
                    s70 = transform!(Event -> Event => Passthrough);
                    s71 = transform!(Event -> Event => Passthrough);
                    s72 = transform!(Event -> Event => Passthrough);
                    s73 = transform!(Event -> Event => Passthrough);
                    s74 = transform!(Event -> Event => Passthrough);
                    s75 = transform!(Event -> Event => Passthrough);
                    s76 = transform!(Event -> Event => Passthrough);
                    s77 = transform!(Event -> Event => Passthrough);
                    s78 = transform!(Event -> Event => Passthrough);
                    s79 = transform!(Event -> Event => Passthrough);
                    s80 = transform!(Event -> Event => Passthrough);
                    s81 = transform!(Event -> Event => Passthrough);
                    s82 = transform!(Event -> Event => Passthrough);
                    s83 = transform!(Event -> Event => Passthrough);
                    s84 = transform!(Event -> Event => Passthrough);
                    s85 = transform!(Event -> Event => Passthrough);
                    s86 = transform!(Event -> Event => Passthrough);
                    s87 = transform!(Event -> Event => Passthrough);
                    s88 = transform!(Event -> Event => Passthrough);
                    s89 = transform!(Event -> Event => Passthrough);
                    s90 = transform!(Event -> Event => Passthrough);
                    s91 = transform!(Event -> Event => Passthrough);
                    s92 = transform!(Event -> Event => Passthrough);
                    s93 = transform!(Event -> Event => Passthrough);
                    s94 = transform!(Event -> Event => Passthrough);
                    s95 = transform!(Event -> Event => Passthrough);
                    s96 = transform!(Event -> Event => Passthrough);
                    s97 = transform!(Event -> Event => Passthrough);
                    s98 = transform!(Event -> Event => Passthrough);
                    s99 = transform!(Event -> Event => Passthrough);
                    snk = sink!(Event => sink);
                },
                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> s3;
                    s3 |> s4;
                    s4 |> s5;
                    s5 |> s6;
                    s6 |> s7;
                    s7 |> s8;
                    s8 |> s9;
                    s9 |> s10;
                    s10 |> s11;
                    s11 |> s12;
                    s12 |> s13;
                    s13 |> s14;
                    s14 |> s15;
                    s15 |> s16;
                    s16 |> s17;
                    s17 |> s18;
                    s18 |> s19;
                    s19 |> s20;
                    s20 |> s21;
                    s21 |> s22;
                    s22 |> s23;
                    s23 |> s24;
                    s24 |> s25;
                    s25 |> s26;
                    s26 |> s27;
                    s27 |> s28;
                    s28 |> s29;
                    s29 |> s30;
                    s30 |> s31;
                    s31 |> s32;
                    s32 |> s33;
                    s33 |> s34;
                    s34 |> s35;
                    s35 |> s36;
                    s36 |> s37;
                    s37 |> s38;
                    s38 |> s39;
                    s39 |> s40;
                    s40 |> s41;
                    s41 |> s42;
                    s42 |> s43;
                    s43 |> s44;
                    s44 |> s45;
                    s45 |> s46;
                    s46 |> s47;
                    s47 |> s48;
                    s48 |> s49;
                    s49 |> s50;
                    s50 |> s51;
                    s51 |> s52;
                    s52 |> s53;
                    s53 |> s54;
                    s54 |> s55;
                    s55 |> s56;
                    s56 |> s57;
                    s57 |> s58;
                    s58 |> s59;
                    s59 |> s60;
                    s60 |> s61;
                    s61 |> s62;
                    s62 |> s63;
                    s63 |> s64;
                    s64 |> s65;
                    s65 |> s66;
                    s66 |> s67;
                    s67 |> s68;
                    s68 |> s69;
                    s69 |> s70;
                    s70 |> s71;
                    s71 |> s72;
                    s72 |> s73;
                    s73 |> s74;
                    s74 |> s75;
                    s75 |> s76;
                    s76 |> s77;
                    s77 |> s78;
                    s78 |> s79;
                    s79 |> s80;
                    s80 |> s81;
                    s81 |> s82;
                    s82 |> s83;
                    s83 |> s84;
                    s84 |> s85;
                    s85 |> s86;
                    s86 |> s87;
                    s87 |> s88;
                    s88 |> s89;
                    s89 |> s90;
                    s90 |> s91;
                    s91 |> s92;
                    s92 |> s93;
                    s93 |> s94;
                    s94 |> s95;
                    s95 |> s96;
                    s96 |> s97;
                    s97 |> s98;
                    s98 |> s99;
                    s99 |> snk;
                }
            },
            _ => unreachable!("depth validated above"),
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))
}
