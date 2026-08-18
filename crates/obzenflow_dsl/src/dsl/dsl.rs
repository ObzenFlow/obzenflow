// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Author-facing flow macros.
//!
//! Stage declarations and topology remain separate. Builder-owned preparation
//! is ordinary Rust outside the macro but inside the enclosing deferred
//! materialiser.

/// Parse topology edges supporting both |> and <| operators.
#[macro_export]
macro_rules! parse_topology {
    ($connections:expr,) => {};

    ($connections:expr, ($reference:ident, $stream:ident) |> $join:ident; $($rest:tt)*) => {
        $connections.extend([
            (
                stringify!($reference).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
            (
                stringify!($stream).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
        ]);
        $crate::parse_topology!($connections, $($rest)*);
    };

    ($connections:expr, $from:ident |> $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($from).to_string(),
            stringify!($to).to_string(),
            obzenflow_topology::EdgeKind::Forward,
        )]);
        $crate::parse_topology!($connections, $($rest)*);
    };

    ($connections:expr, $from:ident <| $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($to).to_string(),
            stringify!($from).to_string(),
            obzenflow_topology::EdgeKind::Backward,
        )]);
        $crate::parse_topology!($connections, $($rest)*);
    };
}

/// Parse topology edges while also collecting join input metadata.
#[macro_export]
macro_rules! parse_topology_with_joins {
    ($connections:expr, $join_connections:expr,) => {};

    ($connections:expr, $join_connections:expr, ($reference:ident, $stream:ident) |> $join:ident; $($rest:tt)*) => {
        $join_connections.extend([(
            stringify!($join).to_string(),
            (
                stringify!($reference).to_string(),
                stringify!($stream).to_string()
            )
        )]);
        $connections.extend([
            (
                stringify!($reference).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
            (
                stringify!($stream).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
        ]);
        $crate::parse_topology_with_joins!($connections, $join_connections, $($rest)*);
    };

    ($connections:expr, $join_connections:expr, $from:ident |> $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($from).to_string(),
            stringify!($to).to_string(),
            obzenflow_topology::EdgeKind::Forward,
        )]);
        $crate::parse_topology_with_joins!($connections, $join_connections, $($rest)*);
    };

    ($connections:expr, $join_connections:expr, $from:ident <| $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($to).to_string(),
            stringify!($from).to_string(),
            obzenflow_topology::EdgeKind::Backward,
        )]);
        $crate::parse_topology_with_joins!($connections, $join_connections, $($rest)*);
    };
}

/// Declare an ObzenFlow pipeline as a single expression.
///
/// The optional sections are name and flow-level backpressure.
/// Middleware is declared on individual stages only (FLOWIP-115r).
#[macro_export]
macro_rules! flow {
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        effect_ports $($rest:tt)*
    } => {{
        compile_error!("flow! has no effect_ports slot; named registrations are collected from lexical via bindings (FLOWIP-133e)");
    }};

    {
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        effect_ports $($rest:tt)*
    } => {{
        compile_error!("flow! has no effect_ports slot; named registrations are collected from lexical via bindings (FLOWIP-133e)");
    }};

    // FLOWIP-115r: keep a deliberate diagnostic for both empty and non-empty
    // uses of the removed key. These arms precede all surviving grammar.
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        middleware: [$($removed_middleware:tt)*],
        $($rest:tt)*
    } => {{
        compile_error!(
            "flow! has no middleware slot; declare middleware on the stage where it applies (FLOWIP-115r)"
        );
    }};

    {
        journals: $journals:expr,
        middleware: [$($removed_middleware:tt)*],
        $($rest:tt)*
    } => {{
        compile_error!(
            "flow! has no middleware slot; declare middleware on the stage where it applies (FLOWIP-115r)"
        );
    }};

    // Canonical named form.
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $($edge:tt)*
        }
    } => {{
        $crate::FlowDefinition::new(
            move |__build_ctx: obzenflow_runtime::run_context::FlowBuildContext| async move {
                use $crate::dsl::stage_descriptor::*;
                use std::collections::HashMap;

                let mut members: HashMap<
                    String,
                    $crate::dsl::composition::FlowMember,
                > = HashMap::new();

                $(
                    let member =
                        $crate::dsl::composition::IntoFlowMember::into_flow_member($descriptor);
                    members.insert(stringify!($stage_name).to_string(), member);
                )*

                for (binding, member) in members.iter_mut() {
                    if member.name() == BINDING_DERIVED_NAME_SENTINEL {
                        member.set_name(binding.clone());
                    }
                }

                let mut connections: Vec<(
                    String,
                    String,
                    obzenflow_topology::EdgeKind,
                )> = Vec::new();
                $crate::parse_topology!(connections, $($edge)*);

                let (stages, lowering_artifacts) =
                    $crate::dsl::composites::lower_composites(members, &mut connections)?;

                // Preserve the pre-substrate validation boundary: these
                // expressions run only after composite lowering succeeds.
                let journals = $journals;
                #[allow(unused_mut)]
                let mut flow_backpressure_clause = None;
                $(
                    flow_backpressure_clause = Some($flow_bp);
                )?

                $crate::dsl::flow_builder::build_flow(
                    $flow_name,
                    journals,
                    stages,
                    connections,
                    lowering_artifacts,
                    __build_ctx,
                    flow_backpressure_clause,
                )
                .await
                .map($crate::dsl::flow_builder::FlowBuildOutput::into_handle)
            },
        )
    }};

    // Canonical default-name form.
    {
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $($edge:tt)*
        }
    } => {{
        $crate::flow! {
            name: "default",
            journals: $journals,
            $(backpressure: $flow_bp,)?
            stages: {
                $($stage_name = $descriptor;)*
            },
            topology: {
                $($edge)*
            }
        }
    }};

    // Teaching diagnostics for the retired unrestricted statement drawer.
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        bindings: $($retired_bindings:tt)*
    } => {{
        compile_error!(
            "flow!: `bindings:` was removed; construct flow values inside \
             `FlowDefinition::materialize(|runtime_config| { ... })`"
        );
    }};

    {
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        bindings: $($retired_bindings:tt)*
    } => {{
        compile_error!(
            "flow!: `bindings:` was removed; construct flow values inside \
             `FlowDefinition::materialize(|runtime_config| { ... })`"
        );
    }};
}

/// Build a flow for tests, returning a FlowTestHarness.
///
/// It accepts the same authored sections as flow!, without a flow-level
/// middleware slot. The invocation returns an async move future.
#[cfg(any(test, feature = "test-support"))]
#[macro_export]
macro_rules! test_flow {
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        effect_ports $($rest:tt)*
    } => {{
        compile_error!("test_flow! has no effect_ports slot; named registrations are collected from lexical via bindings (FLOWIP-133e)");
    }};

    {
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        effect_ports $($rest:tt)*
    } => {{
        compile_error!("test_flow! has no effect_ports slot; named registrations are collected from lexical via bindings (FLOWIP-133e)");
    }};

    {
        name: $flow_name:literal,
        journals: $journals:expr,
        middleware: [$($removed_middleware:tt)*],
        $($rest:tt)*
    } => {{
        compile_error!(
            "test_flow! has no middleware slot; declare middleware on the stage where it applies (FLOWIP-115r)"
        );
    }};

    {
        journals: $journals:expr,
        middleware: [$($removed_middleware:tt)*],
        $($rest:tt)*
    } => {{
        compile_error!(
            "test_flow! has no middleware slot; declare middleware on the stage where it applies (FLOWIP-115r)"
        );
    }};

    // Canonical named form.
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $($edge:tt)*
        }
    } => {{
        async move {
            use $crate::dsl::stage_descriptor::*;
            use std::collections::HashMap;

            let __build_ctx =
                obzenflow_runtime::run_context::FlowBuildContext::for_tests();
            let mut members: HashMap<
                String,
                $crate::dsl::composition::FlowMember,
            > = HashMap::new();

            $(
                let member =
                    $crate::dsl::composition::IntoFlowMember::into_flow_member($descriptor);
                members.insert(stringify!($stage_name).to_string(), member);
            )*

            for (binding, member) in members.iter_mut() {
                if member.name() == BINDING_DERIVED_NAME_SENTINEL {
                    member.set_name(binding.clone());
                }
            }

            let mut connections: Vec<(
                String,
                String,
                obzenflow_topology::EdgeKind,
            )> = Vec::new();
            $crate::parse_topology!(connections, $($edge)*);

            let (stages, lowering_artifacts) =
                $crate::dsl::composites::lower_composites(members, &mut connections)?;

            let journals = $journals;
            #[allow(unused_mut)]
            let mut flow_backpressure_clause = None;
            $(
                flow_backpressure_clause = Some($flow_bp);
            )?

            let output = $crate::dsl::flow_builder::build_flow(
                $flow_name,
                journals,
                stages,
                connections,
                lowering_artifacts,
                __build_ctx,
                flow_backpressure_clause,
            )
            .await?;
            output.into_test_harness()
        }
    }};

    // Canonical default-name form.
    {
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $($edge:tt)*
        }
    } => {{
        $crate::test_flow! {
            name: "default",
            journals: $journals,
            $(backpressure: $flow_bp,)?
            stages: {
                $($stage_name = $descriptor;)*
            },
            topology: {
                $($edge)*
            }
        }
    }};

    {
        name: $flow_name:literal,
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        bindings: $($retired_bindings:tt)*
    } => {{
        compile_error!(
            "test_flow!: `bindings:` was removed; construct flow values inside \
             `FlowDefinition::materialize(|runtime_config| { ... })`"
        );
    }};

    {
        journals: $journals:expr,
        $(backpressure: $flow_bp:expr,)?
        bindings: $($retired_bindings:tt)*
    } => {{
        compile_error!(
            "test_flow!: `bindings:` was removed; construct flow values inside \
             `FlowDefinition::materialize(|runtime_config| { ... })`"
        );
    }};
}
