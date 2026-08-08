// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::flow;

fn main() {
    let effect_ports = ();
    // named_without_backpressure_omitted_empty
    let _ = flow! {
        name: "named_without_backpressure_omitted_empty",
        journals: (),
        middleware: [],
        stages: {},
        topology: {}
    };

    // named_without_backpressure_omitted_nonempty
    let _ = flow! {
        name: "named_without_backpressure_omitted_nonempty",
        journals: (),
        middleware: [()],
        stages: {},
        topology: {}
    };

    // named_without_backpressure_shorthand_empty
    let _ = flow! {
        name: "named_without_backpressure_shorthand_empty",
        journals: (),
        middleware: [],
        effect_ports,
        stages: {},
        topology: {}
    };

    // named_without_backpressure_shorthand_nonempty
    let _ = flow! {
        name: "named_without_backpressure_shorthand_nonempty",
        journals: (),
        middleware: [()],
        effect_ports,
        stages: {},
        topology: {}
    };

    // named_without_backpressure_expression_empty
    let _ = flow! {
        name: "named_without_backpressure_expression_empty",
        journals: (),
        middleware: [],
        effect_ports: (),
        stages: {},
        topology: {}
    };

    // named_without_backpressure_expression_nonempty
    let _ = flow! {
        name: "named_without_backpressure_expression_nonempty",
        journals: (),
        middleware: [()],
        effect_ports: (),
        stages: {},
        topology: {}
    };

    // named_with_backpressure_omitted_empty
    let _ = flow! {
        name: "named_with_backpressure_omitted_empty",
        journals: (),
        middleware: [],
        backpressure: (),
        stages: {},
        topology: {}
    };

    // named_with_backpressure_omitted_nonempty
    let _ = flow! {
        name: "named_with_backpressure_omitted_nonempty",
        journals: (),
        middleware: [()],
        backpressure: (),
        stages: {},
        topology: {}
    };

    // named_with_backpressure_shorthand_empty
    let _ = flow! {
        name: "named_with_backpressure_shorthand_empty",
        journals: (),
        middleware: [],
        backpressure: (),
        effect_ports,
        stages: {},
        topology: {}
    };

    // named_with_backpressure_shorthand_nonempty
    let _ = flow! {
        name: "named_with_backpressure_shorthand_nonempty",
        journals: (),
        middleware: [()],
        backpressure: (),
        effect_ports,
        stages: {},
        topology: {}
    };

    // named_with_backpressure_expression_empty
    let _ = flow! {
        name: "named_with_backpressure_expression_empty",
        journals: (),
        middleware: [],
        backpressure: (),
        effect_ports: (),
        stages: {},
        topology: {}
    };

    // named_with_backpressure_expression_nonempty
    let _ = flow! {
        name: "named_with_backpressure_expression_nonempty",
        journals: (),
        middleware: [()],
        backpressure: (),
        effect_ports: (),
        stages: {},
        topology: {}
    };

    // default_without_backpressure_omitted_empty
    let _ = flow! {
        journals: (),
        middleware: [],
        stages: {},
        topology: {}
    };

    // default_without_backpressure_omitted_nonempty
    let _ = flow! {
        journals: (),
        middleware: [()],
        stages: {},
        topology: {}
    };

    // default_without_backpressure_shorthand_empty
    let _ = flow! {
        journals: (),
        middleware: [],
        effect_ports,
        stages: {},
        topology: {}
    };

    // default_without_backpressure_shorthand_nonempty
    let _ = flow! {
        journals: (),
        middleware: [()],
        effect_ports,
        stages: {},
        topology: {}
    };

    // default_without_backpressure_expression_empty
    let _ = flow! {
        journals: (),
        middleware: [],
        effect_ports: (),
        stages: {},
        topology: {}
    };

    // default_without_backpressure_expression_nonempty
    let _ = flow! {
        journals: (),
        middleware: [()],
        effect_ports: (),
        stages: {},
        topology: {}
    };

    // default_with_backpressure_omitted_empty
    let _ = flow! {
        journals: (),
        middleware: [],
        backpressure: (),
        stages: {},
        topology: {}
    };

    // default_with_backpressure_omitted_nonempty
    let _ = flow! {
        journals: (),
        middleware: [()],
        backpressure: (),
        stages: {},
        topology: {}
    };

    // default_with_backpressure_shorthand_empty
    let _ = flow! {
        journals: (),
        middleware: [],
        backpressure: (),
        effect_ports,
        stages: {},
        topology: {}
    };

    // default_with_backpressure_shorthand_nonempty
    let _ = flow! {
        journals: (),
        middleware: [()],
        backpressure: (),
        effect_ports,
        stages: {},
        topology: {}
    };

    // default_with_backpressure_expression_empty
    let _ = flow! {
        journals: (),
        middleware: [],
        backpressure: (),
        effect_ports: (),
        stages: {},
        topology: {}
    };

    // default_with_backpressure_expression_nonempty
    let _ = flow! {
        journals: (),
        middleware: [()],
        backpressure: (),
        effect_ports: (),
        stages: {},
        topology: {}
    };

}
