// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::commands::Command;
use obzenflow::systems::Port;

#[obzenflow::port]
trait ReadPort {}

#[obzenflow::effects::port]
trait WritePort {}

struct Reader;
struct Writer;

impl ReadPort for Reader {}

impl WritePort for Writer {}

#[obzenflow::command(through = ReadPort)]
struct Read;

#[obzenflow::effects::command(through = WritePort)]
struct Write;

#[test]
fn commands_through_facade_exports_bind_systems() {
    let read = Read::through(Port::to(Reader));
    let write = Write::through(Port::to(Writer));
    let _ports = (read.ports(), write.ports());
}
