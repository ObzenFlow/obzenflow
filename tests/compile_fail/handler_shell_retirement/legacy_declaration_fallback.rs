use obzenflow_adapters::middleware::MiddlewareDeclaration;

fn main() {
    let declaration = MiddlewareDeclaration::legacy_shell("legacy", "legacy");
    let _ = declaration.is_legacy_shell();
}
