// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{ParseStream, Parser};
use syn::{Generics, Ident, Item, LitStr, Path, Token};

const ARGUMENTS: &str =
    "`#[obzenflow::system]` takes the system's name, as in `#[obzenflow::system(\"stripe\")]`";

const NOT_A_TYPE: &str = "`#[obzenflow::system(\"…\")]` names a system on its type, as in \
                          `#[obzenflow::system(\"stripe\")] pub struct StripeProcessor`; the type \
                          names itself once, whatever ports it serves";

const NAME_GRAMMAR: &str =
    "a system name is 1 to 64 characters from `[A-Za-z0-9._-]` and starts with a letter or digit";

struct SystemArgs {
    name: LitStr,
    effects: EffectsPath,
}

impl SystemArgs {
    fn parse(input: ParseStream<'_>, mut effects: EffectsPath) -> syn::Result<Self> {
        let Ok(name) = input.parse::<LitStr>() else {
            return Err(syn::Error::new(input.span(), ARGUMENTS));
        };
        if input.parse::<Option<Token![,]>>()?.is_some() && !input.is_empty() {
            let key: Ident = input.parse()?;
            if key != "effects" {
                return Err(syn::Error::new(key.span(), ARGUMENTS));
            }
            input.parse::<Token![=]>()?;
            effects = EffectsPath::Written(input.parse::<Path>()?);
        }
        if !input.is_empty() {
            return Err(input.error(ARGUMENTS));
        }
        Ok(Self { name, effects })
    }
}

fn name_error(name: &LitStr) -> Option<syn::Error> {
    let value = name.value();
    let bytes = value.as_bytes();
    let within_grammar = !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    (!within_grammar).then(|| syn::Error::new(name.span(), NAME_GRAMMAR))
}

pub(crate) fn expand(attr: TokenStream, item: TokenStream, default: EffectsPath) -> TokenStream {
    let parser = |input: ParseStream<'_>| SystemArgs::parse(input, default);
    let args = match parser.parse2(attr) {
        Ok(args) => args,
        Err(error) => return refused(item, error),
    };
    let (ident, generics) = match syn::parse2::<Item>(item.clone()) {
        Ok(Item::Struct(system)) => (system.ident, system.generics),
        Ok(Item::Enum(system)) => (system.ident, system.generics),
        Ok(Item::Impl(system)) => {
            return refused(item, syn::Error::new_spanned(&system.self_ty, NOT_A_TYPE))
        }
        _ => {
            return refused(
                item,
                syn::Error::new(proc_macro2::Span::call_site(), NOT_A_TYPE),
            )
        }
    };
    if let Some(error) = name_error(&args.name) {
        return refused(item, error);
    }
    let system = system_impl(&args, &ident, &generics);
    quote! {
        #item
        #system
    }
}

fn system_impl(args: &SystemArgs, ident: &Ident, generics: &Generics) -> TokenStream {
    let effects = args.effects.tokens();
    let name = &args.name;
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #effects::System for #ident #type_generics #where_clause {
            const NAME: &'static str = #name;
        }
    }
}

fn refused(item: TokenStream, error: syn::Error) -> TokenStream {
    let error = error.into_compile_error();
    quote! {
        #item
        #error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expanded(attr: TokenStream, item: TokenStream) -> String {
        expand(attr, item, EffectsPath::Facade).to_string()
    }

    #[test]
    fn struct_expands_system_impl() {
        let expanded = expanded(
            quote!("stripe"),
            quote! {
                #[derive(Debug)]
                pub struct StripeProcessor;
            },
        );
        assert!(
            expanded.contains("# [derive (Debug)] pub struct StripeProcessor ;"),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                ":: obzenflow :: effects :: System for StripeProcessor { const NAME : & 'static str = \"stripe\" ; }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn enum_expands_system_impl() {
        let expanded = expanded(
            quote!("ledger"),
            quote!(
                enum Ledger {
                    Memory,
                }
            ),
        );
        assert!(expanded.contains("System for Ledger"), "{expanded}");
    }

    #[test]
    fn generics_propagate_to_system_impl() {
        let expanded = expanded(
            quote!("scripted"),
            quote!(
                struct Scripted<T>(T)
                where
                    T: Send;
            ),
        );
        assert!(
            expanded.contains(
                "impl < T > :: obzenflow :: effects :: System for Scripted < T > where T : Send"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn system_runtime_default_expands_runtime_path() {
        let expanded = expand(
            quote!("ledger"),
            quote!(
                struct Ledger;
            ),
            EffectsPath::Runtime,
        );
        let expected = quote! {
            struct Ledger;
            impl ::obzenflow_runtime::effects::System for Ledger {
                const NAME: &'static str = "ledger";
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn effects_path_override() {
        for default in [EffectsPath::Facade, EffectsPath::Runtime] {
            let expanded = expand(
                quote!("ledger", effects = crate::effects),
                quote!(
                    struct Ledger;
                ),
                default,
            )
            .to_string();
            assert!(
                expanded.contains("crate :: effects :: System for Ledger"),
                "{expanded}"
            );
        }
    }

    #[test]
    fn invalid_names_refused() {
        for name in [
            "",
            "simulated risk engine",
            "-leading-hyphen",
            "https://user:secret@vendor.example",
            &"s".repeat(65),
        ] {
            let expanded = expanded(
                quote!(#name),
                quote!(
                    struct Card;
                ),
            );
            assert!(expanded.contains(NAME_GRAMMAR), "{name}: {expanded}");
            assert!(!expanded.contains("System for"), "{name}: {expanded}");
        }
    }

    #[test]
    fn valid_names_accepted() {
        for name in [
            "simulated-risk-engine",
            "risk_engine",
            "ledger.v2",
            "0ledger",
            &"s".repeat(64),
        ] {
            let expanded = expanded(
                quote!(#name),
                quote!(
                    struct Card;
                ),
            );
            assert!(expanded.contains("System for Card"), "{name}: {expanded}");
        }
    }

    #[test]
    fn missing_name_refused() {
        let expanded = expanded(
            quote!(),
            quote!(
                struct Card;
            ),
        );
        assert!(expanded.contains("takes the system's name"), "{expanded}");
    }

    #[test]
    fn non_type_refused() {
        for item in [
            quote!(impl CardPort for Card {}),
            quote!(impl Card {}),
            quote!(
                trait CardPort {}
            ),
        ] {
            let expanded = expanded(quote!("card"), item);
            assert!(
                expanded.contains("names a system on its type"),
                "{expanded}"
            );
        }
    }
}
