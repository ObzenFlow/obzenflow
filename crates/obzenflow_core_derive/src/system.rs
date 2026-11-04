// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Item, ItemImpl, LitStr, Path, Token};

const ARGUMENTS: &str =
    "`#[obzenflow::system]` takes the system's name, as in `#[obzenflow::system(\"stripe\")]`";

const NOT_A_PORT_IMPL: &str = "`#[obzenflow::system(\"…\")]` marks a system's `impl` of its port, \
                               as in `impl PaymentGatewayPort for StripeProcessor`; a struct that \
                               serves as its own port names itself with \
                               `#[obzenflow::port(system = \"…\")]`";

const NAME_GRAMMAR: &str =
    "a system name is 1 to 64 characters from `[A-Za-z0-9._-]` and starts with a letter or digit";

struct SystemArgs {
    name: LitStr,
    effects: EffectsPath,
}

impl Parse for SystemArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let Ok(name) = input.parse::<LitStr>() else {
            return Err(syn::Error::new(input.span(), ARGUMENTS));
        };
        let mut effects = EffectsPath::default();
        if input.parse::<Option<Token![,]>>()?.is_some() && !input.is_empty() {
            let key: Ident = input.parse()?;
            if key != "effects" {
                return Err(syn::Error::new(key.span(), ARGUMENTS));
            }
            input.parse::<Token![=]>()?;
            effects = EffectsPath::written(input.parse::<Path>()?);
        }
        if !input.is_empty() {
            return Err(input.error(ARGUMENTS));
        }
        Ok(Self { name, effects })
    }
}

pub(crate) fn name_error(name: &LitStr) -> Option<syn::Error> {
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

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse2::<SystemArgs>(attr) {
        Ok(args) => args,
        Err(error) => return refused(item, error),
    };
    let Ok(Item::Impl(port_impl)) = syn::parse2::<Item>(item.clone()) else {
        return refused(
            item,
            syn::Error::new(proc_macro2::Span::call_site(), NOT_A_PORT_IMPL),
        );
    };
    if port_impl.trait_.is_none() {
        return refused(
            item,
            syn::Error::new_spanned(&port_impl.self_ty, NOT_A_PORT_IMPL),
        );
    }
    if let Some(error) = name_error(&args.name) {
        return refused(item, error);
    }
    let system_name = system_name_impl(&args, &port_impl);
    quote! {
        #port_impl
        #system_name
    }
}

fn system_name_impl(args: &SystemArgs, port_impl: &ItemImpl) -> TokenStream {
    let effects = args.effects.tokens();
    let name = &args.name;
    let system = &port_impl.self_ty;
    let (impl_generics, _, where_clause) = port_impl.generics.split_for_impl();
    quote! {
        impl #impl_generics #effects::SystemName for #system #where_clause {
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
        expand(attr, item).to_string()
    }

    #[test]
    fn expands_system_name_impl() {
        let expanded = expanded(
            quote!("stripe"),
            quote! {
                #[async_trait]
                impl PaymentGatewayPort for StripeProcessor {}
            },
        );
        assert!(
            expanded.contains("# [async_trait] impl PaymentGatewayPort for StripeProcessor { }"),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                ":: obzenflow :: effects :: SystemName for StripeProcessor { const NAME : & 'static str = \"stripe\" ; }"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn generics_propagate_to_system_name_impl() {
        let expanded = expanded(
            quote!("scripted"),
            quote!(
                impl<T> CardPort for Scripted<T> where T: Send {}
            ),
        );
        assert!(
            expanded.contains(
                "impl < T > :: obzenflow :: effects :: SystemName for Scripted < T > where T : Send"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn effects_path_override() {
        let expanded = expanded(
            quote!("ledger", effects = crate::effects),
            quote!(impl LedgerPort for Ledger {}),
        );
        assert!(
            expanded.contains("crate :: effects :: SystemName for Ledger"),
            "{expanded}"
        );
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
            let expanded = expanded(quote!(#name), quote!(impl CardPort for Card {}));
            assert!(expanded.contains(NAME_GRAMMAR), "{name}: {expanded}");
            assert!(!expanded.contains("SystemName"), "{name}: {expanded}");
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
            let expanded = expanded(quote!(#name), quote!(impl CardPort for Card {}));
            assert!(
                expanded.contains("SystemName for Card"),
                "{name}: {expanded}"
            );
        }
    }

    #[test]
    fn missing_name_refused() {
        let expanded = expanded(quote!(), quote!(impl CardPort for Card {}));
        assert!(expanded.contains("takes the system's name"), "{expanded}");
    }

    #[test]
    fn non_port_impl_refused() {
        for item in [
            quote!(impl Card {}),
            quote!(
                struct Card;
            ),
            quote!(
                trait CardPort {}
            ),
        ] {
            let expanded = expanded(quote!("card"), item);
            assert!(
                expanded.contains("`#[obzenflow::port(system = \\\"…\\\")]`"),
                "{expanded}"
            );
        }
    }
}
