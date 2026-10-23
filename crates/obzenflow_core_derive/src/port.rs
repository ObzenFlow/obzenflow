// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::{TokenStream, TokenTree};
use quote::{quote, quote_spanned, ToTokens};
use syn::spanned::Spanned;
use syn::{
    GenericParam, Generics, Item, ItemStruct, ItemTrait, ReturnType, TraitItem, TraitItemFn,
};

pub(crate) fn expand(effects: &EffectsPath, item: &Item) -> Result<TokenStream, syn::Error> {
    match item {
        Item::Trait(item) => expand_trait(effects, item),
        Item::Struct(item) => expand_struct(effects, item),
        other => Err(syn::Error::new_spanned(
            other,
            "`#[obzenflow::port]` marks a port trait, or a struct that serves as its own port",
        )),
    }
}

fn expand_trait(effects: &EffectsPath, item: &ItemTrait) -> Result<TokenStream, syn::Error> {
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &item.generics,
            "a port trait takes no generic parameters",
        ));
    }
    let effects = effects.tokens();
    let port = &item.ident;
    let answers = item
        .items
        .iter()
        .filter_map(|item| match item {
            TraitItem::Fn(method) => Some(answer_check(&effects, method)),
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(quote! {
        #item

        #(#answers)*

        impl<ObzenflowPortImpl> #effects::ServesPort<dyn #port, ObzenflowPortImpl>
            for #effects::PortImpl<ObzenflowPortImpl>
        where
            ObzenflowPortImpl: #port + 'static,
        {
            fn coerce(
                port: ::std::sync::Arc<ObzenflowPortImpl>,
            ) -> ::std::sync::Arc<dyn #port> {
                port
            }
        }

        impl #effects::ServesPort<dyn #port, dyn #port> for #effects::PortImpl<dyn #port> {
            fn coerce(port: ::std::sync::Arc<dyn #port>) -> ::std::sync::Arc<dyn #port> {
                port
            }
        }
    })
}

fn answer_check(effects: &TokenStream, method: &TraitItemFn) -> Result<TokenStream, syn::Error> {
    let signature = &method.sig;
    if signature
        .generics
        .lifetimes()
        .any(|param| param.lifetime.ident == "async_trait")
    {
        return Err(syn::Error::new_spanned(
            signature,
            "put `#[obzenflow::port]` above `#[async_trait]`, so it checks each method as written",
        ));
    }
    let answer = match &signature.output {
        ReturnType::Default => quote_spanned!(signature.ident.span() => ()),
        ReturnType::Type(_, returned) => {
            if names_outside_the_port(returned.to_token_stream(), &signature.generics) {
                return Err(syn::Error::new(
                    returned.span(),
                    "a port method returns a concrete `Result<Outcome, EffectError>`, without \
                     `Self`, `impl` or the method's generic parameters",
                ));
            }
            returned.to_token_stream()
        }
    };
    let cfgs = method
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("cfg"));
    Ok(quote! {
        #(#cfgs)*
        const _: fn() = #effects::assert_port_answer::<#answer>;
    })
}

fn names_outside_the_port(tokens: TokenStream, generics: &Generics) -> bool {
    tokens.into_iter().any(|token| match token {
        TokenTree::Group(group) => names_outside_the_port(group.stream(), generics),
        TokenTree::Ident(ident) => {
            ident == "Self"
                || ident == "impl"
                || generics.params.iter().any(|param| match param {
                    GenericParam::Type(param) => ident == param.ident,
                    GenericParam::Const(param) => ident == param.ident,
                    GenericParam::Lifetime(param) => ident == param.lifetime.ident,
                })
        }
        _ => false,
    })
}

fn expand_struct(effects: &EffectsPath, item: &ItemStruct) -> Result<TokenStream, syn::Error> {
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &item.generics,
            "a port struct takes no generic parameters",
        ));
    }
    let effects = effects.tokens();
    let port = &item.ident;
    Ok(quote! {
        #item

        impl #effects::ServesPort<#port, #port> for #effects::PortImpl<#port> {
            fn coerce(port: ::std::sync::Arc<#port>) -> ::std::sync::Arc<#port> {
                port
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_port_method_is_checked_under_its_own_cfg() {
        let item: Item = syn::parse_quote! {
            trait CardPort: Send + Sync {
                fn charge(&self) -> Result<Charged, EffectError>;
                #[cfg(test)]
                fn refund(&self) -> Result<Refunded, EffectError>;
            }
        };

        let expanded = expand(&EffectsPath::default_path(), &item)
            .expect("a port trait expands")
            .to_string();

        assert_eq!(
            expanded.matches("assert_port_answer").count(),
            2,
            "{expanded}"
        );
        assert!(expanded.contains("# [cfg (test)] const _"), "{expanded}");
    }

    #[test]
    fn a_port_below_async_trait_is_refused() {
        let item: Item = syn::parse_quote! {
            trait CardPort: Send + Sync {
                fn charge<'life0, 'async_trait>(
                    &'life0 self,
                ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<
                    Output = Result<Charged, EffectError>,
                > + Send + 'async_trait>>
                where
                    'life0: 'async_trait,
                    Self: 'async_trait;
            }
        };

        let error = expand(&EffectsPath::default_path(), &item)
            .expect_err("an expanded async trait must be refused");

        assert!(
            error
                .to_string()
                .contains("put `#[obzenflow::port]` above `#[async_trait]`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_return_type_naming_self_is_refused() {
        let item: Item = syn::parse_quote! {
            trait CardPort: Send + Sync {
                fn charge(&self) -> Result<Self, EffectError> where Self: Sized;
            }
        };

        let error = expand(&EffectsPath::default_path(), &item)
            .expect_err("a return type naming `Self` must be refused");

        assert!(
            error
                .to_string()
                .contains("a port method returns a concrete `Result<Outcome, EffectError>`"),
            "unexpected error: {error}"
        );
    }
}
