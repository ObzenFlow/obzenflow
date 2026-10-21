// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Item, ItemStruct, ItemTrait};

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
    Ok(quote! {
        #item

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
