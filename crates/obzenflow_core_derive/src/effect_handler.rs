// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Index, Meta};

pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    let effects = effects_path(input)?.tokens();
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(
            input.ident.span(),
            "`EffectHandler` derives on a struct, whose `#[system]` fields hold the systems the handler calls",
        ));
    };

    let mut systems = Vec::new();
    for (position, field) in data.fields.iter().enumerate() {
        let mut marks = field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("system"));
        let Some(mark) = marks.next() else {
            continue;
        };
        if !matches!(mark.meta, Meta::Path(_)) {
            return Err(syn::Error::new_spanned(
                mark,
                "`#[system]` takes no arguments",
            ));
        }
        if let Some(repeated) = marks.next() {
            return Err(syn::Error::new_spanned(
                repeated,
                "a field is marked `#[system]` once",
            ));
        }
        let accessor = match &field.ident {
            Some(ident) => quote!(self.#ident),
            None => {
                let index = Index::from(position);
                quote!(self.#index)
            }
        };
        systems.push((&field.ty, accessor));
    }

    let mut systems_type = quote!(());
    let mut systems_value = quote!(());
    for (ty, accessor) in systems.iter().rev() {
        systems_type = quote!((#ty, #systems_type));
        systems_value = quote!((::core::clone::Clone::clone(&#accessor), #systems_value));
    }
    let body = if systems.is_empty() {
        quote!({})
    } else {
        quote!({ #systems_value })
    };

    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #effects::EffectHandler for #name #ty_generics #where_clause {
            type Systems = #systems_type;

            fn systems(&self) -> Self::Systems #body
        }
    })
}

fn effects_path(input: &DeriveInput) -> Result<EffectsPath, syn::Error> {
    let mut found: Option<EffectsPath> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("effect_handler") {
            continue;
        }
        if found.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate #[effect_handler(...)] attribute",
            ));
        }
        found = Some(attr.parse_args::<EffectsPath>()?);
    }
    Ok(found.unwrap_or_else(EffectsPath::default_path))
}
