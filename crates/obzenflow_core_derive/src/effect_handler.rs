// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::{Group, Span, TokenStream, TokenTree};
use quote::{quote, ToTokens};
use syn::ext::IdentExt;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    parse_quote, parse_quote_spanned, Attribute, Data, DeriveInput, Index, Meta, Token, Type,
    WherePredicate,
};

struct SystemField<'a> {
    ty: &'a Type,
    name: String,
    accessor: TokenStream,
    listed: Option<Vec<Type>>,
}

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
        if let Some(repeated) = marks.next() {
            return Err(syn::Error::new_spanned(
                repeated,
                "a field is marked `#[system]` once",
            ));
        }
        let listed = listed_requests(mark)?;
        let (name, accessor) = match &field.ident {
            Some(ident) => (ident.unraw().to_string(), quote!(self.#ident)),
            None => {
                let index = Index::from(position);
                (position.to_string(), quote!(self.#index))
            }
        };
        systems.push(SystemField {
            ty: &field.ty,
            name,
            accessor,
            listed,
        });
    }

    let mut systems_type = quote!(());
    let mut systems_value = quote!(());
    let mut allowed = quote!(());
    for SystemField {
        ty,
        accessor,
        listed,
        ..
    } in systems.iter().rev()
    {
        systems_type = quote!((#ty, #systems_type));
        systems_value = quote!((::core::clone::Clone::clone(&#accessor), #systems_value));
        let field = match listed {
            None => quote!(#effects::WholeSystem<#ty>),
            Some(requests) => {
                let mut list = quote!(());
                for request in requests.iter().rev() {
                    list = quote!((#request, #list));
                }
                quote!(#effects::NarrowedSystem<#ty, #list>)
            }
        };
        allowed = quote!((#field, #allowed));
    }
    let names = systems.iter().map(|system| &system.name);
    let body = if systems.is_empty() {
        quote!({})
    } else {
        quote!({ #systems_value })
    };

    let mut predicates: Vec<WherePredicate> = Vec::new();
    for SystemField { ty, listed, .. } in &systems {
        for request in listed.iter().flatten() {
            let span = request.span();
            let ty = located_at(ty.to_token_stream(), span);
            let path = located_at(effects.clone(), span);
            predicates.push(parse_quote_spanned! {span=>
                #ty: #path::SystemServesRequest<#request>
            });
        }
    }
    if !input.generics.params.is_empty() {
        predicates.push(parse_quote!(#allowed: #effects::SystemsEffects));
        predicates.push(parse_quote!(
            <#allowed as #effects::SystemsEffects>::Members: #effects::EffectList
        ));
    }
    let mut generics = input.generics.clone();
    if !predicates.is_empty() {
        generics.make_where_clause().predicates.extend(predicates);
    }

    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #effects::EffectHandler for #name #ty_generics #where_clause {
            type Systems = #systems_type;
            type AllowedEffects = #effects::DeclaredEffectSet<
                <#allowed as #effects::SystemsEffects>::Members,
            >;

            const SYSTEM_FIELDS: &'static [&'static str] = &[#(#names),*];

            fn systems(&self) -> Self::Systems #body
        }
    })
}

fn located_at(tokens: TokenStream, span: Span) -> TokenStream {
    tokens
        .into_iter()
        .map(|token| match token {
            TokenTree::Group(group) => {
                let mut moved = Group::new(group.delimiter(), located_at(group.stream(), span));
                moved.set_span(group.span().located_at(span));
                TokenTree::Group(moved)
            }
            mut token => {
                token.set_span(token.span().located_at(span));
                token
            }
        })
        .collect()
}

fn listed_requests(mark: &Attribute) -> Result<Option<Vec<Type>>, syn::Error> {
    let list = match &mark.meta {
        Meta::Path(_) => return Ok(None),
        Meta::List(list) => list,
        Meta::NameValue(_) => {
            return Err(syn::Error::new_spanned(
                mark,
                "`#[system]` lists the requests it allows in parentheses, as \
                 `#[system(StockReservation)]`",
            ))
        }
    };
    let requests = list.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)?;
    if requests.is_empty() {
        return Err(syn::Error::new_spanned(
            mark,
            "`#[system(...)]` lists the requests this field allows; write `#[system]` to allow \
             every effect of its port",
        ));
    }
    let mut written: Vec<String> = Vec::new();
    for request in &requests {
        let name = request.to_token_stream().to_string().replace(' ', "");
        if written.contains(&name) {
            return Err(syn::Error::new_spanned(
                request,
                format!("`{name}` is listed twice in this `#[system(...)]`"),
            ));
        }
        written.push(name);
    }
    Ok(Some(requests.into_iter().collect()))
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
