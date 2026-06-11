// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Derive macros for ObzenFlow.
//!
//! Provides `#[derive(EffectOutcomeFacts)]` for effect outcome carriers
//! (FLOWIP-120m). Use it through `obzenflow_core`, which re-exports the
//! derive next to the trait, the same way serde re-exports its derives.

use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident, Type};

/// Derive an effect outcome carrier (FLOWIP-120m).
///
/// Apply to an enum for a closed sum outcome (exactly one `TypedPayload`
/// fact per tuple variant) or to a named-field struct for a product outcome
/// (exactly one `TypedPayload` fact per field, recorded together). The
/// derive generates the `TypedFactSet` implementation, which the blanket
/// lift makes an `EffectOutcomeFacts` carrier usable as `Effect::Outcome`.
///
/// Reconstruction is exact and fail-closed: a recorded group containing a
/// fact outside the carrier's declared set fails with
/// `TypedFactSetError::UnexpectedFact`, sum groups must hold exactly one
/// fact matching exactly one variant, and product groups must hold exactly
/// one fact per field. Malformed shapes (unit, struct-like, or multi-field
/// variants, tuple or unit structs, generics, repeated member types) are
/// compile errors. Two distinct member types colliding on `EVENT_TYPE`
/// cannot be seen here and are rejected at flow build instead.
///
/// A carrier must not also implement `TypedPayload`: the blanket
/// `TypedPayload -> TypedFactSet` implementation conflicts with the derived
/// one, which is deliberate, because a carrier is transient control-flow
/// machinery and never a persisted wrapper event.
///
/// # Path resolution
///
/// Generated code resolves `::obzenflow_core` in the deriving crate's
/// namespace. If your crate reaches the trait through a re-export and has no
/// direct `obzenflow_core` dependency (for example, only `obzenflow_runtime`
/// in Cargo.toml), point the derive at the re-exported core:
///
/// ```ignore
/// #[derive(Debug, Clone, EffectOutcomeFacts)]
/// #[effect_outcome(crate = obzenflow_runtime::obzenflow_core)]
/// enum Outcome {
///     Ok(SomeFact),
/// }
/// ```
#[proc_macro_derive(EffectOutcomeFacts, attributes(effect_outcome))]
pub fn derive_effect_outcome_facts(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// The path the generated code resolves `obzenflow_core` through: the
/// caller's extern-prelude name by default, or the path named by
/// `#[effect_outcome(crate = <path>)]` when core is reached via a re-export.
fn core_path(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut override_path: Option<syn::Path> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("effect_outcome") {
            continue;
        }
        if override_path.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate #[effect_outcome(...)] attribute (FLOWIP-120m)",
            ));
        }
        let path = attr
            .parse_args_with(|stream: syn::parse::ParseStream<'_>| {
                stream.parse::<syn::Token![crate]>()?;
                stream.parse::<syn::Token![=]>()?;
                let path: syn::Path = stream.parse()?;
                if !stream.is_empty() {
                    return Err(stream.error("unexpected tokens after the path"));
                }
                Ok(path)
            })
            .map_err(|err| {
                syn::Error::new(
                    err.span(),
                    format!("expected #[effect_outcome(crate = <path>)]: {err}"),
                )
            })?;
        override_path = Some(path);
    }
    Ok(match override_path {
        Some(path) => quote!(#path),
        None => quote!(::obzenflow_core),
    })
}

fn expand(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            input.generics.span(),
            "effect outcome carriers cannot be generic; declare a concrete carrier per effect \
             (FLOWIP-120m)",
        ));
    }

    let core = core_path(input)?;
    match &input.data {
        Data::Enum(data) => expand_enum(&input.ident, data, &core),
        Data::Struct(data) => expand_struct(&input.ident, data, &core),
        Data::Union(data) => Err(syn::Error::new(
            data.union_token.span,
            "EffectOutcomeFacts carriers are enums (sum outcomes) or named-field structs \
             (product outcomes), never unions (FLOWIP-120m)",
        )),
    }
}

const ENUM_SHAPE_ERROR: &str =
    "effect outcome enum variants must each hold exactly one TypedPayload fact, e.g. \
     `Authorized(PaymentAuthorized)`; unit, struct-like, multi-field, and empty enums are not \
     valid carriers (FLOWIP-120m)";

const STRUCT_SHAPE_ERROR: &str =
    "effect outcome struct carriers use named fields, one TypedPayload fact per field; tuple \
     and unit structs are not valid carriers (FLOWIP-120m)";

/// Dispatch is by event type, so a member type appearing twice would make
/// reconstruction ambiguous. The comparison is syntactic (type paths), which
/// catches the literal repeat; aliased repeats and distinct types colliding
/// on `EVENT_TYPE` are caught at flow build.
fn reject_duplicate_members(members: &[&Type]) -> Result<(), syn::Error> {
    let mut seen: Vec<String> = Vec::new();
    for member in members {
        let rendered = quote!(#member).to_string();
        if seen.contains(&rendered) {
            return Err(syn::Error::new(
                member.span(),
                format!(
                    "duplicate member type `{rendered}`: dispatch is by event type, so each \
                     member type appears once in a carrier (FLOWIP-120m)"
                ),
            ));
        }
        seen.push(rendered);
    }
    Ok(())
}

fn expand_enum(
    name: &Ident,
    data: &syn::DataEnum,
    core: &proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    if data.variants.is_empty() {
        return Err(syn::Error::new(name.span(), ENUM_SHAPE_ERROR));
    }

    let mut variants: Vec<&Ident> = Vec::new();
    let mut members: Vec<&Type> = Vec::new();
    for variant in &data.variants {
        let Fields::Unnamed(fields) = &variant.fields else {
            return Err(syn::Error::new(variant.span(), ENUM_SHAPE_ERROR));
        };
        if fields.unnamed.len() != 1 {
            return Err(syn::Error::new(variant.span(), ENUM_SHAPE_ERROR));
        }
        variants.push(&variant.ident);
        members.push(&fields.unnamed.first().expect("one field").ty);
    }
    reject_duplicate_members(&members)?;

    Ok(quote! {
        impl #core::event::schema::TypedFactSet for #name {
            fn fact_types() -> ::std::vec::Vec<#core::event::schema::TypedFactType> {
                ::std::vec![
                    #( #core::event::schema::TypedFactType::of::<#members>() ),*
                ]
            }

            fn into_facts(
                self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<#core::event::schema::TypedFact>,
                #core::event::schema::TypedFactSetError,
            > {
                match self {
                    #( Self::#variants(member) => ::std::result::Result::Ok(::std::vec![
                        #core::event::schema::TypedFact::from_payload(member)?,
                    ]), )*
                }
            }

            fn try_from_facts(
                facts: &[#core::event::schema::TypedFact],
            ) -> ::std::result::Result<
                Self,
                #core::event::schema::TypedFactSetError,
            > {
                for fact in facts {
                    let declared = false
                        #( || <#members as #core::event::schema::TypedPayload>::event_type_matches(
                            fact.event_type.as_str(),
                        ) )*;
                    if !declared {
                        return ::std::result::Result::Err(
                            #core::event::schema::TypedFactSetError::UnexpectedFact {
                                event_type: fact.event_type.clone(),
                            },
                        );
                    }
                }
                match facts {
                    [single] => {
                        #(
                            if <#members as #core::event::schema::TypedPayload>::event_type_matches(
                                single.event_type.as_str(),
                            ) {
                                return ::std::result::Result::Ok(Self::#variants(
                                    #core::event::schema::decode_member_fact::<#members>(facts)?,
                                ));
                            }
                        )*
                        // Safe: the undeclared-fact scan above already
                        // rejected any fact no variant matches.
                        ::core::unreachable!(
                            "EffectOutcomeFacts: undeclared facts were rejected above"
                        )
                    }
                    [] => ::std::result::Result::Err(
                        #core::event::schema::missing_fact_group_error(
                            &<Self as #core::event::schema::TypedFactSet>::fact_types(),
                        ),
                    ),
                    [first, rest @ ..] => ::std::result::Result::Err(
                        #core::event::schema::sum_group_arity_error(first, rest),
                    ),
                }
            }
        }
    })
}

fn expand_struct(
    name: &Ident,
    data: &syn::DataStruct,
    core: &proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new(data.fields.span(), STRUCT_SHAPE_ERROR));
    };
    if fields.named.is_empty() {
        return Err(syn::Error::new(name.span(), STRUCT_SHAPE_ERROR));
    }

    let mut idents: Vec<&Ident> = Vec::new();
    let mut members: Vec<&Type> = Vec::new();
    for field in &fields.named {
        idents.push(field.ident.as_ref().expect("named field"));
        members.push(&field.ty);
    }
    reject_duplicate_members(&members)?;

    Ok(quote! {
        impl #core::event::schema::TypedFactSet for #name {
            fn fact_types() -> ::std::vec::Vec<#core::event::schema::TypedFactType> {
                ::std::vec![
                    #( #core::event::schema::TypedFactType::of::<#members>() ),*
                ]
            }

            fn into_facts(
                self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<#core::event::schema::TypedFact>,
                #core::event::schema::TypedFactSetError,
            > {
                // Field order is the committed fact order; the committer's
                // outcome_fact_ordinal preserves it deterministically.
                ::std::result::Result::Ok(::std::vec![
                    #( #core::event::schema::TypedFact::from_payload(self.#idents)?, )*
                ])
            }

            fn try_from_facts(
                facts: &[#core::event::schema::TypedFact],
            ) -> ::std::result::Result<
                Self,
                #core::event::schema::TypedFactSetError,
            > {
                for fact in facts {
                    let declared = false
                        #( || <#members as #core::event::schema::TypedPayload>::event_type_matches(
                            fact.event_type.as_str(),
                        ) )*;
                    if !declared {
                        return ::std::result::Result::Err(
                            #core::event::schema::TypedFactSetError::UnexpectedFact {
                                event_type: fact.event_type.clone(),
                            },
                        );
                    }
                }
                // Multiset equality: the scan above rejects foreign facts,
                // and each member decode requires exactly one fact of its
                // type (MissingFact / DuplicateFact otherwise).
                ::std::result::Result::Ok(Self {
                    #( #idents: #core::event::schema::decode_member_fact::<#members>(facts)?, )*
                })
            }
        }
    })
}
