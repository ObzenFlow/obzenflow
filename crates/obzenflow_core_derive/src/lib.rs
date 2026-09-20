// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Compiler-host derives for Core-owned ObzenFlow contracts.
//!
//! Provides `#[derive(EffectOutcomeFacts)]` for effect outcome carriers
//! (FLOWIP-120m) and `#[derive(StageOutputFacts)]` for pure stage output
//! carriers (FLOWIP-120z). Use them through `obzenflow_core`, which
//! re-exports each derive next to its trait, the same way serde re-exports
//! its derives.

mod stage_output;

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
/// It also generates the `StageFactSet` member projection so the outcome's
/// leaf facts participate in FLOWIP-120z subset proofs, and, for sum
/// carriers, `OneFactStageOutput`.
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
/// namespace. If the direct `obzenflow_core` dependency has been renamed in
/// Cargo.toml, point the derive at that extern-prelude name:
///
/// ```ignore
/// #[derive(Debug, Clone, EffectOutcomeFacts)]
/// #[effect_outcome(crate = flow_core)]
/// enum Outcome {
///     Ok(SomeFact),
/// }
/// ```
///
/// A public schema facade can instead supply the narrow compiler support path:
/// `#[effect_outcome(schema = public_api::schema)]`. This is independent of
/// the dependency's package name and works with renamed dependencies.
#[proc_macro_derive(EffectOutcomeFacts, attributes(effect_outcome))]
pub fn derive_effect_outcome_facts(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derive a pure stage output carrier (FLOWIP-120z).
///
/// Apply to an enum whose variants are single facts
/// (`Validated(ValidatedOrder)`), named-field products of facts
/// (`Invalid { invalid: InvalidOrder, cancelled: OrderCancelled }`, field
/// order is commit order), or explicitly empty filter arms
/// (`#[stage_output(empty)] Skipped`); or to a named-field struct for a
/// bare product. The derive generates `TypedFactSet` (only the selected
/// variant's leaf facts are emitted; the carrier itself is never persisted),
/// `StageFactSet` (the leaf-member projection, deduplicated across
/// variants), and, when every variant is a single fact, `OneFactStageOutput`.
///
/// Reconstruction dispatch is by leaf set, so two variants with an
/// identical leaf-type set are a compile error, as is a repeated leaf
/// within one variant or a unit variant without the explicit
/// `#[stage_output(empty)]` attribute. A carrier must not also implement
/// `TypedPayload`: the blanket implementations conflict, deliberately.
///
/// The `#[stage_output(crate = <path>)]` attribute mirrors
/// `#[effect_outcome(crate = <path>)]` when the direct `obzenflow_core`
/// dependency has been renamed.
/// Use `#[stage_output(schema = public_api::schema)]` for a schema facade.
#[proc_macro_derive(StageOutputFacts, attributes(stage_output))]
pub fn derive_stage_output_facts(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    stage_output::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Select the schema contracts for the effect carrier, through either its
/// Core owner or a caller-supplied public schema module.
fn effect_outcome_schema_path(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    attr_schema_path(input, "effect_outcome", "FLOWIP-120m")
}

/// Select the schema vocabulary without depending on an application facade.
/// Legacy `crate = <path>` selects a Core crate; `schema = <path>` selects
/// the narrow compiler-support module below a public schema facade.
fn attr_schema_path(
    input: &DeriveInput,
    attr_name: &str,
    flowip: &str,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut override_path: Option<proc_macro2::TokenStream> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident(attr_name) {
            continue;
        }
        if override_path.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                format!("duplicate #[{attr_name}(...)] attribute ({flowip})"),
            ));
        }
        let path = attr
            .parse_args_with(|stream: syn::parse::ParseStream<'_>| {
                let is_core = if stream.peek(syn::Token![crate]) {
                    stream.parse::<syn::Token![crate]>()?;
                    true
                } else {
                    let key: Ident = stream.parse()?;
                    if key != "schema" {
                        return Err(syn::Error::new(key.span(), "expected `crate` or `schema`"));
                    }
                    false
                };
                stream.parse::<syn::Token![=]>()?;
                let path: syn::Path = stream.parse()?;
                if !stream.is_empty() {
                    return Err(stream.error("unexpected tokens after the path"));
                }
                Ok(if is_core {
                    quote!(#path::event::schema)
                } else {
                    quote!(#path::__private)
                })
            })
            .map_err(|err| {
                syn::Error::new(
                    err.span(),
                    format!("expected #[{attr_name}(crate = <path>)] or #[{attr_name}(schema = <path>)]: {err}"),
                )
            })?;
        override_path = Some(path);
    }
    Ok(match override_path {
        Some(path) => path,
        None => quote!(::obzenflow_core::event::schema),
    })
}

/// Build the nested type-level member list for a carrier's `Members`
/// projection (FLOWIP-120z).
fn members_list_type(
    schema: &proc_macro2::TokenStream,
    members: &[&Type],
) -> proc_macro2::TokenStream {
    let mut list = quote!(#schema::EmptySet);
    for member in members.iter().rev() {
        list = quote!(#schema::WithMember<#member, #list>);
    }
    list
}

/// Generate the `StageFactSet` implementation shared by both carrier
/// derives (FLOWIP-120z): the type-level `Members` projection, the
/// value-level member metadata, and the const duplicate guard over
/// `EVENT_TYPE`s.
fn stage_fact_set_impl(
    schema: &proc_macro2::TokenStream,
    name: &Ident,
    members: &[&Type],
) -> proc_macro2::TokenStream {
    let list = members_list_type(schema, members);
    quote! {
        impl #schema::StageFactSet for #name {
            type Members = #list;

            fn member_fact_types() -> ::std::vec::Vec<#schema::TypedFactType> {
                ::std::vec![
                    #( #schema::TypedFactType::of::<#members>() ),*
                ]
            }

            const MEMBERS_DISTINCT: () =
                <#list as #schema::FactList>::DISTINCT_EVENT_TYPES;
        }
    }
}

fn expand(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            input.generics.span(),
            "effect outcome carriers cannot be generic; declare a concrete carrier per effect \
             (FLOWIP-120m)",
        ));
    }

    let schema = effect_outcome_schema_path(input)?;
    match &input.data {
        Data::Enum(data) => expand_enum(&input.ident, data, &schema),
        Data::Struct(data) => expand_struct(&input.ident, data, &schema),
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
    schema: &proc_macro2::TokenStream,
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

    let stage_fact_set = stage_fact_set_impl(schema, name, &members);
    Ok(quote! {
        impl #schema::TypedFactSet for #name {
            fn fact_types() -> ::std::vec::Vec<#schema::TypedFactType> {
                ::std::vec![
                    #( #schema::TypedFactType::of::<#members>() ),*
                ]
            }

            fn into_facts(
                self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<#schema::TypedFact>,
                #schema::TypedFactSetError,
            > {
                match self {
                    #( Self::#variants(member) => ::std::result::Result::Ok(::std::vec![
                        #schema::TypedFact::from_payload(member)?,
                    ]), )*
                }
            }

            fn try_from_facts(
                facts: &[#schema::TypedFact],
            ) -> ::std::result::Result<
                Self,
                #schema::TypedFactSetError,
            > {
                for fact in facts {
                    let declared = false
                        #( || <#members as #schema::TypedPayload>::event_type_matches(
                            fact.event_type.as_str(),
                        ) )*;
                    if !declared {
                        return ::std::result::Result::Err(
                            #schema::TypedFactSetError::UnexpectedFact {
                                event_type: fact.event_type.clone(),
                            },
                        );
                    }
                }
                match facts {
                    [single] => {
                        #(
                            if <#members as #schema::TypedPayload>::event_type_matches(
                                single.event_type.as_str(),
                            ) {
                                return ::std::result::Result::Ok(Self::#variants(
                                    #schema::decode_member_fact::<#members>(facts)?,
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
                        #schema::missing_fact_group_error(
                            &<Self as #schema::TypedFactSet>::fact_types(),
                        ),
                    ),
                    [first, rest @ ..] => ::std::result::Result::Err(
                        #schema::sum_group_arity_error(first, rest),
                    ),
                }
            }
        }

        #stage_fact_set

        // A sum carrier lowers to exactly one fact per value, so it also
        // qualifies as an effectful stateful `Output` (FLOWIP-120z).
        impl #schema::OneFactStageOutput for #name {}
    })
}

fn expand_struct(
    name: &Ident,
    data: &syn::DataStruct,
    schema: &proc_macro2::TokenStream,
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

    let stage_fact_set = stage_fact_set_impl(schema, name, &members);
    Ok(quote! {
        impl #schema::TypedFactSet for #name {
            fn fact_types() -> ::std::vec::Vec<#schema::TypedFactType> {
                ::std::vec![
                    #( #schema::TypedFactType::of::<#members>() ),*
                ]
            }

            fn into_facts(
                self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<#schema::TypedFact>,
                #schema::TypedFactSetError,
            > {
                // Field order is the committed fact order; the committer's
                // outcome_fact_ordinal preserves it deterministically.
                ::std::result::Result::Ok(::std::vec![
                    #( #schema::TypedFact::from_payload(self.#idents)?, )*
                ])
            }

            fn try_from_facts(
                facts: &[#schema::TypedFact],
            ) -> ::std::result::Result<
                Self,
                #schema::TypedFactSetError,
            > {
                for fact in facts {
                    let declared = false
                        #( || <#members as #schema::TypedPayload>::event_type_matches(
                            fact.event_type.as_str(),
                        ) )*;
                    if !declared {
                        return ::std::result::Result::Err(
                            #schema::TypedFactSetError::UnexpectedFact {
                                event_type: fact.event_type.clone(),
                            },
                        );
                    }
                }
                // Multiset equality: the scan above rejects foreign facts,
                // and each member decode requires exactly one fact of its
                // type (MissingFact / DuplicateFact otherwise).
                ::std::result::Result::Ok(Self {
                    #( #idents: #schema::decode_member_fact::<#members>(facts)?, )*
                })
            }
        }

        #stage_fact_set
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_derives_route_through_the_selected_schema() {
        for attribute in ["stage_output", "effect_outcome"] {
            for (routing, expected) in [
                ("", ":: obzenflow_core :: event :: schema"),
                ("crate = renamed_core", "renamed_core :: event :: schema"),
                (
                    "schema = public_api::schema",
                    "public_api :: schema :: __private",
                ),
                (
                    "schema = ::renamed::schema",
                    ":: renamed :: schema :: __private",
                ),
            ] {
                let attr = if routing.is_empty() {
                    String::new()
                } else {
                    format!("#[{attribute}({routing})]")
                };
                let input: DeriveInput =
                    syn::parse_str(&format!("{attr} enum Carrier {{ Fact(Fact) }}")).unwrap();
                let expanded = if attribute == "stage_output" {
                    stage_output::expand(&input)
                } else {
                    expand(&input)
                }
                .unwrap()
                .to_string();
                assert!(expanded.contains(&format!("{expected} :: TypedFactSet")));
                if !routing.is_empty() {
                    assert!(!expanded.contains("obzenflow_core"));
                }
            }
        }
    }

    #[test]
    fn schema_routing_rejects_ambiguous_and_malformed_overrides() {
        for attribute in ["stage_output", "effect_outcome"] {
            for attributes in [
                format!("#[{attribute}(schema = one)] #[{attribute}(schema = two)]"),
                format!("#[{attribute}(crate = one)] #[{attribute}(schema = two)]"),
                format!("#[{attribute}(schema = one, crate = two)]"),
                format!("#[{attribute}(schema =)]"),
                format!("#[{attribute}(unknown = one)]"),
            ] {
                let input: DeriveInput =
                    syn::parse_str(&format!("{attributes} enum Carrier {{ Fact(Fact) }}")).unwrap();
                assert!(attr_schema_path(&input, attribute, "schema routing").is_err());
            }
        }
    }

    #[test]
    fn effect_outcome_rejects_duplicate_member_types() {
        let input: DeriveInput = syn::parse_quote! {
            enum Outcome {
                First(Fact),
                Second(Fact),
            }
        };

        let error = expand(&input).expect_err("duplicate members must be rejected");

        assert!(
            error.to_string().contains("duplicate member type `Fact`"),
            "unexpected error: {error}"
        );
    }
}
