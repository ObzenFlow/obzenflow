// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Expansion for `#[derive(StageOutputFacts)]` (FLOWIP-120z): pure stage
//! output carriers as sums of products over `TypedPayload` leaf facts.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, Ident, Type};

const ENUM_SHAPE_ERROR: &str =
    "stage output enum variants hold one fact (`Validated(ValidatedOrder)`), a named-field \
     product of facts (`Invalid { invalid: InvalidOrder, cancelled: OrderCancelled }`), or an \
     explicit `#[stage_output(empty)]` filter arm; multi-field tuple variants and empty enums \
     are not valid carriers (FLOWIP-120z)";

const STRUCT_SHAPE_ERROR: &str =
    "stage output struct carriers use named fields, one TypedPayload fact per field; tuple \
     and unit structs are not valid carriers (FLOWIP-120z)";

/// One parsed enum variant of a stage output carrier.
enum Shape<'a> {
    /// `Validated(ValidatedOrder)`: one leaf fact.
    Unary { ident: &'a Ident, member: &'a Type },
    /// `Invalid { invalid: InvalidOrder, cancelled: OrderCancelled }`: a
    /// product of leaf facts, field order is commit order.
    Product {
        ident: &'a Ident,
        fields: Vec<(&'a Ident, &'a Type)>,
    },
    /// `#[stage_output(empty)] Skipped`: a deliberate zero-fact filter arm.
    Empty { ident: &'a Ident },
}

impl Shape<'_> {
    fn leaves(&self) -> Vec<&Type> {
        match self {
            Shape::Unary { member, .. } => vec![member],
            Shape::Product { fields, .. } => fields.iter().map(|(_, ty)| *ty).collect(),
            Shape::Empty { .. } => Vec::new(),
        }
    }
}

pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            input.generics.span(),
            "stage output carriers cannot be generic; declare a concrete carrier per stage \
             (FLOWIP-120z)",
        ));
    }

    let core = crate::attr_core_path(input, "stage_output", "FLOWIP-120z")?;
    match &input.data {
        Data::Enum(data) => expand_enum(input, data, &core),
        Data::Struct(data) => expand_struct(&input.ident, data, &core),
        Data::Union(data) => Err(syn::Error::new(
            data.union_token.span,
            "StageOutputFacts carriers are enums (sums of products) or named-field structs \
             (bare products), never unions (FLOWIP-120z)",
        )),
    }
}

/// Does the variant carry the `#[stage_output(empty)]` marker? Any other
/// `#[stage_output(...)]` argument on a variant is an error.
fn is_empty_variant(variant: &syn::Variant) -> Result<bool, syn::Error> {
    let mut empty = false;
    for attr in &variant.attrs {
        if !attr.path().is_ident("stage_output") {
            continue;
        }
        attr.parse_args_with(|stream: syn::parse::ParseStream<'_>| {
            let ident: Ident = stream.parse()?;
            if ident != "empty" || !stream.is_empty() {
                return Err(stream.error("expected #[stage_output(empty)] (FLOWIP-120z)"));
            }
            Ok(())
        })
        .map_err(|err| {
            syn::Error::new(
                err.span(),
                format!("expected #[stage_output(empty)]: {err}"),
            )
        })?;
        empty = true;
    }
    Ok(empty)
}

/// Reject a repeated leaf type within one variant: `into_facts` would
/// serialize two facts of one event type, and no group could reconstruct
/// them unambiguously. Syntactic comparison, like the FLOWIP-120m derive;
/// distinct types colliding on `EVENT_TYPE` are caught by the const member
/// guard and at flow build.
fn reject_duplicate_leaves(leaves: &[&Type]) -> Result<(), syn::Error> {
    let mut seen: Vec<String> = Vec::new();
    for leaf in leaves {
        let rendered = quote!(#leaf).to_string();
        if seen.contains(&rendered) {
            return Err(syn::Error::new(
                leaf.span(),
                format!(
                    "duplicate leaf type `{rendered}` within one stage output variant; each \
                     fact type appears once per variant (FLOWIP-120z)"
                ),
            ));
        }
        seen.push(rendered);
    }
    Ok(())
}

fn expand_enum(
    input: &DeriveInput,
    data: &syn::DataEnum,
    core: &TokenStream,
) -> Result<TokenStream, syn::Error> {
    let name = &input.ident;
    if data.variants.is_empty() {
        return Err(syn::Error::new(name.span(), ENUM_SHAPE_ERROR));
    }

    let mut shapes: Vec<Shape<'_>> = Vec::new();
    for variant in &data.variants {
        let empty = is_empty_variant(variant)?;
        let shape = match (&variant.fields, empty) {
            (Fields::Unit, true) => Shape::Empty {
                ident: &variant.ident,
            },
            (Fields::Unit, false) => {
                return Err(syn::Error::new(
                    variant.span(),
                    "a unit variant is a deliberate zero-fact filter arm and must carry an \
                     explicit #[stage_output(empty)] attribute so filtering cannot be \
                     accidental (FLOWIP-120z)",
                ));
            }
            (_, true) => {
                return Err(syn::Error::new(
                    variant.span(),
                    "#[stage_output(empty)] applies only to unit variants (FLOWIP-120z)",
                ));
            }
            (Fields::Unnamed(fields), false) => {
                if fields.unnamed.len() != 1 {
                    return Err(syn::Error::new(variant.span(), ENUM_SHAPE_ERROR));
                }
                Shape::Unary {
                    ident: &variant.ident,
                    member: &fields.unnamed.first().expect("one field").ty,
                }
            }
            (Fields::Named(fields), false) => {
                if fields.named.is_empty() {
                    return Err(syn::Error::new(variant.span(), ENUM_SHAPE_ERROR));
                }
                Shape::Product {
                    ident: &variant.ident,
                    fields: fields
                        .named
                        .iter()
                        .map(|field| (field.ident.as_ref().expect("named field"), &field.ty))
                        .collect(),
                }
            }
        };
        reject_duplicate_leaves(&shape.leaves())?;
        shapes.push(shape);
    }

    reject_identical_leaf_sets(data, &shapes)?;

    // The carrier's member set is the deduplicated union of every variant's
    // leaves, in first-occurrence order; structural sharing across variants
    // is legitimate (FLOWIP-120z).
    let mut leaf_union: Vec<&Type> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for shape in &shapes {
        for leaf in shape.leaves() {
            let rendered = quote!(#leaf).to_string();
            if !seen.contains(&rendered) {
                seen.push(rendered);
                leaf_union.push(leaf);
            }
        }
    }

    let into_arms = shapes.iter().map(|shape| match shape {
        Shape::Unary { ident, .. } => quote! {
            Self::#ident(member) => ::std::result::Result::Ok(::std::vec![
                #core::event::schema::TypedFact::from_payload(member)?,
            ]),
        },
        Shape::Product { ident, fields } => {
            let names: Vec<&Ident> = fields.iter().map(|(ident, _)| *ident).collect();
            quote! {
                // Field order is the committed fact order; the ordinal
                // regime preserves it deterministically.
                Self::#ident { #( #names ),* } => ::std::result::Result::Ok(::std::vec![
                    #( #core::event::schema::TypedFact::from_payload(#names)?, )*
                ]),
            }
        }
        Shape::Empty { ident } => quote! {
            Self::#ident => ::std::result::Result::Ok(::std::vec::Vec::new()),
        },
    });

    // Dispatch is by exact leaf set: with foreign facts and same-type
    // repeats already rejected, a group whose length matches a variant's
    // arity and which contains every leaf of that variant IS that variant's
    // set.
    let dispatch_arms = shapes.iter().map(|shape| match shape {
        Shape::Unary { ident, member } => quote! {
            if facts.len() == 1
                && <#member as #core::event::schema::TypedPayload>::event_type_matches(
                    facts[0].event_type.as_str(),
                )
            {
                return ::std::result::Result::Ok(Self::#ident(
                    #core::event::schema::decode_member_fact::<#member>(facts)?,
                ));
            }
        },
        Shape::Product { ident, fields } => {
            let arity = fields.len();
            let names: Vec<&Ident> = fields.iter().map(|(ident, _)| *ident).collect();
            let types: Vec<&Type> = fields.iter().map(|(_, ty)| *ty).collect();
            quote! {
                if facts.len() == #arity
                    #( && facts.iter().any(|fact| {
                        <#types as #core::event::schema::TypedPayload>::event_type_matches(
                            fact.event_type.as_str(),
                        )
                    }) )*
                {
                    return ::std::result::Result::Ok(Self::#ident {
                        #( #names: #core::event::schema::decode_member_fact::<#types>(facts)?, )*
                    });
                }
            }
        }
        Shape::Empty { ident } => quote! {
            if facts.is_empty() {
                return ::std::result::Result::Ok(Self::#ident);
            }
        },
    });

    let stage_fact_set = crate::stage_fact_set_impl(core, name, &leaf_union);
    let one_fact_output = if shapes
        .iter()
        .all(|shape| matches!(shape, Shape::Unary { .. }))
    {
        quote! {
            // Every variant lowers to exactly one fact, so the carrier also
            // qualifies as an effectful stateful `Output` (FLOWIP-120z).
            impl #core::event::schema::OneFactStageOutput for #name {}
        }
    } else {
        quote!()
    };

    Ok(quote! {
        impl #core::event::schema::TypedFactSet for #name {
            fn fact_types() -> ::std::vec::Vec<#core::event::schema::TypedFactType> {
                ::std::vec![
                    #( #core::event::schema::TypedFactType::of::<#leaf_union>() ),*
                ]
            }

            fn into_facts(
                self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<#core::event::schema::TypedFact>,
                #core::event::schema::TypedFactSetError,
            > {
                match self {
                    #( #into_arms )*
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
                        #( || <#leaf_union as #core::event::schema::TypedPayload>::event_type_matches(
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
                for (index, fact) in facts.iter().enumerate() {
                    if facts[index + 1..]
                        .iter()
                        .any(|later| later.event_type == fact.event_type)
                    {
                        return ::std::result::Result::Err(
                            #core::event::schema::TypedFactSetError::DuplicateFact {
                                event_type: fact.event_type.clone(),
                            },
                        );
                    }
                }
                #( #dispatch_arms )*
                ::std::result::Result::Err(
                    #core::event::schema::missing_fact_group_error(
                        &<Self as #core::event::schema::TypedFactSet>::fact_types(),
                    ),
                )
            }
        }

        #stage_fact_set

        #one_fact_output
    })
}

/// Reconstruction dispatch is by leaf set, so two variants with the same
/// leaf-type set (order-insensitive) can never be told apart from a
/// recorded group. Two `#[stage_output(empty)]` variants share the empty
/// set and are rejected by the same rule.
fn reject_identical_leaf_sets(
    data: &syn::DataEnum,
    shapes: &[Shape<'_>],
) -> Result<(), syn::Error> {
    let mut rendered_sets: Vec<Vec<String>> = Vec::new();
    for shape in shapes {
        let mut rendered: Vec<String> = shape
            .leaves()
            .iter()
            .map(|leaf| quote!(#leaf).to_string())
            .collect();
        rendered.sort();
        rendered_sets.push(rendered);
    }
    for (index, set) in rendered_sets.iter().enumerate() {
        if let Some(earlier) = rendered_sets[..index].iter().position(|other| other == set) {
            let variant = data
                .variants
                .iter()
                .nth(index)
                .expect("shape index matches variant index");
            let earlier_ident = &data
                .variants
                .iter()
                .nth(earlier)
                .expect("earlier index in range")
                .ident;
            return Err(syn::Error::new(
                variant.span(),
                format!(
                    "variants `{earlier_ident}` and `{}` share an identical leaf-type set; \
                     reconstruction dispatch is by leaf set, so every variant's set must be \
                     distinct (FLOWIP-120z)",
                    variant.ident,
                ),
            ));
        }
    }
    Ok(())
}

fn expand_struct(
    name: &Ident,
    data: &syn::DataStruct,
    core: &TokenStream,
) -> Result<TokenStream, syn::Error> {
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
    reject_duplicate_leaves(&members)?;

    let stage_fact_set = crate::stage_fact_set_impl(core, name, &members);
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
                // Field order is the committed fact order; the ordinal
                // regime preserves it deterministically.
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

        #stage_fact_set
    })
}
