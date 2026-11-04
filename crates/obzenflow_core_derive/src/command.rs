// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::{Group, Span, TokenStream, TokenTree};
use quote::{format_ident, quote, ToTokens};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    parenthesized, parse_quote, parse_quote_spanned, token, Attribute, Fields, GenericArgument,
    Ident, Index, Item, ItemStruct, Meta, Path, PathArguments, Token, Type, TypeTraitObject,
    WherePredicate,
};

const ARGUMENTS: &str = "`#[obzenflow::command]` takes `through = Port` or \
                         `through = (PortA, PortB)`, and optionally `effects = <path>`";

const NOT_A_STRUCT: &str = "`#[obzenflow::command]` marks a struct, which declares the ports it \
                            works through";

const UNIT_STRUCT: &str =
    "`#[obzenflow::command(through = …)]` takes a unit struct; write the port as a `#[port]` field \
     instead";

struct CommandArgs {
    through: Option<Vec<Path>>,
    effects: EffectsPath,
}

impl Parse for CommandArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut through = None;
        let mut effects = EffectsPath::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            if key != "through" && key != "effects" {
                return Err(syn::Error::new(key.span(), ARGUMENTS));
            }
            input.parse::<Token![=]>()?;
            if key == "effects" {
                effects = EffectsPath::written(input.parse::<Path>()?);
            } else if input.peek(token::Paren) {
                let ports;
                parenthesized!(ports in input);
                let ports = Punctuated::<Path, Token![,]>::parse_terminated(&ports)?;
                if ports.is_empty() {
                    return Err(syn::Error::new(key.span(), ARGUMENTS));
                }
                through = Some(ports.into_iter().collect());
            } else {
                through = Some(vec![input.parse::<Path>()?]);
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self { through, effects })
    }
}

struct HeldPort {
    ty: Type,
    accessor: TokenStream,
    field: TokenStream,
    listed: Option<Vec<Type>>,
}

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse2::<CommandArgs>(attr) {
        Ok(args) => args,
        Err(error) => return refused(item, error),
    };
    let Ok(Item::Struct(command)) = syn::parse2::<Item>(item.clone()) else {
        return refused(item, syn::Error::new(Span::call_site(), NOT_A_STRUCT));
    };
    let expansion = match &args.through {
        Some(ports) => expand_through(&args.effects, command.clone(), ports),
        None => expand_fields(&args.effects, command.clone()),
    };
    expansion.unwrap_or_else(|error| refused(without_marks(command).to_token_stream(), error))
}

fn without_marks(mut command: ItemStruct) -> ItemStruct {
    for field in command.fields.iter_mut() {
        field.attrs.retain(|attr| !attr.path().is_ident("port"));
    }
    command
}

fn refused(item: TokenStream, error: syn::Error) -> TokenStream {
    let error = error.into_compile_error();
    quote! {
        #item
        #error
    }
}

fn expand_through(
    effects: &EffectsPath,
    mut command: ItemStruct,
    ports: &[Path],
) -> Result<TokenStream, syn::Error> {
    if !matches!(command.fields, Fields::Unit) {
        return Err(syn::Error::new(command.ident.span(), UNIT_STRUCT));
    }
    let mut written: Vec<String> = Vec::new();
    let mut names: Vec<Ident> = Vec::new();
    let mut held = Vec::new();
    let effects_path = effects.tokens();
    for port in ports {
        refuse_generics(port)?;
        let compact = compact_tokens(port);
        if written.contains(&compact) {
            return Err(two_ports(&command.ident, &compact, port));
        }
        written.push(compact);
        let name = field_name(port, &names);
        let ty: Type = parse_quote!(#effects_path::Port<dyn #port>);
        let port_name = port_label(port);
        held.push(HeldPort {
            ty,
            accessor: quote!(self.#name),
            field: quote!(#effects_path::PortField::Through(#port_name)),
            listed: None,
        });
        names.push(name);
    }
    let types = held.iter().map(|port| &port.ty).collect::<Vec<_>>();
    let fields: syn::FieldsNamed = parse_quote!({ #(#names: #types),* });
    command.fields = Fields::Named(fields);
    command.semi_token = None;

    let name = &command.ident;
    let (impl_generics, ty_generics, where_clause) = command.generics.split_for_impl();
    let constructor = quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            /// Builds this command from the ports it works through, in the
            /// order `through` lists them.
            pub fn through(#(#names: #types),*) -> Self {
                Self { #(#names),* }
            }
        }
    };
    let command_impl = command_impl(effects, &command, &held);
    Ok(quote! {
        #command
        #constructor
        #command_impl
    })
}

fn expand_fields(effects: &EffectsPath, command: ItemStruct) -> Result<TokenStream, syn::Error> {
    let effects_path = effects.tokens();
    let mut held = Vec::new();
    let mut written: Vec<String> = Vec::new();
    for (position, field) in command.fields.iter().enumerate() {
        let mut marks = field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("port"));
        let Some(mark) = marks.next() else {
            continue;
        };
        if let Some(repeated) = marks.next() {
            return Err(syn::Error::new_spanned(
                repeated,
                "a field is marked `#[port]` once",
            ));
        }
        let compact = compact_tokens(&field.ty);
        if written.contains(&compact) {
            return Err(two_ports(
                &command.ident,
                &port_of_field(&field.ty),
                &field.ty,
            ));
        }
        written.push(compact);
        let listed = listed_requests(mark)?;
        let (name, accessor) = match &field.ident {
            Some(ident) => (ident.unraw().to_string(), quote!(self.#ident)),
            None => {
                let index = Index::from(position);
                (position.to_string(), quote!(self.#index))
            }
        };
        held.push(HeldPort {
            ty: field.ty.clone(),
            accessor,
            field: quote!(#effects_path::PortField::Named(#name)),
            listed,
        });
    }
    let command = without_marks(command);
    let command_impl = command_impl(effects, &command, &held);
    Ok(quote! {
        #command
        #command_impl
    })
}

fn command_impl(effects: &EffectsPath, command: &ItemStruct, held: &[HeldPort]) -> TokenStream {
    let effects = effects.tokens();
    let mut ports_type = quote!(());
    let mut ports_value = quote!(());
    let mut allowed = quote!(());
    for HeldPort {
        ty,
        accessor,
        listed,
        ..
    } in held.iter().rev()
    {
        ports_type = quote!((#ty, #ports_type));
        ports_value = quote!((::core::clone::Clone::clone(&#accessor), #ports_value));
        let field = match listed {
            None => quote!(#effects::WholePort<#ty>),
            Some(requests) => {
                let mut list = quote!(());
                for request in requests.iter().rev() {
                    list = quote!((#request, #list));
                }
                quote!(#effects::NarrowedPort<#ty, #list>)
            }
        };
        allowed = quote!((#field, #allowed));
    }
    let fields = held.iter().map(|port| &port.field);
    let body = if held.is_empty() {
        quote!({})
    } else {
        quote!({ #ports_value })
    };

    let mut predicates: Vec<WherePredicate> = Vec::new();
    for HeldPort { ty, listed, .. } in held {
        for request in listed.iter().flatten() {
            let span = request.span();
            let ty = located_at(ty.to_token_stream(), span);
            let path = located_at(effects.clone(), span);
            predicates.push(parse_quote_spanned! {span=>
                #ty: #path::PortServesRequest<#request>
            });
        }
    }
    if !command.generics.params.is_empty() {
        predicates.push(parse_quote!(#allowed: #effects::FieldsEffects));
        predicates.push(parse_quote!(
            <#allowed as #effects::FieldsEffects>::Members: #effects::EffectList
        ));
    }
    let mut generics = command.generics.clone();
    if !predicates.is_empty() {
        generics.make_where_clause().predicates.extend(predicates);
    }

    let name = &command.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #effects::Command for #name #ty_generics #where_clause {
            type Ports = #ports_type;
            type AllowedEffects = #effects::DeclaredEffectSet<
                <#allowed as #effects::FieldsEffects>::Members,
            >;

            const PORT_FIELDS: &'static [#effects::PortField] = &[#(#fields),*];

            fn ports(&self) -> Self::Ports #body
        }
    }
}

fn refuse_generics(port: &Path) -> Result<(), syn::Error> {
    let Some(segment) = port
        .segments
        .iter()
        .find(|segment| !matches!(segment.arguments, PathArguments::None))
    else {
        return Ok(());
    };
    let requests = match &segment.arguments {
        PathArguments::AngleBracketed(arguments) => arguments
            .args
            .iter()
            .map(compact_tokens)
            .collect::<Vec<_>>()
            .join(", "),
        other => compact_tokens(other),
    };
    let written = port
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>()
        .join("::");
    Err(syn::Error::new_spanned(
        port,
        format!(
            "`through = {written}<{requests}>`: a port takes no generic parameters; to allow \
             some of its requests, write the port as a `#[port({requests})]` field"
        ),
    ))
}

fn two_ports(command: &Ident, port: &str, tokens: impl ToTokens) -> syn::Error {
    syn::Error::new_spanned(
        tokens,
        format!(
            "`{command}` holds two ports of type `{port}`; a command holds one port per port type"
        ),
    )
}

fn compact_tokens(tokens: impl ToTokens) -> String {
    tokens.to_token_stream().to_string().replace(' ', "")
}

fn port_label(port: &Path) -> String {
    port.segments
        .last()
        .map(|segment| segment.ident.unraw().to_string())
        .unwrap_or_default()
}

fn port_of_field(ty: &Type) -> String {
    let Type::Path(path) = ty else {
        return compact_tokens(ty);
    };
    let arguments = path.path.segments.last().map(|segment| &segment.arguments);
    let Some(PathArguments::AngleBracketed(arguments)) = arguments else {
        return compact_tokens(ty);
    };
    match arguments.args.first() {
        Some(GenericArgument::Type(Type::TraitObject(TypeTraitObject { bounds, .. }))) => {
            compact_tokens(bounds)
        }
        Some(GenericArgument::Type(inner)) => compact_tokens(inner),
        _ => compact_tokens(ty),
    }
}

fn field_name(port: &Path, taken: &[Ident]) -> Ident {
    let label = port_label(port);
    let mut name = String::new();
    let mut previous_lower = false;
    for character in label.chars() {
        if character.is_uppercase() && previous_lower {
            name.push('_');
        }
        name.extend(character.to_lowercase());
        previous_lower = character.is_lowercase() || character.is_ascii_digit();
    }
    if !name.ends_with("port") {
        name.push_str("_port");
    }
    let ident = format_ident!("{name}");
    if taken.contains(&ident) {
        format_ident!("{name}_{}", taken.len())
    } else {
        ident
    }
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
                "`#[port]` lists the requests it allows in parentheses, as \
                 `#[port(StockReservation)]`",
            ))
        }
    };
    let requests = list.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)?;
    if requests.is_empty() {
        return Err(syn::Error::new_spanned(
            mark,
            "`#[port(...)]` lists the requests this field allows; write `#[port]` to allow every \
             effect of its port",
        ));
    }
    let mut written: Vec<String> = Vec::new();
    for request in &requests {
        let name = compact_tokens(request);
        if written.contains(&name) {
            return Err(syn::Error::new_spanned(
                request,
                format!("`{name}` is listed twice in this `#[port(...)]`"),
            ));
        }
        written.push(name);
    }
    Ok(Some(requests.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expanded(attr: TokenStream, item: TokenStream) -> String {
        expand(attr, item).to_string()
    }

    #[test]
    fn through_adds_a_private_field_a_constructor_and_the_command() {
        let expanded = expanded(
            quote!(through = RiskEnginePort),
            quote! {
                #[derive(Debug, Clone)]
                pub struct Scorer;
            },
        );
        assert!(
            expanded.contains(
                "# [derive (Debug , Clone)] pub struct Scorer { risk_engine_port : :: obzenflow :: effects :: Port < dyn RiskEnginePort > }"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                "pub fn through (risk_engine_port : :: obzenflow :: effects :: Port < dyn RiskEnginePort >) -> Self { Self { risk_engine_port } }"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains(":: obzenflow :: effects :: Command for Scorer"),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                "(:: obzenflow :: effects :: WholePort < :: obzenflow :: effects :: Port < dyn RiskEnginePort > > , ())"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains("PortField :: Through (\"RiskEnginePort\")"),
            "{expanded}"
        );
    }

    #[test]
    fn through_two_ports_takes_them_in_the_order_it_lists_them() {
        let expanded = expanded(
            quote!(through = (RiskEnginePort, CaseSystemPort)),
            quote!(
                pub struct Triage;
            ),
        );
        let risk_engine = expanded
            .find("pub fn through (risk_engine_port")
            .expect("the first port comes first");
        let case_system = expanded
            .find("case_system_port : :: obzenflow :: effects :: Port < dyn CaseSystemPort >) -> Self")
            .expect("the second port comes second");
        assert!(risk_engine < case_system, "{expanded}");
        assert!(
            expanded.contains("Self { risk_engine_port , case_system_port }"),
            "{expanded}"
        );
    }

    #[test]
    fn two_ports_with_one_last_segment_get_distinct_fields() {
        let expanded = expanded(
            quote!(through = (billing::CardPort, refunds::CardPort)),
            quote!(
                struct Refund;
            ),
        );
        assert!(expanded.contains("card_port :"), "{expanded}");
        assert!(expanded.contains("card_port_1 :"), "{expanded}");
    }

    #[test]
    fn a_port_named_otherwise_still_reads_as_a_port() {
        let expanded = expanded(
            quote!(through = Ledger),
            quote!(
                struct Book;
            ),
        );
        assert!(expanded.contains("ledger_port :"), "{expanded}");
    }

    #[test]
    fn through_refuses_a_struct_with_fields_of_its_own() {
        for item in [
            quote!(
                struct Scorer {
                    limit: u32,
                }
            ),
            quote!(
                struct Scorer {}
            ),
            quote!(
                struct Scorer(u32);
            ),
        ] {
            let expanded = expanded(quote!(through = RiskEnginePort), item);
            assert!(
                expanded
                    .contains("takes a unit struct; write the port as a `#[port]` field instead"),
                "{expanded}"
            );
            assert!(!expanded.contains("Command for Scorer"), "{expanded}");
        }
    }

    #[test]
    fn through_refuses_generic_parameters_and_points_at_the_field() {
        let one = expanded(
            quote!(through = WarehousePort<StockRelease>),
            quote!(
                struct Releaser;
            ),
        );
        assert!(
            one.contains(
                "`through = WarehousePort<StockRelease>`: a port takes no generic parameters; to allow some of its requests, write the port as a `#[port(StockRelease)]` field"
            ),
            "{one}"
        );

        let two = expanded(
            quote!(through = warehouse::WarehousePort<StockRelease, StockReservation>),
            quote!(
                struct Releaser;
            ),
        );
        assert!(
            two.contains(
                "`through = warehouse::WarehousePort<StockRelease, StockReservation>`: a port takes no generic parameters; to allow some of its requests, write the port as a `#[port(StockRelease, StockReservation)]` field"
            ),
            "{two}"
        );
    }

    #[test]
    fn through_refuses_one_port_twice() {
        let expanded = expanded(
            quote!(through = (PaymentGatewayPort, PaymentGatewayPort)),
            quote!(
                struct ChargeInvoice;
            ),
        );
        assert!(
            expanded.contains(
                "`ChargeInvoice` holds two ports of type `PaymentGatewayPort`; a command holds one port per port type"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn a_field_form_reads_port_fields_and_strips_the_marks() {
        let expanded = expanded(
            quote!(),
            quote! {
                #[derive(Debug, Clone)]
                pub struct Allocator {
                    #[port]
                    warehouse_port: Port<dyn WarehousePort>,
                    capacity_per_sku: u32,
                }
            },
        );
        assert!(!expanded.contains("# [port]"), "{expanded}");
        assert!(
            expanded
                .contains("warehouse_port : Port < dyn WarehousePort > , capacity_per_sku : u32"),
            "{expanded}"
        );
        assert!(
            expanded.contains("PortField :: Named (\"warehouse_port\")"),
            "{expanded}"
        );
        assert!(
            expanded.contains("WholePort < Port < dyn WarehousePort > >"),
            "{expanded}"
        );
        assert!(!expanded.contains("pub fn through"), "{expanded}");
    }

    #[test]
    fn a_listed_field_is_narrowed_and_checked_against_its_port() {
        let expanded = expanded(
            quote!(),
            quote! {
                struct Allocator {
                    #[port(StockReservation, StockRelease)]
                    warehouse_port: Port<dyn WarehousePort>,
                }
            },
        );
        assert!(
            expanded.contains(
                "NarrowedPort < Port < dyn WarehousePort > , (StockReservation , (StockRelease , ())) >"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains(":: PortServesRequest < StockReservation >"),
            "{expanded}"
        );
    }

    #[test]
    fn a_command_with_no_port_allows_nothing() {
        let expanded = expanded(
            quote!(),
            quote!(
                struct Pure;
            ),
        );
        assert!(expanded.contains("type Ports = ()"), "{expanded}");
        assert!(
            expanded.contains("fn ports (& self) -> Self :: Ports { }"),
            "{expanded}"
        );
    }

    #[test]
    fn a_tuple_field_is_read_by_position() {
        let expanded = expanded(
            quote!(),
            quote!(
                struct Pair(u32, #[port] Port<dyn CardPort>);
            ),
        );
        assert!(expanded.contains("self . 1"), "{expanded}");
        assert!(
            expanded.contains("PortField :: Named (\"1\")"),
            "{expanded}"
        );
    }

    #[test]
    fn two_fields_of_one_port_type_are_refused() {
        let expanded = expanded(
            quote!(),
            quote! {
                struct ChargeInvoice {
                    #[port]
                    card: Port<dyn PaymentGatewayPort>,
                    #[port]
                    spare: Port<dyn PaymentGatewayPort>,
                }
            },
        );
        assert!(
            expanded.contains(
                "`ChargeInvoice` holds two ports of type `PaymentGatewayPort`; a command holds one port per port type"
            ),
            "{expanded}"
        );
        assert!(!expanded.contains("# [port]"), "{expanded}");
    }

    #[test]
    fn field_marks_are_checked() {
        let cases = [
            (
                quote!(
                    struct C {
                        #[port]
                        #[port]
                        a: Port<dyn P>,
                    }
                ),
                "a field is marked `#[port]` once",
            ),
            (
                quote!(
                    struct C {
                        #[port = "x"]
                        a: Port<dyn P>,
                    }
                ),
                "lists the requests it allows in parentheses",
            ),
            (
                quote!(
                    struct C {
                        #[port()]
                        a: Port<dyn P>,
                    }
                ),
                "write `#[port]` to allow every effect of its port",
            ),
            (
                quote!(
                    struct C {
                        #[port(R, R)]
                        a: Port<dyn P>,
                    }
                ),
                "`R` is listed twice in this `#[port(...)]`",
            ),
        ];
        for (item, message) in cases {
            let expanded = expanded(quote!(), item);
            assert!(expanded.contains(message), "{message}: {expanded}");
        }
    }

    #[test]
    fn the_effects_path_can_be_named_beside_through() {
        let expanded = expanded(
            quote!(through = CardPort, effects = crate::effects),
            quote!(
                struct Charge;
            ),
        );
        assert!(
            expanded.contains("card_port : crate :: effects :: Port < dyn CardPort >"),
            "{expanded}"
        );
        assert!(
            expanded.contains("crate :: effects :: Command for Charge"),
            "{expanded}"
        );
    }

    #[test]
    fn other_arguments_and_items_are_refused() {
        let expanded_args = expanded(
            quote!(uses = CardPort),
            quote!(
                struct Charge;
            ),
        );
        assert!(
            expanded_args.contains("takes `through = Port`"),
            "{expanded_args}"
        );
        let expanded_empty = expanded(
            quote!(through = ()),
            quote!(
                struct Charge;
            ),
        );
        assert!(
            expanded_empty.contains("takes `through = Port`"),
            "{expanded_empty}"
        );
        let expanded_item = expanded(
            quote!(),
            quote!(
                fn charge() {}
            ),
        );
        assert!(expanded_item.contains("marks a struct"), "{expanded_item}");
    }
}
