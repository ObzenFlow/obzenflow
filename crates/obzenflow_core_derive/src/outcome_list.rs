// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use proc_macro2::{TokenStream, TokenTree};
use quote::{quote, ToTokens};
use syn::parse::{ParseStream, Parser};
use syn::spanned::Spanned;
use syn::{Token, Type};

const COMMAS: &str = "outcomes are separated with `|`: write `outcome![A | B]`, and `(A, B)` for \
                      facts written together";

const REPEATED: &str = "duplicate member in a declared stage fact set: two members share an event \
                        type; each fact type appears exactly once";

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    sorted(input).unwrap_or_else(syn::Error::into_compile_error)
}

fn sorted(input: TokenStream) -> syn::Result<TokenStream> {
    let mut tokens = input.into_iter();
    let mut krate = TokenStream::new();
    for token in tokens.by_ref() {
        match token {
            TokenTree::Punct(punct) if punct.as_char() == ';' => break,
            token => krate.extend([token]),
        }
    }
    let outcomes = outcomes.parse2(tokens.collect())?;
    refuse_repeated_facts(&outcomes)?;
    let mut keyed = outcomes
        .into_iter()
        .map(|outcome| (sort_key(&outcome), outcome))
        .collect::<Vec<_>>();
    keyed.sort_by(|(left, _), (right, _)| left.cmp(right));
    let mut list = TokenStream::new();
    for (_, outcome) in keyed.into_iter().rev() {
        list = if list.is_empty() {
            quote!(#krate::event::schema::OutcomeOr<#outcome>)
        } else {
            quote!(#krate::event::schema::OutcomeOr<#outcome, #list>)
        };
    }
    Ok(list)
}

fn outcomes(input: ParseStream<'_>) -> syn::Result<Vec<Type>> {
    let mut outcomes = vec![input.parse::<Type>()?];
    while !input.is_empty() {
        if input.peek(Token![,]) {
            let comma: Token![,] = input.parse()?;
            return Err(syn::Error::new(comma.span(), COMMAS));
        }
        input.parse::<Token![|]>()?;
        outcomes.push(input.parse::<Type>()?);
    }
    Ok(outcomes)
}

fn facts(outcome: &Type) -> Vec<&Type> {
    match outcome {
        Type::Tuple(tuple) => tuple.elems.iter().collect(),
        Type::Paren(inner) => facts(&inner.elem),
        Type::Group(inner) => facts(&inner.elem),
        fact => vec![fact],
    }
}

fn refuse_repeated_facts(outcomes: &[Type]) -> syn::Result<()> {
    let mut written: Vec<String> = Vec::new();
    for fact in outcomes.iter().flat_map(facts) {
        let rendered = fact.to_token_stream().to_string();
        if written.contains(&rendered) {
            return Err(syn::Error::new(fact.span(), REPEATED));
        }
        written.push(rendered);
    }
    Ok(())
}

fn sort_key(outcome: &Type) -> (Vec<String>, Vec<String>) {
    facts(outcome)
        .into_iter()
        .map(|fact| (type_name(fact), fact.to_token_stream().to_string()))
        .unzip()
}

fn type_name(fact: &Type) -> String {
    match fact {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default(),
        Type::Paren(inner) => type_name(&inner.elem),
        Type::Group(inner) => type_name(&inner.elem),
        other => other.to_token_stream().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expanded(input: TokenStream) -> String {
        expand(input).to_string()
    }

    #[test]
    fn outcomes_sort_by_type_name() {
        let forward = expanded(quote!(krate; PaymentAuthorized | PaymentDeclined));
        let backward = expanded(quote!(krate; PaymentDeclined | PaymentAuthorized));
        assert_eq!(forward, backward);
        assert_eq!(
            forward,
            "krate :: event :: schema :: OutcomeOr < PaymentAuthorized , krate :: event :: \
             schema :: OutcomeOr < PaymentDeclined > >"
        );
    }

    #[test]
    fn qualified_path_sorts_by_last_segment() {
        let qualified = expanded(quote!(krate; facts::PaymentDeclined | PaymentAuthorized));
        assert!(
            qualified.find("PaymentAuthorized") < qualified.find("PaymentDeclined"),
            "{qualified}"
        );
    }

    #[test]
    fn tuple_outcome_keeps_written_order() {
        let list = expanded(quote!(krate; ValidatedOrder | (InvalidOrder, CancelledOrder)));
        assert!(
            list.contains("OutcomeOr < (InvalidOrder , CancelledOrder) ,"),
            "{list}"
        );
    }

    #[test]
    fn commas_between_outcomes_refused() {
        let refused = expanded(quote!(krate; PaymentAuthorized, PaymentDeclined));
        assert!(
            refused.contains("outcomes are separated with `|`"),
            "{refused}"
        );
    }

    #[test]
    fn repeated_fact_refused() {
        let refused = expanded(quote!(krate; Approved | (Approved, Audited)));
        assert!(refused.contains("duplicate member"), "{refused}");
    }
}
