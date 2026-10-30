// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects_path::EffectsPath;
use proc_macro2::{Span, TokenStream, TokenTree};
use quote::{quote, quote_spanned, ToTokens};
use syn::spanned::Spanned;
use syn::{
    Attribute, FnArg, GenericParam, Generics, Ident, ImplItem, Item, ItemImpl, ItemTrait, LitInt,
    LitStr, Meta, PathArguments, ReturnType, Signature, TraitItem, Type, TypePath,
};

const MISSING_GUARANTEE: &str = "`#[effect]` needs its guarantee: `exactly_once` or `at_least_once`
  exactly_once    the call takes effect once even if it runs again: the operation is
                  idempotent, or the method takes `key: &IdempotencyKey` and the other
                  end deduplicates by it, or it takes `tx: EffectCommitHandle<Outcome>`
                  and commits its outcome
  at_least_once   the call may take effect more than once after a crash; the stage row
                  accepts that with `.at_least_once::<Request>()`";

const MISSING_TYPE: &str =
    "`#[effect]` needs `type = \"...\"`, the effect type the journal records this call under";

const ARGUMENTS: &str = "`#[effect]` takes `type = \"...\"`, `exactly_once` or `at_least_once`, \
                         and optionally `schema_version = N`";

const REQUEST_SHAPE: &str =
    "an effect method takes its request by reference after `&self`, as `request: &MyRequest`";

const REMAINING_SHAPE: &str = "after its request, an effect method takes nothing, \
                               `&EffectContext`, `key: &IdempotencyKey` optionally followed by \
                               `&EffectContext`, or a commit handle";

const KEY_AND_HANDLE: &str =
    "an effect method takes an idempotency key or a commit handle, not both";

const RETURN_SHAPE: &str = "a port method returns a concrete `Result<Outcome, EffectError>`, \
                            without `Self`, `impl` or the method's generic parameters";

pub(crate) fn expand(effects: &EffectsPath, item: Item) -> TokenStream {
    match item {
        Item::Trait(item) => expand_trait(effects, item),
        Item::Impl(item) => expand_impl(effects, item),
        Item::Struct(item) => refused(
            item.to_token_stream(),
            syn::Error::new_spanned(
                &item.ident,
                "`#[obzenflow::port]` marks a port trait, or the `impl` block of a struct that \
                 serves as its own port; move it to the `impl` block that holds the struct's \
                 effect methods",
            ),
        ),
        other => refused(
            other.to_token_stream(),
            syn::Error::new_spanned(
                &other,
                "`#[obzenflow::port]` marks a port trait, or the `impl` block of a struct that \
                 serves as its own port",
            ),
        ),
    }
}

fn refused(item: TokenStream, error: syn::Error) -> TokenStream {
    let error = error.into_compile_error();
    quote! {
        #item
        #error
    }
}

fn expand_trait(effects: &EffectsPath, mut item: ItemTrait) -> TokenStream {
    let methods = item
        .items
        .iter()
        .filter_map(|item| match item {
            TraitItem::Fn(method) => Some((&method.sig, method.attrs.as_slice())),
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(error) = methods
        .iter()
        .find_map(|(signature, _)| below_async_trait(signature))
    {
        return refused(item.to_token_stream(), error);
    }
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        let error =
            syn::Error::new_spanned(&item.generics, "a port trait takes no generic parameters");
        return refused(stripped_trait(item).to_token_stream(), error);
    }
    let effects = effects.tokens();
    let port = item.ident.clone();
    let mut errors = Vec::new();
    let mut answers = Vec::new();
    let mut declared = Vec::new();
    for &(signature, attrs) in &methods {
        match read_effect(signature, attrs) {
            Ok(Some(method)) => declared.push(method),
            Ok(None) => answers.push(answer_check(&effects, signature, attrs, &mut errors)),
            Err(error) => errors.push(error),
        }
    }
    let requests = generated_requests(
        &effects,
        &quote!(dyn #port),
        &port.to_token_stream(),
        declared,
        &mut errors,
    );
    item = stripped_trait(item);
    let errors = errors.into_iter().map(syn::Error::into_compile_error);
    quote! {
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

        #(#requests)*

        #(#errors)*
    }
}

fn stripped_trait(mut item: ItemTrait) -> ItemTrait {
    for member in &mut item.items {
        if let TraitItem::Fn(method) = member {
            method.attrs.retain(|attribute| !is_effect(attribute));
        }
    }
    item
}

fn expand_impl(effects: &EffectsPath, item: ItemImpl) -> TokenStream {
    if item.trait_.is_some() {
        let error = syn::Error::new_spanned(
            &item.self_ty,
            "`#[obzenflow::port]` marks a port trait, or the inherent `impl` block of a struct \
             that serves as its own port; a trait's implementation answers the effects its \
             trait declares",
        );
        return refused(stripped_impl(item).to_token_stream(), error);
    }
    let port = match port_struct(&item) {
        Ok(port) => port,
        Err(error) => return refused(stripped_impl(item).to_token_stream(), error),
    };
    let effects = effects.tokens();
    let mut errors = Vec::new();
    let mut declared = Vec::new();
    for member in &item.items {
        if let ImplItem::Fn(method) = member {
            match read_effect(&method.sig, &method.attrs) {
                Ok(Some(method)) => declared.push(method),
                Ok(None) => {}
                Err(error) => errors.push(error),
            }
        }
    }
    let port = port.to_token_stream();
    let requests = generated_requests(&effects, &port, &port, declared, &mut errors);
    let item = stripped_impl(item);
    let errors = errors.into_iter().map(syn::Error::into_compile_error);
    quote! {
        #item

        impl #effects::ServesPort<#port, #port> for #effects::PortImpl<#port> {
            fn coerce(port: ::std::sync::Arc<#port>) -> ::std::sync::Arc<#port> {
                port
            }
        }

        #(#requests)*

        #(#errors)*
    }
}

fn stripped_impl(mut item: ItemImpl) -> ItemImpl {
    for member in &mut item.items {
        if let ImplItem::Fn(method) = member {
            method.attrs.retain(|attribute| !is_effect(attribute));
        }
    }
    item
}

fn port_struct(item: &ItemImpl) -> Result<&TypePath, syn::Error> {
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &item.generics,
            "a port struct takes no generic parameters",
        ));
    }
    match &*item.self_ty {
        Type::Path(path)
            if path.qself.is_none()
                && path
                    .path
                    .segments
                    .iter()
                    .all(|segment| matches!(segment.arguments, PathArguments::None)) =>
        {
            Ok(path)
        }
        Type::Path(path) => Err(syn::Error::new_spanned(
            path,
            "a port struct takes no generic parameters",
        )),
        other => Err(syn::Error::new_spanned(
            other,
            "`#[obzenflow::port]` marks a port trait, or the `impl` block of a struct that \
             serves as its own port",
        )),
    }
}

fn is_effect(attribute: &Attribute) -> bool {
    attribute.path().is_ident("effect")
}

fn below_async_trait(signature: &Signature) -> Option<syn::Error> {
    signature
        .generics
        .lifetimes()
        .any(|param| param.lifetime.ident == "async_trait")
        .then(|| {
            syn::Error::new_spanned(
                signature,
                "put `#[obzenflow::port]` above `#[async_trait]`, so it checks each method as \
                 written",
            )
        })
}

fn answer_check(
    effects: &TokenStream,
    signature: &Signature,
    attrs: &[Attribute],
    errors: &mut Vec<syn::Error>,
) -> TokenStream {
    let answer = match answer_type(signature) {
        Ok(answer) => answer,
        Err(error) => {
            errors.push(error);
            return TokenStream::new();
        }
    };
    let cfgs = cfgs(attrs);
    quote! {
        #(#cfgs)*
        const _: fn() = #effects::assert_port_answer::<#answer>;
    }
}

fn answer_type(signature: &Signature) -> Result<TokenStream, syn::Error> {
    match &signature.output {
        ReturnType::Default => Ok(quote_spanned!(signature.ident.span() => ())),
        ReturnType::Type(_, returned) => {
            if names_outside_the_port(returned.to_token_stream(), &signature.generics) {
                return Err(syn::Error::new(returned.span(), RETURN_SHAPE));
            }
            Ok(returned.to_token_stream())
        }
    }
}

fn cfgs(attrs: &[Attribute]) -> impl Iterator<Item = &Attribute> {
    attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("cfg"))
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Guarantee {
    ExactlyOnce,
    AtLeastOnce,
}

enum Takes {
    Nothing,
    Context,
    Key { key: Span, context: bool },
    Handle,
}

struct EffectMethod<'a> {
    signature: &'a Signature,
    cfgs: Vec<&'a Attribute>,
    effect_type: LitStr,
    schema_version: Option<LitInt>,
    guarantee: Guarantee,
    request: &'a TypePath,
    takes: Takes,
    answer: TokenStream,
}

fn read_effect<'a>(
    signature: &'a Signature,
    attrs: &'a [Attribute],
) -> Result<Option<EffectMethod<'a>>, syn::Error> {
    let mut marked = attrs.iter().filter(|attribute| is_effect(attribute));
    let Some(attribute) = marked.next() else {
        return Ok(None);
    };
    if let Some(second) = marked.next() {
        return Err(syn::Error::new_spanned(
            second,
            "an effect method carries one `#[effect]`",
        ));
    }
    let arguments = effect_arguments(attribute)?;
    let request = request_type(signature)?;
    let takes = remaining_parameters(signature)?;
    match (arguments.guarantee, &takes) {
        (Guarantee::AtLeastOnce, Takes::Key { .. }) => {
            return Err(syn::Error::new(
                arguments.written.span(),
                format!(
                    "`{}` takes an idempotency key, so it is `exactly_once`, not `at_least_once`",
                    signature.ident
                ),
            ));
        }
        (Guarantee::AtLeastOnce, Takes::Handle) => {
            return Err(syn::Error::new(
                arguments.written.span(),
                format!(
                    "`{}` commits through an `EffectCommitHandle`, so it is `exactly_once`, not \
                     `at_least_once`",
                    signature.ident
                ),
            ));
        }
        _ => {}
    }
    Ok(Some(EffectMethod {
        signature,
        cfgs: cfgs(attrs).collect(),
        effect_type: arguments.effect_type,
        schema_version: arguments.schema_version,
        guarantee: arguments.guarantee,
        request,
        takes,
        answer: answer_type(signature)?,
    }))
}

struct EffectArguments {
    effect_type: LitStr,
    schema_version: Option<LitInt>,
    guarantee: Guarantee,
    written: Ident,
}

fn effect_arguments(attribute: &Attribute) -> Result<EffectArguments, syn::Error> {
    let mut effect_type: Option<LitStr> = None;
    let mut schema_version: Option<LitInt> = None;
    let mut guarantee: Option<(Guarantee, Ident)> = None;
    if !matches!(attribute.meta, Meta::Path(_)) {
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("type") {
                effect_type = Some(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("schema_version") {
                schema_version = Some(meta.value()?.parse()?);
                return Ok(());
            }
            let word = match meta.path.get_ident() {
                Some(word) if word == "exactly_once" => (Guarantee::ExactlyOnce, word.clone()),
                Some(word) if word == "at_least_once" => (Guarantee::AtLeastOnce, word.clone()),
                _ => return Err(meta.error(ARGUMENTS)),
            };
            if guarantee.is_some() {
                return Err(
                    meta.error("an effect method is `exactly_once` or `at_least_once`, not both")
                );
            }
            guarantee = Some(word);
            Ok(())
        })?;
    }
    if let Some(version) = &schema_version {
        version.base10_parse::<u32>()?;
    }
    match (effect_type, guarantee) {
        (Some(effect_type), Some((guarantee, written))) => Ok(EffectArguments {
            effect_type,
            schema_version,
            guarantee,
            written,
        }),
        (effect_type, guarantee) => {
            let mut errors = Vec::new();
            if guarantee.is_none() {
                errors.push(syn::Error::new_spanned(attribute, MISSING_GUARANTEE));
            }
            if effect_type.is_none() {
                errors.push(syn::Error::new_spanned(attribute, MISSING_TYPE));
            }
            Err(combined(errors))
        }
    }
}

fn combined(errors: Vec<syn::Error>) -> syn::Error {
    let mut errors = errors.into_iter();
    let mut first = errors
        .next()
        .unwrap_or_else(|| syn::Error::new(Span::call_site(), ARGUMENTS));
    for error in errors {
        first.combine(error);
    }
    first
}

fn request_type(signature: &Signature) -> Result<&TypePath, syn::Error> {
    let mut inputs = signature.inputs.iter();
    let receiver = inputs.next();
    let by_reference = matches!(
        receiver,
        Some(FnArg::Receiver(receiver))
            if receiver.reference.is_some() && receiver.mutability.is_none()
    );
    if !by_reference {
        let span = receiver.map_or_else(|| signature.ident.span(), Spanned::span);
        return Err(syn::Error::new(span, REQUEST_SHAPE));
    }
    let request = inputs.next();
    let Some(FnArg::Typed(request)) = request else {
        let span = request.map_or_else(|| signature.ident.span(), Spanned::span);
        return Err(syn::Error::new(span, REQUEST_SHAPE));
    };
    match &*request.ty {
        Type::Reference(reference) if reference.mutability.is_none() => match &*reference.elem {
            Type::Path(path)
                if path.qself.is_none()
                    && !names_outside_the_port(path.to_token_stream(), &signature.generics) =>
            {
                Ok(path)
            }
            _ => Err(syn::Error::new(request.span(), REQUEST_SHAPE)),
        },
        _ => Err(syn::Error::new(request.span(), REQUEST_SHAPE)),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Parameter {
    Context,
    Key,
    Handle,
    Other,
}

fn parameter(argument: &FnArg) -> Parameter {
    let FnArg::Typed(argument) = argument else {
        return Parameter::Other;
    };
    match &*argument.ty {
        Type::Reference(reference) if reference.mutability.is_none() => {
            match last_segment(&reference.elem) {
                Some(name) if name == "EffectContext" => Parameter::Context,
                Some(name) if name == "IdempotencyKey" => Parameter::Key,
                _ => Parameter::Other,
            }
        }
        other => match last_segment(other) {
            Some(name) if name == "EffectCommitHandle" => Parameter::Handle,
            _ => Parameter::Other,
        },
    }
}

fn last_segment(ty: &Type) -> Option<&Ident> {
    match ty {
        Type::Path(path) if path.qself.is_none() => {
            path.path.segments.last().map(|segment| &segment.ident)
        }
        _ => None,
    }
}

fn remaining_parameters(signature: &Signature) -> Result<Takes, syn::Error> {
    let remaining = signature
        .inputs
        .iter()
        .skip(2)
        .map(|argument| (parameter(argument), argument))
        .collect::<Vec<_>>();
    let key = remaining
        .iter()
        .position(|(parameter, _)| *parameter == Parameter::Key);
    let handle = remaining
        .iter()
        .position(|(parameter, _)| *parameter == Parameter::Handle);
    if let (Some(key), Some(handle)) = (key, handle) {
        return Err(syn::Error::new(
            remaining[key.max(handle)].1.span(),
            KEY_AND_HANDLE,
        ));
    }
    let kinds = remaining
        .iter()
        .map(|(parameter, _)| *parameter)
        .collect::<Vec<_>>();
    match kinds.as_slice() {
        [] => Ok(Takes::Nothing),
        [Parameter::Context] => Ok(Takes::Context),
        [Parameter::Key] => Ok(Takes::Key {
            key: key_type_span(remaining[0].1),
            context: false,
        }),
        [Parameter::Key, Parameter::Context] => Ok(Takes::Key {
            key: key_type_span(remaining[0].1),
            context: true,
        }),
        [Parameter::Handle] => Ok(Takes::Handle),
        _ => {
            let offending = remaining
                .iter()
                .find(|(parameter, _)| *parameter == Parameter::Other)
                .unwrap_or(&remaining[0]);
            Err(syn::Error::new(offending.1.span(), REMAINING_SHAPE))
        }
    }
}

fn key_type_span(argument: &FnArg) -> Span {
    match argument {
        FnArg::Typed(argument) => match &*argument.ty {
            Type::Reference(reference) => {
                last_segment(&reference.elem).map_or_else(|| argument.ty.span(), Ident::span)
            }
            other => other.span(),
        },
        FnArg::Receiver(receiver) => receiver.span(),
    }
}

fn generated_requests(
    effects: &TokenStream,
    port: &TokenStream,
    owner: &TokenStream,
    declared: Vec<EffectMethod<'_>>,
    errors: &mut Vec<syn::Error>,
) -> Vec<TokenStream> {
    let mut requests: Vec<(String, &Ident)> = Vec::new();
    let mut effect_types: Vec<(String, &Ident)> = Vec::new();
    let mut members = Vec::new();
    let mut generated = Vec::new();
    for method in &declared {
        let request = method.request.to_token_stream().to_string();
        if let Some((_, earlier)) = requests.iter().find(|(seen, _)| *seen == request) {
            errors.push(syn::Error::new_spanned(
                method.request,
                format!(
                    "`{}` is already the request of `{earlier}`; each request type answers one \
                     effect method",
                    request.replace(' ', "")
                ),
            ));
            continue;
        }
        let effect_type = method.effect_type.value();
        if let Some((_, earlier)) = effect_types.iter().find(|(seen, _)| *seen == effect_type) {
            errors.push(syn::Error::new_spanned(
                &method.effect_type,
                format!(
                    "`{effect_type}` already records `{earlier}`; each effect method records \
                     under its own effect type"
                ),
            ));
            continue;
        }
        requests.push((request, &method.signature.ident));
        effect_types.push((effect_type, &method.signature.ident));
        members.push(method.request);
        generated.push(generated_request(effects, port, owner, method));
    }
    let mut list = quote!(());
    for request in members.iter().rev() {
        list = quote!((#request, #list));
    }
    generated.push(quote! {
        impl #effects::PortEffects for #port {
            type Members = <#list as #effects::RequestList>::Members;
        }
    });
    generated
}

fn generated_request(
    effects: &TokenStream,
    port: &TokenStream,
    owner: &TokenStream,
    method: &EffectMethod<'_>,
) -> TokenStream {
    let request = method.request;
    let effect_type = &method.effect_type;
    let schema_version = method
        .schema_version
        .as_ref()
        .map(|version| quote!(const SCHEMA_VERSION: u32 = #version;));
    let answer = &method.answer;
    let name = &method.signature.ident;
    let awaited = method.signature.asyncness.map(|_| quote!(.await));
    let cfgs = &method.cfgs;
    let safety = match (method.guarantee, &method.takes) {
        (Guarantee::AtLeastOnce, _) => quote!(AtLeastOnce),
        (Guarantee::ExactlyOnce, Takes::Key { .. }) => quote!(RequiresKey),
        (Guarantee::ExactlyOnce, Takes::Handle) => quote!(Transactional),
        (Guarantee::ExactlyOnce, Takes::Nothing | Takes::Context) => quote!(Idempotent),
    };
    let (context, body) = match &method.takes {
        Takes::Nothing => (quote!(_), quote!(#owner::#name(port, self)#awaited)),
        Takes::Context => (quote!(ctx), quote!(#owner::#name(port, self, ctx)#awaited)),
        Takes::Key { context: false, .. } => (
            quote!(_),
            quote! {
                let key = key_of(self);
                #owner::#name(port, self, &key)#awaited
            },
        ),
        Takes::Key { context: true, .. } => (
            quote!(ctx),
            quote! {
                let key = key_of(self);
                #owner::#name(port, self, &key, ctx)#awaited
            },
        ),
        Takes::Handle => (
            quote!(ctx),
            quote! {
                let tx = ctx.transaction(self)?;
                #owner::#name(port, self, tx)#awaited
            },
        ),
    };
    let key = match &method.takes {
        Takes::Key { key, .. } => key_items(effects, request, name, *key),
        _ => TokenStream::new(),
    };
    quote! {
        #(#cfgs)*
        const _: () = {
            #key

            #[#effects::__async_trait]
            impl #effects::CallRequest for #request {
                const EFFECT_TYPE: &'static str = #effect_type;
                #schema_version
                type Safety = #effects::#safety;
                type Port = #port;
                type Outcome = <#answer as #effects::PortAnswer>::Outcome;

                async fn call(
                    &self,
                    port: &Self::Port,
                    #context: &mut #effects::EffectContext,
                ) -> ::core::result::Result<Self::Outcome, #effects::EffectError> {
                    #body
                }
            }
        };
    }
}

fn key_items(effects: &TokenStream, request: &TypePath, method: &Ident, key: Span) -> TokenStream {
    let message = LitStr::new(
        &format!("`{{Self}}` needs an idempotency key, because `{method}` takes one"),
        key,
    );
    let derived = quote_spanned!(key => Keyed::idempotency_key(request));
    quote! {
        #[diagnostic::on_unimplemented(
            message = #message,
            note = "implement `Keyed` for `{Self}`, deriving the key from the request's business identity"
        )]
        trait Keyed {
            fn idempotency_key(&self) -> #effects::IdempotencyKey;
        }

        #[diagnostic::do_not_recommend]
        impl<T: ?Sized + #effects::Keyed> Keyed for T {
            fn idempotency_key(&self) -> #effects::IdempotencyKey {
                #effects::Keyed::idempotency_key(self)
            }
        }

        fn key_of(request: &#request) -> #effects::IdempotencyKey {
            #derived
        }

        impl #effects::KeyRule<#request> for #effects::RequiresKey {
            fn key(request: &#request) -> ::core::option::Option<#effects::IdempotencyKey> {
                ::core::option::Option::Some(key_of(request))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expanded(item: Item) -> String {
        expand(&EffectsPath::default_path(), item).to_string()
    }

    #[test]
    fn every_port_method_is_checked_under_its_own_cfg() {
        let expanded = expanded(syn::parse_quote! {
            trait CardPort: Send + Sync {
                fn charge(&self) -> Result<Charged, EffectError>;
                #[cfg(test)]
                fn refund(&self) -> Result<Refunded, EffectError>;
            }
        });

        assert_eq!(
            expanded.matches("assert_port_answer").count(),
            2,
            "{expanded}"
        );
        assert!(expanded.contains("# [cfg (test)] const _"), "{expanded}");
    }

    #[test]
    fn a_port_below_async_trait_is_refused() {
        let expanded = expanded(syn::parse_quote! {
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
        });

        assert!(
            expanded.contains("put `#[obzenflow::port]` above `#[async_trait]`"),
            "{expanded}"
        );
    }

    #[test]
    fn a_return_type_naming_self_is_refused() {
        let expanded = expanded(syn::parse_quote! {
            trait CardPort: Send + Sync {
                fn charge(&self) -> Result<Self, EffectError> where Self: Sized;
            }
        });

        assert!(
            expanded.contains("a port method returns a concrete `Result<Outcome, EffectError>`"),
            "{expanded}"
        );
    }

    #[test]
    fn an_effect_method_generates_its_request_and_leaves_the_trait_unmarked() {
        let expanded = expanded(syn::parse_quote! {
            trait WarehousePort: Send + Sync {
                #[effect(type = "allocation.reserve_stock", exactly_once)]
                async fn reserve(
                    &self,
                    request: &StockReservation,
                ) -> Result<StockReserved, EffectError>;
            }
        });

        assert!(!expanded.contains("# [effect"), "{expanded}");
        assert!(
            expanded.contains("impl :: obzenflow :: effects :: CallRequest for StockReservation"),
            "{expanded}"
        );
        assert!(
            expanded.contains("const EFFECT_TYPE : & 'static str = \"allocation.reserve_stock\""),
            "{expanded}"
        );
        assert!(
            expanded.contains("type Safety = :: obzenflow :: effects :: Idempotent"),
            "{expanded}"
        );
        assert!(
            expanded.contains("WarehousePort :: reserve (port , self) . await"),
            "{expanded}"
        );
    }

    #[test]
    fn each_guarantee_and_parameter_shape_selects_its_class() {
        let expanded = expanded(syn::parse_quote! {
            trait LedgerPort: Send + Sync {
                #[effect(type = "ledger.authorize", exactly_once)]
                fn authorize(
                    &self,
                    request: &Authorize,
                    key: &IdempotencyKey,
                    ctx: &EffectContext,
                ) -> Result<Authorized, EffectError>;
                #[effect(type = "ledger.post", exactly_once, schema_version = 2)]
                fn post(
                    &self,
                    request: &Post,
                    tx: EffectCommitHandle<Posted>,
                ) -> Result<Posted, EffectError>;
                #[effect(type = "ledger.notify", at_least_once)]
                fn notify(&self, request: &Notify) -> Result<Notified, EffectError>;
            }
        });

        assert!(
            expanded.contains("type Safety = :: obzenflow :: effects :: RequiresKey"),
            "{expanded}"
        );
        assert!(
            expanded.contains("LedgerPort :: authorize (port , self , & key , ctx)"),
            "{expanded}"
        );
        assert!(
            expanded.contains("because `authorize` takes one"),
            "{expanded}"
        );
        assert!(
            expanded.contains("type Safety = :: obzenflow :: effects :: Transactional"),
            "{expanded}"
        );
        assert!(
            expanded.contains("let tx = ctx . transaction (self) ?"),
            "{expanded}"
        );
        assert!(
            expanded.contains("const SCHEMA_VERSION : u32 = 2"),
            "{expanded}"
        );
        assert!(
            expanded.contains("type Safety = :: obzenflow :: effects :: AtLeastOnce"),
            "{expanded}"
        );
    }

    #[test]
    fn an_effect_without_its_guarantee_or_type_is_refused() {
        let expanded = expanded(syn::parse_quote! {
            trait WarehousePort: Send + Sync {
                #[effect]
                fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError>;
            }
        });

        assert!(
            expanded.contains("`#[effect]` needs its guarantee: `exactly_once` or `at_least_once`"),
            "{expanded}"
        );
        assert!(
            expanded.contains("`#[effect]` needs `type = \\\"...\\\"`"),
            "{expanded}"
        );
    }

    #[test]
    fn a_keyed_or_committing_method_is_exactly_once() {
        let expanded = expanded(syn::parse_quote! {
            trait GatewayPort: Send + Sync {
                #[effect(type = "payment.authorize", at_least_once)]
                fn authorize(
                    &self,
                    request: &AuthorizeRequest,
                    key: &IdempotencyKey,
                ) -> Result<Authorized, EffectError>;
                #[effect(type = "ledger.post", at_least_once)]
                fn post(&self, request: &Post, tx: EffectCommitHandle<Posted>) -> Result<Posted, EffectError>;
                #[effect(type = "ledger.both", exactly_once)]
                fn both(
                    &self,
                    request: &Both,
                    key: &IdempotencyKey,
                    tx: EffectCommitHandle<Posted>,
                ) -> Result<Posted, EffectError>;
            }
        });

        assert!(
            expanded.contains(
                "`authorize` takes an idempotency key, so it is `exactly_once`, not `at_least_once`"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                "`post` commits through an `EffectCommitHandle`, so it is `exactly_once`, not `at_least_once`"
            ),
            "{expanded}"
        );
        assert!(expanded.contains(KEY_AND_HANDLE), "{expanded}");
    }

    #[test]
    fn a_method_shape_outside_the_accepted_ones_is_refused() {
        let expanded = expanded(syn::parse_quote! {
            trait WarehousePort: Send + Sync {
                #[effect(type = "allocation.reserve_stock", exactly_once)]
                fn reserve(&self, request: StockReservation) -> Result<StockReserved, EffectError>;
                #[effect(type = "allocation.release_stock", exactly_once)]
                fn release(&self, request: &StockRelease, count: u32) -> Result<StockReleased, EffectError>;
            }
        });

        assert!(expanded.contains(REQUEST_SHAPE), "{expanded}");
        assert!(
            expanded.contains("after its request, an effect method takes nothing"),
            "{expanded}"
        );
    }

    #[test]
    fn a_request_or_effect_type_written_twice_is_refused() {
        let expanded = expanded(syn::parse_quote! {
            trait WarehousePort: Send + Sync {
                #[effect(type = "allocation.reserve_stock", exactly_once)]
                fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError>;
                #[effect(type = "allocation.reserve_again", exactly_once)]
                fn reserve_again(&self, request: &StockReservation) -> Result<StockReserved, EffectError>;
                #[effect(type = "allocation.reserve_stock", exactly_once)]
                fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError>;
            }
        });

        assert!(
            expanded.contains(
                "`StockReservation` is already the request of `reserve`; each request type answers one effect method"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                "`allocation.reserve_stock` already records `reserve`; each effect method records under its own effect type"
            ),
            "{expanded}"
        );
    }

    #[test]
    fn a_struct_port_is_marked_on_its_impl_block() {
        let expanded = expanded(syn::parse_quote! {
            impl CallCounter {
                fn new() -> Self {
                    Self
                }

                #[effect(type = "effect_replay.counting", exactly_once)]
                fn count(&self, request: &Counting) -> Result<Counted, EffectError> {
                    Ok(Counted)
                }
            }
        });

        assert!(
            expanded.contains("ServesPort < CallCounter , CallCounter >"),
            "{expanded}"
        );
        assert!(expanded.contains("type Port = CallCounter"), "{expanded}");
        assert!(
            expanded.contains("CallCounter :: count (port , self)"),
            "{expanded}"
        );
        assert!(!expanded.contains("# [effect"), "{expanded}");
    }

    #[test]
    fn each_port_lists_its_requests_in_method_order() {
        let on_trait = expanded(syn::parse_quote! {
            trait WarehousePort: Send + Sync {
                #[effect(type = "allocation.reserve_stock", exactly_once)]
                fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError>;
                fn audit(&self) -> Result<Audited, EffectError>;
                #[effect(type = "allocation.release_stock", exactly_once)]
                fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError>;
            }
        });
        let on_struct = expanded(syn::parse_quote! {
            impl CallCounter {
                #[effect(type = "effect_replay.counting", exactly_once)]
                fn count(&self, request: &Counting) -> Result<Counted, EffectError> {
                    Ok(Counted)
                }
            }
        });
        let effect_free = expanded(syn::parse_quote! {
            trait LedgerPort: Send + Sync {}
        });

        assert!(
            on_trait.contains(
                "impl :: obzenflow :: effects :: PortEffects for dyn WarehousePort { type Members = \
                 < (StockReservation , (StockRelease , ())) as"
            ),
            "{on_trait}"
        );
        assert!(
            on_struct.contains(
                "impl :: obzenflow :: effects :: PortEffects for CallCounter { type Members = \
                 < (Counting , ()) as"
            ),
            "{on_struct}"
        );
        assert!(
            effect_free.contains(
                "impl :: obzenflow :: effects :: PortEffects for dyn LedgerPort { type Members = \
                 < () as"
            ),
            "{effect_free}"
        );
    }

    #[test]
    fn a_struct_trait_impl_or_generic_block_is_refused() {
        let on_struct = expanded(syn::parse_quote! {
            struct CallCounter;
        });
        let on_trait_impl = expanded(syn::parse_quote! {
            impl CardPort for Card {}
        });
        let on_generic = expanded(syn::parse_quote! {
            impl<T> Counter<T> {}
        });

        assert!(
            on_struct
                .contains("move it to the `impl` block that holds the struct's effect methods"),
            "{on_struct}"
        );
        assert!(
            on_trait_impl
                .contains("a trait's implementation answers the effects its trait declares"),
            "{on_trait_impl}"
        );
        assert!(
            on_generic.contains("a port struct takes no generic parameters"),
            "{on_generic}"
        );
    }
}
