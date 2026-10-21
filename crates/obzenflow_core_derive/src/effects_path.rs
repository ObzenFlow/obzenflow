// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Path, Token};

pub(crate) struct EffectsPath(Option<Path>);

impl Parse for EffectsPath {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self(None));
        }
        let key: Ident = input.parse()?;
        if key != "effects" {
            return Err(syn::Error::new(key.span(), "expected `effects = <path>`"));
        }
        input.parse::<Token![=]>()?;
        let path: Path = input.parse()?;
        if !input.is_empty() {
            return Err(input.error("unexpected tokens after the effects path"));
        }
        Ok(Self(Some(path)))
    }
}

impl EffectsPath {
    pub(crate) fn default_path() -> Self {
        Self(None)
    }

    pub(crate) fn tokens(&self) -> TokenStream {
        match &self.0 {
            Some(path) => quote!(#path),
            None => quote!(::obzenflow::effects),
        }
    }
}
