// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use proc_macro2::TokenStream;
use quote::quote;
use syn::Path;

#[derive(Default)]
pub(crate) struct EffectsPath(Option<Path>);

impl EffectsPath {
    pub(crate) fn written(path: Path) -> Self {
        Self(Some(path))
    }

    pub(crate) fn tokens(&self) -> TokenStream {
        match &self.0 {
            Some(path) => quote!(#path),
            None => quote!(::obzenflow::effects),
        }
    }
}
