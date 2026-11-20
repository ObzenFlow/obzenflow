// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use proc_macro2::TokenStream;
use quote::quote;
use syn::Path;

pub(crate) enum EffectsPath {
    Facade,
    Runtime,
    Written(Path),
}

impl EffectsPath {
    pub(crate) fn tokens(&self) -> TokenStream {
        match self {
            Self::Facade => quote!(::obzenflow::effects),
            Self::Runtime => quote!(::obzenflow_runtime::effects),
            Self::Written(path) => quote!(#path),
        }
    }
}
