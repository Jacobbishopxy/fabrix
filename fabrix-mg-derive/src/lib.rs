//! CRUD derive macro
//!
//! This macro generates CRUD methods for a given struct.
//!
//! Featured functions catalogue:
//! - `get_field_id`
//! - `get_attr_id`
//! - `single_index_format`
//! - `compound_index_format`
//!
//! These functions are used to find out tagged fields or attributes in the compiling time,
//! and the results of these functions are used in proc-macro's token streams (see function
//! `impl_crud`).

use syn::{parse_macro_input, DeriveInput};

mod dr;
mod indexes;

use dr::*;

/// macro for CRUD derive
#[proc_macro_derive(CRUD, attributes(crud))]
pub fn derive_crud(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    let stream = impl_crud(&input);

    // Debug use:
    // println!("{}", &stream);

    proc_macro::TokenStream::from(stream)
}
