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

mod indexes;

use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, token::Comma, Attribute, Data, DeriveInput, Field,
    Fields, Ident, Lit, Meta, NestedMeta,
};

use indexes::*;

const TAG: &str = "crud";
const ID: &str = "id";
const SINGLE_INDEX: &str = "single_index";
const COMPOUND_INDEX: &str = "compound_index";

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

type NamedFields = Punctuated<Field, Comma>;

/// turn ast into `Punctuated<Field, Comma>`, and filter out any type that is not a Rust struct
fn named_fields(ast: &DeriveInput) -> NamedFields {
    match &ast.data {
        Data::Struct(s) => {
            if let Fields::Named(ref named_fields) = s.fields {
                named_fields.named.clone()
            } else {
                unimplemented!("derive(Builder) only supports named fields")
            }
        }
        other => unimplemented!(
            "CRUD only supports Struct and is not implemented for {:?}",
            other
        ),
    }
}

/// find out whether a field name is `id`
///
/// ```rust,ignore
/// struct TestCrud {
///     id: Option<ID>,
///     ...
/// }
/// ```
fn get_field_id(named_fields: &NamedFields) -> Option<Ident> {
    for field in named_fields.iter() {
        if field.ident.as_ref().unwrap() == ID {
            return Some(field.ident.as_ref().unwrap().clone());
        }
    }
    None
}

/// find out a field whose attribute is `id`
///
/// ```rust,ignore
/// struct TestCrud {
///     #[crud(id)]
///     idx: Option<ID>,
///     ...
/// }
/// ```
///
/// same as:
///
/// ```rust,ignore
/// fn get_attr_id(named_fields: &NamedFields) -> Option<Ident> {
///     for field in named_fields.iter() {
///         for attr in field.attrs.iter() {
///             if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
///                 if meta_list.path.is_ident(TAG) {
///                     for nested_meta in meta_list.nested.iter() {
///                         if let NestedMeta::Meta(Meta::Path(path)) = nested_meta {
///                             if path.is_ident(ID) {
///                                 return Some(field.ident.as_ref().unwrap().clone());
///                             }
///                         }
///                     }
///                 }
///             }
///         }
///     }
///     None
/// }
/// ```
fn get_attr_id(named_fields: &NamedFields) -> Option<Ident> {
    // get the `id` sub-attribute field as an `Ident`
    let nested_meta_find_map = |field: &Field, nested_meta: &NestedMeta| match nested_meta {
        NestedMeta::Meta(Meta::Path(path)) if path.is_ident(ID) => {
            Some(field.ident.as_ref().unwrap().clone())
        }
        _ => None,
    };

    // iterate an attribute until we find the `id` sub-attribute
    let attrs_find_map = |field: &Field, attr: &Attribute| match attr.parse_meta() {
        Ok(Meta::List(meta_list)) if meta_list.path.is_ident(TAG) => meta_list
            .nested
            .iter()
            .find_map(|nested_meta| nested_meta_find_map(field, nested_meta)),
        _ => None,
    };

    // iterate all attributes until we find one that contains the `id` sub-attribute
    let field_find_map = |field: &Field| {
        field
            .attrs
            .iter()
            .find_map(|attr| attrs_find_map(field, attr))
    };

    // iterate all fields until we find a field whose attribute is `crud(id)`
    named_fields.iter().find_map(field_find_map)
}

/// find out fields whose attribute is `single_index`
///
/// ```rust,ignore
/// struct TestCrud {
///     id: Option<ID>,
///     #[crud(single_index = "unique,asc")]
///     name: String,
///     #[crud(single_index = "unique,desc,text")]
///     tag: String,
/// }
/// ```
///
/// same as:
///
/// ```rust,ignore
/// fn index_format(named_fields: &NamedFields) -> Vec<IndexOptions> {
///     let mut result = Vec::<SingleIndex>::default();
///     for field in named_fields.iter() {
///         for attr in field.attrs.iter() {
///             if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
///                 if meta_list.path.is_ident(TAG) {
///                     for nm in meta_list.nested.iter() {
///                         if let NestedMeta::Meta(Meta::NameValue(mnv)) = nm {
///                             if mnv.path.is_ident(INDEX) {
///                                 if let Lit::Str(ref s) = mnv.lit {
///                                     result.push(SingleIndex::new_from_str(
///                                         field.ident.as_ref().unwrap().to_string(),
///                                         &s.value(),
///                                     ));
///                                 }
///                             }
///                         }
///                     }
///                 }
///             }
///         }
///     }
///     result
/// }
/// ```
fn single_index_format(named_fields: &NamedFields) -> Option<SingleIndexOptions> {
    // parse single_index attribute
    let nested_meta_filter_map = |field: &Field, nested_meta: &NestedMeta| match nested_meta {
        NestedMeta::Meta(Meta::NameValue(mnv)) if mnv.path.is_ident(SINGLE_INDEX) => {
            match mnv.lit {
                Lit::Str(ref s) => Some(SingleIndex::new_from_str(
                    field.ident.as_ref().unwrap().to_string(),
                    &s.value(),
                )),
                _ => None,
            }
        }
        _ => None,
    };

    // iterate an attribute and filter out `crud(single_index = "...")`
    let attrs_fmap = |field: &Field, attr: &Attribute| match attr.parse_meta() {
        Ok(Meta::List(meta_list)) if meta_list.path.is_ident(TAG) => meta_list
            .nested
            .iter()
            .filter_map(|nested_meta| nested_meta_filter_map(field, nested_meta))
            .collect::<Vec<SingleIndex>>(),
        _ => vec![],
    };

    // iterate through all attributes in a field
    let field_fmap = |field: &Field| {
        field
            .attrs
            .iter()
            .flat_map(|attr| attrs_fmap(field, attr))
            .collect::<Vec<SingleIndex>>()
    };

    // iterate through all fields
    let single_index = named_fields
        .iter()
        .flat_map(field_fmap)
        .collect::<Vec<SingleIndex>>();

    if single_index.is_empty() {
        None
    } else {
        Some(SingleIndexOptions(single_index))
    }
}

/// find out fields whose attribute is `compound_index`
///
/// ```rust,ignore
/// struct TestCrud {
///     id: Option<ID>,
///     #[crud(compound_index = "unique,asc")]
///     name: String,
///     #[crud(compound_index)]
///     tag: String,
/// }
/// ```
///
/// same as:
///
/// ```rust,ignore
/// fn compound_index_format(named_fields: &NamedFields) -> Option<CompoundIndexOptions> {
///     let mut result = CompoundIndexOptions::default();
///     for field in named_fields.iter() {
///         for attr in field.attrs.iter() {
///             if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
///                 if meta_list.path.is_ident(TAG) {
///                     for nested_meta in meta_list.nested.iter() {
///                         match nested_meta {
///                             NestedMeta::Meta(Meta::NameValue(mnv))
///                                 if mnv.path.is_ident(COMPOUND_INDEX) =>
///                             {
///                                 if let Lit::Str(ref s) = mnv.lit {
///                                     result.update_from_str(
///                                         field.ident.as_ref().unwrap().to_string(),
///                                         &s.value(),
///                                     );
///                                 }
///                             }
///                             NestedMeta::Meta(Meta::Path(mp)) if mp.is_ident(COMPOUND_INDEX) => {
///                                 result.add_keys(field.ident.as_ref().unwrap().to_string());
///                             }
///                             _ => {}
///                         }
///                     }
///                 }
///             }
///         }
///     }
///
///     if result.keys.is_empty() {
///         None
///     } else {
///         Some(result)
///     }
/// }
/// ```
fn compound_index_format(named_fields: &NamedFields) -> Option<CompoundIndexOptions> {
    // parse compound_index attribute
    let nested_meta_fold = |mut result: CompoundIndexOptions,
                            field: &Field,
                            nested_meta: &NestedMeta| {
        match nested_meta {
            // - `crud(compound_index = "...")`
            NestedMeta::Meta(Meta::NameValue(mnv)) if mnv.path.is_ident(COMPOUND_INDEX) => {
                if let Lit::Str(ref s) = mnv.lit {
                    result.update_from_str(field.ident.as_ref().unwrap().to_string(), &s.value());
                }
            }
            // - `crud(compound_index)`
            NestedMeta::Meta(Meta::Path(mp)) if mp.is_ident(COMPOUND_INDEX) => {
                result.add_keys(field.ident.as_ref().unwrap().to_string());
            }
            _ => {}
        }

        result
    };

    // iterate an attribute and filter out `crud(compound_index = "...")`
    let attrs_fold = |result: CompoundIndexOptions, field: &Field, attr: &Attribute| {
        if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
            if meta_list.path.is_ident(TAG) {
                return meta_list
                    .nested
                    .iter()
                    .fold(result, |cio, nm| nested_meta_fold(cio, field, nm));
            }
        }

        result
    };

    // iterate through all attributes in a field
    let field_fold = |result: CompoundIndexOptions, field: &Field| {
        field
            .attrs
            .iter()
            .fold(result, |cio, attr| attrs_fold(cio, field, attr))
    };

    // iterate through all fields
    let compound_index = named_fields
        .iter()
        .fold(CompoundIndexOptions::default(), field_fold);

    if compound_index.keys.is_empty() {
        None
    } else {
        Some(compound_index)
    }
}

/// get `IndexOptions` from a struct
fn index_format(named_fields: &NamedFields) -> IndexOptions {
    let single_index = single_index_format(named_fields);
    let compound_index = compound_index_format(named_fields);

    match (single_index, compound_index) {
        (Some(si), None) => IndexOptions::Single(si),
        (None, Some(ci)) => IndexOptions::Compound(ci),
        (Some(_), Some(_)) => {
            panic!("single_index & compound_index are currently not allowed at the same time")
        }
        _ => IndexOptions::None,
    }
}

/// main process of handling derive stream
fn impl_crud(input: &DeriveInput) -> proc_macro2::TokenStream {
    // name of the struct
    let name = input.ident.clone();
    let named_fields = named_fields(input);
    // index options of the struct
    let io = index_format(&named_fields);

    // get ID either from field `id` or field whose attribute is `id`
    let id = match (get_field_id(&named_fields), get_attr_id(&named_fields)) {
        (Some(id), _) => id,
        (None, Some(oid)) => oid,
        _ => panic!("No `id` field nor `oid` attribute were found!"),
    };

    let expanded = quote! {
        // impl `BaseCRUD`
        impl BaseCRUD for #name {
            fn get_id(&self) -> ::std::option::Option<bson::oid::ObjectId> {
                self.#id
            }

            fn remove_id(&mut self) {
                self.#id = None;
            }

            fn mutate_id(&mut self, oid: bson::oid::ObjectId) -> anyhow::Result<()> {
                self.#id = Some(oid);
                Ok(())
            }

            fn show_indexes() -> crud::IndexOptions {
                #io
            }
        }

        // impl `MongoCRUD`
        #[async_trait::async_trait]
        impl MongoCRUD<#name> for crud::MongoClient {}
    };

    expanded
}
