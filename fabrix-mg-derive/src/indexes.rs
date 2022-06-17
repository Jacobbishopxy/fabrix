//！ Indexes
//！
//！ Single

use std::str::FromStr;

use quote::{quote, ToTokens};

pub const ASC: &str = "asc";
pub const DESC: &str = "desc";
pub const UNIQUE: &str = "unique";
pub const TEXT: &str = "text";

/// Index direction
#[derive(Debug, Clone)]
pub enum Dir {
    Asc,
    Desc,
}

impl Default for Dir {
    fn default() -> Self {
        Dir::Asc
    }
}

/// `crud_derive::Dir` -> `crud::Dir`
impl ToTokens for Dir {
    // since `crud_derive::Dir` is not a public API (cannot be exported in a proc-macro crate),
    // we need to convert it to a public API (defined in `crud` crate).
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(match self {
            Dir::Asc => quote! { crud::Dir::Asc },
            Dir::Desc => quote! { crud::Dir::Desc },
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct KeyPair(pub String, pub Dir);

impl ToTokens for KeyPair {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = &self.0;
        let dir = &self.1;
        tokens.extend(quote! {
            (
                #name.to_owned(),
                #dir.clone(),
            )
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommonOption {
    pub dir: Dir,
    pub unique: bool,
    pub text: bool,
}

impl FromStr for CommonOption {
    type Err = ();

    // ignore any string who does not match the `IndexOptions` fields' format
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut options = CommonOption::default();
        s.split(',').for_each(|i| match i {
            ASC => options.dir = Dir::Asc,
            DESC => options.dir = Dir::Desc,
            UNIQUE => options.unique = true,
            TEXT => options.text = true,
            _ => {}
        });
        Ok(options)
    }
}

/// Index options (MongoDB)
#[derive(Debug, Clone, Default)]
pub struct SingleIndex {
    pub key: KeyPair,
    pub unique: bool,
    pub text: bool,
}

impl SingleIndex {
    pub fn new_from_str(name: String, s: &str) -> Self {
        // parse from string
        let common_option = s.parse::<CommonOption>().unwrap();
        SingleIndex {
            key: KeyPair(name, common_option.dir),
            unique: common_option.unique,
            text: common_option.text,
        }
    }
}

/// new type for `Vec<crud_derive::IndexOptions>`
/// we need it because `syn::ToTokens` cannot be implemented for `Vec<_>`
#[derive(Debug, Clone)]
pub struct SingleIndexOptions(pub Vec<SingleIndex>);

/// `crud_derive::IndexOptions` -> `crud::IndexOptions`
impl ToTokens for SingleIndex {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = &self.key.0;
        let dir = &self.key.1;
        let unique = &self.unique;
        let text = &self.text;
        tokens.extend(quote! {
            crud::SingleIndex::new((#name.to_string(), #dir), #unique, #text)
        })
    }
}

/// `crud_derive::IndexOptionsCollection` -> `Vec<crud::IndexOptions>`
impl ToTokens for SingleIndexOptions {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let d = &self.0;
        tokens.extend(quote! {
            crud::IndexOptions::Single(crud::SingleIndexOptions(vec![#(#d),*]))
        })
    }
}

/// Compound index options
#[derive(Debug, Clone, Default)]
pub struct CompoundIndexOptions {
    pub keys: Vec<KeyPair>,
    pub unique: bool,
    pub text: bool,
}

impl CompoundIndexOptions {
    pub fn update_from_str(&mut self, name: String, s: &str) {
        // parse from string
        let common_option = s.parse::<CommonOption>().unwrap();
        let kp = KeyPair(name, common_option.dir);

        self.keys.push(kp);
        self.unique = common_option.unique;
        self.text = common_option.text;
    }

    pub fn add_keys(&mut self, name: String) {
        self.keys.push(KeyPair(name, Dir::default()));
    }
}

impl ToTokens for CompoundIndexOptions {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let keys = &self.keys;
        let unique = &self.unique;
        let text = &self.text;
        tokens.extend(quote! {
            crud::IndexOptions::Compound(crud::CompoundIndexOptions {
                keys: vec![#(#keys),*],
                unique: #unique,
                text: #text,
            })
        })
    }
}

#[derive(Debug, Clone)]
pub enum IndexOptions {
    Single(SingleIndexOptions),
    Compound(CompoundIndexOptions),
    None,
}

impl ToTokens for IndexOptions {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            IndexOptions::Single(v) => v.to_tokens(tokens),
            IndexOptions::Compound(v) => v.to_tokens(tokens),
            IndexOptions::None => tokens.extend(quote! { crud::IndexOptions::None }),
        }
    }
}
