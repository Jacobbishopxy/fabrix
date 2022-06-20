//! Indexes
//!
//! MongoDB indexes

use bson::doc;
use mongodb::{options::IndexOptions as MongoIndexOptions, IndexModel as MongoIndexModel};

const INDEXES_PREFIX: &str = "idx";

#[derive(Debug, Clone)]
pub enum Dir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct SingleIndex {
    pub key: (String, Dir),
    pub unique: bool,
    pub text: bool,
}

impl SingleIndex {
    pub fn new(key: (String, Dir), unique: bool, text: bool) -> Self {
        SingleIndex { key, unique, text }
    }
}

#[derive(Debug, Clone)]
pub struct SingleIndexOptions(pub Vec<SingleIndex>);

#[derive(Debug, Clone)]
pub struct CompoundIndexOptions {
    pub keys: Vec<(String, Dir)>,
    pub unique: bool,
    pub text: bool,
}

impl CompoundIndexOptions {
    pub fn new(keys: Vec<(String, Dir)>, unique: bool, text: bool) -> Self {
        CompoundIndexOptions { keys, unique, text }
    }
}

/// index options represent indexes in a collection, the default `_id` index is not included.
#[derive(Debug)]
pub enum IndexOptions {
    Single(SingleIndexOptions),
    Compound(CompoundIndexOptions),
    None,
}

/// Turn `IndexOptions` into `Vec<mongodb::MongoIndexModel>`.
/// Both single-index and compound-index are named in `MongoIndexOptions`.
impl IndexOptions {
    pub(crate) fn generate_mongo_index_module(&self) -> Vec<MongoIndexModel> {
        match self {
            IndexOptions::Single(s) => {
                s.0.iter()
                    .map(|si| {
                        let name = si.key.0.to_owned();
                        let dir: i32 = match si.key.1 {
                            Dir::Asc => 1,
                            Dir::Desc => -1,
                        };
                        let unique = si.unique;
                        // let text = si.text;

                        let mio = MongoIndexOptions::builder()
                            .name(format!("_{}_{}", INDEXES_PREFIX, name))
                            .unique(unique)
                            .build();
                        MongoIndexModel::builder()
                            .keys(doc! { name : dir })
                            .options(mio)
                            .build()
                    })
                    .collect()
            }
            IndexOptions::Compound(c) => {
                let unique = c.unique;
                // let text = c.text;

                let mut indexes_name = String::new();
                let keys = c.keys.iter().fold(doc! {}, |mut acc, (name, dir)| {
                    indexes_name.push_str(name);
                    indexes_name.push('_');
                    let dir: i32 = match dir {
                        Dir::Asc => 1,
                        Dir::Desc => -1,
                    };
                    acc.extend(doc! { name.to_owned() : dir });
                    acc
                });

                let mio = MongoIndexOptions::builder()
                    .name(format!("_{}_{}", INDEXES_PREFIX, indexes_name))
                    .unique(unique)
                    .build();
                let im = MongoIndexModel::builder().keys(keys).options(mio).build();

                vec![im]
            }
            IndexOptions::None => vec![],
        }
    }
}
