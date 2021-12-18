//! Fabrix SqlBuilder ADT

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{FieldInfo, Series, SqlError, SqlResult, Value, ValueType};

/// Table Schema
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub dtype: ValueType,
    pub is_nullable: bool,
}

/// order type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum OrderType {
    Asc,
    Desc,
}

/// an order contains a column name and it's order type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Order {
    pub name: String,
    pub order: Option<OrderType>,
}

/// index with its' unique name, table belonged, and related index/ indices
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<Order>,
}

/// foreign key direction
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForeignKeyDir {
    pub table: String,
    pub column: String,
}

/// foreign key action
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
    SetDefault,
}

impl Default for ForeignKeyAction {
    fn default() -> Self {
        ForeignKeyAction::NoAction
    }
}

/// foreign key with its' unique name, from & to table relations, and actions
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForeignKey {
    pub name: String,
    pub from: ForeignKeyDir,
    pub to: ForeignKeyDir,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]

pub struct NameAlias {
    pub from: String,
    pub to: String,
}

/// column name, can be alias. used it in `select`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum ColumnAlias {
    Simple(String),
    Alias(NameAlias),
}

impl ColumnAlias {
    pub fn original_name(&self) -> String {
        match self {
            ColumnAlias::Simple(s) => s.to_owned(),
            ColumnAlias::Alias(s) => s.from.to_owned(),
        }
    }

    pub fn name(&self) -> String {
        match self {
            ColumnAlias::Simple(s) => s.to_owned(),
            ColumnAlias::Alias(s) => s.to.to_owned(),
        }
    }
}

impl From<&str> for ColumnAlias {
    fn from(s: &str) -> Self {
        ColumnAlias::Simple(s.to_owned())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Conjunction {
    AND,
    OR,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Equation {
    Not,
    Equal(Value),
    NotEqual(Value),
    Greater(Value),
    GreaterEqual(Value),
    Less(Value),
    LessEqual(Value),
    In(Vec<Value>),
    Between((Value, Value)),
    Like(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Condition {
    pub column: String,
    pub equation: Equation,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Expression {
    Conjunction(Conjunction),
    Simple(Condition),
    Nest(Vec<Expression>),
}

/// Expression Statement
pub struct ExprStatement(Vec<Expression>);

// TODO: expression builder ... legitimate construction processing
impl ExprStatement {
    pub fn builder() -> Vec<Expression> {
        todo!()
    }
}

/// Select statement
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Select {
    pub table: String,
    pub columns: Vec<ColumnAlias>,
    pub filter: Option<Vec<Expression>>,
    pub order: Option<Vec<Order>>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// TODO: methods required: from_json, from_json_string
impl Select {
    pub fn new(table: String) -> Self {
        Select {
            table,
            columns: vec![],
            filter: None,
            order: None,
            limit: None,
            offset: None,
        }
    }

    pub fn columns_name(&self, alias: bool) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| if alias { c.name() } else { c.original_name() })
            .collect_vec()
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Delete {
    pub table: String,
    pub filter: Vec<Expression>,
}

// TODO: methods required: from_json, from_json_string
impl Delete {
    pub fn new(table: String) -> Self {
        Delete {
            table,
            filter: Vec::new(),
        }
    }
}

/// saving strategy for `save` function
/// Variants:
/// - `FailIfExists`: if table exists, do nothing
/// - `Replace`: drop if exists, create new table
/// - `Append`: ignore primary key, append to an existing table
/// - `Upsert`: if table exists: insert if id not exists, update if id exists
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SaveStrategy {
    FailIfExists,
    Replace,
    Append,
    Upsert,
}

/// index type is used for defining Sql column type
#[derive(Debug, Clone)]
pub enum IndexType {
    Int,
    BigInt,
    Uuid,
}

impl From<&str> for IndexType {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "int" | "i" => IndexType::Int,
            "bigint" | "b" => IndexType::BigInt,
            "uuid" | "u" => IndexType::Uuid,
            _ => IndexType::Int,
        }
    }
}

impl<'a> TryFrom<&'a FieldInfo> for IndexOption<'a> {
    type Error = SqlError;

    fn try_from(value: &'a FieldInfo) -> Result<Self, Self::Error> {
        let dtype = value.dtype();
        let index_type = match dtype {
            ValueType::U8 => Ok(IndexType::Int),
            ValueType::U16 => Ok(IndexType::Int),
            ValueType::U32 => Ok(IndexType::Int),
            ValueType::U64 => Ok(IndexType::BigInt),
            ValueType::I8 => Ok(IndexType::Int),
            ValueType::I16 => Ok(IndexType::Int),
            ValueType::I32 => Ok(IndexType::Int),
            ValueType::I64 => Ok(IndexType::BigInt),
            ValueType::Uuid => Ok(IndexType::Uuid),
            _ => Err(SqlError::new_common_error(format!(
                "{:?} cannot convert to index type",
                dtype
            ))),
        }?;

        Ok(IndexOption {
            name: value.name(),
            index_type,
        })
    }
}

/// index option
///
/// Only be used in `create_table`
#[derive(Debug, Clone)]
pub struct IndexOption<'a> {
    pub name: &'a str,
    pub index_type: IndexType,
}

impl<'a> IndexOption<'a> {
    pub fn new<T>(name: &'a str, index_type: T) -> Self
    where
        T: Into<IndexType>,
    {
        let index_type: IndexType = index_type.into();
        IndexOption { name, index_type }
    }

    pub fn try_from_series(series: &'a Series) -> SqlResult<Self> {
        let dtype = series.dtype();
        let index_type = match dtype {
            ValueType::U8 => Ok(IndexType::Int),
            ValueType::U16 => Ok(IndexType::Int),
            ValueType::U32 => Ok(IndexType::Int),
            ValueType::U64 => Ok(IndexType::BigInt),
            ValueType::I8 => Ok(IndexType::Int),
            ValueType::I16 => Ok(IndexType::Int),
            ValueType::I32 => Ok(IndexType::Int),
            ValueType::I64 => Ok(IndexType::BigInt),
            ValueType::Uuid => Ok(IndexType::Uuid),
            _ => Err(SqlError::new_common_error(format!(
                "{:?} is not an appropriate index type",
                dtype
            ))),
        }?;

        Ok(IndexOption {
            name: series.name(),
            index_type,
        })
    }
}

pub struct ExecutionResult {
    pub rows_affected: u64,
}

impl From<u64> for ExecutionResult {
    fn from(v: u64) -> Self {
        ExecutionResult { rows_affected: v }
    }
}
