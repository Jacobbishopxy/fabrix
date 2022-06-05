//! Fabrix SqlBuilder ADT
//!
//! 1. TableSchema
//! 1. TableConstraint
//! 1. ColumnConstraint
//! 1. ColumnIndex
//! 1. Order
//! 1. Index
//! 1. ForeignKeyDir
//! 1. ForeignKeyAction
//! 1. ForeignKey
//! 1. Function
//! 1. Column
//! 1. AlterTable
//! 1. Conjunction
//! 1. Opposition
//! 1. Equation
//! 1. Condition
//! 1. Expression
//! 1. Expressions
//! 1. InitState
//! 1. ConjunctionState
//! 1. OppositionState
//! 1. SimpleState
//! 1. NestState
//! 1. ExpressionTransit
//! 1. ExpressionsBuilder
//! 1. JoinType
//! 1. ColumnTbl
//! 1. Join
//! 1. Select
//! 1. Delete
//! 1. SaveStrategy
//! 1. IndexType
//! 1. IndexOption
//! 1. ExecutionResult

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::xpr_transit;
use crate::{FieldInfo, Series, SqlError, SqlResult, Value, ValueType};

// ================================================================================================
// Schema
// ================================================================================================

/// Table Schema
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub dtype: ValueType,
    pub is_nullable: bool,
}

impl From<FieldInfo> for TableSchema {
    fn from(fi: FieldInfo) -> Self {
        TableSchema {
            name: fi.name,
            dtype: fi.dtype,
            is_nullable: true,
        }
    }
}

// ================================================================================================
// Constraint
// ================================================================================================

/// Table constraint type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TableConstraintType {
    Check,
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey,
}

impl FromStr for TableConstraintType {
    type Err = SqlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "CHECK" => Ok(TableConstraintType::Check),
            "NOT NULL" => Ok(TableConstraintType::NotNull),
            "UNIQUE" => Ok(TableConstraintType::Unique),
            "PRIMARY KEY" => Ok(TableConstraintType::PrimaryKey),
            "FOREIGN KEY" => Ok(TableConstraintType::ForeignKey),
            _ => Err(SqlError::new_common_error("invalid constraint type")),
        }
    }
}

/// Table constraint
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableConstraint {
    pub constraint_name: String,
    pub constraint_type: TableConstraintType,
}

impl TableConstraint {
    pub fn new(constraint_name: String, constraint_type: TableConstraintType) -> Self {
        TableConstraint {
            constraint_name,
            constraint_type,
        }
    }
}

/// Column constraint
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnConstraint {
    pub constraint_name: String,
    pub column_name: String,
}

impl ColumnConstraint {
    pub fn new(name: String, column: String) -> Self {
        ColumnConstraint {
            constraint_name: name,
            column_name: column,
        }
    }
}

/// Column index
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnIndex {
    pub index_name: String,
    pub column_name: String,
}

impl ColumnIndex {
    pub fn new(name: String, column: String) -> Self {
        ColumnIndex {
            index_name: name,
            column_name: column,
        }
    }
}

// ================================================================================================
// Order
// ================================================================================================

/// order type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Order {
    Asc(String),
    Desc(String),
}

impl Order {
    pub fn name(&self) -> &str {
        match self {
            Order::Asc(name) => name,
            Order::Desc(name) => name,
        }
    }
}

// ================================================================================================
// Index
// ================================================================================================

/// index with its' unique name, table belonged, and related index/ indices
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<Order>,
}

// ================================================================================================
// Foreign Key
// ================================================================================================

/// foreign key direction
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDir {
    pub table: String,
    pub column: String,
}

/// foreign key action
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    pub name: String,
    pub from: ForeignKeyDir,
    pub to: ForeignKeyDir,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

// ================================================================================================
// Function
// ================================================================================================

/// Function
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Function {
    #[serde(rename = "alias")]
    Alias(String),
    #[serde(rename = "max")]
    Max,
    #[serde(rename = "min")]
    Min,
    #[serde(rename = "sum")]
    Sum,
    #[serde(rename = "avg")]
    Avg,
    #[serde(rename = "abs")]
    Abs,
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "ifnull")]
    IfNull(String),
    #[serde(rename = "cast")]
    Cast(String),
    #[serde(rename = "coalesce")]
    Coalesce(Vec<String>),
    #[serde(rename = "charlen")]
    CharLength,
    #[serde(rename = "lower")]
    Lower,
    #[serde(rename = "upper")]
    Upper,
}

// ================================================================================================
// Column
// ================================================================================================

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum Column {
    Col(String),
    ColFn(String, Function),
    Tbl(String, String),
}

impl Column {
    pub fn col<C: Into<String>>(column: C) -> Self {
        Column::Col(column.into())
    }

    pub fn col_fn<C: Into<String>>(column: C, function: Function) -> Self {
        Column::ColFn(column.into(), function)
    }

    pub fn tbl<C: Into<String>>(table: C, column: C) -> Self {
        Column::Tbl(table.into(), column.into())
    }

    pub fn name(&self) -> &str {
        match self {
            Column::Col(name) => name,
            Column::ColFn(name, _) => name,
            Column::Tbl(_, column) => column,
        }
    }

    pub fn name_display(&self) -> String {
        match self {
            Column::Col(name) => name.to_string(),
            // TODO: better display, e.g. `max(col)`
            Column::ColFn(name, function) => format!("{}({:?})", name, function),
            Column::Tbl(table, column) => format!("{}.{}", table, column),
        }
    }

    pub fn table(&self) -> Option<&str> {
        match self {
            Column::Col(_) => None,
            Column::ColFn(_, _) => None,
            Column::Tbl(table, _) => Some(table),
        }
    }

    pub fn function(&self) -> Option<&Function> {
        if let Column::ColFn(_, function) = self {
            Some(function)
        } else {
            None
        }
    }

    pub fn has_func(&self) -> bool {
        matches!(self, Column::ColFn(_, _))
    }
}

impl From<&str> for Column {
    fn from(s: &str) -> Self {
        Column::col(s)
    }
}

impl From<(&str, Function)> for Column {
    fn from((s, f): (&str, Function)) -> Self {
        Column::col_fn(s, f)
    }
}

impl From<(&str, &str)> for Column {
    fn from((t, c): (&str, &str)) -> Self {
        Column::tbl(t, c)
    }
}

// ================================================================================================
// AlterTable
// ================================================================================================

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AlterTable {
    Add {
        table: String,
        column: String,
        dtype: ValueType,
        is_nullable: bool,
    },
    Delete {
        table: String,
        column: String,
    },
    Modify {
        table: String,
        column: String,
        dtype: ValueType,
        is_nullable: bool,
    },
}

// ================================================================================================
// Expression & Expressions (filter)
// ================================================================================================

/// Conjunction: And/Or
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Conjunction {
    #[serde(rename = "and")]
    AND,
    #[serde(rename = "or")]
    OR,
}

/// Opposition: Not
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Opposition {
    #[serde(rename = "not")]
    NOT,
}

/// Equation
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Equation {
    #[serde(rename = "=")]
    Equal(Value),
    #[serde(rename = "!=")]
    NotEqual(Value),
    #[serde(rename = ">")]
    Greater(Value),
    #[serde(rename = ">=")]
    GreaterEqual(Value),
    #[serde(rename = "<")]
    Less(Value),
    #[serde(rename = "<=")]
    LessEqual(Value),
    #[serde(rename = "in")]
    In(Vec<Value>),
    #[serde(rename = "between")]
    Between((Value, Value)),
    #[serde(rename = "%")]
    Like(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Condition {
    #[serde(flatten)]
    pub column: String,
    #[serde(flatten)]
    pub equation: Equation,
}

impl Condition {
    pub fn new<C: Into<String>>(column: C, equation: Equation) -> Self {
        Condition {
            column: column.into(),
            equation,
        }
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn equation(&self) -> &Equation {
        &self.equation
    }
}

/// Expression
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub(crate) enum Expression {
    Conjunction(Conjunction),
    Opposition(Opposition),
    Simple(Condition),
    Nest(Vec<Expression>),
}

impl From<Expressions> for Expression {
    fn from(v: Expressions) -> Self {
        Expression::Nest(v.0)
    }
}

impl From<Opposition> for Expression {
    fn from(v: Opposition) -> Self {
        Expression::Opposition(v)
    }
}

impl From<Conjunction> for Expression {
    fn from(c: Conjunction) -> Self {
        Expression::Conjunction(c)
    }
}

impl From<Condition> for Expression {
    fn from(c: Condition) -> Self {
        Expression::Simple(c)
    }
}

/// Expressions
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(transparent)]
pub struct Expressions(pub(crate) Vec<Expression>);

// ================================================================================================
// Expression builder
// A finite state machine used for building expressions
// ================================================================================================

// Init
pub struct InitState;

// AND/OR
pub struct ConjunctionState {
    stack: Vec<Expression>,
}

// NOT
pub struct OppositionState {
    stack: Vec<Expression>,
}

// Simple expression
pub struct SimpleState {
    stack: Vec<Expression>,
}

// Nested expression
pub struct NestState {
    stack: Vec<Expression>,
}

impl From<Condition> for SimpleState {
    fn from(c: Condition) -> Self {
        SimpleState {
            stack: vec![Expression::Simple(c)],
        }
    }
}

impl From<Expressions> for NestState {
    fn from(val: Expressions) -> Self {
        NestState {
            stack: vec![Expression::Nest(val.0)],
        }
    }
}

// Trait represents transition from one state to another
pub trait ExpressionTransit<T, S> {
    fn append(self, state: T) -> S;

    fn finish(self) -> Expressions;
}

impl ExpressionTransit<Condition, SimpleState> for InitState {
    fn append(self, state: Condition) -> SimpleState {
        SimpleState {
            stack: vec![Expression::Simple(state)],
        }
    }

    fn finish(self) -> Expressions {
        Expressions::default()
    }
}

impl ExpressionTransit<Expressions, NestState> for InitState {
    fn append(self, state: Expressions) -> NestState {
        NestState {
            stack: vec![Expression::Nest(state.0)],
        }
    }

    fn finish(self) -> Expressions {
        Expressions::default()
    }
}

impl ExpressionTransit<Opposition, OppositionState> for InitState {
    fn append(self, state: Opposition) -> OppositionState {
        OppositionState {
            stack: vec![Expression::Opposition(state)],
        }
    }

    fn finish(self) -> Expressions {
        Expressions::default()
    }
}

// Simple -> Conjunction
xpr_transit!(Conjunction, SimpleState => ConjunctionState);

// Nest -> Conjunction
xpr_transit!(Conjunction, NestState => ConjunctionState);

// Opposite -> Simple
xpr_transit!(Condition, OppositionState => SimpleState);

// Opposite -> Nest
xpr_transit!(Expressions, OppositionState => NestState);

// Conjunction -> Opposition
xpr_transit!(Opposition, ConjunctionState => OppositionState);

// Conjunction -> Simple
xpr_transit!(Condition, ConjunctionState => SimpleState);

// Conjunction -> Nest
xpr_transit!(Expressions, ConjunctionState => NestState);

/// Expressions builder
///
/// Build a legal expression
pub struct ExpressionsBuilder;

impl ExpressionsBuilder {
    pub fn init() -> InitState {
        InitState
    }

    pub fn append<T, S>(self, value: T) -> S
    where
        T: Into<S>,
    {
        value.into()
    }
}

// ================================================================================================
// Join
// ================================================================================================

/// JoinType
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum JoinType {
    Join,
    Inner,
    Left,
    Right,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnTbl(pub String, pub String);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub join_type: JoinType,
    pub on: Vec<(ColumnTbl, ColumnTbl)>,
}

impl Join {
    pub fn new(join_type: JoinType, on: Vec<(ColumnTbl, ColumnTbl)>) -> SqlResult<Self> {
        if on.is_empty() {
            return Err(SqlError::new_common_error("on cannot be empty"));
        }
        Ok(Join { join_type, on })
    }

    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    pub fn left_table(&self) -> &str {
        &self.on[0].0 .0
    }

    pub fn right_table(&self) -> &str {
        &self.on[0].0 .1
    }

    pub fn on(&self) -> &[(ColumnTbl, ColumnTbl)] {
        &self.on
    }

    pub fn is_valid(&self) -> bool {
        !self.on.is_empty()
    }
}

// ================================================================================================
// Select
// ================================================================================================

/// Select statement
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Select {
    pub table: String,
    pub columns: Vec<Column>,
    pub filter: Option<Expressions>,
    pub order: Option<Vec<Order>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub join: Option<Join>,
    pub include_primary_key: Option<bool>,
}

impl Select {
    pub fn new<T: Into<String>>(table: T) -> Self {
        Select {
            table: table.into(),
            columns: vec![],
            filter: None,
            order: None,
            limit: None,
            offset: None,
            join: None,
            include_primary_key: None,
        }
    }

    pub fn get_table(&self) -> &str {
        &self.table
    }

    pub fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn get_filter(&self) -> Option<&Expressions> {
        self.filter.as_ref()
    }

    pub fn get_order(&self) -> Option<&[Order]> {
        self.order.as_deref()
    }

    pub fn get_limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn get_offset(&self) -> Option<usize> {
        self.offset
    }

    pub fn get_join(&self) -> Option<&Join> {
        self.join.as_ref()
    }

    pub fn get_include_primary_key(&self) -> Option<bool> {
        self.include_primary_key
    }

    pub fn columns_name(&self) -> Vec<String> {
        self.columns.iter().map(Column::name_display).collect()
    }

    pub fn columns<C>(mut self, columns: &[C]) -> Self
    where
        Column: From<C>,
        C: Clone,
    {
        self.columns
            .extend(columns.iter().map(|c| (c.clone()).into()));
        self
    }

    pub fn filter(mut self, filter: &Expressions) -> Self {
        self.filter = Some(filter.to_owned());
        self
    }

    pub fn order(mut self, order: &[Order]) -> Self {
        self.order = Some(order.to_owned());
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn join(mut self, join: Join) -> Self {
        self.join = Some(join);
        self
    }

    pub fn include_primary_key(mut self, include: bool) -> Self {
        self.include_primary_key = Some(include);
        self
    }
}

// ================================================================================================
// Delete
// ================================================================================================

/// Delete statement
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Delete {
    pub table: String,
    pub filter: Expressions,
}

impl Delete {
    pub fn new(table: String) -> Self {
        Delete {
            table,
            filter: Expressions::default(),
        }
    }

    pub fn filter(&mut self, filter: &Expressions) -> &mut Self {
        self.filter = filter.to_owned();
        self
    }
}

// ================================================================================================
// SaveStrategy
// ================================================================================================

/// saving strategy for `save` function
/// Variants:
/// - `FailIfExists`: if table exists, do nothing
/// - `Replace`: drop if exists, create new table
/// - `Append`: ignore primary key, append to an existing table; index will be ignored
/// - `Upsert`: if table exists: insert if id not exists, update if id exists; index will not be ignored
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SaveStrategy {
    FailIfExists,
    Replace,
    // Index is always ignored
    Append,
    // Index is always used
    Upsert,
}

// ================================================================================================
// IndexType & IndexOption
// ================================================================================================

/// index type is used for defining Sql column type
#[derive(Debug, Clone)]
pub enum IndexType {
    Int,
    BigInt,
    Uuid,
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::Int
    }
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

/// index option
///
/// Only be used in `create_table`
#[derive(Debug, Clone)]
pub struct IndexOption {
    pub name: String,
    pub index_type: IndexType,
}

impl Default for IndexOption {
    fn default() -> Self {
        IndexOption {
            name: "_id".to_string(),
            index_type: IndexType::default(),
        }
    }
}

impl IndexOption {
    pub fn new<N, T>(name: N, index_type: T) -> Self
    where
        N: Into<String>,
        T: Into<IndexType>,
    {
        let name = name.into();
        let index_type: IndexType = index_type.into();
        IndexOption { name, index_type }
    }

    pub fn try_from_series(series: &Series) -> SqlResult<Self> {
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
            ValueType::F32 => Ok(IndexType::Int),
            ValueType::F64 => Ok(IndexType::BigInt),
            _ => Err(SqlError::new_common_error(format!(
                "{:?} is not an appropriate index type",
                dtype
            ))),
        }?;

        Ok(IndexOption {
            name: series.name().to_owned(),
            index_type,
        })
    }
}

impl TryFrom<FieldInfo> for IndexOption {
    type Error = SqlError;

    fn try_from(value: FieldInfo) -> Result<Self, Self::Error> {
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
            ValueType::F32 => Ok(IndexType::Int),
            ValueType::F64 => Ok(IndexType::BigInt),
            _ => Err(SqlError::new_common_error(format!(
                "{:?} cannot convert to index type",
                dtype
            ))),
        }?;

        Ok(IndexOption {
            name: value.name().to_owned(),
            index_type,
        })
    }
}

// ================================================================================================
// ExecutionResult
// ================================================================================================

pub struct ExecutionResult {
    pub rows_affected: u64,
}

impl From<u64> for ExecutionResult {
    fn from(v: u64) -> Self {
        ExecutionResult { rows_affected: v }
    }
}

#[cfg(test)]
mod test_sql_adt {

    use super::*;

    #[test]
    fn build_filter() {
        let e = ExpressionsBuilder::init()
            .append(
                ExpressionsBuilder::init()
                    .append(Condition::new("name", Equation::Equal("foo".into())))
                    .append(Conjunction::AND)
                    .append(Opposition::NOT)
                    .append(Condition::new("age", Equation::Equal(10.into())))
                    .append(Conjunction::OR)
                    .append(Condition::new("age", Equation::Equal(20.into())))
                    .finish(),
            )
            .append(Conjunction::OR)
            .append(Condition::new("name", Equation::Equal("bar".into())))
            .finish();

        println!("{:?}", e);
    }

    #[test]
    fn expression_serialize() {
        let e = Expressions(vec![
            Expression::Opposition(Opposition::NOT),
            Expression::Simple(Condition::new("a", Equation::Equal(Value::I16(1)))),
            Expression::Conjunction(Conjunction::OR),
            Expression::Nest(vec![
                Expression::Simple(Condition::new("b", Equation::Equal(Value::U32(2)))),
                Expression::Conjunction(Conjunction::AND),
                Expression::Simple(Condition::new("c", Equation::Like("%foo%".into()))),
            ]),
        ]);
        let foo = serde_json::to_string(&e);
        assert!(foo.is_ok());
        let foo = foo.unwrap();
        println!("{:?}", foo);

        assert_eq!(
            foo,
            "[\"not\",{\"column\":\"a\",\"=\":1},\"or\",[{\"column\":\"b\",\"=\":2},\"and\",{\"column\":\"c\",\"%\":\"%foo%\"}]]"
        );
    }

    #[test]
    fn select_serialize() {
        let e = Expressions(vec![
            Expression::Opposition(Opposition::NOT),
            Expression::Simple(Condition::new("a", Equation::Equal(Value::I16(1)))),
            Expression::Conjunction(Conjunction::OR),
            Expression::Nest(vec![
                Expression::Simple(Condition::new("b", Equation::Equal(Value::U32(2)))),
                Expression::Conjunction(Conjunction::AND),
                Expression::Simple(Condition::new("c", Equation::Like("%foo%".into()))),
            ]),
        ]);

        let select = Select::new("test")
            .columns(&[
                Column::col("v1"),
                Column::col_fn("v2", Function::Alias("v2_ext".to_owned())),
            ])
            .filter(&e)
            .limit(10);

        let s = serde_json::to_string(&select);
        assert!(s.is_ok());
        println!("{:?}", s.unwrap());
    }
}
