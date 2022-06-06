//! Sql builder

use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use sea_query::{
    Cond, ConditionExpression, DeleteStatement, Expr, Func, JoinType as SJoinType, SelectStatement,
    Value as SValue,
};

use super::{alias, sql_adt, sv_2_v};
use crate::core::Value2ChronoHelper;
use crate::{Decimal, SqlError, SqlResult, Uuid, Value, ValueType};

// ================================================================================================
// SqlBuilder
// ================================================================================================

/// SqlBuilder
///
/// Used to categorize database types
#[derive(Debug, Clone)]
pub enum SqlBuilder {
    Mysql,
    Postgres,
    Sqlite,
}

impl std::fmt::Display for SqlBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mysql => write!(f, "mysql"),
            Self::Postgres => write!(f, "postgres"),
            Self::Sqlite => write!(f, "sqlite"),
        }
    }
}

impl FromStr for SqlBuilder {
    type Err = SqlError;

    fn from_str(s: &str) -> SqlResult<Self> {
        match s {
            "mysql" | "m" => Ok(SqlBuilder::Mysql),
            "postgres" | "p" => Ok(SqlBuilder::Postgres),
            "sqlite" | "s" => Ok(SqlBuilder::Sqlite),
            _ => Err(SqlError::new_common_error(format!(
                "{} is not a valid sql builder",
                s
            ))),
        }
    }
}

/// Don't use it in general type conversion, use `try_from_value_to_svalue` instead
impl From<Value> for SValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Bool(v) => SValue::Bool(Some(v)),
            Value::U8(v) => SValue::TinyUnsigned(Some(v)),
            Value::U16(v) => SValue::SmallUnsigned(Some(v)),
            Value::U32(v) => SValue::Unsigned(Some(v)),
            Value::U64(v) => SValue::BigUnsigned(Some(v)),
            Value::I8(v) => SValue::TinyInt(Some(v)),
            Value::I16(v) => SValue::SmallInt(Some(v)),
            Value::I32(v) => SValue::Int(Some(v)),
            Value::I64(v) => SValue::BigInt(Some(v)),
            Value::F32(v) => SValue::Float(Some(v)),
            Value::F64(v) => SValue::Double(Some(v)),
            Value::String(v) => SValue::String(Some(Box::new(v))),
            v @ Value::Date(..) => SValue::ChronoDate(Some(Box::new(
                Value2ChronoHelper::convert_value_to_naive_date(v).unwrap(),
            ))),
            v @ Value::Time(..) => SValue::ChronoTime(Some(Box::new(
                Value2ChronoHelper::convert_value_to_naive_time(v).unwrap(),
            ))),
            v @ Value::DateTime(..) => SValue::ChronoDateTime(Some(Box::new(
                Value2ChronoHelper::convert_value_to_naive_datetime(v).unwrap(),
            ))),
            Value::Decimal(v) => SValue::Decimal(Some(Box::new(v.0))),
            Value::Uuid(v) => SValue::Uuid(Some(Box::new(v.0))),
            Value::Bytes(v) => SValue::Bytes(Some(Box::new(v.0))),
            // Temporary workaround
            Value::Null => SValue::Bool(None),
        }
    }
}

impl From<&Value> for SValue {
    fn from(v: &Value) -> Self {
        v.clone().into()
    }
}

/// Type conversion: from polars DataType to SeqQuery Value
fn from_data_type_to_null_svalue(dtype: &ValueType) -> SValue {
    match dtype {
        ValueType::Bool => SValue::Bool(None),
        ValueType::U8 => SValue::TinyUnsigned(None),
        ValueType::U16 => SValue::SmallUnsigned(None),
        ValueType::U32 => SValue::Unsigned(None),
        ValueType::U64 => SValue::BigUnsigned(None),
        ValueType::I8 => SValue::TinyInt(None),
        ValueType::I16 => SValue::SmallInt(None),
        ValueType::I32 => SValue::Int(None),
        ValueType::I64 => SValue::BigInt(None),
        ValueType::F32 => SValue::Float(None),
        ValueType::F64 => SValue::Double(None),
        ValueType::String => SValue::String(None),
        ValueType::Date => SValue::ChronoDate(None),
        ValueType::Time => SValue::ChronoTime(None),
        ValueType::DateTime => SValue::ChronoDateTime(None),
        ValueType::Decimal => SValue::Decimal(None),
        ValueType::Uuid => SValue::Uuid(None),
        ValueType::Bytes => SValue::Bytes(None),
        _ => panic!("unsupported data type conversion"),
    }
}

/// Type conversion: from Value to `sea-query` Value
pub(crate) fn try_from_value_to_svalue(
    value: Value,
    dtype: &ValueType,
    nullable: bool,
) -> SqlResult<SValue> {
    match value {
        Value::Bool(v) => Ok(SValue::Bool(Some(v))),
        Value::U8(v) => Ok(SValue::TinyUnsigned(Some(v))),
        Value::U16(v) => Ok(SValue::SmallUnsigned(Some(v))),
        Value::U32(v) => Ok(SValue::Unsigned(Some(v))),
        Value::U64(v) => Ok(SValue::BigUnsigned(Some(v))),
        Value::I8(v) => Ok(SValue::TinyInt(Some(v))),
        Value::I16(v) => Ok(SValue::SmallInt(Some(v))),
        Value::I32(v) => Ok(SValue::Int(Some(v))),
        Value::I64(v) => Ok(SValue::BigInt(Some(v))),
        Value::F32(v) => Ok(SValue::Float(Some(v))),
        Value::F64(v) => Ok(SValue::Double(Some(v))),
        Value::String(v) => Ok(SValue::String(Some(Box::new(v)))),
        v @ Value::Date(..) => Ok(SValue::ChronoDate(Some(Box::new(
            Value2ChronoHelper::convert_value_to_naive_date(v)?,
        )))),
        v @ Value::Time(..) => Ok(SValue::ChronoTime(Some(Box::new(
            Value2ChronoHelper::convert_value_to_naive_time(v)?,
        )))),
        v @ Value::DateTime(..) => Ok(SValue::ChronoDateTime(Some(Box::new(
            Value2ChronoHelper::convert_value_to_naive_datetime(v)?,
        )))),
        Value::Decimal(v) => Ok(SValue::Decimal(Some(Box::new(v.0)))),
        Value::Uuid(v) => Ok(SValue::Uuid(Some(Box::new(v.0)))),
        Value::Bytes(v) => Ok(SValue::Bytes(Some(Box::new(v.0)))),
        Value::Null => {
            if nullable {
                Ok(from_data_type_to_null_svalue(dtype))
            } else {
                Err(SqlError::new_common_error(format!(
                    "convert {:?} to {:?} error",
                    value, dtype
                )))
            }
        }
    }
}

#[allow(dead_code)]
/// Type conversion: from `SeaQuery` Value to Value
pub(crate) fn from_svalue_to_value(svalue: SValue, nullable: bool) -> SqlResult<Value> {
    match svalue {
        SValue::Bool(ov) => sv_2_v!(ov, nullable),
        SValue::TinyInt(ov) => sv_2_v!(ov, nullable),
        SValue::SmallInt(ov) => sv_2_v!(ov, nullable),
        SValue::Int(ov) => sv_2_v!(ov, nullable),
        SValue::BigInt(ov) => sv_2_v!(ov, nullable),
        SValue::TinyUnsigned(ov) => sv_2_v!(ov, nullable),
        SValue::SmallUnsigned(ov) => sv_2_v!(ov, nullable),
        SValue::Unsigned(ov) => sv_2_v!(ov, nullable),
        SValue::BigUnsigned(ov) => sv_2_v!(ov, nullable),
        SValue::Float(ov) => sv_2_v!(ov, nullable),
        SValue::Double(ov) => sv_2_v!(ov, nullable),
        SValue::String(ov) => sv_2_v!(ov, String, nullable),
        SValue::ChronoDate(ov) => sv_2_v!(ov, NaiveDate, nullable),
        SValue::ChronoTime(ov) => sv_2_v!(ov, NaiveTime, nullable),
        SValue::ChronoDateTime(ov) => sv_2_v!(ov, NaiveDateTime, nullable),
        SValue::Decimal(ov) => sv_2_v!(ov, Decimal, nullable),
        SValue::Uuid(ov) => sv_2_v!(ov, Uuid, nullable),
        _ => Err(SqlError::new_common_error("unsupported type")),
    }
}

// ================================================================================================
// FilterBuilder
// Delete, Select
// ================================================================================================

/// delete or select statement, since their `where` clause are the same
pub(crate) enum DeleteOrSelect<'a> {
    Delete(&'a mut DeleteStatement),
    Select(&'a mut SelectStatement),
}

/// RecursiveState
///
/// Used in `cond_builder` to build `ConditionExpression`
#[derive(Default)]
struct RecursiveState {
    cond: Option<Cond>,
    negate: bool,
}

impl RecursiveState {
    fn new() -> Self {
        RecursiveState::default()
    }

    fn set_cond_if_empty(&mut self, cond: Cond) {
        if self.cond.is_none() {
            self.cond = Some(cond)
        }
    }

    fn set_negate(&mut self) {
        self.negate = true;
    }

    fn reset_negate(&mut self) {
        self.negate = false;
    }

    fn add<C: Into<ConditionExpression>>(&mut self, cond: C) {
        self.cond = Some(match self.cond.take() {
            Some(c) => c.add(cond),
            None => Cond::all().add(cond),
        });
    }
}

/// A general function to build Sql conditions for Delete and Select statements
pub(crate) fn filter_builder(s: &mut DeleteOrSelect, flt: &sql_adt::Expressions) {
    let mut state = RecursiveState::new();
    cond_builder(&flt.0, &mut state);

    match s {
        DeleteOrSelect::Delete(d) => {
            state.cond.take().map(|c| d.cond_where(c));
        }
        DeleteOrSelect::Select(s) => {
            state.cond.take().map(|c| s.cond_where(c));
        }
    }
}

/// condition builder
fn cond_builder(flt: &[sql_adt::Expression], state: &mut RecursiveState) {
    let mut iter = flt.iter().peekable();

    while let Some(e) = iter.next() {
        // move forward and get the next expr
        if let Some(ne) = iter.peek() {
            // if same type of expression in a row, skip the former one
            if &e == ne {
                continue;
            }

            match ne {
                // Simple/Nest -> Conjunction
                sql_adt::Expression::Conjunction(c) => {
                    let permit = matches!(
                        e,
                        sql_adt::Expression::Simple(_) | sql_adt::Expression::Nest(_)
                    );
                    if permit {
                        match c {
                            sql_adt::Conjunction::AND => state.set_cond_if_empty(Cond::all()),
                            sql_adt::Conjunction::OR => state.set_cond_if_empty(Cond::any()),
                        }
                    }
                }
                // Opposition -> Simple
                sql_adt::Expression::Simple(_) => {
                    if matches!(e, sql_adt::Expression::Opposition(_)) {
                        state.set_negate()
                    }
                }
                // Opposition -> Nest
                sql_adt::Expression::Nest(_) => {
                    if matches!(e, sql_adt::Expression::Opposition(_)) {
                        state.set_negate()
                    }
                }
                _ => {}
            }
        }

        match e {
            sql_adt::Expression::Simple(s) => {
                let expr = Expr::col(alias!(s.column()));
                let expr = match s.equation() {
                    sql_adt::Equation::Equal(v) => expr.eq(v),
                    sql_adt::Equation::NotEqual(v) => expr.ne(v),
                    sql_adt::Equation::Greater(v) => expr.gt(v),
                    sql_adt::Equation::GreaterEqual(v) => expr.gte(v),
                    sql_adt::Equation::Less(v) => expr.lt(v),
                    sql_adt::Equation::LessEqual(v) => expr.lte(v),
                    sql_adt::Equation::In(v) => expr.is_in(v),
                    sql_adt::Equation::Between(v) => expr.between(&v.0, &v.1),
                    sql_adt::Equation::Like(v) => expr.like(v),
                };
                if state.negate {
                    state.add(Cond::all().not().add(expr));
                } else {
                    state.add(expr);
                }
                state.reset_negate();
            }
            sql_adt::Expression::Nest(n) => {
                let mut ns = RecursiveState::new();
                cond_builder(n, &mut ns);
                if let Some(c) = ns.cond.take() {
                    if state.negate {
                        state.add(c.not());
                    } else {
                        state.add(c);
                    }
                }
                state.reset_negate();
            }
            _ => {}
        }
    }
}

// ================================================================================================
// ColumnBuilder
// ================================================================================================

/// column_builder
pub(crate) fn column_builder(statement: &mut SelectStatement, column: &sql_adt::Column) {
    match column.function() {
        Some(f) => match f {
            sql_adt::Function::Alias(a) => {
                statement.expr_as(Expr::col(alias!(column.name())), alias!(a));
            }
            sql_adt::Function::Max => {
                statement.expr(Func::max(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::Min => {
                statement.expr(Func::min(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::Sum => {
                statement.expr(Func::sum(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::Avg => {
                statement.expr(Func::avg(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::Abs => {
                statement.expr(Func::abs(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::Count => {
                statement.expr(Func::count(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::IfNull(n) => {
                statement.expr(Func::if_null(
                    Expr::col(alias!(column.name())),
                    Expr::col(alias!(n)),
                ));
            }
            sql_adt::Function::Cast(c) => {
                statement.expr(Func::cast_as(column.name(), alias!(c)));
            }
            sql_adt::Function::Coalesce(co) => {
                statement.expr(Func::coalesce(
                    co.iter().map(|c| Expr::col(alias!(c))).collect::<Vec<_>>(),
                ));
            }
            sql_adt::Function::CharLength => {
                statement.expr(Func::char_length(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::Lower => {
                statement.expr(Func::lower(Expr::col(alias!(column.name()))));
            }
            sql_adt::Function::Upper => {
                statement.expr(Func::upper(Expr::col(alias!(column.name()))));
            }
        },
        None => {
            statement.column(alias!(column.name()));
        }
    }
}

// ================================================================================================
// JoinBuilder
// ================================================================================================

impl From<&sql_adt::JoinType> for SJoinType {
    fn from(jt: &sql_adt::JoinType) -> Self {
        match jt {
            sql_adt::JoinType::Join => SJoinType::Join,
            sql_adt::JoinType::Inner => SJoinType::InnerJoin,
            sql_adt::JoinType::Left => SJoinType::LeftJoin,
            sql_adt::JoinType::Right => SJoinType::RightJoin,
        }
    }
}

/// join_builder
pub(crate) fn join_builder(statement: &mut SelectStatement, join: &sql_adt::Join) {
    // if join.on() is empty, it is not necessary to join
    if join.is_valid() {
        let mut conditions = Cond::all();

        for c in join.on() {
            let expr = Expr::tbl(alias!(join.left_table()), alias!(&c.0))
                .equals(alias!(join.right_table()), alias!(&c.1));
            conditions = conditions.add(expr);
        }

        statement.join(
            join.join_type().into(),
            alias!(join.right_table()),
            conditions,
        );
    }
}
