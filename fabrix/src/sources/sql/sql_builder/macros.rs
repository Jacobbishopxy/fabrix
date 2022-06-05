/// statement macro
macro_rules! statement {
    ($builder:expr, $statement:expr) => {{
        match $builder {
            $crate::SqlBuilder::Mysql => $statement.to_string(sea_query::MysqlQueryBuilder),
            $crate::SqlBuilder::Postgres => $statement.to_string(sea_query::PostgresQueryBuilder),
            $crate::SqlBuilder::Sqlite => $crate::sql_builder::macros::sqlite_str_replace(
                $statement.to_string(sea_query::SqliteQueryBuilder),
            ),
        }
    }};
}

// WARNING: a temporary work-around to solve Sqlx parsing problems
// when Sea-Query generating Sqlite SQL strings.
pub(crate) fn sqlite_str_replace(s: String) -> String {
    s.replace("\\'", "''").replace("\\\"", "\"")
}

pub(crate) use statement;

/// sea query alias macro
macro_rules! alias {
    ($name:expr) => {
        sea_query::Alias::new($name)
    };
}

pub(crate) use alias;

/// from `SeaQuery` Value to Value
macro_rules! sv_2_v {
    ($option_value:expr, $nullable:ident) => {
        if $nullable {
            Ok($crate::value!($option_value))
        } else {
            match $option_value {
                Some(v) => Ok($crate::value!(v)),
                None => Err($crate::SqlError::new_common_error("unsupported type")),
            }
        }
    };
    ($option_value:expr, $null_type:ty, $nullable:ident) => {
        if $nullable {
            match $option_value {
                Some(v) => Ok($crate::value!(*v)),
                None => Ok($crate::value!(None::<$null_type>)),
            }
        } else {
            match $option_value {
                Some(v) => Ok($crate::value!(*v)),
                None => Err($crate::SqlError::new_common_error("unsupported type")),
            }
        }
    };
}

pub(crate) use sv_2_v;

#[macro_export]
macro_rules! xpr_and {
    // Case: And
    () => {
        $crate::sql_adt::Conjunction::AND
    };
}

pub use xpr_and;

#[macro_export]
macro_rules! xpr_or {
    // Case: And
    () => {
        $crate::sql_adt::Conjunction::OR
    };
}

pub use xpr_or;

#[macro_export]
macro_rules! xpr_not {
    // Case: Not
    () => {
        $crate::sql_adt::Opposition::NOT
    };
}

pub use xpr_not;

#[macro_export]
macro_rules! xpr_fn {
    // Case: Function
    ($column:expr, $function:expr) => {
        $crate::sql_adt::Condition {
            column: String::from($column),
            equation: match $function {
                "max" => $crate::sql_adt::Function::Max,
                "min" => $crate::sql_adt::Function::Min,
                "sum" => $crate::sql_adt::Function::Sum,
                "avg" => $crate::sql_adt::Function::Avg,
                "abs" => $crate::sql_adt::Function::Abs,
                "count" => $crate::sql_adt::Function::Count,
                "charlen" => $crate::sql_adt::Function::CharLength,
                "lower" => $crate::sql_adt::Function::Lower,
                "upper" => $crate::sql_adt::Function::Upper,
                _ => unimplemented!(),
            },
        }
    };

    ($column:expr, $function:expr, $value:expr) => {
        $crate::sql_adt::Condition {
            column: String::from($column),
            equation: match $function {
                "alias" => $crate::sql_adt::Function::Alias($value),
                "ifnull" => $crate::sql_adt::Function::IfNull($value),
                "cast" => $crate::sql_adt::Function::Cast($value),
                _ => unimplemented!(),
            },
        }
    };

    ($column:expr, $function:expr, [$($value:expr),* $(,)*]) => {
        $crate::sql_adt::Condition {
            column: String::from($column),
            equation: match $function {
                "coalesce" => {
                    let mut values = Vec::<String>::new();
                    $(
                        values.push($value);
                    )*

                    $crate::sql_adt::Function::Coalesce($values)
                },
                _ => unimplemented!(),
            },
        }
    };
}

#[macro_export]
macro_rules! xpr_join {
    () => {
        // TODO:
    };
}

pub use xpr_join;

/// expression macro
/// `sql_adt::Expression::Nest(...)`
/// `sql_adt::Expression::Simple(...)`
#[macro_export]
macro_rules! xpr {
    // Expression builder from conditions
    ([$($xpr:expr),* $(,)*]) => {{
        use $crate::sql_adt::ExpressionTransit;

        let expr = $crate::sql_adt::ExpressionsBuilder::init();

        $(
            let expr = expr.append($xpr);
        )*

        expr.finish()
    }};

    // Expression builder from expressions
    ($xpr:expr) => {{
        use $crate::sql_adt::ExpressionTransit;

        let expr = $crate::sql_adt::ExpressionsBuilder::from_expressions($xpr);

        expr.finish()
    }};

    // Case: Between
    ($column:expr, $equation:expr, [$value1:expr, $value2:expr]) => {
        $crate::sql_adt::Condition {
            column: $column.into(),
            equation: match $equation {
                "between" => $crate::sql_adt::Equation::Between((
                    $crate::value!($value1),
                    $crate::value!($value2),
                )),
                "in" => $crate::sql_adt::Equation::In(vec![
                    $crate::value!($value1),
                    $crate::value!($value2),
                ]),
                _ => unimplemented!(),
            },
        }
    };

    // Case: In
    ($column:expr, $equation:expr, [$($value:expr),* $(,)*]) => {
        $crate::sql_adt::Condition {
            column: $column.into(),
            equation: match $equation {
                "in" => {
                    let mut values = Vec::<$crate::Value>::new();
                    $(
                        values.push($crate::value!($value));
                    )*
                    $crate::sql_adt::Equation::In(values)
                }
                _ => unimplemented!(),
            },
        }
    };

    // Case: other equations
    ($column:expr, $equation:expr, $value:expr) => {
        $crate::sql_adt::Condition {
            column: $column.into(),
            equation: match $equation {
                "=" => $crate::sql_adt::Equation::Equal($crate::value!($value)),
                "!=" => $crate::sql_adt::Equation::NotEqual($crate::value!($value)),
                ">" => $crate::sql_adt::Equation::Greater($crate::value!($value)),
                ">=" => $crate::sql_adt::Equation::GreaterEqual($crate::value!($value)),
                "<" => $crate::sql_adt::Equation::Less($crate::value!($value)),
                "<=" => $crate::sql_adt::Equation::LessEqual($crate::value!($value)),
                "%" => $crate::sql_adt::Equation::Like($value.to_string()),
                _ => unimplemented!(),
            },
        }
    };
}

pub use xpr;

/// ExpressionTransit impl macro
///
/// # Example
/// ```rust,ignore
/// impl ExpressionTransit<Condition, SimpleState> for OppositeState {
///     fn append(mut self, state: Condition) -> SimpleState {
///         self.stack.push(Expression::from(state));
///         SimpleState { stack: self.stack }
///     }
///
///     fn finish(self) -> Expressions {
///         Expressions(self.stack)
///     }
/// }
/// ```
macro_rules! xpr_transit {
    ($value:ident, $from_state:ident => $into_state:ident) => {
        impl $crate::sql_adt::ExpressionTransit<$value, $into_state> for $from_state {
            fn append(mut self, from_state: $value) -> $into_state {
                self.stack
                    .push($crate::sql_adt::Expression::from(from_state));
                $into_state { stack: self.stack }
            }

            fn finish(self) -> $crate::sql_adt::Expressions {
                $crate::sql_adt::Expressions(self.stack)
            }
        }
    };
}

pub(crate) use xpr_transit;

#[cfg(test)]
mod sql_adt_macros {
    use crate::{sql_adt, sql_adt::ExpressionTransit, value};

    use super::*;

    #[test]
    fn test_xpr_nest() {
        let e1 = xpr!([
            xpr!([
                xpr!("name", "=", "Jacob"),
                xpr_and!(),
                xpr!("age", "in", [10, 30, 50]),
            ]),
            xpr_or!(),
            xpr_not!(),
            xpr!("ord", "between", [1, 100])
        ]);

        let e2 = sql_adt::ExpressionsBuilder::init()
            .append(
                sql_adt::ExpressionsBuilder::init()
                    .append(sql_adt::Condition::new(
                        "name",
                        sql_adt::Equation::Equal(value!("Jacob")),
                    ))
                    .append(sql_adt::Conjunction::AND)
                    .append(sql_adt::Condition::new(
                        "age",
                        sql_adt::Equation::In(vec![value!(10), value!(30), value!(50)]),
                    ))
                    .finish(),
            )
            .append(sql_adt::Conjunction::OR)
            .append(sql_adt::Opposition::NOT)
            .append(sql_adt::Condition::new(
                "ord",
                sql_adt::Equation::Between((value!(1), value!(100))),
            ))
            .finish();

        assert_eq!(e1, e2);
    }
}
