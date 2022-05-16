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

/// expression macro
/// `sql_adt::Expression::Conjunction(sql_adt::Conjunction::AND)`
#[macro_export]
macro_rules! xpr_and {
    () => {
        $crate::sql_adt::Conjunction::AND
    };
}

pub use xpr_and;

/// expression macro
/// `sql_adt::Expression::Conjunction(sql_adt::Conjunction::OR)`
#[macro_export]
macro_rules! xpr_or {
    () => {
        $crate::sql_adt::Conjunction::OR
    };
}

pub use xpr_or;

// TODO:

/// expression macro
/// `sql_adt::Expression::Nest(...)`
#[macro_export]
macro_rules! xpr_nest {
    ($($xpr:expr),*) => {
        $crate::sql_adt::Expressions(vec![$($xpr),*])
    };
}

pub use xpr_nest;

/// expression macro
/// `sql_adt::Expression::Simple(...)`
#[macro_export]
macro_rules! xpr_simple {
    ($column:expr, $equation:expr) => {
        $crate::sql::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "not" => $crate::sql::sql_adt::Equation::Not,
                _ => unimplemented!(),
            },
        }
    };

    ($column:expr, $equation:expr, [$value1:expr, $value2:expr]) => {
        $crate::sql::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "between" => $crate::sql::sql_adt::Equation::Between((
                    $crate::value!($value1),
                    $crate::value!($value2),
                )),
                "in" => $crate::sql::sql_adt::Equation::In(vec![
                    $crate::value!($value1),
                    $crate::value!($value2),
                ]),
                _ => unimplemented!(),
            },
        }
    };
    ($column:expr, $equation:expr, [$($value:expr),* $(,)*]) => {
        $crate::sql_adt::Condition {
            column: String::from($column),
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
    ($column:expr, $equation:expr, $value:expr) => {
        $crate::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "=" => $crate::sql_adt::Equation::Equal($crate::value!($value)),
                "!=" => $crate::sql_adt::Equation::NotEqual($crate::value!($value)),
                ">" => $crate::sql_adt::Equation::Greater($crate::value!($value)),
                ">=" => $crate::sql_adt::Equation::GreaterEqual($crate::value!($value)),
                "<" => $crate::sql_adt::Equation::Less($crate::value!($value)),
                "<=" => $crate::sql_adt::Equation::LessEqual($crate::value!($value)),
                "%" => $crate::sql_adt::Equation::Like(String::from(stringify!($value))),
                _ => unimplemented!(),
            },
        }
    };
}

pub use xpr_simple;
