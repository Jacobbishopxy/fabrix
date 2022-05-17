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
/// `sql_adt::Expression::Nest(...)`
/// `sql_adt::Expression::Simple(...)`
#[macro_export]
macro_rules! xpr {
    // Expression builder
    ([$($xpr:expr),* $(,)*]) => {{
        use $crate::sql_adt::ExpressionSetup;

        let expr = $crate::sql_adt::ExpressionsBuilder::new();

        $(
            let expr = expr.append($xpr);
        )*

        expr.finish()
    }};

    // Case: And/Or
    ($value:expr) => {
        match $value {
            "and" => $crate::sql_adt::Conjunction::AND,
            "or" => $crate::sql_adt::Conjunction::OR,
            _ => unimplemented!(),
        }
    };

    // Case: Not
    ($column:expr, $equation:expr) => {
        $crate::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "not" => $crate::sql_adt::Equation::Not,
                _ => unimplemented!(),
            },
        }
    };

    // Case: Between
    ($column:expr, $equation:expr, [$value1:expr, $value2:expr]) => {
        $crate::sql_adt::Condition {
            column: String::from($column),
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

    // Case: other equations
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

pub use xpr;

#[cfg(test)]
mod sql_adt_macros {
    use super::*;

    #[test]
    fn test_xpr_nest() {
        let a = xpr!([
            xpr!("name", "=", "Jacob"),
            xpr!("and"),
            xpr!("age", "in", [10, 30, 50]),
        ]);

        let b = xpr!([xpr!("ord", "between", [1, 100]), xpr!("or"), a]);

        println!("{:?}", b);
    }
}
