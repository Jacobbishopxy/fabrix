/// statement macro
macro_rules! statement {
    ($builder:expr, $statement:expr) => {{
        match $builder {
            $crate::SqlBuilder::Mysql => $statement.to_string(sea_query::MysqlQueryBuilder),
            $crate::SqlBuilder::Postgres => $statement.to_string(sea_query::PostgresQueryBuilder),
            $crate::SqlBuilder::Sqlite => $statement.to_string(sea_query::SqliteQueryBuilder),
        }
    }};
    ($accumulator:expr; $builder:expr, $statement:expr) => {{
        match $builder {
            $crate::SqlBuilder::Postgres => {
                $accumulator.push($statement.to_string(sea_query::PostgresQueryBuilder));
            }
            $crate::SqlBuilder::Mysql => {
                $accumulator.push($statement.to_string(sea_query::MysqlQueryBuilder));
            }
            $crate::SqlBuilder::Sqlite => {
                $accumulator.push($statement.to_string(sea_query::SqliteQueryBuilder));
            }
        }
    }};
}

pub(crate) use statement;

/// sea query alias macro
macro_rules! alias {
    ($name:expr) => {
        sea_query::Alias::new($name)
    };
}

pub(crate) use alias;

/// expression macro
/// `sql_adt::Expression::Conjunction(sql_adt::Conjunction::AND)`
#[macro_export]
macro_rules! xpr_and {
    () => {
        $crate::sql::sql_adt::Expression::Conjunction($crate::sql::sql_adt::Conjunction::AND)
    };
}

pub use xpr_and;

/// expression macro
/// `sql_adt::Expression::Conjunction(sql_adt::Conjunction::OR)`
#[macro_export]
macro_rules! xpr_or {
    () => {
        $crate::sql::sql_adt::Expression::Conjunction($crate::sql::sql_adt::Conjunction::OR)
    };
}

pub use xpr_or;

/// expression macro
/// `sql_adt::Expression::Nest(...)`
#[macro_export]
macro_rules! xpr_nest {
    ($($xpr:expr),*) => {
        $crate::sql::sql_adt::Expression::Nest(vec![$($xpr),*])
    };
}

pub use xpr_nest;

/// expression macro
/// `sql_adt::Expression::Simple(...)`
#[macro_export]
macro_rules! xpr_simple {
    ($column:expr, $equation:expr) => {
        $crate::sql::sql_adt::Expression::Simple($crate::sql::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "not" => $crate::sql::sql_adt::Equation::Not,
                _ => unimplemented!(),
            },
        })
    };
    ($column:expr, $equation:expr, $value:expr) => {
        $crate::sql::sql_adt::Expression::Simple($crate::sql::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "=" => $crate::sql::sql_adt::Equation::Equal($crate::value!($value)),
                "!=" => $crate::sql::sql_adt::Equation::NotEqual($crate::value!($value)),
                ">" => $crate::sql::sql_adt::Equation::Greater($crate::value!($value)),
                ">=" => $crate::sql::sql_adt::Equation::GreaterEqual($crate::value!($value)),
                "<" => $crate::sql::sql_adt::Equation::Less($crate::value!($value)),
                "<=" => $crate::sql::sql_adt::Equation::LessEqual($crate::value!($value)),
                "%" => $crate::sql::sql_adt::Equation::Like(String::from(stringify!($value))),
                _ => unimplemented!(),
            },
        })
    };
    ($column:expr, $equation:expr, [$($value:expr),* $(,)*]) => {
        $crate::sql::sql_adt::Expression::Simple($crate::sql::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "in" => {
                    let values = $value.map(|i| $crate::value!(i)).collect();
                    $crate::sql::sql_adt::Equation::In(values)
                }
                _ => unimplemented!(),
            },
        })
    };
    ($column:expr, $equation:expr, [$value1:expr, $value2:expr]) => {
        $crate::sql::sql_adt::Expression::Simple($crate::sql::sql_adt::Condition {
            column: String::from($column),
            equation: match $equation {
                "between" => $crate::sql::sql_adt::Equation::Between((
                    $crate::value!($value1),
                    $crate::value!($value2),
                )),
                _ => unimplemented!(),
            },
        })
    };
}

pub use xpr_simple;
