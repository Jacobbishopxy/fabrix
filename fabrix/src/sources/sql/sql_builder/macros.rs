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
