//! Sql Builder: ddl query

use crate::{DdlQuery, SqlBuilder};

impl DdlQuery for SqlBuilder {
    /// check whether table exists (use `fetch_optional` method)
    fn check_table_exists(&self, table_name: &str) -> String {
        let que: &str;
        match self {
            Self::Sqlite => {
                que = r#"
                    SELECT
                        1
                    FROM
                        sqlite_master
                    WHERE
                        type = 'table' AND name = '?'
                    LIMIT 1
                "#;
            }
            _ => {
                que = r#"
                    SELECT
                        1
                    FROM
                        information_schema.tables
                    WHERE
                        table_name = '?'
                    LIMIT 1
                "#;
            }
        }
        que.replace("?", table_name).to_owned()
    }

    /// check a table's schema
    fn check_table_schema(&self, table_name: &str) -> String {
        let que: &str;
        match self {
            Self::Mysql => {
                que = r#"
                SELECT
                    column_name,
                    data_type,
                    is_nullable
                FROM
                    information_schema.columns
                WHERE
                    table_name = '?'
                "#;
            }
            Self::Postgres => {
                que = r#"
                SELECT
                    column_name,
                    udt_name,
                    is_nullable
                FROM
                    information_schema.columns
                WHERE
                    table_name = '?'
                "#;
            }
            Self::Sqlite => {
                que = r#"
                SELECT
                    name,
                    type,
                    CASE WHEN `notnull` = 0 THEN 'YES' else 'NO' END AS is_nullable
                FROM
                    PRAGMA_TABLE_INFO('?')
                "#;
            }
        }
        que.replace("?", table_name).to_owned()
    }

    /// list all tables in the current database
    fn list_tables(&self) -> String {
        let que: &str;
        match self {
            SqlBuilder::Mysql => {
                que = r#"
                SHOW TABLES
                "#;
            }
            SqlBuilder::Postgres => {
                que = r#"
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                "#;
            }
            SqlBuilder::Sqlite => {
                que = r#"
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                "#;
            }
        }
        que.to_owned()
    }

    /// get primary key in a table
    fn get_primary_key(&self, table_name: &str) -> String {
        let que: &str;
        match self {
            SqlBuilder::Mysql => {
                que = r#"
                SELECT
                    COLUMN_NAME
                FROM
                    INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_NAME = '?'
                    AND COLUMN_KEY = 'PRI'
                "#;
            }
            SqlBuilder::Postgres => {
                que = r#"
                SELECT
                    c.column_name
                FROM
                    information_schema.key_column_usage AS c
                LEFT JOIN information_schema.table_constraints AS t
                ON
                    t.constraint_name = c.constraint_name
                WHERE
                    t.table_name = '?'
                    AND t.constraint_type = 'PRIMARY KEY'
                "#;
            }
            SqlBuilder::Sqlite => {
                que = r#"
                SELECT
                    l.name
                FROM
                    pragma_table_info("?") as l
                WHERE
                    l.pk = 1
                "#;
            }
        }
        que.replace("?", table_name).to_owned()
    }
}
