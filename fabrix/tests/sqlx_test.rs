//! Sqlx test
//!
//! simple test case

use itertools::Itertools;
use sqlx::{Row, SqlitePool};

const DB_STR: &str = "sqlite://dev.sqlite";

#[tokio::test]
async fn query_str_escape() {
    let pool = SqlitePool::connect(DB_STR).await.unwrap();

    let que = r#"INSERT INTO "string_test" ("id", "string") VALUES (96, ''''), (97, '"')"#;
    println!("{:?}", que);

    let res = sqlx::query(que).execute(&pool).await.unwrap();

    println!("{:?}", res.rows_affected());
}

#[tokio::test]
async fn query_inserted_data() {
    let pool = SqlitePool::connect(DB_STR).await.unwrap();

    let que = r#"SELECT * FROM string_test"#;

    let rows = sqlx::query(que).fetch_all(&pool).await.unwrap();

    let res = rows
        .iter()
        .map(|row| {
            let id = row.try_get::<i32, _>(0).unwrap();
            let string = row.try_get::<String, _>(1).unwrap();
            (id, string)
        })
        .collect_vec();

    println!("{:?}", res);
}
