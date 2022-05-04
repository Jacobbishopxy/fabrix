//! Sea-Query binding test
//!
//! To enable this test case, add the following sea-query features to your `Cargo.toml`:
//! - "sqlx-postgres"
//! - "sqlx-mysql"
//! - "sqlx-sqlite"

// use sea_query::*;
// use sqlx::{sqlite::SqlitePool, Row};

// sea_query::sea_query_driver_sqlite!();

// macro_rules! alias {
//     ($name:expr) => {
//         sea_query::Alias::new($name)
//     };
// }

// const DB_STR: &str = "sqlite://dev.sqlite";
// const TABLE_NAME: &str = "sq_binding_test";

// #[tokio::test]
// async fn create_table() {
//     let pool = SqlitePool::connect(DB_STR).await.unwrap();

//     let sql = Table::create()
//         .table(alias!(TABLE_NAME))
//         .if_not_exists()
//         .col(
//             ColumnDef::new(alias!("id"))
//                 .primary_key()
//                 .auto_increment()
//                 .not_null()
//                 .integer(),
//         )
//         .col(ColumnDef::new(alias!("name")).not_null().string())
//         .build(SqliteQueryBuilder);

//     let res = sqlx::query(&sql).execute(&pool).await;

//     println!("{:?}", res);
// }

// #[tokio::test]
// async fn insert_data() {
//     let pool = SqlitePool::connect(DB_STR).await.unwrap();

//     let (sql, values) = Query::insert()
//         .into_table(alias!(TABLE_NAME))
//         .columns(vec![alias!("name")])
//         .values_panic(vec!["'".into()])
//         .values_panic(vec![r#"""#.into()])
//         .build(SqliteQueryBuilder);

//     let row = sea_query_driver_sqlite::bind_query(sqlx::query(&sql), &values)
//         .execute(&pool)
//         .await
//         .unwrap();

//     let ra = row.rows_affected();

//     println!("{:?}", ra);
// }

// #[tokio::test]
// async fn read_all() {
//     let pool = SqlitePool::connect(DB_STR).await.unwrap();

//     let (sql, values) = Query::select()
//         .columns(vec![alias!("id"), alias!("name")])
//         .from(alias!(TABLE_NAME))
//         .build(SqliteQueryBuilder);

//     let rows = sea_query_driver_sqlite::bind_query(sqlx::query(&sql), &values)
//         .fetch_all(&pool)
//         .await
//         .unwrap();

//     let res = rows
//         .iter()
//         .map(|row| {
//             let id = row.try_get::<i32, _>(0).unwrap();
//             let name = row.try_get::<String, _>(1).unwrap();
//             (id, name)
//         })
//         .collect::<Vec<_>>();

//     println!("{:?}", res);
// }
