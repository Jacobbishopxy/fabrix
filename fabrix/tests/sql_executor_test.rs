//! Sql executor test case
//!
//! test case:
//! 1. create sqlite db at local path
//! 1. test sql connection
//! 1. test save: fail_if_exists
//! 1. test save: replace
//! 1. test save: append
//! 1. test save: upsert
//! 1. test delete

use std::str::FromStr;

use fabrix::sql_adt::ExpressionTransit;
use fabrix::{datetime, fx, xpr, xpr_and, xpr_or};
use fabrix::{sql_adt, SqlEngine, SqlExecutor};
use fabrix::{DatabaseMysql, DatabasePg, DatabaseSqlite};

const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
const CONN3: &str = "sqlite://dev.sqlite";

const TABLE_NAME: &str = "dev";

/*
To create a new sqlite database, run the following commands:
(project_root/fabrix/dev.sqlite)

cargo test --package fabrix --test sql_executor_test -- test_create_sqlite_db --exact --nocapture
*/
#[tokio::test]
async fn test_create_sqlite_db() -> anyhow::Result<()> {
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
    use sqlx::ConnectOptions;
    use std::str::FromStr;

    let conn = SqliteConnectOptions::from_str(CONN3)?
        .journal_mode(SqliteJournalMode::Wal)
        .create_if_missing(true)
        .connect()
        .await;

    assert!(conn.is_ok());

    Ok(())
}

/*
cargo test --package fabrix --test sql_executor_test -- test_connection --exact --nocapture
*/
#[tokio::test]
async fn test_connection() {
    let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
    let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
    let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();

    let r1 = exc1.connect().await;
    assert!(r1.is_ok());

    let r2 = exc2.connect().await;
    assert!(r2.is_ok());

    let r3 = exc3.connect().await;
    assert!(r3.is_ok());
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_fail_if_exists --exact --nocapture
*/
#[tokio::test]
async fn test_save_fail_if_exists() {
    let df = fx![
        "ord";
        "names" => ["Jacob", "Sam", "James", "Lucas", "Mia"],
        "ord" => [10,11,12,20,22],
        "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8)],
        "dt" => [
            datetime!(2016, 1, 8, 9, 10, 11),
            datetime!(2017, 1, 7, 9, 10, 11),
            datetime!(2018, 1, 6, 9, 10, 11),
            datetime!(2019, 1, 5, 9, 10, 11),
            datetime!(2020, 1, 4, 9, 10, 11),
        ]
    ]
    .unwrap();

    let save_strategy = sql_adt::SaveStrategy::FailIfExists;

    // mysql
    let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res1.is_ok());

    // postgres
    let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res2.is_ok());

    // sqlite
    let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res3.is_ok());
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_replace --exact --nocapture
*/
#[tokio::test]
async fn test_save_replace() {
    // df
    let df = fx![
        "ord";
        "names" => ["Jacob", "Sam", "James", "Lucas", "Mia", "Livia"],
        "ord" => [10,11,12,20,22,31],
        "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8), Some(11.2)],
        "note" => [Some("FS"), Some("OP"), Some("TEC"), None, Some("SS"), None],
        "dt" => [
            datetime!(2016, 1, 8, 9, 10, 11),
            datetime!(2017, 1, 7, 9, 10, 11),
            datetime!(2018, 1, 6, 9, 10, 11),
            datetime!(2019, 1, 5, 9, 10, 11),
            datetime!(2020, 1, 4, 9, 10, 11),
            datetime!(2020, 1, 3, 9, 10, 11),
        ]
    ]
    .unwrap();

    let save_strategy = sql_adt::SaveStrategy::Replace;

    // mysql
    let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res1.is_ok());

    // postgres
    let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res2.is_ok());

    // sqlite
    let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res3.is_ok());
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_append --exact --nocapture
*/
#[tokio::test]
async fn test_save_append() {
    // df
    let df = fx![
        "ord";
        "names" => ["Fila", "Ada", "Kevin"],
        "ord" => [25,17,32],
        "val" => [None, Some(7.1), Some(2.4)],
        "note" => [Some(""), Some("M"), None],
        "dt" => [
            datetime!(2010, 2, 5, 9, 10, 11),
            datetime!(2011, 2, 4, 9, 10, 11),
            datetime!(2012, 2, 3, 9, 10, 11),
        ]
    ]
    .unwrap();

    let save_strategy = sql_adt::SaveStrategy::Append;

    // mysql
    let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res1.is_ok());

    // postgres
    let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res2.is_ok());

    // sqlite
    let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res3.is_ok());
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_upsert --exact --nocapture
*/
#[tokio::test]
async fn test_save_upsert() {
    // df
    let df = fx![
        "ord";
        "ord" => [10,15,20],
        "val" => [Some(12.7), Some(7.1), Some(8.9)],
    ]
    .unwrap();

    let save_strategy = sql_adt::SaveStrategy::Upsert;

    // mysql
    let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res1.is_ok());

    // postgres
    let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res2.is_ok());

    // sqlite
    let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert!(res3.is_ok());
}

/*
cargo test --package fabrix --test sql_executor_test -- test_delete --exact --nocapture
*/
#[tokio::test]
async fn test_delete() {
    let filter = sql_adt::ExpressionsBuilder::from_condition(xpr!("ord", "=", 15))
        .append(xpr_or!())
        .append(
            sql_adt::ExpressionsBuilder::from_condition(xpr!("names", "=", "Livia"))
                .append(xpr_and!())
                .append(xpr!("val", ">=", 10.0))
                .finish(),
        )
        .finish();

    let delete = sql_adt::Delete {
        table: TABLE_NAME.to_owned(),
        filter,
    };

    // mysql
    let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.delete(&delete).await;
    assert!(res1.is_ok());

    // postgres
    let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.delete(&delete).await;
    assert!(res2.is_ok());

    // sqlite
    let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.delete(&delete).await;
    assert!(res3.is_ok());
}
