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

#![feature(assert_matches)]
use std::assert_matches::assert_matches;

use fabrix::{adt, df, value, DateTime, SqlEngine, SqlExecutor};

const CONN1: &'static str = "mysql://root:secret@localhost:3306/dev";
const CONN2: &'static str = "postgres://root:secret@localhost:5432/dev";
const CONN3: &'static str = "sqlite://dev.sqlite";

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

    assert_matches!(conn, Ok(_));

    Ok(())
}

/*
cargo test --package fabrix --test sql_executor_test -- test_connection --exact --nocapture
*/
#[tokio::test]
async fn test_connection() {
    let mut exc1 = SqlExecutor::from_str(CONN1);
    let mut exc2 = SqlExecutor::from_str(CONN2);
    let mut exc3 = SqlExecutor::from_str(CONN3);

    let r1 = exc1.connect().await;
    assert_matches!(r1, Ok(_));

    let r2 = exc2.connect().await;
    assert_matches!(r2, Ok(_));

    let r3 = exc3.connect().await;
    assert_matches!(r3, Ok(_));
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_fail_if_exists --exact --nocapture
*/
#[tokio::test]
async fn test_save_fail_if_exists() {
    let df = df![
        "ord";
        "names" => ["Jacob", "Sam", "James", "Lucas", "Mia"],
        "ord" => [10,11,12,20,22],
        "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8)],
        "dt" => [
            DateTime(chrono::NaiveDate::from_ymd(2016, 1, 8).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2017, 1, 7).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2018, 1, 6).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2019, 1, 5).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2020, 1, 4).and_hms(9, 10, 11)),
        ]
    ]
    .unwrap();

    let save_strategy = adt::SaveStrategy::FailIfExists;

    // mysql
    let mut exc1 = SqlExecutor::from_str(CONN1);
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res1, Ok(_));

    // postgres
    let mut exc2 = SqlExecutor::from_str(CONN2);
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res2, Ok(_));

    // sqlite
    let mut exc3 = SqlExecutor::from_str(CONN3);
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res3, Ok(_));
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_replace --exact --nocapture
*/
#[tokio::test]
async fn test_save_replace() {
    // df
    let df = df![
        "ord";
        "names" => ["Jacob", "Sam", "James", "Lucas", "Mia", "Livia"],
        "ord" => [10,11,12,20,22,31],
        "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8), Some(11.2)],
        "note" => [Some("FS"), Some("OP"), Some("TEC"), None, Some("SS"), None],
        "dt" => [
            DateTime(chrono::NaiveDate::from_ymd(2016, 1, 8).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2017, 1, 7).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2018, 1, 6).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2019, 1, 5).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2020, 1, 4).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2020, 1, 3).and_hms(9, 10, 11)),
        ]
    ]
    .unwrap();

    let save_strategy = adt::SaveStrategy::Replace;

    // mysql
    let mut exc1 = SqlExecutor::from_str(CONN1);
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res1, Ok(_));

    // postgres
    let mut exc2 = SqlExecutor::from_str(CONN2);
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res2, Ok(_));

    // sqlite
    let mut exc3 = SqlExecutor::from_str(CONN3);
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res3, Ok(_));
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_append --exact --nocapture
*/
#[tokio::test]
async fn test_save_append() {
    // df
    let df = df![
        "ord";
        "names" => ["Fila", "Ada", "Kevin"],
        "ord" => [25,17,32],
        "val" => [None, Some(7.1), Some(2.4)],
        "note" => [Some(""), Some("M"), None],
        "dt" => [
            DateTime(chrono::NaiveDate::from_ymd(2010, 2, 5).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2011, 2, 4).and_hms(9, 10, 11)),
            DateTime(chrono::NaiveDate::from_ymd(2012, 2, 3).and_hms(9, 10, 11)),
        ]
    ]
    .unwrap();

    let save_strategy = adt::SaveStrategy::Append;

    // mysql
    let mut exc1 = SqlExecutor::from_str(CONN1);
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res1, Ok(_));

    // postgres
    let mut exc2 = SqlExecutor::from_str(CONN2);
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res2, Ok(_));

    // sqlite
    let mut exc3 = SqlExecutor::from_str(CONN3);
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res3, Ok(_));
}

/*
cargo test --package fabrix --test sql_executor_test -- test_save_upsert --exact --nocapture
*/
#[tokio::test]
async fn test_save_upsert() {
    // df
    let df = df![
        "ord";
        "ord" => [10,15,20],
        "val" => [Some(12.7), Some(7.1), Some(8.9)],
    ]
    .unwrap();

    let save_strategy = adt::SaveStrategy::Upsert;

    // mysql
    let mut exc1 = SqlExecutor::from_str(CONN1);
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res1, Ok(_));

    // postgres
    let mut exc2 = SqlExecutor::from_str(CONN2);
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res2, Ok(_));

    // sqlite
    let mut exc3 = SqlExecutor::from_str(CONN3);
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.save(TABLE_NAME, df.clone(), &save_strategy).await;
    assert_matches!(res3, Ok(_));
}

/*
cargo test --package fabrix --test sql_executor_test -- test_delete --exact --nocapture
*/
#[tokio::test]
async fn test_delete() {
    let delete = adt::Delete {
        table: TABLE_NAME.to_owned(),
        filter: vec![
            adt::Expression::Simple(adt::Condition {
                column: "ord".to_owned(),
                equation: adt::Equation::Equal(value!(15)),
            }),
            adt::Expression::Conjunction(adt::Conjunction::OR),
            adt::Expression::Nest(vec![
                adt::Expression::Simple(adt::Condition {
                    column: "names".to_owned(),
                    equation: adt::Equation::Equal(value!("Livia")),
                }),
                adt::Expression::Conjunction(adt::Conjunction::AND),
                adt::Expression::Simple(adt::Condition {
                    column: "val".to_owned(),
                    equation: adt::Equation::Greater(value!(10.0)),
                }),
            ]),
        ],
    };

    // mysql
    let mut exc1 = SqlExecutor::from_str(CONN1);
    exc1.connect().await.expect("connection is ok");

    let res1 = exc1.delete(&delete).await;
    assert_matches!(res1, Ok(_));

    // postgres
    let mut exc2 = SqlExecutor::from_str(CONN2);
    exc2.connect().await.expect("connection is ok");

    let res2 = exc2.delete(&delete).await;
    assert_matches!(res2, Ok(_));

    // sqlite
    let mut exc3 = SqlExecutor::from_str(CONN3);
    exc3.connect().await.expect("connection is ok");

    let res3 = exc3.delete(&delete).await;
    assert_matches!(res3, Ok(_));
}