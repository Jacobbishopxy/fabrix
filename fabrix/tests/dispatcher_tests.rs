//! Dispatcher tests
//!
//! Cross sources tests for the dispatcher

use std::fs::File;
use std::path::Path;

use ::fabrix::{
    sql_adt, CsvReadOptions, CsvReader, CsvWriteOptions, CsvWriter, DatabaseSqlite, Dispatcher,
    ParquetWriteOptions, ParquetWriter, SqlEngine, SqlReadOptions, SqlReader, SqlWriteOptions,
    SqlWriter,
};
use polars::prelude::{col, IntoLazy, QuantileInterpolOptions};

const CSV_READ: &str = "../mock/test.csv";
const CSV_WRITE: &str = "../cache/dispatcher_test.csv";
const PARQUET_WRITE: &str = "../cache/dispatcher_test.parquet";
const CONN: &str = "sqlite://dev.sqlite";
const TABLE: &str = "dispatcher_test";

#[tokio::test]
async fn read_csv_write_db() {
    let reader = CsvReader::new(File::open(CSV_READ).expect("file not found"));
    let writer = SqlWriter::<DatabaseSqlite>::new_from_str(CONN)
        .await
        .expect("cannot establish connection");

    let mut dispatcher = Dispatcher::new(reader, writer);

    let ro = CsvReadOptions::default();
    let res = dispatcher.sync_read(&ro);
    assert!(res.is_ok());
    assert!(dispatcher.has_data());

    let wo = SqlWriteOptions {
        table_name: Some(TABLE),
        save_strategy: Some(sql_adt::SaveStrategy::FailIfExists),
    };

    let res = dispatcher.async_write(&wo).await;
    assert!(res.is_ok());

    let columns = vec![
        "id",
        "first_name",
        "last_name",
        "email",
        "gender",
        "issued_date",
        "issued_times",
    ];

    let select = sql_adt::Select::new(TABLE).columns(&columns).limit(10);

    let res = dispatcher.writer().writer().select(&select).await;
    assert!(res.is_ok());
    assert!(!dispatcher.has_data());
    println!("{:?}", res.unwrap());
}

#[tokio::test]
async fn read_db_write_parquet() {
    let reader = SqlReader::<DatabaseSqlite>::new_from_str(CONN)
        .await
        .expect("cannot establish connection");
    let writer = ParquetWriter::new(File::create(PARQUET_WRITE).expect("file cannot be created"));

    let mut dispatcher = Dispatcher::new(reader, writer);

    let columns = vec![
        "id".into(),
        "first_name".into(),
        "last_name".into(),
        "email".into(),
        "gender".into(),
        "issued_date".into(),
        "issued_times".into(),
    ];

    let ro = SqlReadOptions {
        table: Some(TABLE),
        columns: Some(&columns),
        filter: None,
        order: None,
        limit: Some(10),
        offset: None,
        join: None,
        group_by: None,
    };
    let res = dispatcher.async_read(&ro).await;
    assert!(res.is_ok());
    assert!(dispatcher.has_data());

    let wo = ParquetWriteOptions::default();
    let res = dispatcher.sync_write(&wo);
    assert!(res.is_ok());
    assert!(!dispatcher.has_data());
    assert!(Path::new(PARQUET_WRITE).exists());
}

#[tokio::test]
async fn read_db_operate_write_csv() {
    let reader = SqlReader::<DatabaseSqlite>::new_from_str(CONN)
        .await
        .expect("cannot establish connection");
    let writer = CsvWriter::new(File::create(CSV_WRITE).expect("file cannot be created"));

    let mut dispatcher = Dispatcher::new(reader, writer);

    let columns = vec![
        "id".into(),
        "first_name".into(),
        "last_name".into(),
        "email".into(),
        "gender".into(),
        "issued_date".into(),
        "issued_times".into(),
    ];

    let ro = SqlReadOptions {
        table: Some(TABLE),
        columns: Some(&columns),
        filter: None,
        order: None,
        limit: None,
        offset: None,
        join: None,
        group_by: None,
    };
    let res = dispatcher.async_read(&ro).await;
    assert!(res.is_ok());
    assert!(dispatcher.has_data());

    let fx = dispatcher.fabrix_take().unwrap();
    let new = fx
        .data
        .lazy()
        .groupby([col("gender")])
        .agg([
            col("issued_times").min().alias("min_issued_times"),
            col("issued_times").max().alias("max_issued_times"),
            col("issued_times")
                .quantile(0.5, QuantileInterpolOptions::Nearest)
                .alias("median_issued_times"),
        ])
        .collect()
        .unwrap();

    dispatcher.fabrix_put(new);

    let wo = CsvWriteOptions::default();
    let res = dispatcher.sync_write(&wo);
    assert!(res.is_ok());
    assert!(!dispatcher.has_data());
    assert!(Path::new(CSV_WRITE).exists());
}
