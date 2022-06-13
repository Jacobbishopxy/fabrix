//! Read Xl and save to database
//!
//! The difference between `test_xl2db_async_no_index` and `test_xl2db_async_with_index` is that
//! the former does not use index, while the latter uses first column as its index.

use std::fs::File;

use ::fabrix::prelude::*;

// const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
// const CONN3: &str = "sqlite://dev.sqlite";

const XL_PATH: &str = "../mock/test.xlsx";
const XL_SHEET_NAME: &str = "data";
const XL_SHEET_NAME2: &str = "data_t";

const SQL_TABLE_NAME: &str = "test_xl2db";

#[tokio::test]
async fn test_xl2db_async_no_index() {
    let source: XlWorkbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();

    let mut xl2db = XlDbHelper::<DatabasePg>::new(CONN2).await.unwrap();

    let mut xle = XlDbExecutor::<File, DatabasePg>::new_with_source(source);

    let res = xle
        .async_consume_fn_mut(
            Some(30),
            XL_SHEET_NAME,
            |d| {
                xl2db
                    .convertor
                    .convert_row_wised(d, ())
                    .map_err(|e| XlError::Unexpected(format!("fabrix error {:?}", e)))
            },
            |d| {
                Box::pin(async {
                    xl2db
                        .consumer
                        .lock()
                        .await
                        .replace_existing_table(SQL_TABLE_NAME, d, true)
                        // .append_table("test_table", d)
                        .await
                        .map_err(|e| XlError::Unexpected(format!("fabrix error {:?}", e)))
                })
            },
        )
        .await;

    println!("{:?}", res);

    let select = sql_adt::Select::new(SQL_TABLE_NAME).columns(&[
        "id",
        "first_name",
        "last_name",
        "email",
        "ip_address",
        "birth",
        "issued_date",
        "issued_times",
    ]);

    let res = xl2db
        .consumer
        .lock()
        .await
        .executor
        .select(&select)
        .await
        .unwrap();

    println!("{:?}", res);

    let res = xl2db
        .consumer
        .lock()
        .await
        .executor
        .get_table_schema(SQL_TABLE_NAME)
        .await
        .unwrap();

    println!("{:?}", res);
}

#[tokio::test]
async fn test_xl2db_async_with_index_row_wised() {
    let source: XlWorkbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();

    let mut xl2db = XlDbHelper::<DatabasePg>::new(CONN2).await.unwrap();

    let mut xle = XlDbExecutor::<File, DatabasePg>::new_with_source(source);

    let res = xle
        .async_consume_fn_mut(
            Some(30),
            XL_SHEET_NAME,
            |d| {
                xl2db
                    .convertor
                    .convert_row_wised(d, 0)
                    .map_err(|e| XlError::Unexpected(format!("fabrix error {:?}", e)))
            },
            |d| {
                Box::pin(async {
                    xl2db
                        .consumer
                        .lock()
                        .await
                        .replace_existing_table(SQL_TABLE_NAME, d, false)
                        .await
                        .map_err(|e| XlError::Unexpected(format!("fabrix error {:?}", e)))
                })
            },
        )
        .await;

    println!("{:?}", res);
}

#[tokio::test]
async fn test_xl2db_async_with_index_col_wised() {
    let source: XlWorkbook<File> = XlSource::Path(XL_PATH.to_owned()).try_into().unwrap();

    let xl2db = XlDbHelper::<DatabasePg>::new(CONN2).await.unwrap();

    let mut xle = XlDbExecutor::<File, DatabasePg>::new_with_source(source);

    let res = xle
        .async_consume_fn_mut(
            None,
            XL_SHEET_NAME2,
            |d| {
                xl2db
                    .convertor
                    .convert_col_wised(d, "id")
                    .map_err(|e| XlError::Unexpected(format!("fabrix error {:?}", e)))
            },
            |d| {
                Box::pin(async {
                    xl2db
                        .consumer
                        .lock()
                        .await
                        .replace_existing_table(SQL_TABLE_NAME, d, false)
                        .await
                        .map_err(|e| XlError::Unexpected(format!("fabrix error {:?}", e)))
                })
            },
        )
        .await;

    println!("{:?}", res);
}
