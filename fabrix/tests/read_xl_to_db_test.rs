//! Read Xl and save to database
//!
//! The difference between `test_xl2db_async_no_index` and `test_xl2db_async_with_index` is that
//! the former does not use index, while the latter uses first column as its index.

use fabrix::prelude::*;
use fabrix::sql::*;
use fabrix::xl::*;

// const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
// const CONN3: &str = "sqlite://dev.sqlite";

#[tokio::test]
async fn test_xl2db_async_no_index() {
    let source = XlSource::Path("../mock/test.xlsx");

    let mut xl2db = XlDbHelper::new(CONN2).await.unwrap();

    let mut xle = XlDbExecutor::new_with_source(source).unwrap();

    let res = xle
        .async_consume_fn_mut(
            Some(30),
            "data",
            |d| xl2db.convertor.convert_row_wised(d, XlIndexSelection::None),
            |d| {
                Box::pin(async {
                    xl2db
                        .consumer
                        .lock()
                        .await
                        .replace_existing_table("test_table", d, true)
                        // .append_table("test_table", d)
                        .await
                })
            },
        )
        .await;

    println!("{:?}", res);

    let mut select = sql_adt::Select::new("test_table");
    select.columns(&[
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
        .get_table_schema("test_table")
        .await
        .unwrap();

    println!("{:?}", res);
}

#[tokio::test]
async fn test_xl2db_async_with_index() {
    let source = XlSource::Path("../mock/test.xlsx");

    let mut xl2db = XlDbHelper::new(CONN2).await.unwrap();

    let mut xle = XlDbExecutor::new_with_source(source).unwrap();

    let res = xle
        .async_consume_fn_mut(
            Some(30),
            "data",
            |d| {
                xl2db
                    .convertor
                    .convert_row_wised(d, XlIndexSelection::Num(0))
            },
            |d| {
                Box::pin(async {
                    xl2db
                        .consumer
                        .lock()
                        .await
                        .replace_existing_table("test_table", d, false)
                        // .append_table("test_table", d)
                        .await
                })
            },
        )
        .await;

    println!("{:?}", res);
}
