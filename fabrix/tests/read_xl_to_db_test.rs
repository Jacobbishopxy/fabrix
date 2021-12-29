//! Read Xl and save to database
//!
//! # Examples

use fabrix::prelude::*;
use fabrix::sql::*;
use fabrix::xl::*;

// const CONN1: &'static str = "mysql://root:secret@localhost:3306/dev";
const CONN2: &'static str = "postgres://root:secret@localhost:5432/dev";
// const CONN3: &'static str = "sqlite://dev.sqlite";

#[tokio::test]
async fn test_xl2db_async() {
    let source = XlSource::Path("../mock/test.xlsx");

    let mut xl2db = XlDbHelper::new(CONN2).await.unwrap();

    let mut xle = XlDbExecutor::new_with_source(source).unwrap();

    let res = xle
        .async_consume_fn_mut(
            Some(30),
            "data",
            |d| xl2db.convertor.convert_row_wised_no_index(d),
            |d| {
                Box::pin(async {
                    xl2db
                        .consumer
                        .lock()
                        .await
                        .replace_existing_table("test_table", d)
                        // .append_table("test_table", d)
                        .await
                        .map(|_| ())
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
