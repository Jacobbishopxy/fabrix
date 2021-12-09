//! Read Xl and save to database
//!
//! # Examples

use fabrix::prelude::*;
use fabrix::sql::*;
use fabrix::xl::*;

const CONN3: &'static str = "sqlite://dev.sqlite";

#[tokio::test]
async fn test_xl2db_async() {
    let source = XlSource::Path("../mock/test.xlsx");

    let mut xl2db = XlDbHelper::new(CONN3).await.unwrap();

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
                        .await
                        .map(|_| ())
                })
            },
        )
        .await;

    println!("{:?}", res);

    let select = sql_adt::Select {
        table: "test_table".into(),
        columns: vec![
            "id".into(),
            "first_name".into(),
            "last_name".into(),
            "email".into(),
            "ip_address".into(),
            "birth".into(),
            "issued_date".into(),
            "issued_times".into(),
        ],
        ..Default::default()
    };

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
