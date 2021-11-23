//! Read Xl and save to database
//!
//! # Examples

use async_trait::async_trait;

use fabrix::prelude::*;

const CONN3: &'static str = "sqlite://dev.sqlite";

pub struct TestXl2DbAsync(SqlExecutor);

impl TestXl2DbAsync {
    async fn new(conn_str: &str) -> FabrixResult<Self> {
        let mut ec = SqlExecutor::from_str(conn_str);
        ec.connect().await?;
        Ok(TestXl2DbAsync(ec))
    }

    fn sql_executor(&self) -> &SqlExecutor {
        &self.0
    }

    async fn create_table_and_append(
        &mut self,
        table_name: &str,
        data: DataFrame,
    ) -> FabrixResult<()> {
        todo!()
    }
}

// TODO: neither `Xl2DbAsync` nor `Xl2Db` can handle uncertain input params
// for example, in the following case `tale_name` and `index_name` can not
// be placed in `to_dataframe` and `save` respectively
#[async_trait]
impl Xl2DbAsync for TestXl2DbAsync {
    fn to_dataframe(rows: D2) -> FabrixResult<DataFrame> {
        todo!()
    }

    async fn save(&mut self, data: DataFrame) -> FabrixResult<()> {
        self.create_table_and_append("test", data).await?;
        Ok(())
    }
}

#[tokio::test]
async fn test_xl2db_async() {
    let source = XlSource::Path("../mock/test.xlsx");

    let consumer = TestXl2DbAsync::new(CONN3).await.unwrap();

    let mut xle = XlAsyncExecutor::new_with_source(consumer, source).unwrap();

    let res = xle.read_sheet("data", None).await;

    println!("{:?}", res);

    let select = adt::Select {
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

    let res = xle.consumer().sql_executor().select(&select).await;

    println!("{:?}", res);
}
