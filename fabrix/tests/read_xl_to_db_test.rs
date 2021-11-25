//! Read Xl and save to database
//!
//! # Examples

use fabrix::prelude::*;

const CONN3: &'static str = "sqlite://dev.sqlite";

pub struct TestXl2Db(SqlExecutor);

impl TestXl2Db {
    async fn new(conn_str: &str) -> FabrixResult<Self> {
        let mut ec = SqlExecutor::from_str(conn_str);
        ec.connect().await?;
        Ok(TestXl2Db(ec))
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

    async fn append_to_existed_table(
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
impl Xl2Db for TestXl2Db {
    fn to_dataframe(rows: D2) -> FabrixResult<DataFrame> {
        todo!()
    }

    fn save(&mut self, data: DataFrame) -> FabrixResult<()> {
        todo!()
    }
}

#[tokio::test]
async fn test_xl2db_async() {
    let source = XlSource::Path("../mock/test.xlsx");

    let consumer = TestXl2Db::new(CONN3).await.unwrap();

    let mut xle = XlExecutor::new_with_source(consumer, source).unwrap();

    let res = xle.read_sheet("data", None);

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

    let res = xle.consumer().sql_executor().select(&select).await;

    println!("{:?}", res);
}
