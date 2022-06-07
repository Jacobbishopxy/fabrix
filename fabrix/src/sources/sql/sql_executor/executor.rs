//! Database executor

use std::str::FromStr;

use async_trait::async_trait;

use super::{
    conn_e_err, conn_n_err,
    loader::{DatabaseType, LoaderTransaction},
    types::string_try_into_value_type,
    FabrixDatabaseLoader, SqlConnInfo,
};
use crate::{
    sql_adt, D1Value, DdlMutation, DdlQuery, DmlMutation, DmlQuery, Fabrix, Series, SqlBuilder,
    SqlError, SqlResult, Value, ValueType,
};

#[async_trait]
pub trait SqlHelper {
    // ================================================================================================
    // Query
    // ================================================================================================

    /// check if table exists
    async fn get_table_exists(&self, table_name: &str) -> bool;

    /// get schema from a table
    async fn get_table_schema(&self, table_name: &str) -> SqlResult<Vec<sql_adt::TableSchema>>;

    /// get table constraint from a table
    async fn get_table_constraint(
        &self,
        table_name: &str,
    ) -> SqlResult<Vec<sql_adt::TableConstraint>>;

    /// get column constraint from a table
    async fn get_column_constraint(
        &self,
        table_name: &str,
    ) -> SqlResult<Vec<sql_adt::ColumnConstraint>>;

    /// get column index from a table
    async fn get_column_index(&self, table_name: &str) -> SqlResult<Vec<sql_adt::ColumnIndex>>;

    /// list all tables
    async fn list_tables(&self) -> SqlResult<Vec<String>>;

    /// get primary key from a table
    async fn get_primary_key(&self, table_name: &str) -> SqlResult<String>;

    /// get existing ids, supposing that the primary key is a single column, and the value is a string
    async fn get_existing_ids(&self, table_name: &str, ids: &Series) -> SqlResult<D1Value>;

    /// get all indexes from a table
    // async fn get_indexes(&self, table_name: &str) -> SqlResult<Vec<String>>;

    // ================================================================================================
    // Mutation
    // ================================================================================================

    /// drop a table
    async fn drop_table(&self, table_name: &str) -> SqlResult<()>;

    /// rename a table
    async fn rename_table(&self, from: &str, to: &str) -> SqlResult<()>;

    /// truncate a table
    async fn truncate_table(&self, table_name: &str) -> SqlResult<()>;

    /// create an index
    async fn create_index(
        &self,
        table_name: &str,
        column_name: &str,
        index_name: Option<&str>,
    ) -> SqlResult<()>;

    /// drop an index
    async fn drop_index(&self, table_name: &str, index_name: &str) -> SqlResult<()>;
}

/// An engin is an interface to describe sql executor's business logic
#[async_trait]
pub trait SqlEngine: SqlHelper {
    /// connect to the database
    async fn connect(&mut self) -> SqlResult<()>;

    /// disconnect from the database
    async fn disconnect(&mut self) -> SqlResult<()>;

    /// insert data into a table, dataframe index is the primary key
    async fn insert(&self, table_name: &str, data: Fabrix) -> SqlResult<u64>;

    /// update data in a table, dataframe index is the primary key
    async fn update(&self, table_name: &str, data: Fabrix) -> SqlResult<u64>;

    /// save data into a table
    /// saving strategy:
    /// 1. Replace: no matter the table is exist, create a new table
    /// 1. Append: if the table is exist, append data to the table, otherwise failed
    /// 1. Upsert: update and insert
    /// 1. Fail: if the table is exist, do nothing, otherwise create a new table
    async fn save(
        &self,
        table_name: &str,
        data: Fabrix,
        strategy: &sql_adt::SaveStrategy,
    ) -> SqlResult<usize>;

    /// delete data from an existing table.
    async fn delete(&self, delete: &sql_adt::Delete) -> SqlResult<u64>;

    /// get data from db. If the table has primary key, DataFrame's index will be the primary key
    async fn select(&self, select: &sql_adt::Select) -> SqlResult<Fabrix>;
}

/// Executor is the core struct of db mod.
/// It plays a role of CRUD and provides data manipulation functionality.
pub struct SqlExecutor<T>
where
    T: DatabaseType,
{
    driver: SqlBuilder,
    conn_str: String,
    pool: Option<T>,
}

impl<T> SqlExecutor<T>
where
    T: DatabaseType,
{
    /// constructor
    pub fn new(conn_info: SqlConnInfo) -> Self {
        SqlExecutor {
            driver: conn_info.driver.clone(),
            conn_str: conn_info.to_string(),
            pool: None,
        }
    }
}

impl<T> FromStr for SqlExecutor<T>
where
    T: DatabaseType,
{
    type Err = SqlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut conn = s.split(':');
        let driver_str = conn
            .next()
            .ok_or_else(|| SqlError::new_common_error("Invalid conn str"))?;

        let driver = SqlBuilder::from_str(driver_str)?;
        Ok(SqlExecutor {
            driver,
            conn_str: s.to_string(),
            pool: None,
        })
    }
}

#[async_trait]
impl<T> SqlHelper for SqlExecutor<T>
where
    T: DatabaseType,
{
    async fn get_table_exists(&self, table_name: &str) -> bool {
        let que = self.driver.check_table_exists(table_name);

        // BEWARE: use fetch_optional instead of fetch_one is because `check_table_exists`
        // will only return one row or none
        if let Ok(Some(_)) = self.pool.as_ref().unwrap().fetch_optional(&que).await {
            return true;
        }

        false
    }

    async fn get_table_schema(&self, table_name: &str) -> SqlResult<Vec<sql_adt::TableSchema>> {
        conn_n_err!(self.pool);
        let que = self.driver.check_table_schema(table_name);
        let schema = [ValueType::String, ValueType::String, ValueType::String];
        let res = self
            .pool
            .as_ref()
            .unwrap()
            .fetch_all_with_schema(&que, &schema)
            .await?
            .into_iter()
            .map(|v| {
                let type_str = try_value_into_string(&v[1])?.to_uppercase();
                let dtype =
                    string_try_into_value_type(&self.driver, &type_str).unwrap_or(ValueType::Null);
                let is_nullable = try_value_into_string(&v[2])? == "YES";

                let res = sql_adt::TableSchema {
                    name: try_value_into_string(&v[0])?,
                    dtype,
                    is_nullable,
                };

                Ok(res)
            })
            .collect::<SqlResult<Vec<sql_adt::TableSchema>>>()?;

        Ok(res)
    }

    async fn get_table_constraint(
        &self,
        table_name: &str,
    ) -> SqlResult<Vec<sql_adt::TableConstraint>> {
        conn_n_err!(self.pool);
        if let SqlBuilder::Sqlite = &self.driver {
            return Err(SqlError::new_common_error(
                "Sqlite does not support constraints",
            ));
        }
        let que = self.driver.check_table_constraint(table_name);
        let schema = [ValueType::String, ValueType::String];
        let res = self
            .pool
            .as_ref()
            .unwrap()
            .fetch_all_with_schema(&que, &schema)
            .await?
            .into_iter()
            .map(|v| {
                let constraint_name = try_value_into_string(&v[0])?;
                let constraint_type = try_value_into_string(&v[1])?;
                let constraint_type = sql_adt::TableConstraintType::from_str(&constraint_type)?;
                Ok(sql_adt::TableConstraint::new(
                    constraint_name,
                    constraint_type,
                ))
            })
            .collect::<SqlResult<Vec<sql_adt::TableConstraint>>>()?;

        Ok(res)
    }

    async fn get_column_constraint(
        &self,
        table_name: &str,
    ) -> SqlResult<Vec<sql_adt::ColumnConstraint>> {
        conn_n_err!(self.pool);
        if let SqlBuilder::Sqlite = &self.driver {
            return Err(SqlError::new_common_error(
                "Sqlite does not support constraints",
            ));
        }
        let que = self.driver.check_column_constraint(table_name);
        let schema = [ValueType::String, ValueType::String];
        let res = self
            .pool
            .as_ref()
            .unwrap()
            .fetch_all_with_schema(&que, &schema)
            .await?
            .into_iter()
            .map(|v| {
                let constraint_name = try_value_into_string(&v[0])?;
                let column_name = try_value_into_string(&v[1])?;
                Ok(sql_adt::ColumnConstraint::new(constraint_name, column_name))
            })
            .collect::<SqlResult<Vec<sql_adt::ColumnConstraint>>>()?;

        Ok(res)
    }

    async fn get_column_index(&self, table_name: &str) -> SqlResult<Vec<sql_adt::ColumnIndex>> {
        conn_n_err!(self.pool);
        if let SqlBuilder::Sqlite = &self.driver {
            return Err(SqlError::new_common_error(
                "Sqlite does not support indexes",
            ));
        }
        let que = self.driver.check_column_index(table_name);
        let schema = [ValueType::String, ValueType::String];
        let res = self
            .pool
            .as_ref()
            .unwrap()
            .fetch_all_with_schema(&que, &schema)
            .await?
            .into_iter()
            .map(|v| {
                let index_name = try_value_into_string(&v[0])?;
                let column_name = try_value_into_string(&v[1])?;
                Ok(sql_adt::ColumnIndex::new(index_name, column_name))
            })
            .collect::<SqlResult<Vec<sql_adt::ColumnIndex>>>()?;

        Ok(res)
    }

    async fn list_tables(&self) -> SqlResult<Vec<String>> {
        conn_n_err!(self.pool);
        let que = self.driver.list_tables();
        let schema = [ValueType::String];
        let res = self
            .pool
            .as_ref()
            .unwrap()
            .fetch_all_with_schema(&que, &schema)
            .await?;
        if let Some(values) = res.first() {
            return values.iter().map(try_value_into_string).collect();
        }

        Err(SqlError::new_common_error("tables not found"))
    }

    async fn get_primary_key(&self, table_name: &str) -> SqlResult<String> {
        conn_n_err!(self.pool);
        let que = self.driver.get_primary_key(table_name);
        let schema = [ValueType::String];
        let res = self
            .pool
            .as_ref()
            .unwrap()
            .fetch_optional_with_schema(&que, &schema)
            .await?;

        if let Some(v) = res {
            if let Some(k) = v.first() {
                return try_value_into_string(k);
            }
        }

        Err(SqlError::new_common_error("primary key not found"))
    }

    async fn get_existing_ids(&self, table_name: &str, ids: &Series) -> SqlResult<D1Value> {
        conn_n_err!(self.pool);
        let que = self.driver.select_existing_ids(table_name, ids)?;
        let schema = vec![ids.dtype().to_owned()];
        let res = self
            .pool
            .as_ref()
            .unwrap()
            .fetch_all_with_schema(&que, schema.as_slice())
            .await?
            .iter_mut()
            .map(|v| v.remove(0))
            .collect::<Vec<Value>>();

        Ok(res)
    }

    async fn drop_table(&self, table_name: &str) -> SqlResult<()> {
        conn_n_err!(self.pool);
        let que = self.driver.drop_table(table_name);
        self.pool.as_ref().unwrap().execute(&que).await?;
        Ok(())
    }

    async fn rename_table(&self, from: &str, to: &str) -> SqlResult<()> {
        conn_n_err!(self.pool);
        let que = self.driver.rename_table(from, to);
        self.pool.as_ref().unwrap().execute(&que).await?;
        Ok(())
    }

    async fn truncate_table(&self, table_name: &str) -> SqlResult<()> {
        conn_n_err!(self.pool);
        let que = self.driver.truncate_table(table_name);
        self.pool.as_ref().unwrap().execute(&que).await?;
        Ok(())
    }

    async fn create_index(
        &self,
        table_name: &str,
        column_name: &str,
        index_name: Option<&str>,
    ) -> SqlResult<()> {
        conn_n_err!(self.pool);
        let que = self
            .driver
            .create_index(table_name, column_name, index_name);
        self.pool.as_ref().unwrap().execute(&que).await?;
        Ok(())
    }

    async fn drop_index(&self, table_name: &str, index_name: &str) -> SqlResult<()> {
        conn_n_err!(self.pool);
        let que = self.driver.drop_index(table_name, index_name);
        self.pool.as_ref().unwrap().execute(&que).await?;
        Ok(())
    }
}

#[async_trait]
impl<T> SqlEngine for SqlExecutor<T>
where
    T: DatabaseType,
{
    async fn connect(&mut self) -> SqlResult<()> {
        conn_e_err!(self.pool);
        self.pool = Some(T::connect(&self.conn_str).await?);
        Ok(())
    }

    async fn disconnect(&mut self) -> SqlResult<()> {
        conn_n_err!(self.pool);
        self.pool.as_ref().unwrap().disconnect().await;
        Ok(())
    }

    async fn insert(&self, table_name: &str, data: Fabrix) -> SqlResult<u64> {
        conn_n_err!(self.pool);
        let que = self.driver.insert(table_name, data)?;
        let res = self.pool.as_ref().unwrap().execute(&que).await?;

        Ok(res.rows_affected)
    }

    async fn update(&self, table_name: &str, data: Fabrix) -> SqlResult<u64> {
        conn_n_err!(self.pool);
        let que = self.driver.update(table_name, data)?;

        let res = self
            .pool
            .as_ref()
            .unwrap()
            .execute_many(&que)
            .await?
            .rows_affected;

        Ok(res)
    }

    async fn save(
        &self,
        table_name: &str,
        data: Fabrix,
        strategy: &sql_adt::SaveStrategy,
    ) -> SqlResult<usize> {
        conn_n_err!(self.pool);

        match strategy {
            sql_adt::SaveStrategy::FailIfExists => {
                if self.get_table_exists(table_name).await {
                    return Err(SqlError::new_common_error(
                        "table already exist, table cannot be saved",
                    ));
                }

                // start a transaction
                let txn = self.pool.as_ref().unwrap().begin_transaction().await?;

                let res = txn_create_and_insert(&self.driver, txn, table_name, data).await?;

                Ok(res as usize)
            }
            sql_adt::SaveStrategy::Replace => {
                // start a transaction
                let mut txn = self.pool.as_ref().unwrap().begin_transaction().await?;

                if self.get_table_exists(table_name).await {
                    let del_str = self.driver.drop_table(table_name);
                    txn.execute(&del_str).await?;
                }

                let res = txn_create_and_insert(&self.driver, txn, table_name, data).await?;

                Ok(res as usize)
            }
            sql_adt::SaveStrategy::Append => {
                // insert to an existing table and ignore primary key
                // this action is supposed that primary key can be auto generated
                let que = self.driver.insert(table_name, data)?;
                let res = self.pool.as_ref().unwrap().execute(&que).await?;

                Ok(res.rows_affected as usize)
            }
            sql_adt::SaveStrategy::Upsert => {
                if let Some(s) = data.index() {
                    // get existing ids from selected table
                    let existing_ids = self.get_existing_ids(table_name, &s).await?;

                    let existing_ids = Series::from_values_default_name(existing_ids, false)?;

                    // declare a df for inserting
                    let mut df_to_insert = data;
                    // popup a df for updating
                    let df_to_update = df_to_insert.popup_rows(&existing_ids)?;

                    let r1 = self.insert(table_name, df_to_insert).await?;
                    let r2 = self.update(table_name, df_to_update).await?;

                    Ok((r1 + r2) as usize)
                } else {
                    let r1 = self.insert(table_name, data).await?;
                    Ok(r1 as usize)
                }
            }
        }
    }

    async fn delete(&self, delete: &sql_adt::Delete) -> SqlResult<u64> {
        conn_n_err!(self.pool);
        let que = self.driver.delete(delete);
        let res = self.pool.as_ref().unwrap().execute(&que).await?;

        Ok(res.rows_affected)
    }

    async fn select(&self, select: &sql_adt::Select) -> SqlResult<Fabrix> {
        conn_n_err!(self.pool);

        let mut df = None::<Fabrix>;

        // if `select.include_primary_key` is true, try to get the primary key
        // if primary key does not exist, ignore it
        if let Some(true) = select.include_primary_key {
            if let Ok(pk) = self.get_primary_key(&select.table).await {
                let mut new_select = select.clone();
                add_primary_key_to_select(&pk, &mut new_select);
                let que = self.driver.select(&new_select);
                let res = self.pool.as_ref().unwrap().fetch_all_to_rows(&que).await?;
                let mut res = Fabrix::from_rows(res)?;
                res.set_index_tag(0)?;
                df = Some(res);
            }
        };

        let mut df = match df {
            Some(d) => d,
            None => {
                let que = self.driver.select(select);
                let res = self.pool.as_ref().unwrap().fetch_all(&que).await?;
                Fabrix::from_row_values(res, None, false)?
            }
        };

        df.set_column_names(&select.columns_name())?;

        Ok(df)
    }
}

/// select primary key and other columns from a table
fn add_primary_key_to_select<T: Into<String>>(primary_key: T, select: &mut sql_adt::Select) {
    select.columns.insert(0, sql_adt::Column::col(primary_key));
}

/// `Value` -> String
fn try_value_into_string(value: &Value) -> SqlResult<String> {
    match value {
        Value::String(v) => Ok(v.to_owned()),
        _ => Err(SqlError::new_common_error("value is not a string")),
    }
}

/// create table
async fn txn_create_and_insert<'a>(
    driver: &SqlBuilder,
    mut txn: LoaderTransaction<'a>,
    table_name: &str,
    data: Fabrix,
) -> SqlResult<usize> {
    // create table string
    let index_option = data
        .index_field()
        .map(sql_adt::IndexOption::try_from)
        .transpose()?;
    // if index_option is not None, data.fields should remove the index field
    let fields = match data.index_tag() {
        Some(it) => {
            let mut fields = data.fields();
            fields.remove(it.loc());
            fields
        }
        None => data.fields(),
    };
    let create_str = driver.create_table(table_name, &fields, index_option.as_ref());

    // create table
    if let Err(e) = txn.execute(&create_str).await {
        txn.rollback().await?;
        return Err(e);
    }

    // insert string
    let insert_str = driver.insert(table_name, data)?;

    // insert data
    match txn.execute(&insert_str).await {
        Ok(res) => {
            // commit transaction
            txn.commit().await?;
            Ok(res.rows_affected as usize)
        }
        Err(e) => {
            txn.rollback().await?;
            Err(e)
        }
    }
}

#[cfg(test)]
mod test_executor {
    use super::*;
    use crate::{
        date, datetime, fx, series, xpr, xpr_and, xpr_col, xpr_join, xpr_or, DatabaseMysql,
        DatabasePg, DatabaseSqlite,
    };

    const CONN1: &str = "mysql://root:secret@localhost:3306/dev";
    const CONN2: &str = "postgres://root:secret@localhost:5432/dev";
    const CONN3: &str = "sqlite://dev.sqlite";

    // table name
    const TABLE_NAME: &str = "dev";

    #[tokio::test]
    async fn test_connection() {
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        let con = exc.connect().await;
        assert!(con.is_ok());

        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        let con = exc.connect().await;
        assert!(con.is_ok());

        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        let con = exc.connect().await;
        assert!(con.is_ok());
    }

    #[tokio::test]
    async fn test_get_primary_key() {
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();

        exc.connect().await.expect("connection is ok");

        println!("{:?}", exc.get_primary_key("dev").await);
    }

    #[tokio::test]
    async fn test_save_and_select() {
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();

        exc.connect().await.expect("connection is ok");

        let df = fx![
            "id" =>	[96,97,98,99,100],
            "first_name" =>	["Blondie","Etti","Early","Adelina","Kristien"],
            "last_name" => ["D'Ruel","Klimko","Dowtry","Tunn","Rabl"],
            "email" => ["bdruel2n@sun.com","eklimko2o@arizona.edu","edowtry2p@nba.com","atunn2q@reuters.com","krabl2r@yahoo.com"],
            "gender" =>	["Genderqueer","Bigender","Non-binary","Agender","Polygender"],
            "ip_address" =>	["151.50.91.25","41.14.13.78","39.216.183.46","156.252.19.192","213.123.199.87"],
            "company" => [Some("Tekfly"),Some("Twinder"),None,Some("Omba"),Some("Mita")],
            "city" => ["Changshengqiao","Papetoai","Kabarnet","Nanqi","Colmar"],
            "birth" => ["6/16/1984","8/2/1994","7/16/1998","9/14/1980","3/20/1991"],
        ]
        .unwrap();

        let res = exc
            .save(TABLE_NAME, df, &sql_adt::SaveStrategy::Replace)
            .await;

        println!("{:?}", res);
        assert_eq!(res.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_save_quotes_into_sqlite() {
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();

        exc.connect().await.expect("connection is ok");

        let df = fx![
            "id" =>	[96,97],
            "string" => [r#"'"#,r#"""#,],
        ]
        .unwrap();

        let res = exc
            .save("string_test", df, &sql_adt::SaveStrategy::Replace)
            .await;

        println!("{:?}", res);
        assert_eq!(res.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_save_fail_if_exists() {
        // df
        let df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "James", "Lucas", "Mia"],
            "ord" => [10,11,12,20,22],
            "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8)],
            "dt" => [
                datetime!(2016,1,8,9,10,11),
                datetime!(2017,1,7,9,10,11),
                datetime!(2018,1,6,9,10,11),
                datetime!(2019,1,5,9,10,11),
                datetime!(2020,1,4,9,10,11),
            ]
        ]
        .unwrap();

        let save_strategy = sql_adt::SaveStrategy::FailIfExists;

        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        println!("{:?}", res);

        // postgres
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        println!("{:?}", res);

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_save_replace() {
        // df
        let df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "James", "Lucas", "Mia", "Livia"],
            "ord" => [10,11,12,20,22,31],
            "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8), Some(11.2)],
            "note" => [Some("FS"), Some("OP"), Some("TEC"), None, Some("SS"), None],
            "d" => [
                date!(2016,1,8),
                date!(2017,1,7),
                date!(2018,1,6),
                date!(2019,1,5),
                date!(2020,1,4),
                date!(2021,1,3),
            ],
            "dt" => [
                datetime!(2016,1,8,9,10,11),
                datetime!(2017,1,7,9,10,11),
                datetime!(2018,1,6,9,10,11),
                datetime!(2019,1,5,9,10,11),
                datetime!(2020,1,4,9,10,11),
                datetime!(2021,1,3,9,10,11),
            ]
        ]
        .unwrap();

        let save_strategy = sql_adt::SaveStrategy::Replace;

        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        println!("{:?}", res);
        assert!(res.is_ok());

        // postgres
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        println!("{:?}", res);
        assert!(res.is_ok());

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        println!("{:?}", res);
        assert!(res.is_ok());
    }

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
                datetime!(2010,2,5,9,10,11),
                datetime!(2011,2,4,9,10,11),
                datetime!(2012,2,3,9,10,11),
            ]
        ]
        .unwrap();

        let save_strategy = sql_adt::SaveStrategy::Append;

        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        assert!(res.is_ok());

        // postgres
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        assert!(res.is_ok());

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        assert!(res.is_ok());
    }

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
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        assert!(res.is_ok());

        // postgres
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        assert!(res.is_ok());

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.save(TABLE_NAME, df.clone(), &save_strategy).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete() {
        let filter = xpr!([
            xpr!("ord", "=", 15),
            xpr_or!(),
            xpr!([
                xpr!("names", "=", "Livia"),
                xpr_and!(),
                xpr!("val", ">=", 10.0),
            ])
        ]);

        let delete = sql_adt::Delete {
            table: TABLE_NAME.to_owned(),
            filter,
        };

        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.delete(&delete).await;
        assert!(res.is_ok());

        // postgres
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.delete(&delete).await;
        assert!(res.is_ok());

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.delete(&delete).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_select_primary_key() {
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.get_primary_key(TABLE_NAME).await;
        assert!(res.is_ok());

        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.get_primary_key(TABLE_NAME).await;
        assert!(res.is_ok());

        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.get_primary_key(TABLE_NAME).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_select() {
        let select = sql_adt::Select {
            table: "dev".to_owned(),
            columns: vec![
                xpr_col!("names"),
                xpr_col!("val"),
                xpr_col!("note"),
                xpr_col!("dt"),
                xpr_col!("ord"),
            ],
            ..Default::default()
        };

        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let df = exc.select(&select).await.unwrap();
        println!("{:?}", df);
        assert!(df.width() > 0);
        assert!(df.height() > 0);

        // postgres
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let df = exc.select(&select).await.unwrap();
        println!("{:?}", df);
        assert!(df.width() > 0);
        assert!(df.height() > 0);

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let df = exc.select(&select).await.unwrap();
        println!("{:?}", df);
        assert!(df.width() > 0);
        assert!(df.height() > 0);
    }

    #[tokio::test]
    async fn test_get_table_schema() {
        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let schema = exc.get_table_schema(TABLE_NAME).await.unwrap();
        println!("{:?}\n", schema);
        assert!(!schema.is_empty());

        // pg
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let schema = exc.get_table_schema(TABLE_NAME).await.unwrap();
        println!("{:?}\n", schema);
        assert!(!schema.is_empty());

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let schema = exc.get_table_schema(TABLE_NAME).await.unwrap();
        println!("{:?}\n", schema);
        assert!(!schema.is_empty());
    }

    #[tokio::test]
    async fn test_get_table_constraint() {
        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let schema = exc.get_table_constraint(TABLE_NAME).await.unwrap();
        println!("{:?}\n", schema);
        assert!(!schema.is_empty());

        // pg
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let schema = exc.get_table_constraint(TABLE_NAME).await.unwrap();
        println!("{:?}\n", schema);
        assert!(!schema.is_empty());

        // sqlite
        // WARN SQLITE doesn't support check constraints
    }

    #[tokio::test]
    async fn test_get_existing_ids() {
        let ids = series!("ord" => [10,11,14,20,21]);

        // mysql
        let mut exc = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.get_existing_ids(TABLE_NAME, &ids).await;
        assert!(res.is_ok());
        println!("{:?}", res.unwrap());

        // pg
        let mut exc = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.get_existing_ids(TABLE_NAME, &ids).await;
        assert!(res.is_ok());
        println!("{:?}", res.unwrap());

        // sqlite
        let mut exc = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc.connect().await.expect("connection is ok");

        let res = exc.get_existing_ids(TABLE_NAME, &ids).await;
        assert!(res.is_ok());
        println!("{:?}", res.unwrap());
    }

    #[tokio::test]
    async fn save_tables_with_join_relation() {
        let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc1.connect().await.expect("connection is ok");

        let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc2.connect().await.expect("connection is ok");

        let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc3.connect().await.expect("connection is ok");

        let company = fx!(
            "id";
            "id" => [1, 2, 3, 4, 5],
            "name" => ["Google", "Microsoft", "Apple", "Facebook", "Amazon"],
            "val" => [100, 200, 300, 400, 500],
            "addr" => ["1600 Amphitheatre Parkway", "One Microsoft Way", "1 Infinite Loop", "1 Microsoft Way", "1 Infinite Loop"],
        ).unwrap();

        let employee = fx!(
            "id";
            "id" => [1, 2, 3, 4, 5, 6],
            "name" => ["John", "Mike", "Mary", "Jane", "Tom", "Jerry"],
            "age" => [20, 30, 22, 24, 28, 31],
            "company_id" => [4, 2, 2, 3, 4, 1],
        )
        .unwrap();

        let save_strategy = sql_adt::SaveStrategy::Replace;

        let res = exc1
            .save("company", company.clone(), &save_strategy)
            .await
            .expect("save is ok");
        assert_eq!(res, 5);
        println!("effected rows: {:?}", res);

        let res = exc1
            .save("employee", employee.clone(), &save_strategy)
            .await
            .expect("save is ok");
        assert_eq!(res, 6);
        println!("effected rows: {:?}", res);

        let res = exc2
            .save("company", company.clone(), &save_strategy)
            .await
            .expect("save is ok");
        assert_eq!(res, 5);
        println!("effected rows: {:?}", res);

        let res = exc2
            .save("employee", employee.clone(), &save_strategy)
            .await
            .expect("save is ok");
        assert_eq!(res, 6);
        println!("effected rows: {:?}", res);

        let res = exc3
            .save("company", company, &save_strategy)
            .await
            .expect("save is ok");
        assert_eq!(res, 5);
        println!("effected rows: {:?}", res);

        let res = exc3
            .save("employee", employee, &save_strategy)
            .await
            .expect("save is ok");
        assert_eq!(res, 6);
        println!("effected rows: {:?}", res);
    }

    #[tokio::test]
    async fn select_with_join() {
        let mut exc1 = SqlExecutor::<DatabaseMysql>::from_str(CONN1).unwrap();
        exc1.connect().await.expect("connection is ok");

        let mut exc2 = SqlExecutor::<DatabasePg>::from_str(CONN2).unwrap();
        exc2.connect().await.expect("connection is ok");

        let mut exc3 = SqlExecutor::<DatabaseSqlite>::from_str(CONN3).unwrap();
        exc3.connect().await.expect("connection is ok");

        let cols = vec![
            xpr_col!("employee", "id"),
            xpr_col!("employee", "name"),
            xpr_col!("employee", "age"),
            xpr_col!("company", "id"),
            xpr_col!("company", "name"),
            xpr_col!("company", "val"),
            xpr_col!("company", "addr"),
        ];
        let join = xpr_join!("left", "employee", "company", [("company_id", "id")]).unwrap();
        let select = sql_adt::Select::new("employee").columns(&cols).join(&join);

        let fx = exc1.select(&select).await;
        assert!(fx.is_ok());
        println!("{:?}", fx);

        let fx = exc2.select(&select).await;
        assert!(fx.is_ok());
        println!("{:?}", fx);

        let fx = exc3.select(&select).await;
        assert!(fx.is_ok());
        println!("{:?}", fx);
    }
}
