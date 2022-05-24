//! Sql builder interface

use super::sql_adt;
use crate::{Fabrix, FieldInfo, Series, SqlResult};

// DDL Query
pub trait DdlQuery {
    fn check_table_exists(&self, table_name: &str) -> String;

    fn check_table_schema(&self, table_name: &str) -> String;

    fn check_table_constraint(&self, table_name: &str) -> String;

    fn check_column_constraint(&self, table_name: &str) -> String;

    fn list_tables(&self) -> String;

    fn get_primary_key(&self, table_name: &str) -> String;
}

// DDL Mutation
pub trait DdlMutation {
    fn create_table(
        &self,
        table_name: &str,
        columns: &[FieldInfo],
        index_option: Option<&sql_adt::IndexOption>,
    ) -> String;

    fn alter_table(&self, alter: &sql_adt::AlterTable) -> String;

    fn drop_table(&self, table_name: &str) -> String;

    fn rename_table(&self, from: &str, to: &str) -> String;

    fn truncate_table(&self, table_name: &str) -> String;

    fn create_index(&self, table_name: &str, column_name: &str, index_name: Option<&str>)
        -> String;

    fn drop_index(&self, table_name: &str, index_name: &str) -> String;

    fn create_foreign_key(&self, foreign_key: &sql_adt::ForeignKey) -> String;

    fn drop_foreign_key(&self, table_name: &str, key_name: &str) -> String;
}

// DML Query
pub trait DmlQuery {
    fn select_existing_ids(&self, table_name: &str, index: &Series) -> SqlResult<String>;

    fn select(&self, select: &sql_adt::Select) -> String;
}

// DML Mutation
pub trait DmlMutation {
    fn insert(&self, table_name: &str, fx: Fabrix) -> SqlResult<String>;

    fn update(&self, table_name: &str, fx: Fabrix) -> SqlResult<String>;

    fn delete(&self, delete: &sql_adt::Delete) -> String;
}
