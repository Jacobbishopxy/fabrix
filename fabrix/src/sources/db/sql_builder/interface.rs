//! Sql builder interface

use crate::{adt, DataFrame, FabrixResult, FieldInfo, Series};

// DDL Query
pub trait DdlQuery {
    fn check_table_exists(&self, table_name: &str) -> String;

    fn check_table_schema(&self, table_name: &str) -> String;

    fn list_tables(&self) -> String;

    fn get_primary_key(&self, table_name: &str) -> String;
}

// DDL Mutation
pub trait DdlMutation {
    fn create_table(
        &self,
        table_name: &str,
        columns: &[FieldInfo],
        index_option: Option<&adt::IndexOption>,
    ) -> String;

    fn delete_table(&self, table_name: &str) -> String;

    // fn alter_table(&self) -> Vec<String>;

    // fn drop_table(&self, table_name: &str) -> String;

    // fn rename_table(&self, from: &str, to: &str) -> String;

    // fn truncate_table(&self, table_name: &str) -> String;

    // fn create_index(&self) -> String;

    // fn drop_index(&self) -> String;

    // fn create_foreign_key(&self) -> String;

    // fn drop_foreign_key(&self) -> String;
}

// DML Query
pub trait DmlQuery {
    fn select_existing_ids(&self, table_name: &str, index: &Series) -> FabrixResult<String>;

    fn select(&self, select: &adt::Select) -> String;
}

// DML Mutation
pub trait DmlMutation {
    fn insert(&self, table_name: &str, df: DataFrame, ignore_index: bool) -> FabrixResult<String>;

    fn update(
        &self,
        table_name: &str,
        df: DataFrame,
        index_option: &adt::IndexOption,
    ) -> FabrixResult<Vec<String>>;

    fn delete(&self, delete: &adt::Delete) -> String;
}
