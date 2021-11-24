//! Sql Builder: ddl mutation

use sea_query::{ColumnDef, Table};

use super::{alias, statement};
use crate::{sql_adt, DdlMutation, FieldInfo, SqlBuilder, ValueType};

impl DdlMutation for SqlBuilder {
    /// given a `Dataframe` columns, generate SQL create_table string
    fn create_table(
        &self,
        table_name: &str,
        columns: &[FieldInfo],
        index_option: Option<&sql_adt::IndexOption>,
    ) -> String {
        let mut statement = Table::create();
        statement.table(alias!(table_name)).if_not_exists();

        if let Some(idx) = index_option {
            statement.col(&mut gen_primary_col(idx));
        }

        columns.iter().for_each(|c| {
            statement.col(&mut gen_col(c));
        });

        statement!(self, statement)
    }

    /// drop a table by its name
    fn delete_table(&self, table_name: &str) -> String {
        let mut statement = Table::drop();
        statement.table(alias!(table_name));

        statement!(self, statement)
    }
}

/// generate a primary column
fn gen_primary_col(index_option: &sql_adt::IndexOption) -> ColumnDef {
    let mut cd = ColumnDef::new(alias!(&index_option.name));

    match index_option.index_type {
        sql_adt::IndexType::Int => cd.integer(),
        sql_adt::IndexType::BigInt => cd.big_integer(),
        sql_adt::IndexType::Uuid => cd.uuid(),
    };

    cd.not_null().auto_increment().primary_key();

    cd
}

/// generate column by `DataframeColumn`
fn gen_col(field: &FieldInfo) -> ColumnDef {
    let mut c = ColumnDef::new(alias!(&field.name));
    match field.dtype() {
        ValueType::Bool => c.boolean(),
        ValueType::U8 => c.integer(),
        ValueType::U16 => c.integer(),
        ValueType::U32 => c.integer(),
        ValueType::U64 => c.big_integer(),
        ValueType::I8 => c.integer(),
        ValueType::I16 => c.integer(),
        ValueType::I32 => c.integer(),
        ValueType::I64 => c.big_integer(),
        ValueType::F32 => c.double(),
        ValueType::F64 => c.float(),
        ValueType::String => c.string(),
        ValueType::Date => c.date(),
        ValueType::Time => c.time(),
        ValueType::DateTime => c.date_time(),
        ValueType::Decimal => c.decimal(),
        ValueType::Uuid => c.uuid(),
        _ => unimplemented!(),
    };

    // if !field.has_null {
    //     c.not_null();
    // }

    c
}
