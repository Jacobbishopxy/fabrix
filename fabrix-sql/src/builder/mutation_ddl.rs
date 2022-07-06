//! Sql Builder: ddl mutation

use fabrix_core::{FieldInfo, ValueType};
use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table};

use super::{alias, sql_adt, statement};
use crate::{DdlMutation, SqlBuilder};

impl DdlMutation for SqlBuilder {
    /// given a `Dataframe` columns, generate SQL create_table string
    fn create_table(
        &self,
        table_name: &str,
        columns: &[FieldInfo],
        index_option: Option<&sql_adt::IndexOption>,
        if_not_exists: Option<bool>,
    ) -> String {
        let mut statement = Table::create();
        statement.table(alias!(table_name));

        if let Some(true) = if_not_exists {
            statement.if_not_exists();
        }

        if let Some(idx) = index_option {
            statement.col(&mut gen_primary_col(idx));
        }

        columns.iter().for_each(|c| {
            statement.col(&mut gen_col(c));
        });

        statement!(self, statement)
    }

    fn alter_table(&self, alter: &sql_adt::AlterTable) -> String {
        let mut statement = Table::alter();
        match alter {
            sql_adt::AlterTable::Add {
                table,
                column,
                dtype,
                is_nullable: _,
            } => {
                statement.table(alias!(table));
                //
                let field = FieldInfo::new(column, dtype.clone());
                statement.add_column(&mut gen_col(&field));
            }
            sql_adt::AlterTable::Delete { table, column } => {
                statement.table(alias!(table));
                //
                statement.drop_column(alias!(column));
            }
            sql_adt::AlterTable::Modify {
                table,
                column,
                dtype,
                is_nullable: _,
            } => {
                statement.table(alias!(table));
                let field = FieldInfo::new(column, dtype.clone());
                statement.modify_column(&mut gen_col(&field));
            }
        };

        statement!(self, statement)
    }

    fn drop_table(&self, table_name: &str) -> String {
        let mut statement = Table::drop();
        statement.table(alias!(table_name));

        statement!(self, statement)
    }

    fn rename_table(&self, from: &str, to: &str) -> String {
        let mut statement = Table::rename();
        statement.table(alias!(from), alias!(to));

        statement!(self, statement)
    }

    fn truncate_table(&self, table_name: &str) -> String {
        let mut statement = Table::truncate();
        statement.table(alias!(table_name));

        statement!(self, statement)
    }

    fn create_index(
        &self,
        table_name: &str,
        column_name: &str,
        index_name: Option<&str>,
    ) -> String {
        let mut statement = Index::create();
        let default_name = format!("idx_{column_name}");
        statement
            .name(index_name.unwrap_or(&default_name))
            .table(alias!(table_name))
            .col(alias!(column_name));

        statement!(self, statement)
    }

    fn drop_index(&self, table_name: &str, index_name: &str) -> String {
        let mut statement = Index::drop();
        statement.name(index_name).table(alias!(table_name));

        statement!(self, statement)
    }

    fn create_foreign_key(&self, foreign_key: &sql_adt::ForeignKey) -> String {
        // Sqlite does not support modification of foreign key constraints to existing tables
        let mut statement = ForeignKey::create();
        let on_delete = &foreign_key.on_delete;
        let on_update = &foreign_key.on_update;
        statement
            .name(&foreign_key.name)
            .from(
                alias!(&foreign_key.from.table),
                alias!(&foreign_key.from.column),
            )
            .to(
                alias!(&foreign_key.to.table),
                alias!(&foreign_key.to.column),
            )
            .on_delete(on_delete.into())
            .on_update(on_update.into());

        statement!(self, statement)
    }

    fn drop_foreign_key(&self, table_name: &str, key_name: &str) -> String {
        // Sqlite does not support modification of foreign key constraints to existing tables
        let mut statement = ForeignKey::drop();
        statement.table(alias!(table_name)).name(key_name);

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

// TODO: is_nullable

/// generate column by `DataframeColumn`
fn gen_col(field: &FieldInfo) -> ColumnDef {
    let mut c = ColumnDef::new(alias!(field.name()));
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

impl From<&sql_adt::ForeignKeyAction> for ForeignKeyAction {
    fn from(v: &sql_adt::ForeignKeyAction) -> Self {
        match v {
            sql_adt::ForeignKeyAction::NoAction => ForeignKeyAction::NoAction,
            sql_adt::ForeignKeyAction::Cascade => ForeignKeyAction::Cascade,
            sql_adt::ForeignKeyAction::SetNull => ForeignKeyAction::SetNull,
            sql_adt::ForeignKeyAction::SetDefault => ForeignKeyAction::SetDefault,
            sql_adt::ForeignKeyAction::Restrict => ForeignKeyAction::Restrict,
        }
    }
}

#[cfg(test)]
mod test_mutation_ddl {

    use super::*;

    #[test]
    fn test_create_table_mysql() {
        let index_option = sql_adt::IndexOption::new("idx", sql_adt::IndexType::Uuid);

        let create_table = SqlBuilder::Mysql.create_table(
            "test",
            &[
                FieldInfo::new("v1", ValueType::U8),
                FieldInfo::new("v2", ValueType::String),
                FieldInfo::new("v3", ValueType::U64),
                FieldInfo::new("v4", ValueType::Bool),
                FieldInfo::new("v5", ValueType::Date),
            ],
            Some(&index_option),
            Some(true),
        );
        println!("{}", create_table);

        assert_eq!(
            create_table,
            r#"CREATE TABLE IF NOT EXISTS `test` ( `idx` binary(16) NOT NULL AUTO_INCREMENT PRIMARY KEY, `v1` int, `v2` varchar(255), `v3` bigint, `v4` bool, `v5` date )"#
        );
    }

    #[test]
    fn test_create_table_pg() {
        let index_option = sql_adt::IndexOption::new("idx", sql_adt::IndexType::BigInt);

        let create_table = SqlBuilder::Postgres.create_table(
            "test",
            &[
                FieldInfo::new("v1", ValueType::U8),
                FieldInfo::new("v2", ValueType::String),
                FieldInfo::new("v3", ValueType::U64),
                FieldInfo::new("v4", ValueType::Bool),
                FieldInfo::new("v5", ValueType::Date),
            ],
            Some(&index_option),
            Some(true),
        );
        println!("{}", create_table);

        assert_eq!(
            create_table,
            r#"CREATE TABLE IF NOT EXISTS "test" ( "idx" bigserial NOT NULL PRIMARY KEY, "v1" integer, "v2" varchar, "v3" bigint, "v4" bool, "v5" date )"#
        );
    }

    #[test]
    fn test_delete_table() {
        let delete_table = SqlBuilder::Sqlite.drop_table("test");

        println!("{:?}", delete_table);

        assert_eq!(delete_table, r#"DROP TABLE "test""#);
    }
}
