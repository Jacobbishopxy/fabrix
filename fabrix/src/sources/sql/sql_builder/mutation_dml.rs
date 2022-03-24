//! Sql Builder: dml mutation

use sea_query::{Expr, Query};

use super::{alias, filter_builder, sql_adt, statement, try_from_value_to_svalue, DeleteOrSelect};
use crate::{DataFrame, DmlMutation, SqlBuilder, SqlResult};

impl DmlMutation for SqlBuilder {
    /// given a `Dataframe`, insert it into an existing table
    fn insert(&self, table_name: &str, df: DataFrame, ignore_index: bool) -> SqlResult<String> {
        // announce an insert statement
        let mut statement = Query::insert();
        // given a table name, insert into it
        statement.into_table(alias!(table_name));

        let mut columns = Vec::new();
        let mut column_info = Vec::new();
        // if the index is not ignored, insert as the primary key
        if !ignore_index {
            column_info = vec![df.index.field()];
            columns = vec![alias!(df.index.name())];
        }
        // the rest of the dataframe's columns
        columns.extend(df.fields().iter().map(|c| alias!(&c.name)));
        statement.columns(columns);

        column_info.extend(df.fields());
        for c in df.into_iter() {
            let mut record = Vec::new();
            if !ignore_index {
                let index_type = c.index_dtype();
                record = vec![try_from_value_to_svalue(c.index, &index_type, false)?];
            }
            record.extend(
                c.data
                    .into_iter()
                    .zip(column_info.iter())
                    .map(|(v, inf)| try_from_value_to_svalue(v, inf.dtype(), true))
                    .collect::<SqlResult<Vec<_>>>()?,
            );

            // make sure columns length equals records length
            statement.values(record)?;
        }

        Ok(statement!(self, statement))
    }

    /// given a `Dataframe`, update to an existing table in terms of df index
    ///
    /// Since bulk update is not supported by `sea-query` yet, we need to stack each row-updated
    /// into a vector and then update the whole vector sequentially.
    fn update(&self, table_name: &str, df: DataFrame) -> SqlResult<Vec<String>> {
        let column_info = df.fields();
        let index_field = df.index_field();
        let index_type = index_field.dtype();
        let index_name = index_field.name();
        let mut res = vec![];

        for row in df.into_iter() {
            let mut statement = Query::update();
            statement.table(alias!(table_name));

            let itr = row.data.into_iter().zip(column_info.iter());
            let mut updates = vec![];

            for (v, inf) in itr {
                let alias = alias!(&inf.name);
                let svalue = try_from_value_to_svalue(v, inf.dtype(), true)?;
                updates.push((alias, svalue));
            }

            statement.values(updates).and_where(
                Expr::col(alias!(index_name))
                    .eq(try_from_value_to_svalue(row.index, index_type, false)?),
            );

            statement!(res; self, statement)
        }

        Ok(res)
    }

    /// delete from an existing table
    fn delete(&self, delete: &sql_adt::Delete) -> String {
        let mut statement = Query::delete();
        statement.from_table(alias!(&delete.table));

        filter_builder(&mut DeleteOrSelect::Delete(&mut statement), &delete.filter);

        statement!(self, statement)
    }
}

#[cfg(test)]
mod test_mutation_dml {
    use sea_query::{MysqlQueryBuilder, PostgresQueryBuilder, SqliteQueryBuilder};

    use super::*;
    use crate::{df, xpr_and, xpr_nest, xpr_or, xpr_simple};

    #[test]
    fn test_insert() {
        let df = df![
            "v1" => [1, 2, 3],
            "v2" => ["a", "b", "c"],
            "v3" => [1.0, 2.0, 3.0],
            "v4" => [true, false, true],
        ]
        .unwrap();

        let insert = SqlBuilder::Postgres.insert("test", df, true).unwrap();
        println!("{:?}", insert);

        assert_eq!(
            insert,
            r#"INSERT INTO "test" ("v1", "v2", "v3", "v4") VALUES (1, 'a', 1, TRUE), (2, 'b', 2, FALSE), (3, 'c', 3, TRUE)"#
        );
    }

    #[test]
    fn test_insert2() {
        let df = df![
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

        let insert = SqlBuilder::Sqlite.insert("test", df, true).unwrap();
        println!("{:?}", insert);
    }

    #[test]
    fn test_insert3() {
        let query = Query::insert()
            .into_table(alias!("test"))
            .columns(vec![alias!("name"), alias!("age")])
            .values_panic(vec!["D'Ruel".into(), 2i32.into()])
            .values_panic(vec!["A'd".into(), 3i32.into()])
            .to_owned();

        let str_sqlite = query.to_string(SqliteQueryBuilder);
        println!("sqlite: {:?}", str_sqlite);

        let str_postgres = query.to_string(PostgresQueryBuilder);
        println!("postgres: {:?}", str_postgres);

        let str_mysql = query.to_string(MysqlQueryBuilder);
        println!("mysql: {:?}", str_mysql);
    }

    #[test]
    fn test_update() {
        let df = df![
            "id";
            "id" => [1, 2, 3],
            "v1" => [10, 20, 30],
            "v2" => ["a", "b", "c"],
            "v3" => [1.0, 2.0, 3.0],
            "v4" => [true, false, true],
        ]
        .unwrap();

        let update = SqlBuilder::Postgres.update("test", df).unwrap();

        println!("{:?}", update);

        assert_eq!(
            update,
            vec![
                r#"UPDATE "test" SET "v1" = 10, "v2" = 'a', "v3" = 1, "v4" = TRUE WHERE "id" = 1"#,
                r#"UPDATE "test" SET "v1" = 20, "v2" = 'b', "v3" = 2, "v4" = FALSE WHERE "id" = 2"#,
                r#"UPDATE "test" SET "v1" = 30, "v2" = 'c', "v3" = 3, "v4" = TRUE WHERE "id" = 3"#,
            ],
        );
    }

    #[test]
    fn test_delete() {
        let delete = sql_adt::Delete {
            table: "test".to_string(),
            filter: vec![
                xpr_simple!("ord", "=", 15),
                xpr_or!(),
                xpr_nest!(
                    xpr_simple!("names", "=", "X"),
                    xpr_and!(),
                    xpr_simple!("val", ">=", 10.0)
                ),
            ],
        };

        let foo = SqlBuilder::Mysql.delete(&delete);

        println!("{:?}", foo);

        assert_eq!(
            "DELETE FROM `test` WHERE `ord` = 15 OR (`names` = 'X' AND `val` >= 10)",
            foo
        );
    }
}
