//! Sql Builder: dml mutation

use sea_query::{Expr, Query};

use super::{alias, filter_builder, sql_adt, statement, try_from_value_to_svalue, DeleteOrSelect};
use crate::{DmlMutation, Fabrix, SqlBuilder, SqlResult};

impl DmlMutation for SqlBuilder {
    /// given a `Dataframe`, insert it into an existing table
    fn insert(&self, table_name: &str, fx: Fabrix) -> SqlResult<String> {
        // announce an insert statement
        let mut statement = Query::insert();
        // given a table name, insert into it
        statement.into_table(alias!(table_name));

        let columns = fx
            .fields()
            .iter()
            .map(|c| alias!(&c.name))
            .collect::<Vec<_>>();
        statement.columns(columns);
        let column_info = fx.fields();

        for row in fx.into_iter() {
            let record = row
                .data
                .into_iter()
                .zip(column_info.iter())
                .map(|(v, fi)| try_from_value_to_svalue(v, fi.dtype(), true))
                .collect::<SqlResult<Vec<_>>>()?;

            // make sure columns length equals records length
            statement.values(record)?;
        }

        Ok(statement!(self, statement))
    }

    /// given a `Dataframe`, update to an existing table in terms of df index
    ///
    /// Since bulk update is not supported by `sea-query` yet, we need to stack each row-updated
    /// into a vector and then update the whole vector sequentially.
    fn update(&self, table_name: &str, fx: Fabrix) -> SqlResult<String> {
        match fx.index_tag() {
            Some(it) => {
                let column_info = fx.fields();
                let column_name = it.name().to_owned();
                let column_loc = it.loc();
                let mut res = String::new();
                for row in fx.into_iter() {
                    let mut statement = Query::update();
                    statement.table(alias!(table_name));

                    let index_value = row.data().get(column_loc).unwrap().to_owned();
                    let index_type = row.index_dtype().unwrap();
                    let itr = row.data.into_iter().zip(column_info.iter());
                    let mut updates = vec![];

                    for (i, (v, inf)) in itr.enumerate() {
                        // skip the index column
                        if i != column_loc {
                            let svalue = try_from_value_to_svalue(v, inf.dtype(), true)?;
                            updates.push((alias!(&inf.name), svalue));
                        }
                    }

                    statement.values(updates).and_where(
                        Expr::col(alias!(&column_name)).eq(try_from_value_to_svalue(
                            index_value,
                            &index_type,
                            true,
                        )?),
                    );

                    res.push_str(&statement!(self, statement));
                    res.push_str(";\n");
                }

                Ok(res)
            }
            None => self.insert(table_name, fx),
        }
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
    use crate::{fx, sql_adt::ExpressionTransit, xpr};

    #[test]
    fn test_insert() {
        let df = fx![
            "v1" => [1, 2, 3],
            "v2" => ["a", "b", "c"],
            "v3" => [1.0, 2.0, 3.0],
            "v4" => [true, false, true],
        ]
        .unwrap();

        let insert = SqlBuilder::Postgres.insert("test", df).unwrap();
        println!("{:?}", insert);

        assert_eq!(
            insert,
            r#"INSERT INTO "test" ("v1", "v2", "v3", "v4") VALUES (1, 'a', 1, TRUE), (2, 'b', 2, FALSE), (3, 'c', 3, TRUE)"#
        );
    }

    #[test]
    fn test_insert2() {
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

        let insert = SqlBuilder::Sqlite.insert("test", df).unwrap();
        println!("{:?}", insert);
    }

    #[test]
    fn test_insert3() {
        let df = fx![
            "id" =>	[96,97],
            "string" => ["'","\""],
        ]
        .unwrap();

        let insert = SqlBuilder::Sqlite.insert("string_test", df).unwrap();
        println!("{:?}", insert);
    }

    #[test]
    fn test_insert4() {
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
        let df = fx![
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

        let u1 =
            r#"UPDATE "test" SET "v1" = 10, "v2" = 'a', "v3" = 1, "v4" = TRUE WHERE "id" = 1;"#;
        let u2 =
            r#"UPDATE "test" SET "v1" = 20, "v2" = 'b', "v3" = 2, "v4" = FALSE WHERE "id" = 2;"#;
        let u3 =
            r#"UPDATE "test" SET "v1" = 30, "v2" = 'c', "v3" = 3, "v4" = TRUE WHERE "id" = 3;"#;
        let end = "";

        assert_eq!(update, [u1, u2, u3, end].join("\n"));
    }

    #[test]
    fn test_delete() {
        let filter = sql_adt::ExpressionsBuilder::from_condition(xpr!("ord", "=", 15))
            .append(xpr!("or"))
            .append(
                sql_adt::ExpressionsBuilder::from_condition(xpr!("names", "=", "X"))
                    .append(xpr!("and"))
                    .append(xpr!("val", ">=", 10.0))
                    .finish(),
            )
            .finish();

        let delete = sql_adt::Delete {
            table: "test".to_string(),
            filter,
        };

        let foo = SqlBuilder::Mysql.delete(&delete);

        println!("{:?}", foo);

        assert_eq!(
            "DELETE FROM `test` WHERE `ord` = 15 OR (`names` = 'X' AND `val` >= 10)",
            foo
        );
    }
}
