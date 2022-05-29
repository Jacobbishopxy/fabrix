//! Sql Builder: dml query

use sea_query::{Expr, Order, Query};

use super::{alias, filter_builder, sql_adt, statement, try_from_value_to_svalue, DeleteOrSelect};
use crate::{DmlQuery, Series, SqlBuilder, SqlResult};

impl DmlQuery for SqlBuilder {
    /// given a list of ids, check existed ids (used for `upsert` method). Make sure index contains only not-null values
    fn select_existing_ids(&self, table_name: &str, index: &Series) -> SqlResult<String> {
        let mut statement = Query::select();
        let (index_name, index_dtype) = (index.name(), index.dtype());
        let ids = index
            .into_iter()
            .map(|i| try_from_value_to_svalue(i, index_dtype, false))
            .collect::<SqlResult<Vec<_>>>()?;

        statement
            .column(alias!(index_name))
            .from(alias!(table_name))
            .and_where(Expr::col(alias!(index_name)).is_in(ids));

        Ok(statement!(self, statement))
    }

    /// select from an existing table
    fn select(&self, select: &sql_adt::Select) -> String {
        let mut statement = Query::select();

        for c in &select.columns {
            statement.column(alias!(&c.original_name()));
        }

        statement.from(alias!(&select.table));

        if let Some(flt) = &select.filter {
            filter_builder(&mut DeleteOrSelect::Select(&mut statement), flt);
        }

        if let Some(ord) = &select.order {
            ord.iter().for_each(|o| match o {
                sql_adt::Order::Asc(name) => {
                    statement.order_by(alias!(name), Order::Asc);
                }
                sql_adt::Order::Desc(name) => {
                    statement.order_by(alias!(name), Order::Desc);
                }
            })
        }

        if let Some(l) = &select.limit {
            statement.limit(*l as u64);
        }

        if let Some(o) = &select.offset {
            statement.offset(*o as u64);
        }

        statement!(self, statement)
    }
}

#[cfg(test)]
mod test_query_dml {

    use super::*;
    use crate::{series, xpr, xpr_and, xpr_not, xpr_or};

    #[test]
    fn test_select_exist_ids() {
        let ids = series!("index" => [1, 2, 3, 4, 5]);
        let sql = SqlBuilder::Mysql.select_existing_ids("dev", &ids);
        println!("{:?}", sql);

        assert!(sql.is_ok());
    }

    #[test]
    fn test_select() {
        let filter = xpr!([
            xpr!("ord", "=", 15),
            xpr_or!(),
            xpr!([xpr!("names", "=", "X"), xpr_and!(), xpr!("val", ">=", 10.0),])
        ]);

        let select = SqlBuilder::Postgres.select(&sql_adt::Select {
            table: "test".to_string(),
            columns: vec![
                sql_adt::ColumnAlias::Simple("v1".to_string()),
                sql_adt::ColumnAlias::Simple("v2".to_string()),
                sql_adt::ColumnAlias::Simple("v3".to_string()),
                sql_adt::ColumnAlias::Simple("v4".to_string()),
            ],
            filter: Some(filter),
            order: Some(vec![
                sql_adt::Order::Asc("v1".to_string()),
                sql_adt::Order::Asc("v4".to_string()),
            ]),
            limit: Some(10),
            offset: Some(20),
            include_primary_key: None,
        });
        println!("{:?}", select);

        assert_eq!(
            select,
            r#"SELECT "v1", "v2", "v3", "v4" FROM "test" WHERE "ord" = 15 OR ("names" = 'X' AND "val" >= 10) ORDER BY "v1" ASC, "v4" ASC LIMIT 10 OFFSET 20"#
        );
    }

    #[test]
    fn test_xpr_select() {
        // same as above, but using xpr!

        let filter = xpr!([
            xpr!("ord", "=", 15),
            xpr_or!(),
            xpr!([xpr!("names", "=", "X"), xpr_and!(), xpr!("val", ">=", 10.0)])
        ]);

        let select = sql_adt::Select::new("test")
            .columns(&["v1", "v2", "v3", "v4"])
            .filter(&filter)
            .order(&[
                sql_adt::Order::Asc("v1".to_owned()),
                sql_adt::Order::Asc("v4".to_owned()),
            ])
            .limit(10)
            .offset(20);

        let select = SqlBuilder::Postgres.select(&select);
        println!("{:?}", select);

        assert_eq!(
            select,
            r#"SELECT "v1", "v2", "v3", "v4" FROM "test" WHERE "ord" = 15 OR ("names" = 'X' AND "val" >= 10) ORDER BY "v1" ASC, "v4" ASC LIMIT 10 OFFSET 20"#
        );
    }

    #[test]
    fn test_complex_select() {
        let filter = xpr!([
            xpr!([xpr!("names", "=", "X"), xpr_and!(), xpr!("val", ">=", 10.0)]),
            xpr_and!(),
            xpr_not!(),
            xpr!([
                xpr!("names", "in", ["Z", "A"]),
                xpr_or!(),
                xpr!("spec", "!=", "cat")
            ])
        ]);

        let select = sql_adt::Select::new("test")
            .columns(&["v1", "v2", "v3", "v4"])
            .filter(&filter);

        let select = SqlBuilder::Postgres.select(&select);
        println!("{:?}", select);

        assert_eq!(
            select,
            r#"SELECT "v1", "v2", "v3", "v4" FROM "test" WHERE ("names" = 'X' AND "val" >= 10) AND (NOT ("names" IN ('Z', 'A') OR "spec" <> 'cat'))"#
        );
    }

    #[test]
    fn test_complex_select2() {
        let filter = xpr!([
            xpr_not!(),
            xpr!("name", "=", "X"),
            xpr_or!(),
            xpr!([
                xpr!("names", "in", ["Z", "A"]),
                xpr_or!(),
                xpr!("spec", "!=", "cat")
            ])
        ]);

        let select = sql_adt::Select::new("test")
            .columns(&["v1", "v2", "v3", "v4"])
            .filter(&filter);

        let select = SqlBuilder::Postgres.select(&select);
        println!("{:?}", select);

        assert_eq!(
            select,
            r#"SELECT "v1", "v2", "v3", "v4" FROM "test" WHERE (NOT ("name" = 'X')) OR ("names" IN ('Z', 'A') OR "spec" <> 'cat')"#
        );
    }
}
