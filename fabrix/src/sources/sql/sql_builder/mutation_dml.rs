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
                    .map(|(v, inf)| try_from_value_to_svalue(v, &inf.dtype(), true))
                    .collect::<SqlResult<Vec<_>>>()?,
            );

            // make sure columns length equals records length
            statement.values(record)?;
        }

        Ok(statement!(self, statement))
    }

    /// given a `Dataframe`, update to an existing table in terms of df index
    fn update(
        &self,
        table_name: &str,
        df: DataFrame,
        index_option: &sql_adt::IndexOption,
    ) -> SqlResult<Vec<String>> {
        let column_info = df.fields();
        let indices_type = df.index_dtype().clone();
        let mut res = vec![];

        for row in df.into_iter() {
            let mut statement = Query::update();
            statement.table(alias!(table_name));

            let itr = row.data.into_iter().zip(column_info.iter());
            let mut updates = vec![];

            for (v, inf) in itr {
                let alias = alias!(&inf.name);
                let svalue = try_from_value_to_svalue(v, &inf.dtype(), true)?;
                updates.push((alias, svalue));
            }

            statement.values(updates).and_where(
                Expr::col(alias!(&index_option.name)).eq(try_from_value_to_svalue(
                    row.index,
                    &indices_type,
                    false,
                )?),
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
    use super::*;
    use crate::value;

    #[test]
    fn test_delete() {
        let delete = sql_adt::Delete {
            table: "test".to_string(),
            filter: vec![
                sql_adt::Expression::Simple(sql_adt::Condition {
                    column: "ord".to_owned(),
                    equation: sql_adt::Equation::Equal(value!(15)),
                }),
                sql_adt::Expression::Conjunction(sql_adt::Conjunction::OR),
                sql_adt::Expression::Nest(vec![
                    sql_adt::Expression::Simple(sql_adt::Condition {
                        column: "names".to_owned(),
                        equation: sql_adt::Equation::Equal(value!("X")),
                    }),
                    sql_adt::Expression::Conjunction(sql_adt::Conjunction::AND),
                    sql_adt::Expression::Simple(sql_adt::Condition {
                        column: "val".to_owned(),
                        equation: sql_adt::Equation::GreaterEqual(value!(10.0)),
                    }),
                ]),
                // this is not a correct syntax, but it works and should only be used for testing
                sql_adt::Expression::Conjunction(sql_adt::Conjunction::OR),
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
