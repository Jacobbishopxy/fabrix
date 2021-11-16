//! Sql Builder: dml query

use sea_query::{Expr, Order, Query};

use super::{adt, alias, filter_builder, statement, try_from_value_to_svalue, DeleteOrSelect};
use crate::{DmlQuery, FabrixResult, Series, SqlBuilder};

impl DmlQuery for SqlBuilder {
    /// given a list of ids, check existed ids (used for `upsert` method). Make sure index contains only not-null values
    fn select_existing_ids(&self, table_name: &str, index: &Series) -> FabrixResult<String> {
        let mut statement = Query::select();
        let (index_name, index_dtype) = (index.name(), index.dtype());
        let ids = index
            .into_iter()
            .map(|i| try_from_value_to_svalue(i, &index_dtype, false))
            .collect::<FabrixResult<Vec<_>>>()?;

        statement
            .column(alias!(index_name))
            .from(alias!(table_name))
            .and_where(Expr::col(alias!(index_name)).is_in(ids));

        Ok(statement!(self, statement))
    }

    /// select from an existing table
    fn select(&self, select: &adt::Select) -> String {
        let mut statement = Query::select();

        for c in &select.columns {
            statement.column(alias!(&c.original_name()));
        }

        statement.from(alias!(&select.table));

        if let Some(flt) = &select.filter {
            filter_builder(&mut DeleteOrSelect::Select(&mut statement), flt);
        }

        if let Some(ord) = &select.order {
            ord.iter().for_each(|o| match &o.order {
                Some(ot) => match ot {
                    adt::OrderType::Asc => {
                        statement.order_by(alias!(&o.name), Order::Asc);
                    }
                    adt::OrderType::Desc => {
                        statement.order_by(alias!(&o.name), Order::Desc);
                    }
                },
                None => {
                    statement.order_by(alias!(&o.name), Order::Asc);
                }
            })
        }

        if let Some(l) = &select.limit {
            statement.limit(l.clone());
        }

        if let Some(o) = &select.offset {
            statement.offset(o.clone());
        }

        statement!(self, statement)
    }
}

#[cfg(test)]
mod test_sql {

    use super::*;
    use crate::series;

    #[test]
    fn test_select_exist_ids() {
        let ids = series!("index" => [1, 2, 3, 4, 5]);
        let sql = SqlBuilder::Mysql.select_existing_ids("dev", &ids);

        println!("{:?}", sql);
    }
}
