//! Sql Builder: Util

use sea_query::{Cond, DeleteStatement, Expr, SelectStatement};

use super::{alias, sql_adt};

/// delete or select statement, since their `where` clause are the same
pub(crate) enum DeleteOrSelect<'a> {
    Delete(&'a mut DeleteStatement),
    Select(&'a mut SelectStatement),
}

/// A general function to build Sql conditions for Delete and Select statements
pub(crate) fn filter_builder(s: &mut DeleteOrSelect, flt: &sql_adt::Expressions) {
    let mut vec_cond = vec![];

    cond_builder(&mut vec_cond, &flt.0);

    vec_cond.iter().for_each(|c| match s {
        DeleteOrSelect::Delete(qs) => {
            qs.cond_where(c.clone());
        }
        DeleteOrSelect::Select(qs) => {
            qs.cond_where(c.clone());
        }
    });
}

// TODO: since `NOT` has been added to Conjunction, we should reconsider the construction of `Cond`

/// condition builder
fn cond_builder(vec_cond: &mut Vec<Cond>, flt: &[sql_adt::Expression]) {
    let mut iter = flt.iter().enumerate().peekable();

    while let Some((i, e)) = iter.next() {
        if let Some((i, e)) = iter.peek() {
            // odd index
            if i % 2 == 1 {
                match e {
                    sql_adt::Expression::Conjunction(c) => match c {
                        sql_adt::Conjunction::AND => vec_cond.push(Cond::all()),
                        sql_adt::Conjunction::OR => vec_cond.push(Cond::any()),
                    },
                    // make sure odd index is conjunction
                    _ => panic!("wrong expression {:?}, needs a conjunction", e),
                }
            }
        }

        // even index is simple/nest expression
        if i % 2 == 0 {
            match e {
                sql_adt::Expression::Simple(c) => {
                    let tmp_expr = Expr::col(alias!(&c.column));
                    let tmp_expr = match &c.equation {
                        sql_adt::Equation::Equal(d) => tmp_expr.eq(d),
                        sql_adt::Equation::NotEqual(d) => tmp_expr.ne(d),
                        sql_adt::Equation::Greater(d) => tmp_expr.gt(d),
                        sql_adt::Equation::GreaterEqual(d) => tmp_expr.gte(d),
                        sql_adt::Equation::Less(d) => tmp_expr.lt(d),
                        sql_adt::Equation::LessEqual(d) => tmp_expr.lte(d),
                        sql_adt::Equation::In(d) => tmp_expr.is_in(d),
                        sql_adt::Equation::Between(d) => tmp_expr.between(&d.0, &d.1),
                        sql_adt::Equation::Like(d) => tmp_expr.like(d),
                    };
                    let last = vec_cond.last().unwrap().clone();
                    let mut_last = vec_cond.last_mut().unwrap();
                    *mut_last = last.add(tmp_expr);
                }
                sql_adt::Expression::Nest(ve) => {
                    cond_builder(vec_cond, ve);
                }
                // make sure even index is simple/nest expression
                _ => panic!("wrong expression {:?}, needs a simple or nested variant", e),
            }
        }
    }
}
