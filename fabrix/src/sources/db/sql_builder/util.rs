//! Sql Builder: Util

use sea_query::{Cond, DeleteStatement, Expr, SelectStatement};

use super::{adt, alias};

/// delete or select statement, since their `where` clause are the same
pub(crate) enum DeleteOrSelect<'a> {
    Delete(&'a mut DeleteStatement),
    Select(&'a mut SelectStatement),
}

/// A general function to build Sql conditions for Delete and Select statements
pub(crate) fn filter_builder(s: &mut DeleteOrSelect, flt: &[adt::Expression]) {
    let mut vec_cond = vec![];

    cond_builder(&mut vec_cond, flt);

    vec_cond.iter().for_each(|c| match s {
        DeleteOrSelect::Delete(qs) => {
            qs.cond_where(c.clone());
        }
        DeleteOrSelect::Select(qs) => {
            qs.cond_where(c.clone());
        }
    });
}

/// condition builder
fn cond_builder(vec_cond: &mut Vec<Cond>, flt: &[adt::Expression]) {
    let mut iter = flt.iter().enumerate().peekable();

    loop {
        if let Some((i, e)) = iter.next() {
            // peek next element, it should be a logical operator (AND/OR)
            if let Some((i, e)) = iter.peek() {
                // odd index
                if i % 2 == 1 {
                    match e {
                        adt::Expression::Conjunction(c) => match c {
                            adt::Conjunction::AND => vec_cond.push(Cond::all()),
                            adt::Conjunction::OR => vec_cond.push(Cond::any()),
                        },
                        _ => panic!("wrong expression {:?}, needs a conjunction", e),
                    }
                }
            }

            // even index is simple/nest expression
            if i % 2 == 0 {
                match e {
                    adt::Expression::Simple(c) => {
                        let tmp_expr = Expr::col(alias!(&c.column));
                        let tmp_expr = match &c.equation {
                            adt::Equation::Not => tmp_expr.not(),
                            adt::Equation::Equal(d) => tmp_expr.eq(d),
                            adt::Equation::NotEqual(d) => tmp_expr.ne(d),
                            adt::Equation::Greater(d) => tmp_expr.gt(d),
                            adt::Equation::GreaterEqual(d) => tmp_expr.gte(d),
                            adt::Equation::Less(d) => tmp_expr.lt(d),
                            adt::Equation::LessEqual(d) => tmp_expr.lte(d),
                            adt::Equation::In(d) => tmp_expr.is_in(d),
                            adt::Equation::Between(d) => tmp_expr.between(&d.0, &d.1),
                            adt::Equation::Like(d) => tmp_expr.like(&d),
                        };
                        let last = vec_cond.last().unwrap().clone();
                        let mut_last = vec_cond.last_mut().unwrap();
                        *mut_last = last.add(tmp_expr);
                    }
                    adt::Expression::Nest(ve) => {
                        cond_builder(vec_cond, ve);
                    }
                    _ => panic!("wrong expression {:?}, needs a simple or nested variant", e),
                }
            }
        } else {
            break;
        }
    }
}
