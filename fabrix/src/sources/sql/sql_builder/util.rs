//! Sql Builder: Util

use sea_query::{Cond, DeleteStatement, Expr, SelectStatement};

use super::{alias, sql_adt};

/// delete or select statement, since their `where` clause are the same
pub(crate) enum DeleteOrSelect<'a> {
    Delete(&'a mut DeleteStatement),
    Select(&'a mut SelectStatement),
}

struct RecursiveState {
    conds: Vec<Cond>,
    opposite: bool,
}

impl RecursiveState {
    fn new() -> Self {
        Self {
            conds: Vec::new(),
            opposite: false,
        }
    }
}

/// A general function to build Sql conditions for Delete and Select statements
pub(crate) fn filter_builder(s: &mut DeleteOrSelect, flt: &sql_adt::Expressions) {
    let mut state = RecursiveState::new();
    cond_builder(&flt.0, &mut state);

    state.conds.iter().for_each(|c| match s {
        DeleteOrSelect::Delete(qs) => {
            qs.cond_where(c.clone());
        }
        DeleteOrSelect::Select(qs) => {
            qs.cond_where(c.clone());
        }
    });
}

/// condition builder
fn cond_builder(flt: &[sql_adt::Expression], state: &mut RecursiveState) {
    let mut iter = flt.iter().peekable();

    while let Some(e) = iter.next() {
        let peek = iter.peek();

        if let Some(ne) = peek {
            // if same type of expression in a row, skip the former one
            if &e == ne {
                continue;
            }

            match ne {
                sql_adt::Expression::Conjunction(c) => {
                    let permit = matches!(
                        e,
                        sql_adt::Expression::Simple(_) | sql_adt::Expression::Nest(_)
                    );
                    // TODO: opposite dysfunctional
                    if permit {
                        match c {
                            sql_adt::Conjunction::AND => {
                                let cond = if state.opposite {
                                    // println!("{:?}", 66);
                                    state.opposite = false;
                                    Cond::all().not()
                                } else {
                                    Cond::all()
                                };
                                state.conds.push(cond);
                            }
                            sql_adt::Conjunction::OR => {
                                let cond = if state.opposite {
                                    // println!("{:?}", 76);
                                    state.opposite = false;
                                    Cond::any().not()
                                } else {
                                    Cond::any()
                                };
                                state.conds.push(cond);
                            }
                        }
                    }
                }
                sql_adt::Expression::Opposition(_) => {
                    if matches!(e, sql_adt::Expression::Conjunction(_)) {
                        state.opposite = true;
                    }
                }
                _ => {}
            }
        }

        match e {
            sql_adt::Expression::Simple(s) => {
                let tmp_expr = Expr::col(alias!(s.column()));
                let tmp_expr = match s.equation() {
                    sql_adt::Equation::Equal(v) => tmp_expr.eq(v),
                    sql_adt::Equation::NotEqual(v) => tmp_expr.ne(v),
                    sql_adt::Equation::Greater(v) => tmp_expr.gt(v),
                    sql_adt::Equation::GreaterEqual(v) => tmp_expr.gte(v),
                    sql_adt::Equation::Less(v) => tmp_expr.lt(v),
                    sql_adt::Equation::LessEqual(v) => tmp_expr.lte(v),
                    sql_adt::Equation::In(v) => tmp_expr.is_in(v),
                    sql_adt::Equation::Between(v) => tmp_expr.between(&v.0, &v.1),
                    sql_adt::Equation::Like(v) => tmp_expr.like(v),
                };

                let last = state.conds.pop().unwrap_or_else(Cond::all);
                state.conds.push(last.add(tmp_expr));
            }
            sql_adt::Expression::Nest(n) => {
                cond_builder(n, state);
            }
            _ => {}
        }
    }
}
