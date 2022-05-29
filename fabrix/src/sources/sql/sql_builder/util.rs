//! Sql Builder: Util

use sea_query::{Cond, ConditionExpression, DeleteStatement, Expr, SelectStatement};

use super::{alias, sql_adt};

/// delete or select statement, since their `where` clause are the same
pub(crate) enum DeleteOrSelect<'a> {
    Delete(&'a mut DeleteStatement),
    Select(&'a mut SelectStatement),
}

#[derive(Default)]
struct RecursiveState {
    cond: Option<Cond>,
    negate: bool,
}

impl RecursiveState {
    fn new() -> Self {
        RecursiveState::default()
    }

    fn set_cond_if_empty(&mut self, cond: Cond) {
        if self.cond.is_none() {
            self.cond = Some(cond)
        }
    }

    fn set_negate(&mut self, negate: bool) {
        self.negate = negate;
    }

    fn add<C: Into<ConditionExpression>>(&mut self, cond: C) {
        self.cond = Some(match self.cond.take() {
            Some(c) => c.add(cond),
            None => Cond::all().add(cond),
        });
    }

    fn negate_cond(&mut self) {
        self.cond = Some(match self.cond.take() {
            Some(c) => c.not(),
            None => Cond::all().not(),
        })
    }
}

/// A general function to build Sql conditions for Delete and Select statements
pub(crate) fn filter_builder(s: &mut DeleteOrSelect, flt: &sql_adt::Expressions) {
    let mut state = RecursiveState::new();
    cond_builder(&flt.0, &mut state);

    match s {
        DeleteOrSelect::Delete(d) => {
            state.cond.take().map(|c| d.cond_where(c));
        }
        DeleteOrSelect::Select(s) => {
            state.cond.take().map(|c| s.cond_where(c));
        }
    }
}

// TODO: remove negate

fn cond_builder(flt: &[sql_adt::Expression], state: &mut RecursiveState) {
    let mut iter = flt.iter().peekable();

    while let Some(e) = iter.next() {
        // move forward and get the next expr
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
                    if permit {
                        match c {
                            sql_adt::Conjunction::AND => {
                                if state.negate {
                                    state.set_cond_if_empty(Cond::all().not());
                                } else {
                                    state.set_cond_if_empty(Cond::all());
                                }
                                state.set_negate(false);
                            }
                            sql_adt::Conjunction::OR => {
                                if state.negate {
                                    state.set_cond_if_empty(Cond::any().not())
                                } else {
                                    state.set_cond_if_empty(Cond::any())
                                }
                                state.set_negate(false);
                            }
                        }
                    }
                }
                sql_adt::Expression::Opposition(_) => {
                    if matches!(e, sql_adt::Expression::Conjunction(_)) {
                        state.set_negate(true)
                    }
                }
                _ => {}
            }
        }

        match e {
            sql_adt::Expression::Simple(s) => {
                let expr = Expr::col(alias!(s.column()));
                let expr = match s.equation() {
                    sql_adt::Equation::Equal(v) => expr.eq(v),
                    sql_adt::Equation::NotEqual(v) => expr.ne(v),
                    sql_adt::Equation::Greater(v) => expr.gt(v),
                    sql_adt::Equation::GreaterEqual(v) => expr.gte(v),
                    sql_adt::Equation::Less(v) => expr.lt(v),
                    sql_adt::Equation::LessEqual(v) => expr.lte(v),
                    sql_adt::Equation::In(v) => expr.is_in(v),
                    sql_adt::Equation::Between(v) => expr.between(&v.0, &v.1),
                    sql_adt::Equation::Like(v) => expr.like(v),
                };

                if state.negate {
                    state.negate_cond();
                }
                state.add(expr);
                state.set_negate(false);
            }
            sql_adt::Expression::Nest(n) => {
                let mut ns = RecursiveState::new();
                ns.set_negate(state.negate);
                cond_builder(n, &mut ns);
                if let Some(c) = ns.cond.take() {
                    state.add(c);
                    state.set_negate(false);
                }
            }
            _ => {}
        }
    }
}
