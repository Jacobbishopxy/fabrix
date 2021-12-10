//! xl -> json
//!
//!

use bson2::Bson;

use crate::xl;

// pub type XlBsonExecutor = xl::XlExecutor<>;

pub struct XlBson;

pub struct XlBsonConvertor {}

impl xl::XlConsumer<XlBsonConvertor> for XlBson {
    type UnitOut = Bson;
    type FinalOut = Bson;

    fn transform(_cell: xl::Cell) -> Self::UnitOut {
        todo!()
    }
}
