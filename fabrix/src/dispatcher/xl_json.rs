//! xl -> json

use serde_json::Value as JsonValue;

use crate::xl;

pub struct XlJson;

pub struct XlJsonConvertor {}

impl xl::XlConsumer<XlJsonConvertor> for XlJson {
    type UnitOut = JsonValue;
    type FinalOut = JsonValue;

    fn transform(_cell: xl::Cell) -> Self::UnitOut {
        todo!()
    }
}

#[cfg(test)]
mod xl_json_tests {
    // use super::*;

    #[test]
    fn name() {
        unimplemented!();
    }
}
