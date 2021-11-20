//!  Xl reader

use crate::sources::file::{Cell, XlDataConsumer, XlDataConsumerErr};
use crate::{FabrixError, Value};

impl XlDataConsumerErr for FabrixError {
    fn new<T>(msg: T) -> Self
    where
        T: AsRef<str>,
    {
        FabrixError::from_common_error(msg.as_ref().to_string())
    }
}

// TODO:
pub trait XlDataDfConsumer {}

impl<T> XlDataConsumer for T
where
    T: XlDataDfConsumer,
{
    type OutType = Value;

    type ErrorType = FabrixError;

    fn transform(cell: Cell) -> Result<Self::OutType, Self::ErrorType> {
        todo!()
    }

    fn consume_row(batch: Vec<Self::OutType>) -> Result<(), Self::ErrorType> {
        todo!()
    }

    fn consume_batch(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
        todo!()
    }
}
