//! Types
//!
//! Bson types and Fabrix types

use std::time::UNIX_EPOCH;

use bson::{Binary, Bson, Document};
use fabrix_core::{value, Bytes, Fabrix, Value};

use crate::{MgError, MgResult};

pub fn try_from_value_to_bson(value: Value) -> MgResult<Bson> {
    match value {
        Value::Bool(v) => Ok(Bson::Boolean(v)),
        Value::U8(v) => Ok(Bson::Int32(v as i32)),
        Value::U16(v) => Ok(Bson::Int32(v as i32)),
        Value::U32(v) => Ok(Bson::Int64(v as i64)),
        Value::U64(_) => Err(MgError::InvalidArgument(
            "value U64 cannot be converted to Bson",
        )),
        Value::I8(v) => Ok(Bson::Int32(v as i32)),
        Value::I16(v) => Ok(Bson::Int32(v as i32)),
        Value::I32(v) => Ok(Bson::Int32(v)),
        Value::I64(v) => Ok(Bson::Int64(v)),
        Value::F32(v) => Ok(Bson::Double(v as f64)),
        Value::F64(v) => Ok(Bson::Double(v)),
        Value::Date(_) => Err(MgError::InvalidArgument(
            "value Date cannot be converted to Bson",
        )),
        Value::Time(_) => Err(MgError::InvalidArgument(
            "value Time cannot be converted to Bson",
        )),
        Value::DateTime(v) => Ok(Bson::DateTime(bson::DateTime::from_millis(v))),
        Value::String(v) => Ok(Bson::String(v)),
        Value::Decimal(_) => Err(MgError::InvalidArgument(
            "value Decimal cannot be converted to Bson",
        )),
        Value::Uuid(v) => Ok(Bson::String(v.to_string())),
        Value::Bytes(v) => Ok(Bson::Binary(Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: v.0,
        })),
        Value::Null => Ok(Bson::Null),
    }
}

fn to_le_i64(val: bson::Timestamp) -> i64 {
    let upper = (val.time.to_le() as u64) << 32;
    let lower = val.increment.to_le() as u64;

    (upper | lower) as i64
}

pub fn try_from_bson_to_value(bson: Bson) -> MgResult<Value> {
    match bson {
        Bson::Double(v) => Ok(value!(v)),
        Bson::String(v) => Ok(value!(v)),
        Bson::Array(_) => Err(MgError::InvalidArgument(
            "bson array cannot be converted to value",
        )),
        Bson::Document(_) => Err(MgError::InvalidArgument(
            "bson document cannot be converted to value",
        )),
        Bson::Boolean(v) => Ok(value!(v)),
        Bson::Null => Ok(Value::Null),
        Bson::RegularExpression(_) => Err(MgError::InvalidArgument(
            "bson regular_expression cannot be converted to value",
        )),
        Bson::JavaScriptCode(_) => Err(MgError::InvalidArgument(
            "bson javascript_code cannot be converted to value",
        )),
        Bson::JavaScriptCodeWithScope(_) => Err(MgError::InvalidArgument(
            "bson javascript_code_with_scope cannot be converted to value",
        )),
        Bson::Int32(v) => Ok(value!(v)),
        Bson::Int64(v) => Ok(value!(v)),
        Bson::Timestamp(v) => Ok(Value::DateTime(to_le_i64(v))),
        Bson::Binary(v) => Ok(Value::Bytes(Bytes(v.bytes))),
        Bson::ObjectId(v) => Ok(Value::String(v.to_string())),
        Bson::DateTime(v) => {
            let v = v
                .to_system_time()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| MgError::InvalidArgument("bson datetime conversion failed"))?
                .as_nanos();
            Ok(Value::DateTime(v as i64))
        }
        Bson::Symbol(v) => Ok(value!(v)),
        Bson::Decimal128(_) => Err(MgError::InvalidArgument(
            "bson decimal128 cannot be converted to value",
        )),
        Bson::Undefined => Ok(Value::Null),
        Bson::MaxKey => Err(MgError::InvalidArgument(
            "bson max_key cannot be converted to value",
        )),
        Bson::MinKey => Err(MgError::InvalidArgument(
            "bson min_key cannot be converted to value",
        )),
        Bson::DbPointer(_) => Err(MgError::InvalidArgument(
            "bson db_pointer cannot be converted to value",
        )),
    }
}

pub fn try_from_fabrix_to_document(fabrix: Fabrix) -> MgResult<Document> {
    todo!()
}

pub fn try_from_document_to_fabrix(document: Document) -> MgResult<Fabrix> {
    todo!()
}
