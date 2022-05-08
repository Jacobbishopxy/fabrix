//! Fabrix core macros
//!
//! This module contains macros used by the Fabrix core:
//! 1. impl_custom_value
//! 1. impl_custom_value2

/// impl custom object type value. Used in `value.rs`.
///
/// Equivalent to:
///
/// rust```
/// impl Debug for Date {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "{}", self.0)
///     }
/// }
///
/// impl Display for Date {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "{}", self.0)
///     }
/// }
///
/// impl PolarsObject for Date {
///     fn type_name() -> &'static str {
///         "Date"
///     }
/// }
/// ```
macro_rules! impl_custom_value {
    ($dtype:ty, $name:expr) => {
        impl Debug for $dtype {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl Display for $dtype {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl PolarsObject for $dtype {
            fn type_name() -> &'static str {
                $name
            }
        }
    };
}

/// the only difference between this and `impl_custom_value` is that this
/// macro `Debug` & `Display` use `self` in the `write!` format string.
macro_rules! impl_custom_value2 {
    ($dtype:ty, $name:expr) => {
        impl Debug for $dtype {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self)
            }
        }

        impl Display for $dtype {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self)
            }
        }

        impl PolarsObject for $dtype {
            fn type_name() -> &'static str {
                $name
            }
        }
    };
}

pub(crate) use impl_custom_value;
pub(crate) use impl_custom_value2;

/// Type conversion: standard type into Value. Used in `value.rs`.
///
/// Equivalent to:
///
/// ```rust,ignore
/// impl From<Option<bool>> for Value {
///     fn from(ov: Option<bool>) -> Self {
///         match ov {
///             Some(v) => Value::Bool(v)
///             None => Value::Null
///         }
///         Value(Value::Bool(v))
///     }
/// }
/// ```
///
/// and:
///
/// ```rust,ignore
/// impl From<bool> for Value {
///     fn from(v: bool) -> Self {
///         Value(Value::Bool(v))
///     }
/// }
/// ```
macro_rules! impl_value_from {
    (Option<$ftype:ty>, $val_var:ident) => {
        impl From<Option<$ftype>> for Value {
            fn from(ov: Option<$ftype>) -> Self {
                match ov {
                    Some(v) => $crate::Value::$val_var(v),
                    None => $crate::Value::Null,
                }
            }
        }
    };
    ($ftype:ty, $val_var:ident) => {
        impl From<$ftype> for Value {
            fn from(v: $ftype) -> Self {
                $crate::Value::$val_var(v)
            }
        }
    };
    (Option<$ftype:ty>, $wrapper:expr, $val_var:ident) => {
        impl From<Option<$ftype>> for Value {
            fn from(ov: Option<$ftype>) -> Self {
                match ov {
                    Some(v) => $crate::Value::$val_var($wrapper(v)),
                    None => $crate::Value::Null,
                }
            }
        }
    };
    ($ftype:ty, $wrapper:expr, $val_var:ident) => {
        impl From<$ftype> for Value {
            fn from(v: $ftype) -> Self {
                $crate::Value::$val_var($wrapper(v))
            }
        }
    };
}

pub(crate) use impl_value_from;

/// Type conversion: Value try_into standard type. Used in `value.rs`.
///
/// Equivalent to:
///
/// ```rust,ignore
/// impl TryFrom<Value> for Option<bool> {
///     type Error = CoreError;
///     fn try_from(value: Value) -> Result<Self, Self::Error> {
///         match value {
///             Value::Null => Ok(None),
///             Value::Boolean(v) => Ok(Some(v)),
///             _ => Err(CoreError::new_parse_info_error(value, "bool")),
///         }
///     }
/// }
/// ```
///
/// and:
///
/// ```rust,ignore
/// impl TryFrom<Value> for bool {
///     type Error = CoreError;
///     fn try_from(value: Value) -> Result<Self, Self::Error> {
///         match value {
///             Value::Boolean(v) => Ok(v),
///             _ => Err(CoreError::new_parse_info_error(value, "bool")),
///         }
///     }
/// }
/// ```
macro_rules! impl_try_from_value {
    ($val_var:ident, Option<$ftype:ty>, $hint:expr) => {
        impl TryFrom<$crate::Value> for Option<$ftype> {
            type Error = $crate::CoreError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::Null => Ok(None),
                    $crate::Value::$val_var(v) => Ok(Some(v)),
                    _ => Err($crate::CoreError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
    ($val_var:ident, $ftype:ty, $hint:expr) => {
        impl TryFrom<$crate::Value> for $ftype {
            type Error = $crate::CoreError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::$val_var(v) => Ok(v),
                    _ => Err($crate::CoreError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
}

pub(crate) use impl_try_from_value;

/// impl polars' `NamedForm` trait for `Series::new`. Used in `series.rs`.
///
/// Equivalent to:
///
/// ```rust,ignore
/// impl<T: AsRef<bool>> polars::prelude::NamedFrom<T, bool> for Series {
///     fn new(name: &str, v: T) -> Self {
///         let ps = polars::prelude::ChunkedArray::<BooleanType>::new_from_slice(name, v.as_ref())
///             .into_series();
///         Series::from(ps)
///     }
/// }
/// ```
macro_rules! impl_named_from_ref {
    ($dtype:ty, $polars_type:ident, $method:ident) => {
        impl<T: AsRef<$dtype>> polars::prelude::NamedFrom<T, $dtype> for $crate::Series {
            fn new(name: &str, v: T) -> Self {
                let ps = polars::prelude::ChunkedArray::<$polars_type>::$method(name, v.as_ref())
                    .into_series();
                $crate::Series::from(ps)
            }
        }
    };
}

pub(crate) use impl_named_from_ref;

macro_rules! impl_named_from_owned {
    ($dtype:ty, $polars_type:ident) => {
        impl polars::prelude::NamedFromOwned<$dtype> for $crate::Series {
            fn from_vec(name: &str, v: $dtype) -> Self {
                let ps =
                    polars::prelude::ChunkedArray::<$polars_type>::from_vec(name, v).into_series();
                $crate::Series(ps)
            }
        }
    };
}

pub(crate) use impl_named_from_owned;

/// new polars' ChunkedArray from Vec<Value>
///
/// ```rust
/// let r = values
///     .into_iter()
///     .map(|v| bool::try_from(v))
///     .collect::<CoreResult<Vec<_>>>()?;
/// ChunkedArray::<BooleanType>::new_from_slice(IDX, &r[..])
/// ```
macro_rules! chunked_array_from_values {
    ($name:expr, $values:expr; Option<$ftype:ty>, $polars_type:ident) => {{
        let r = $values
            .into_iter()
            .map(|v| Option::<$ftype>::try_from(v).unwrap_or(None))
            .collect::<Vec<_>>();
        polars::prelude::ChunkedArray::<$polars_type>::from_slice_options($name, &r[..])
    }};
    ($name:expr, $values:expr; $ftype:ty, $polars_type:ident) => {{
        let r = $values
            .into_iter()
            .map(|v| <$ftype>::try_from(v))
            .collect::<$crate::CoreResult<Vec<_>>>()?;
        polars::prelude::ChunkedArray::<$polars_type>::from_slice($name, &r[..])
    }};
    ($name:expr; Option<$ftype:ty>, $polars_type:ident) => {{
        let vec: Vec<Option<$ftype>> = vec![];
        polars::prelude::ChunkedArray::<$polars_type>::from_slice_options($name, &vec)
    }};
    ($name:expr; $ftype:ty, $polars_type:ident) => {{
        let vec: Vec<$ftype> = vec![];
        polars::prelude::ChunkedArray::<$polars_type>::from_slice($name, &vec)
    }};
}

/// new Series from Vec<Value>, with nullable option
macro_rules! sfv {
    ($nullable:expr; $name:expr, $values:expr; $ftype:ty, $polars_type:ident) => {{
        let s = match $nullable {
            true => chunked_array_from_values!($name, $values; Option<$ftype>, $polars_type),
            false => chunked_array_from_values!($name, $values; $ftype, $polars_type),
        };
        Ok($crate::Series(s.into_series()))
    }};
    ($nullable:expr; $name:expr; $ftype:ty, $polars_type:ident) => {{
        let s = match $nullable {
            true => chunked_array_from_values!($name; Option<$ftype>, $polars_type),
            false => chunked_array_from_values!($name; $ftype, $polars_type),
        };
        Ok($crate::Series(s.into_series()))
    }};
}

pub(crate) use chunked_array_from_values;
pub(crate) use sfv;

/// Series IntoIterator process
///
/// for instance:
/// ```rust
/// let arr = self.0.bool().unwrap();
/// SeriesIntoIterator::Bool(
///     arr.clone(),
///     Stepper::new(arr.len()),
/// )
/// ```
///
/// and custom type:
///
/// ```rust
/// let arr = self.0.as_any()
///     .downcast_ref::<ObjectChunked<ObjectTypeDate>>()
///     .unwrap();
/// SeriesIntoIterator::Date(
///     arr.clone(),
///     Stepper::new(arr.len()),
/// )
/// ```
macro_rules! sii {
    ($fn_call:expr, $series_iter_var:ident) => {{
        use polars::prelude::ChunkLen;
        let arr = $fn_call.unwrap();
        $crate::core::SeriesIntoIterator::$series_iter_var(
            arr.clone(),
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
    ($fn_call:expr, $downcast_type:ident, $series_iter_var:ident) => {{
        use polars::prelude::ChunkLen;
        let arr = $fn_call
            .downcast_ref::<polars::prelude::ObjectChunked<$downcast_type>>()
            .unwrap();
        $crate::core::SeriesIntoIterator::$series_iter_var(
            arr.clone(),
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
}

pub(crate) use sii;

/// Series Iterator process
///
/// for instance:
/// ```rust
/// let arr = self.0.bool().unwrap();
/// SeriesIterator::Bool(
///     arr,
///     Stepper::new(arr.len()),
/// )
/// ```
macro_rules! si {
    ($fn_call:expr, $series_iter_var:ident) => {{
        use polars::prelude::ChunkLen;
        let arr = $fn_call.unwrap();
        $crate::core::SeriesIterator::$series_iter_var(
            arr,
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
    ($fn_call:expr, $downcast_type:ident, $series_iter_var:ident) => {{
        use polars::prelude::ChunkLen;
        let arr = $fn_call
            .downcast_ref::<polars::prelude::ObjectChunked<$downcast_type>>()
            .unwrap();
        $crate::core::SeriesIterator::$series_iter_var(
            arr,
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
}

pub(crate) use si;

/// The `next` function for Series iterator
///
/// for instance:
///
/// ```rust
/// if s.exhausted() {
///     None
/// } else {
///     let res = match arr.get(s.step) {
///         Some(v) => value!(v),
///         None => Value::default(),
///     };
///     s.step += 1;
///     Some(res)
/// }
/// ```
macro_rules! s_fn_next {
    ($arr:expr, $stepper:expr) => {{
        if $stepper.exhausted() {
            None
        } else {
            let res = $crate::value!($arr.get($stepper.step));
            $stepper.forward();
            Some(res)
        }
    }};
}

macro_rules! sc_fn_next {
    ($arr:expr, $stepper:expr) => {{
        if $stepper.exhausted() {
            None
        } else {
            let res = $crate::value!($arr.get($stepper.step).cloned());
            $stepper.forward();
            Some(res)
        }
    }};
}

pub(crate) use s_fn_next;
pub(crate) use sc_fn_next;

#[cfg(test)]
mod test_polars_dev {

    use polars::prelude::*;

    #[test]
    fn test_chunked_arr_from_iter() {
        let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let iter = v.into_iter().map(|i| i as f32);

        let foo = Float32Chunked::from_iter_values("a", iter);

        println!("{:?}", foo);
    }

    #[test]
    fn test_iterator_peek() {
        let a = vec![1, 2, 3, 4, 5];

        let mut iter = a.into_iter().skip_while(|x| *x < 3);

        println!("{:?}", iter.next());
    }
}
