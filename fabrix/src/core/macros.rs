//! Fabrix core macros

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
///     type Error = FabrixError;
///     fn try_from(value: Value) -> Result<Self, Self::Error> {
///         match value {
///             Value::Null => Ok(None),
///             Value::Boolean(v) => Ok(Some(v)),
///             _ => Err(FabrixError::new_parse_info_error(value, "bool")),
///         }
///     }
/// }
/// ```
///
/// and:
///
/// ```rust,ignore
/// impl TryFrom<Value> for bool {
///     type Error = FabrixError;
///     fn try_from(value: Value) -> Result<Self, Self::Error> {
///         match value {
///             Value::Boolean(v) => Ok(v),
///             _ => Err(FabrixError::new_parse_info_error(value, "bool")),
///         }
///     }
/// }
/// ```
macro_rules! impl_try_from_value {
    ($val_var:ident, Option<$ftype:ty>, $hint:expr) => {
        impl TryFrom<$crate::Value> for Option<$ftype> {
            type Error = $crate::SqlError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::Null => Ok(None),
                    $crate::Value::$val_var(v) => Ok(Some(v)),
                    _ => Err($crate::SqlError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
    ($val_var:ident, $ftype:ty, $hint:expr) => {
        impl TryFrom<$crate::Value> for $ftype {
            type Error = $crate::SqlError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::$val_var(v) => Ok(v),
                    _ => Err($crate::SqlError::new_parse_info_error(value, $hint)),
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
macro_rules! impl_named_from {
    ($dtype:ty, $series_var:ident, $method:ident) => {
        impl<T: AsRef<$dtype>> polars::prelude::NamedFrom<T, $dtype> for $crate::Series {
            fn new(name: &str, v: T) -> Self {
                let ps = polars::prelude::ChunkedArray::<$series_var>::$method(name, v.as_ref())
                    .into_series();
                $crate::Series::from(ps)
            }
        }
    };
}

pub(crate) use impl_named_from;

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
        let arr = $fn_call.unwrap();
        $crate::core::SeriesIntoIterator::$series_iter_var(
            arr.clone(),
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
    ($fn_call:expr, $downcast_type:ident, $series_iter_var:ident) => {{
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
        let arr = $fn_call.unwrap();
        $crate::core::SeriesIterator::$series_iter_var(
            arr,
            $crate::core::util::Stepper::new(arr.len()),
        )
    }};
    ($fn_call:expr, $downcast_type:ident, $series_iter_var:ident) => {{
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
