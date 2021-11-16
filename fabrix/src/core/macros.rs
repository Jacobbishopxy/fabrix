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

pub(crate) use impl_custom_value;

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
            type Error = $crate::FabrixError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::Null => Ok(None),
                    $crate::Value::$val_var(v) => Ok(Some(v)),
                    _ => Err($crate::FabrixError::new_parse_info_error(value, $hint)),
                }
            }
        }
    };
    ($val_var:ident, $ftype:ty, $hint:expr) => {
        impl TryFrom<$crate::Value> for $ftype {
            type Error = $crate::FabrixError;

            fn try_from(value: $crate::Value) -> Result<Self, Self::Error> {
                match value {
                    $crate::Value::$val_var(v) => Ok(v),
                    _ => Err($crate::FabrixError::new_parse_info_error(value, $hint)),
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
