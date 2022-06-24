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
macro_rules! impl_custom_value_inner {
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
macro_rules! impl_custom_value_outer {
    ($dtype:ty, $name:expr) => {
        impl Debug for $dtype {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0)
            }
        }

        impl Display for $dtype {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0)
            }
        }

        impl PolarsObject for $dtype {
            fn type_name() -> &'static str {
                $name
            }
        }
    };
}

pub(crate) use impl_custom_value_inner;
pub(crate) use impl_custom_value_outer;

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

        impl<T: AsRef<$dtype>> $crate::FabrixSeriesNamedFromRef<T, $dtype> for $crate::Series {
            fn from_ref(name: &str, v: T) -> Self {
                $crate::Series::new(name, v)
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

        impl $crate::FabrixSeriesNamedFromOwned<$dtype> for $crate::Series {
            fn from_owned(name: &str, v: $dtype) -> Self {
                $crate::Series::from_vec(name, v)
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
        $crate::SeriesIterator::$series_iter_var(arr, $crate::util::Stepper::new(arr.len()))
    }};
    ($fn_call:expr, $downcast_type:ident, $series_iter_var:ident) => {{
        use polars::prelude::ChunkLen;
        let arr = $fn_call
            .downcast_ref::<polars::prelude::ObjectChunked<$downcast_type>>()
            .unwrap();
        $crate::SeriesIterator::$series_iter_var(arr, $crate::util::Stepper::new(arr.len()))
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

/// value creation macro
#[macro_export]
macro_rules! value {
    ($val:expr) => {{
        $crate::Value::from($val)
    }};
}

/// date creation macro
#[macro_export]
macro_rules! date {
    ($year:expr, $month:expr, $day:expr) => {
        chrono::NaiveDate::from_ymd($year, $month, $day)
    };
}

/// time creation macro
#[macro_export]
macro_rules! time {
    ($hour:expr, $minute:expr, $second:expr) => {
        chrono::NaiveTime::from_hms($hour, $minute, $second)
    };
}

/// datetime creation macro
#[macro_export]
macro_rules! datetime {
    ($year:expr, $month:expr, $day:expr, $hour:expr, $minute:expr, $second:expr) => {
        chrono::NaiveDate::from_ymd($year, $month, $day).and_hms($hour, $minute, $second)
    };
}

/// decimal creation macro
#[macro_export]
macro_rules! decimal {
    ($value:expr, $scale:expr) => {
        $crate::Decimal::new($value, $scale)
    };
}

/// uuid creation macro
#[macro_export]
macro_rules! uuid {
    () => {{
        $crate::Uuid(uuid::Uuid::new_v4())
    }};
    ($string:expr) => {{
        use std::str::FromStr;

        $crate::Uuid(uuid::Uuid::from_str($string).unwrap_or_else(|_| uuid::Uuid::nil()))
    }};
}

/// bytes creation macro
#[macro_export]
macro_rules! bytes {
    ($string:expr) => {
        $crate::Bytes::from($string)
    };
}

/// df creation macro
/// Supporting:
/// 1. dataframe with default index
/// 1. dataframe with given index
#[macro_export]
macro_rules! fx {
    ($($col_name:expr => $slice:expr),+ $(,)*) => {{
        use $crate::FabrixSeriesNamedFromRef;

        let columns = vec![
            $(
                $crate::Series::from_ref($col_name, $slice),
            )+
        ];

        $crate::Fabrix::from_series_no_index(columns)
    }};
    ($index_name:expr; $($col_name:expr => $slice:expr),+ $(,)*) => {{
        use $crate::FabrixSeriesNamedFromRef;

        let columns = vec![
            $(
                $crate::Series::from_ref($col_name, $slice),
            )+
        ];

        $crate::Fabrix::from_series(columns, $index_name)
    }};
}

/// series creation macro
/// Supporting:
/// 1. series with default name
/// 1. series with given name
#[macro_export]
macro_rules! series {
    ($slice:expr) => {{
        // use polars::prelude::NamedFrom;
        use $crate::FabrixSeriesNamedFromRef;

        $crate::Series::from_ref($crate::IDX, $slice)
        // $crate::Series::new($crate::IDX, $slice)
    }};
    ($name:expr => $slice:expr) => {{
        // use polars::prelude::NamedFrom;
        use $crate::FabrixSeriesNamedFromRef;

        $crate::Series::from_ref($name, $slice)
        // $crate::Series::new($name, $slice)
    }};
}

/// custom serialize for `SeriesIterator`
///
/// for instance:
/// ```rust
/// let mut seq = serializer.serialize_seq(Some(self.len()))?;
/// for e in self.into_iter() {
///     seq.serialize_element(&e)?;
/// }
/// seq.end()
/// ```
macro_rules! se_series_iterator {
    ($sz:expr, $arr:expr, $stp:expr) => {{
        let mut seq = $sz.serialize_seq(Some($stp.len))?;
        for e in $arr.into_iter() {
            seq.serialize_element(&e)?;
        }
        seq.end()
    }};
}

pub(crate) use se_series_iterator;

/// custom serialize for `Series`
///
/// for instance:
/// ```rust
/// let mut map = serializer.serialize_map(Some(3))?;
/// map.serialize_entry("name", self.name())?;
/// map.serialize_entry("datatype", self.dtype())?;
/// map.serialize_entry("values", &self.iter())?
/// map.end()
/// ```
macro_rules! se_series {
    ($sz:expr, $self:expr) => {{
        let mut map = $sz.serialize_map(Some(3))?;
        // name:
        map.serialize_entry("name", $self.name())?;
        // datatype:
        map.serialize_entry("datatype", $self.dtype())?;
        // values:
        map.serialize_entry("values", &$self.iter())?;

        map.end()
    }};
}

pub(crate) use se_series;

/// custom deserialize for `Series`'s values
///
/// for instance:
/// ```rust
/// let values: Vec<Option<u8>> = map.next_value()?;
/// Ok(Series::new(&name, values))
/// ```
macro_rules! de_series_values {
    ($m:expr, $t:ty, $n:expr) => {{
        let values: Vec<Option<$t>> = $m.next_value()?;
        Ok($crate::Series::new(&$n, values))
    }};
}

pub(crate) use de_series_values;

/// rows creation macro
/// Supporting:
/// 1. rows with default indices
/// 1. rows with given indices
#[macro_export]
macro_rules! rows {
    ($([$($val:expr),* $(,)*]),+ $(,)*) => {{
        let mut buf: Vec<$crate::Row> = Vec::new();
        $({
            let mut row: Vec<$crate::Value> = Vec::new();
            $(
                row.push($crate::value!($val));
            )*
            buf.push($crate::Row::new(None, row));
        })+

        buf
    }};
    ($index_loc:expr; $([$($val:expr),* $(,)*]),+ $(,)*) => {{
        let mut buf: Vec<$crate::Row> = Vec::new();
        $({
            let mut row: Vec<$crate::Value> = Vec::new();
            $(
                row.push($crate::value!($val));
            )*
            buf.push($crate::Row::new(Some($index_loc), row));
        })+

        buf
    }};
}

#[cfg(test)]
mod test_macros {

    #[test]
    fn test_value() {
        println!("{:?}", value!("Jacob"));
    }

    #[test]
    fn test_series_new() {
        // use chrono::NaiveDate;

        let series = series!([Some("Jacob"), None, Some("Sam"), Some("Jason")]);
        println!("{:?}", series);

        let series = series!("name" => ["Jacob", "Sam", "Jason"]);
        println!("{:?}", series);

        // let series = series!("date" => [
        //     NaiveDate::from_ymd(2019, 1, 1),
        //     NaiveDate::from_ymd(2019, 1, 2),
        //     NaiveDate::from_ymd(2019, 1, 3),
        // ]);
        // println!("{:?}", series);
    }

    #[test]
    fn test_df_new1() {
        let df = fx![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);
        println!("{:?}", df.dtypes());
        println!("{:?}", df.get_column("names").unwrap());
    }

    #[test]
    fn test_df_new2() {
        let df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);
        println!("{:?}", df.fields());
        println!("{:?}", df.get_column("names").unwrap());
    }

    #[test]
    fn test_rows_new() {
        let rows = rows!(
            [0, "Jacob", "A", 10],
            [1, "Sam", "A", 9],
            [2, "James", "A", 9],
        );

        println!("{:?}", rows);

        let rows = rows!(
            0;
            [1, "Jacob", "A", 10],
            [2, "Sam", "A", 9],
            [3, "James", "A", 9],
        );

        println!("{:?}", rows);
    }
}
