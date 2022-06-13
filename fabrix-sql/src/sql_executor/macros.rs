//！ Fabrix Sql Executor Macros

/// impl SqlTypeTagMarker for SqlTypeTag, used in `types.rs`
///
/// Equivalent to:
///
/// ```rust,ignore
/// impl SqlTypeTagMarker for SqlTypeTag<bool> {
///     fn to_str(&self) -> &str {
///         self.0
///     }
///
///     fn to_dtype(&self) -> ValueType {
///         ValueType::Bool
///     }
///
///     fn extract_value(
///         &self,
///         sql_row: &SqlRow,
///         idx: usize,
///     ) -> SqlResult<Value> {
///         match sql_row {
///             SqlRow::Mysql(r) => {
///                 let v: Option<bool> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             },
///             SqlRow::Pg(r) => {
///                 let v: Option<bool> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             },
///             SqlRow::Sqlite(r) => {
///                 let v: Option<bool> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             },
///         }
///     }
///
///     fn extract_optional_value(&self, sql_row: &SqlRow, idx: usize) -> SqlResult<Option<Value>> {
///         match sql_row {
///             SqlRow::Mysql(row) => {
///                 let v: Option<bool> = row.try_get(idx)?;
///                 Ok(v.map(|v| v.into()))
///             }
///             SqlRow::Pg(row) => {
///                 let v: Option<bool> = row.try_get(idx)?;
///                 Ok(v.map(|v| v.into()))
///             }
///             SqlRow::Sqlite(row) => {
///                 let v: Option<bool> = row.try_get(idx)?;
///                 Ok(v.map(|v| v.into()))
///             }
///         }
///     }
/// }
/// ```
///
/// and custom type:
///
/// ```rust,ignore
/// impl SqlTypeTagMarker for SqlTypeTag<Decimal> {
///     fn to_str(&self) -> &str {
///         self.0
///     }
///
///     fn to_dtype(&self) -> DataType {
///         DataType::Object("Decimal")
///     }
///
///     fn extract_value(&self, sql_row: &SqlRow, idx: usize) -> SqlResult<Value> {
///         match sql_row {
///             SqlRow::Mysql(r) => {
///                 let v: Option<RDecimal> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             }
///             SqlRow::Pg(r) => {
///                 let v: Option<RDecimal> = r.try_get(idx)?;
///                 match v {
///                     Some(r) => Ok(value!(r)),
///                     None => Ok(Value::Null),
///                 }
///             }
///             _ => Err(SqlError::MismatchedSqlRow),
///         }
///     }
///
///     fn extract_optional_value(&self, sql_row: &SqlRow, idx: usize) -> SqlResult<Option<Value>> {
///         match sql_row {
///             SqlRow::Mysql(r) => {
///                 let v: Option<RDecimal> = r.try_get(idx)?;
///                 Ok(v.map(|v| v.into()))
///             }
///             SqlRow::Pg(r) => {
///                 let v: Option<RDecimal> = r.try_get(idx)?;
///                 Ok(v.map(|v| v.into()))
///             }
///             _ => Err(SqlError::MismatchedSqlRow),
///         }
///     }
/// }
/// ```
macro_rules! impl_sql_type_tag_marker {
    ($dtype:ident, $value_type:ident; [$($sql_row_var:ident),*] $(,)* $($residual:expr)?) => {
        impl SqlTypeTagMarker for SqlTypeTag<$dtype> {
            fn to_str(&self) -> &str {
                self.0
            }

            fn to_dtype(&self) -> fabrix_core::ValueType {
                fabrix_core::ValueType::$value_type
            }

            fn extract_value(
                &self,
                sql_row: &SqlRow,
                idx: usize,
            ) -> $crate::SqlResult<fabrix_core::Value> {
                match sql_row {
                    $(
                        SqlRow::$sql_row_var(r) => {
                            let v: Option<$dtype> = r.try_get(idx)?;
                            match v {
                                Some(r) => Ok(fabrix_core::value!(r)),
                                None => Ok(fabrix_core::Value::Null),
                            }
                        },
                    )*
                    $(
                        _ => Err($crate::SqlError::MismatchedSqlRow($residual.to_owned()))
                    )?
                }
            }

            fn extract_optional_value(&self, sql_row: &SqlRow, idx: usize) -> $crate::SqlResult<Option<fabrix_core::Value>> {
                match sql_row {
                    $(
                        SqlRow::$sql_row_var(r) => {
                            let v: Option<$dtype> = r.try_get(idx)?;
                            Ok(v.map(|v| v.into()))
                        },
                    )*
                    $(
                        _ => Err($crate::SqlError::MismatchedSqlRow($residual.to_owned()))
                    )?
                }
            }
        }
    };
    ($dtype:ident <= $inner_type:ty, $value_type:ident; [$($sql_row_var:ident),*] $(,)* $($residual:expr)?) => {
        impl SqlTypeTagMarker for SqlTypeTag<$dtype> {
            fn to_str(&self) -> &str {
                self.0
            }

            fn to_dtype(&self) -> fabrix_core::ValueType {
                fabrix_core::ValueType::$value_type
            }

            fn extract_value(
                &self,
                sql_row: &SqlRow,
                idx: usize,
            ) -> $crate::SqlResult<fabrix_core::Value> {
                match sql_row {
                    $(
                        SqlRow::$sql_row_var(r) => {
                            let v: Option<$inner_type> = r.try_get(idx)?;
                            match v {
                                Some(r) => Ok(fabrix_core::value!(r)),
                                None => Ok(fabrix_core::Value::Null),
                            }
                        },
                    )*
                    $(
                        _ => Err($crate::SqlError::MismatchedSqlRow($residual.to_owned()))
                    )?
                }
            }

            fn extract_optional_value(&self, sql_row: &SqlRow, idx: usize) -> $crate::SqlResult<Option<fabrix_core::Value>> {
                match sql_row {
                    $(
                        SqlRow::$sql_row_var(r) => {
                            let v: Option<$inner_type> = r.try_get(idx)?;
                            Ok(v.map(|v| v.into()))
                        },
                    )*
                    $(
                        _ => Err($crate::SqlError::MismatchedSqlRow($residual.to_owned()))
                    )?
                }
            }
        }
    };
}

/// tmap pair
macro_rules! tmap_pair {
    ($key:expr, $value:ident) => {
        ($key, Box::new(SqlTypeTag::<$value>::new($key)) as Sttm)
    };
}

pub(crate) use impl_sql_type_tag_marker;
pub(crate) use tmap_pair;

/// connection error. used in `executor.rs`
macro_rules! conn_e_err {
    ($pool:expr) => {
        if $pool.is_some() {
            return Err($crate::SqlError::ConnectionAlreadyEstablished);
        }
    };
}

/// connection error. used in `executor.rs`
macro_rules! conn_n_err {
    ($pool:expr) => {
        if $pool.is_none() {
            return Err($crate::SqlError::ConnectionNotEstablished);
        }
    };
}

pub(crate) use conn_e_err;
pub(crate) use conn_n_err;

/// fetch process. used in `loader.rs`
///
/// Equivalent to:
///
/// ```rust
/// sqlx::query(query)
///     .try_map(|row| srp.process(row).map_err(|e| e.turn_into_sqlx_decode_error()))
///     .fetch_all(pool)
///     .await?
/// ```
macro_rules! fetch_process {
    ($pool:expr, $query:expr, $srp:expr, $process_method:ident, $fetch_method:ident) => {
        sqlx::query($query)
            .try_map(|row| {
                $srp.$process_method(row)
                    .map_err(|e| e.turn_into_sqlx_decode_error())
            })
            .$fetch_method($pool)
            .await?
    };
}

/// fetch process with customized processing fn. used in `loader.rs`
///
/// Equivalent to:
///
/// ```rust
/// sqlx::query(query)
///     .try_map(|row| srp.process_by_fn(row, &f).map_err(|e| e.turn_into_sqlx_decode_error()))
///     .fetch_all(pool)
///     .await?
/// ```
macro_rules! fetch_process_cst {
    ($pool:expr, $query:expr, $srp:expr, $customized_method:expr, $fetch_method:ident) => {
        sqlx::query($query)
            .try_map(|row| {
                $srp.process_by_fn(row, $customized_method)
                    .map_err(|e| e.turn_into_sqlx_decode_error())
            })
            .$fetch_method($pool)
            .await?
    };
}

pub(crate) use fetch_process;
pub(crate) use fetch_process_cst;

/// get SqlTypeTagMarker from SqlTypeTag. used in `types.rs`
///
/// Equivalent to:
///
/// ```rust
/// PG_TMAP.get("BOOL").unwrap().as_ref()
/// ```
macro_rules! static_sttm_get {
    ($map:expr, $key:expr) => {
        $map.get($key).unwrap().as_ref()
    };
}

pub(crate) use static_sttm_get;
