//! Fabrix DataFrame
//!
//! This module contains the DataFrame struct, which is used to store a collection of Series.
//!
//! Methods:
//! 1. new
//! 1. new_empty
//! 1. from_series
//! 1. from_series_with_index
//! 1. from_series_default_index
//! 1. rechunk
//! 1. get_column
//! 1. get_columns
//! 1. data
//! 1. index
//! 1. get_column_names
//! 1. set_column_names
//! 1. rename
//! 1. index_field
//! 1. index_dtype
//! 1. data_dtypes
//! 1. index_has_null
//! 1. has_null
//! 1. dtypes
//! 1. is_dtypes_match
//! 1. fields
//! 1. shape
//! 1. width
//! 1. height
//! 1. hconcat
//! 1. hconcat_mut
//! 1. vconcat
//! 1. vconcat_mut
//! 1. take_rows_by_idx
//! 1. take_rows
//! 1. pop_row
//! 1. remove_row_by_idx
//! 1. remove_row
//! 1. remove_rows_by_idx
//! 1. remove_rows
//! 1. remove_slice
//! 1. popup_rows_by_idx
//! 1. popup_rows
//! 1. slice
//! 1. take_cols

use itertools::Itertools;
use polars::frame::select::Selection;
use polars::prelude::{BooleanChunked, DataFrame as PDataFrame, Field, NewChunkedArray};

use super::{cis_err, inf_err, oob_err, FieldInfo, Series, IDX};
use crate::{CoreError, CoreResult, Value, ValueType};

/// DataFrame is a data structure used in Fabrix crate, it wrapped `polars` Series as DF index and
/// `polars` DataFrame for holding 2 dimensional data. Make sure index series is not nullable.
#[derive(Debug, Clone)]
pub struct DataFrame {
    pub(crate) data: PDataFrame,
    pub(crate) index: Series,
}

impl DataFrame {
    /// DataFrame constructor
    pub fn new(data: PDataFrame, index: Series) -> Self {
        DataFrame { data, index }
    }

    /// DataFrame constructor, create an empty dataframe by data fields and index field
    pub fn new_empty(
        data_fields: Vec<Field>,
        index_field: Field,
        nullable: bool,
    ) -> CoreResult<Self> {
        let data = data_fields
            .into_iter()
            .map(|d| Series::empty_series_from_field(d, nullable))
            .collect::<CoreResult<Vec<Series>>>()?;
        let index = Series::empty_series_from_field(index_field, nullable)?;

        DataFrame::from_series(data, index)
    }

    /// Create a DataFrame from Vec<Series> (data) and Series (index)
    pub fn from_series(series: Vec<Series>, index: Series) -> CoreResult<Self> {
        let data = PDataFrame::new(series.into_iter().map(|s| s.0).collect())?;
        Ok(DataFrame { data, index })
    }

    /// Create a DataFrame from Vec<Series> and index name
    pub fn from_series_with_index(series: Vec<Series>, index_name: &str) -> CoreResult<Self> {
        let index;
        let mut series = series;
        match series.iter().position(|s| s.name() == index_name) {
            Some(i) => {
                index = series.swap_remove(i);
            }
            None => {
                return Err(CoreError::new_common_error(format!(
                    "index {:?} does not exist",
                    index_name
                )))
            }
        }

        let data = series.into_iter().map(|s| s.0).collect();
        let data = PDataFrame::new(data)?;

        Ok(DataFrame { data, index })
    }

    /// Create a DataFrame from Vec<Series>, index is automatically generated
    pub fn from_series_default_index(series: Vec<Series>) -> CoreResult<Self> {
        let len = series.first().ok_or(cis_err("Vec<Series>"))?.len() as u64;
        let data = PDataFrame::new(series.into_iter().map(|s| s.0).collect())?;
        let index = Series::from_integer_default_name(&len)?;

        Ok(DataFrame { data, index })
    }

    /// rechunk: aggregate all chunks to a contiguous array of memory
    pub fn rechunk(&mut self) {
        self.index.rechunk();
        self.data.rechunk();
    }

    /// get a cloned column
    pub fn get_column(&self, name: &str) -> Option<Series> {
        match self.data.column(name) {
            Ok(s) => Some(Series(s.clone())),
            Err(_) => None,
        }
    }

    /// get a vector of cloned columns
    pub fn get_columns<'a, S>(&self, names: S) -> Option<Vec<Series>>
    where
        S: Selection<'a, &'a str>,
    {
        match self.data.select_series(names) {
            Ok(r) => Some(r.into_iter().map(|s| Series(s)).collect()),
            Err(_) => None,
        }
    }

    /// get a reference of FDataFrame's data
    pub fn data(&self) -> &PDataFrame {
        &self.data
    }

    /// get a reference of FDataFrame's index
    pub fn index(&self) -> &Series {
        &self.index
    }

    /// get column names
    pub fn get_column_names(&self) -> Vec<&str> {
        self.data.get_column_names()
    }

    /// set column names
    pub fn set_column_names<N>(&mut self, names: &[N]) -> CoreResult<&mut Self>
    where
        N: AsRef<str>,
    {
        self.data.set_column_names(names)?;
        Ok(self)
    }

    /// rename
    pub fn rename(&mut self, origin: &str, new: &str) -> CoreResult<&mut Self> {
        self.data.rename(origin, new)?;
        Ok(self)
    }

    /// index field
    pub fn index_field(&self) -> FieldInfo {
        self.index.field()
    }

    /// series dtype
    pub fn index_dtype(&self) -> ValueType {
        self.index.dtype()
    }

    /// dataframe dtypes
    pub fn data_dtypes(&self) -> Vec<ValueType> {
        self.data.dtypes().iter().map(|t| t.into()).collect_vec()
    }

    /// index check null.
    /// WARNING: object column will cause panic, since `polars` hasn't implemented yet
    pub fn index_has_null(&self) -> bool {
        self.index.has_null()
    }

    /// dataframe check null columns
    /// WARNING: object column will cause panic, since `polars` hasn't implemented yet
    pub fn has_null(&self) -> Vec<bool> {
        self.data
            .iter()
            .map(|s| !s.is_not_null().all_true())
            .collect()
    }

    /// series dtype + dataframe dtypes
    pub fn dtypes(&self) -> (ValueType, Vec<ValueType>) {
        (self.index_dtype(), self.data_dtypes())
    }

    /// is dtypes match
    pub fn is_dtypes_match(&self, df: &DataFrame) -> bool {
        self.dtypes() == df.dtypes()
    }

    /// get DataFrame fields info
    pub fn fields(&self) -> Vec<FieldInfo> {
        self.data
            .iter()
            .map(|s| FieldInfo::new(s.name().to_owned(), s.dtype().into()))
            .collect_vec()
    }

    /// get shape
    pub fn shape(&self) -> (usize, usize) {
        self.data.shape()
    }

    /// get width
    pub fn width(&self) -> usize {
        self.data.width()
    }

    /// get height
    pub fn height(&self) -> usize {
        self.data.height()
    }

    /// horizontal stack, return cloned data
    pub fn hconcat(&self, columns: &[Series]) -> CoreResult<DataFrame> {
        let raw_columns = columns
            .into_iter()
            .cloned()
            .map(|v| v.0)
            .collect::<Vec<_>>();
        let data = self.data.hstack(&raw_columns[..])?;

        Ok(DataFrame::new(data, self.index.clone()))
    }

    /// horizontal stack, self mutation
    pub fn hconcat_mut(&mut self, columns: &[Series]) -> CoreResult<&mut Self> {
        let raw_columns = columns
            .into_iter()
            .cloned()
            .map(|v| v.0)
            .collect::<Vec<_>>();

        self.data = self.data.hstack(&raw_columns[..])?;

        Ok(self)
    }

    // TODO: dtypes safety check is optional?
    /// vertical stack, return cloned data
    pub fn vconcat(&self, df: &DataFrame) -> CoreResult<DataFrame> {
        if !self.is_dtypes_match(&df) {
            return Err(CoreError::new_df_dtypes_mismatch_error(
                self.dtypes(),
                df.dtypes(),
            ));
        }
        let data = self.data.vstack(df.data())?;
        let mut index = self.index.0.clone();
        index.append(&df.index.0)?;

        Ok(DataFrame::new(data, Series(index)))
    }

    // TODO: dtypes safety check is optional?
    /// vertical concat, self mutation
    pub fn vconcat_mut(&mut self, df: &DataFrame) -> CoreResult<&mut Self> {
        if !self.is_dtypes_match(&df) {
            return Err(CoreError::new_df_dtypes_mismatch_error(
                self.dtypes(),
                df.dtypes(),
            ));
        }
        self.data.vstack_mut(df.data())?;
        self.index.0.append(&df.index.0)?;

        Ok(self)
    }

    /// take cloned rows by an indices array
    pub fn take_rows_by_idx(&self, indices: &[usize]) -> CoreResult<DataFrame> {
        let iter = indices.to_vec().into_iter();
        let data = self.data.take_iter(iter)?;

        Ok(DataFrame {
            data,
            index: self.index.take(&indices)?,
        })
    }

    /// take cloned DataFrame by an index Series
    pub fn take_rows(&self, index: &Series) -> CoreResult<DataFrame> {
        let idx = self.index.find_indices(index);

        Ok(self.take_rows_by_idx(&idx[..])?)
    }

    /// pop row
    pub fn pop_row(&mut self) -> CoreResult<&mut Self> {
        let len = self.height();
        if len == 0 {
            return Err(cis_err("dataframe"));
        }

        *self = self.slice(0, len - 1);

        Ok(self)
    }

    /// remove a row by idx
    pub fn remove_row_by_idx(&mut self, idx: usize) -> CoreResult<&mut Self> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        let (mut s1, s2) = (self.slice(0, idx), self.slice(idx as i64 + 1, len));

        s1.vconcat_mut(&s2)?;
        *self = s1;

        Ok(self)
    }

    /// remove a row
    pub fn remove_row(&mut self, index: Value) -> CoreResult<&mut Self> {
        match self.index.find_index(&index) {
            Some(idx) => self.remove_row_by_idx(idx),
            None => Err(inf_err(&index)),
        }
    }

    /// remove rows by idx
    pub fn remove_rows_by_idx(&mut self, idx: &[usize]) -> CoreResult<&mut Self> {
        if idx.is_empty() {
            return Err(cis_err("idx"));
        }

        // create a `BooleanChunked` and get residual data
        let mut data_rsd = vec![true; self.height()];
        idx.iter().for_each(|i| data_rsd[*i] = false);
        let idx_rsd = BooleanChunked::new_from_slice(IDX, &data_rsd);
        let data_rsd = self.data.filter(&idx_rsd)?;
        let index_rsd = Series(self.index.0.filter(&idx_rsd)?);

        self.data = data_rsd;
        self.index = index_rsd;

        Ok(self)
    }

    /// remove rows. expensive
    pub fn remove_rows<'a>(&mut self, indices: Vec<Value>) -> CoreResult<&mut Self> {
        let idx = Series::from_values_default_name(indices, false)?;
        let idx = self.index.find_indices(&idx);

        self.remove_rows_by_idx(&idx)
    }

    /// remove a slice of rows from the dataframe
    pub fn remove_slice(&mut self, offset: i64, length: usize) -> CoreResult<&mut Self> {
        let len = self.height();
        let offset = if offset >= 0 {
            offset
        } else {
            len as i64 + offset
        };
        let (mut d1, d2) = (
            self.slice(0, offset as usize),
            self.slice(offset + length as i64, len),
        );

        d1.vconcat_mut(&d2)?;
        *self = d1;

        Ok(self)
    }

    /// popup rows by indices array
    pub fn popup_rows_by_idx(&mut self, indices: &[usize]) -> CoreResult<DataFrame> {
        // get df
        let pop = self.take_rows_by_idx(indices)?;
        // create a `BooleanChunked` and get residual data
        self.remove_rows_by_idx(indices)?;

        Ok(pop)
    }

    /// popup rows
    pub fn popup_rows(&mut self, index: &Series) -> CoreResult<DataFrame> {
        let idx = self.index.find_indices(index);

        Ok(self.popup_rows_by_idx(&idx)?)
    }

    /// slice the DataFrame along the rows
    pub fn slice(&self, offset: i64, length: usize) -> DataFrame {
        let data = self.data.slice(offset, length);
        let index = self.index.slice(offset, length);

        DataFrame::new(data, index.into())
    }

    /// take cloned DataFrame by column names
    pub fn take_cols<'a, S>(&self, cols: S) -> CoreResult<DataFrame>
    where
        S: Selection<'a, &'a str>,
    {
        let data = self.data.select(cols)?;
        Ok(DataFrame {
            data,
            index: self.index.clone(),
        })
    }
}

#[cfg(test)]
mod test_fabrix_dataframe {

    use crate::{df, series};

    #[test]
    fn test_df_new1() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);
        println!("{:?}", df.data_dtypes());
        println!("{:?}", df.get_column("names").unwrap());
    }

    #[test]
    fn test_df_new2() {
        let df = df![
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
    fn test_df_op() {
        let df = df![
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df.get_columns(&["names", "val"]).unwrap());
        println!("{:?}", df.take_rows_by_idx(&[0, 2]));
        println!("{:?}", df.take_cols(&["names", "val"]).unwrap());

        // watch out that the default index type is u64
        let flt = series!([1u64, 3]);
        println!("{:?}", df.take_rows(&flt));

        println!("{:?}", df.slice(2, 3));
    }
}
