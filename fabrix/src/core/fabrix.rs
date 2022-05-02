//! Fabrix DataFrame
//!
//! This module contains the DataFrame struct, which is used to store a collection of Series.
//!
//! Methods:
//! 1. new
//! 1. new_no_index
//! 1. new_empty
//! 1. from_series
//! 1. from_series_no_index
//! 1. rechunk
//! 1. get_column
//! 1. get_columns
//! 1. data
//! 1. index_tag
//! 1. index
//! 1. get_column_names
//! 1. set_column_names
//! 1. rename
//! 1. dtypes
//! 1. index_has_null
//! 1. has_null
//! 1. fields
//! 1. index_fields
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
use polars::{
    datatypes::IdxCa,
    prelude::{BooleanChunked, DataFrame, Field, IntoVec, NewChunkedArray},
};

use super::{cis_err, inf_err, lnm_err, nnf_err, oob_err, vnf_err, FieldInfo, Series, IDX};
use crate::{CoreResult, Value, ValueType};

// TODO:
// 1. replace index field by Option<String>, and index is a specific column of dataframe;
// 2. data field should be public
// 3. all sources should also be modified

#[derive(Clone, Debug, Default)]
pub struct IndexTag {
    pub loc: usize,
    pub name: String,
    pub data_type: ValueType,
}

impl IndexTag {
    pub fn loc(&self) -> usize {
        self.loc
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &ValueType {
        &self.data_type
    }
}

pub trait IntoIndexTag {
    fn into_index_tag(self, fields: &[Field]) -> CoreResult<IndexTag>;
}

impl IntoIndexTag for usize {
    fn into_index_tag(self, fields: &[Field]) -> CoreResult<IndexTag> {
        match fields.get(self) {
            Some(field) => Ok(IndexTag {
                loc: self,
                name: field.name().clone(),
                data_type: field.data_type().into(),
            }),
            None => Err(lnm_err(fields.len(), self)),
        }
    }
}

impl IntoIndexTag for &str {
    fn into_index_tag(self, fields: &[Field]) -> CoreResult<IndexTag> {
        match fields.iter().position(|f| f.name() == self) {
            Some(loc) => Ok(IndexTag {
                loc,
                name: self.to_string(),
                data_type: fields[loc].data_type().into(),
            }),
            None => Err(nnf_err(self)),
        }
    }
}

impl IntoIndexTag for String {
    fn into_index_tag(self, fields: &[Field]) -> CoreResult<IndexTag> {
        match fields.iter().position(|f| f.name() == &self) {
            Some(loc) => Ok(IndexTag {
                loc,
                name: self,
                data_type: fields[loc].data_type().into(),
            }),
            None => Err(nnf_err(&self)),
        }
    }
}

/// DataFrame is a data structure used in Fabrix crate, it wrapped `polars` Series as DF index and
/// `polars` DataFrame for holding 2 dimensional data. Make sure index series is not nullable.
#[derive(Clone)]
pub struct Fabrix {
    pub data: DataFrame,
    pub index_tag: Option<IndexTag>,
}

impl Fabrix {
    /// DataFrame constructor
    pub fn new(data: DataFrame, index_tag: impl IntoIndexTag) -> CoreResult<Self> {
        let fields = data.fields();
        Ok(Self {
            data,
            index_tag: Some(index_tag.into_index_tag(&fields)?),
        })
    }

    /// DataFrame constructor, no index
    pub fn new_no_index(data: DataFrame) -> Self {
        Self {
            data,
            index_tag: None,
        }
    }

    /// DataFrame constructor, create an empty dataframe by data fields and index field
    pub fn new_empty(fields: Vec<FieldInfo>, index_tag: impl IntoIndexTag) -> CoreResult<Self> {
        let fields: Vec<Field> = fields.into_iter().map(|fi| fi.into()).collect();
        let empty_series = fields
            .iter()
            .map(|f| Series::empty_series_from_field(f, false))
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_series(empty_series, index_tag)
    }

    /// DataFrame constructor, create an empty dataframe by data fields
    pub fn new_empty_no_index(fields: Vec<FieldInfo>) -> CoreResult<Self> {
        let fields: Vec<Field> = fields.into_iter().map(|fi| fi.into()).collect();
        let empty_series = fields
            .iter()
            .map(|f| Series::empty_series_from_field(f, false))
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_series_no_index(empty_series)
    }

    /// Create a DataFrame from Vec<Series>
    pub fn from_series(series: Vec<Series>, index_tag: impl IntoIndexTag) -> CoreResult<Self> {
        let data = DataFrame::new(series.into_iter().map(|s| s.0).collect())?;
        Fabrix::new(data, index_tag)
    }

    /// Create a DataFrame from Vec<Series> without index
    pub fn from_series_no_index(series: Vec<Series>) -> CoreResult<Self> {
        let data = DataFrame::new(series.into_iter().map(|s| s.0).collect())?;
        Ok(Self {
            data,
            index_tag: None,
        })
    }

    /// rechunk: aggregate all chunks to a contiguous array of memory
    pub fn rechunk(&mut self) {
        self.data.rechunk();
    }

    /// get a column
    pub fn get_column<S>(&self, name: S) -> Option<Series>
    where
        S: AsRef<str>,
    {
        match self.data.column(name.as_ref()) {
            Ok(s) => Some(Series(s.clone())),
            Err(_) => None,
        }
    }

    /// get a vector of cloned columns
    pub fn get_columns(&self, names: impl IntoVec<String>) -> Option<Vec<Series>> {
        match self.data.select_series(names) {
            Ok(r) => Some(r.into_iter().map(Series).collect()),
            Err(_) => None,
        }
    }

    /// get a reference of FDataFrame's data
    pub fn data(&self) -> &DataFrame {
        &self.data
    }

    /// get a reference of FDataFrame's index_tag
    pub fn index_tag(&self) -> Option<&IndexTag> {
        self.index_tag.as_ref()
    }

    /// set index_tag
    pub fn set_index_tag(&mut self, index_tag: impl IntoIndexTag) -> CoreResult<&IndexTag> {
        let fields = self.data.fields();
        self.index_tag = Some(index_tag.into_index_tag(&fields)?);
        Ok(self.index_tag.as_ref().unwrap())
    }

    /// get a reference of FDataFrame's index
    pub fn index(&self) -> Option<Series> {
        self.index_tag.as_ref().and_then(|it| {
            self.data
                .column(it.name.as_str())
                .ok()
                .map(|s| Series(s.clone()))
        })
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

    /// dataframe dtypes
    pub fn dtypes(&self) -> Vec<ValueType> {
        self.data.dtypes().iter().map(|t| t.into()).collect_vec()
    }

    /// index check null.
    /// WARNING: object column will cause panic, since `polars` hasn't implemented yet
    pub fn index_has_null(&self) -> Option<bool> {
        match &self.index_tag {
            Some(it) => self
                .data
                .column(it.name.as_str())
                .ok()
                .map(|s| s.is_not_null().all()),
            None => None,
        }
    }

    /// dataframe check null columns
    /// WARNING: object column will cause panic, since `polars` hasn't implemented yet
    pub fn has_null(&self) -> Vec<bool> {
        self.data.iter().map(|s| !s.is_not_null().all()).collect()
    }

    /// is dtypes match
    pub fn is_dtypes_match(&self, df: &Fabrix) -> bool {
        self.dtypes() == df.dtypes()
    }

    /// get DataFrame fields info
    pub fn fields(&self) -> Vec<FieldInfo> {
        self.data
            .iter()
            .map(|s| FieldInfo::new(s.name().to_owned(), s.dtype().into()))
            .collect_vec()
    }

    pub fn index_field(&self) -> Option<FieldInfo> {
        self.index_tag
            .as_ref()
            .map(|it| FieldInfo::new(it.name.clone(), it.data_type.clone()))
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
    pub fn hconcat(&self, columns: Vec<Series>) -> CoreResult<Fabrix> {
        let raw_columns = columns.into_iter().map(|v| v.0).collect::<Vec<_>>();
        let data = self.data.hstack(raw_columns.as_slice())?;

        Ok(Self {
            data,
            index_tag: self.index_tag.clone(),
        })
    }

    /// horizontal stack, self mutation
    pub fn hconcat_mut(&mut self, columns: Vec<Series>) -> CoreResult<&mut Self> {
        let raw_columns = columns.into_iter().map(|v| v.0).collect::<Vec<_>>();
        self.data = self.data.hstack(raw_columns.as_slice())?;

        Ok(self)
    }

    /// vertical stack, return cloned data
    pub fn vconcat(&self, df: &Fabrix) -> CoreResult<Fabrix> {
        let data = self.data.vstack(df.data())?;

        Ok(Self {
            data,
            index_tag: self.index_tag.clone(),
        })
    }

    /// vertical concat, self mutation
    pub fn vconcat_mut(&mut self, df: &Fabrix) -> CoreResult<&mut Self> {
        self.data.vstack_mut(df.data())?;

        Ok(self)
    }

    /// take cloned rows by an indices array
    pub fn take_rows_by_idx(&self, indices: &[usize]) -> CoreResult<Fabrix> {
        let iter = indices.iter().copied();
        let data = self.data.take_iter(iter)?;

        Ok(Self {
            data,
            index_tag: self.index_tag.clone(),
        })
    }

    /// take cloned DataFrame by an index Series
    pub fn take_rows(&self, index: &Series) -> CoreResult<Fabrix> {
        match &self.index_tag {
            Some(it) => {
                let s = self.data.column(it.name.as_str())?;
                let iter = Series(s.clone())
                    .find_indices(index)
                    .into_iter()
                    .map(|i| i as u32)
                    .collect();
                let data = self.data.take(&IdxCa::new_vec("idx", iter))?;

                Ok(Fabrix {
                    data,
                    index_tag: self.index_tag.clone(),
                })
            }
            None => Err(inf_err()),
        }
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
    pub fn remove_row(&mut self, index: &Value) -> CoreResult<&mut Self> {
        match &self.index_tag {
            Some(idx) => {
                let s = self.data.column(idx.name.as_str())?;
                match Series(s.clone()).find_index(index) {
                    Some(idx) => self.remove_row_by_idx(idx as usize),
                    None => Err(vnf_err(index)),
                }
            }
            None => Err(inf_err()),
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
        let idx_rsd = BooleanChunked::from_slice(IDX, &data_rsd);
        let data_rsd = self.data.filter(&idx_rsd)?;

        self.data = data_rsd;

        Ok(self)
    }

    /// remove rows. expensive
    pub fn remove_rows(&mut self, indices: Vec<Value>) -> CoreResult<&mut Self> {
        let idx = Series::from_values_default_name(indices, false)?;

        match &self.index_tag {
            Some(it) => {
                let s = self.data.column(it.name.as_str())?;
                let idx = Series(s.clone())
                    .find_indices(&idx)
                    .into_iter()
                    .map(|i| i as u32)
                    .collect();
                self.data = self.data.take(&IdxCa::new_vec("idx", idx))?;

                Ok(self)
            }
            None => Err(inf_err()),
        }
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
    pub fn popup_rows_by_idx(&mut self, indices: &[usize]) -> CoreResult<Fabrix> {
        // get df
        let pop = self.take_rows_by_idx(indices)?;
        // create a `BooleanChunked` and get residual data
        self.remove_rows_by_idx(indices)?;

        Ok(pop)
    }

    /// popup rows
    pub fn popup_rows(&mut self, index: &Series) -> CoreResult<Fabrix> {
        match &self.index_tag {
            Some(it) => {
                let s = self.data.column(it.name.as_str())?;
                let idx = Series(s.clone()).find_indices(index);
                let pop = self.popup_rows_by_idx(&idx)?;

                Ok(pop)
            }
            None => Err(inf_err()),
        }
    }

    /// slice the DataFrame along the rows
    #[must_use]
    pub fn slice(&self, offset: i64, length: usize) -> Self {
        let data = self.data.slice(offset, length);

        Self {
            data,
            index_tag: self.index_tag.clone(),
        }
    }

    /// take cloned DataFrame by column names
    pub fn take_cols<I, S>(&self, cols: I) -> CoreResult<Fabrix>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let data = self.data.select(cols)?;
        Ok(Self {
            data,
            index_tag: self.index_tag.clone(),
        })
    }
}

#[cfg(test)]
mod test_fabrix_dataframe {

    use crate::{fx, series, FieldInfo, ValueType};

    #[test]
    fn test_df_new1() {
        let df = fx![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ];

        assert!(df.is_ok());

        let df = df.unwrap();

        assert_eq!(
            df.dtypes(),
            vec![ValueType::String, ValueType::I32, ValueType::I32]
        );

        assert_eq!(df.get_column("names").unwrap().len(), 3);
    }

    #[test]
    fn test_df_new2() {
        let df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ];

        assert!(df.is_ok());

        let df = df.unwrap();

        println!("{:?}", df.fields());
        assert_eq!(
            df.fields(),
            vec![
                FieldInfo::new("names", ValueType::String),
                FieldInfo::new("ord", ValueType::I32),
                FieldInfo::new("val", ValueType::I32)
            ]
        );
    }

    #[test]
    fn test_df_op1() {
        let df = fx![
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ];

        assert!(df.is_ok());

        let mut df = df.unwrap();
        let it = df.set_index_tag(1);
        assert!(it.is_ok());

        let test1 = df.get_columns(&["names", "val"]);
        assert!(test1.is_some());
        assert_eq!(test1.unwrap().len(), 2);

        let test2 = df.take_rows_by_idx(&[0, 2]);
        assert!(test2.is_ok());
        assert_eq!(test2.unwrap().shape(), (2, 3));

        let test3 = df.take_cols(&["names", "val"]);
        assert!(test3.is_ok());
        assert_eq!(test3.unwrap().shape(), (3, 2));

        // watch out that the default index type is u64
        let flt = series!([1u64, 3]);
        let test4 = df.take_rows(&flt);
        assert!(test4.is_ok());
        assert_eq!(test4.unwrap().shape(), (0, 3)); // 1 row, since index 3u64 doesn't exist
    }

    #[test]
    fn test_df_op2() {
        let mut df1 = fx![
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        let df2 = fx![
            "names" => ["Sam", "James", "Jason"],
            "ord" => [2,3,4],
            "val" => [Some(20), None, Some(9)]
        ]
        .unwrap();

        // vconcat needs fields (type and name) to be the same
        let res = df1.vconcat_mut(&df2);
        assert!(res.is_ok());
    }

    // TODO: test the rest of the methods
}
