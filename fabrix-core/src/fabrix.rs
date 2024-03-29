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
//! 1. from_column_values
//! 1. empty
//! 1. rechunk
//! 1. get_column
//! 1. get_columns
//! 1. data
//! 1. index_tag
//! 1. index
//! 1. generate_index
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
use polars::datatypes::IdxCa;
use polars::prelude::{BooleanChunked, DataFrame, Field, NewChunkedArray};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};

use super::{
    cis_err, idl_err, inf_err, lnm_err, nnf_err, oob_err, vnf_err, FieldInfo, Series, IDX,
};
use crate::{CoreError, CoreResult, D2Value, Value, ValueType};

// ================================================================================================
// IndexTag
// ================================================================================================

/// IndexTag
///
/// Used in Fabrix in order to identify the index of a DataFrame.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct IndexTag {
    pub loc: usize,
    pub name: String,
    pub data_type: ValueType,
}

impl IndexTag {
    pub fn new(loc: usize, name: &str, data_type: ValueType) -> Self {
        Self {
            loc,
            name: name.to_string(),
            data_type,
        }
    }

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

impl<T> IntoIndexTag for T {
    default fn into_index_tag(self, _fields: &[Field]) -> CoreResult<IndexTag> {
        Err(CoreError::Unknown)
    }
}

impl IntoIndexTag for usize {
    fn into_index_tag(self, fields: &[Field]) -> CoreResult<IndexTag> {
        match fields.get(self) {
            Some(field) => Ok(IndexTag {
                loc: self,
                name: field.name().to_owned(),
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

impl<T> IntoIndexTag for Option<T>
where
    T: IntoIndexTag,
{
    fn into_index_tag(self, fields: &[Field]) -> CoreResult<IndexTag> {
        match self {
            Some(i) => i.into_index_tag(fields),
            None => Err(CoreError::EmptyIndexTag),
        }
    }
}

// ================================================================================================
// Fabrix
// ================================================================================================

/// Fabrix
///
/// A data structure used in Fabrix crate, it wrapped `polars` DataFrame as data.
#[derive(Clone)]
pub struct Fabrix {
    pub data: DataFrame,
    pub index_tag: Option<IndexTag>,
}

impl Fabrix {
    /// DataFrame constructor
    pub fn new(data: DataFrame, index_tag: impl IntoIndexTag) -> CoreResult<Self> {
        let fields = data.fields();
        let index_tag = match index_tag.into_index_tag(&fields) {
            Ok(it) => Ok(Some(it)),
            Err(e) => match e {
                CoreError::EmptyIndexTag => Ok(None),
                e => Err(e),
            },
        }?;

        Ok(Self { data, index_tag })
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

    /// Pure empty dataframe
    pub fn empty() -> Self {
        Self {
            data: DataFrame::default(),
            index_tag: None,
        }
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

    /// Create a DataFrame from by D2Value, column-wise
    pub fn from_column_values(
        values: D2Value,
        index_col: Option<usize>,
        has_header: bool,
    ) -> CoreResult<Self> {
        let series = values
            .into_iter()
            .map(|mut s| {
                if has_header {
                    if s.len() < 2 {
                        return Err(idl_err());
                    }
                    let name = s.remove(0).to_string();
                    Series::from_values(s, name, true)
                } else {
                    Series::from_values_default_name(s, true)
                }
            })
            .collect::<CoreResult<Vec<_>>>()?;

        match index_col {
            Some(i) => Fabrix::from_series(series, i),
            None => Fabrix::from_series_no_index(series),
        }
    }

    pub fn data(&self) -> &DataFrame {
        &self.data
    }

    pub fn index_tag(&self) -> Option<&IndexTag> {
        self.index_tag.as_ref()
    }

    /// get a column
    pub fn get_column<S>(&self, name: S) -> CoreResult<&Series>
    where
        S: AsRef<str>,
    {
        Ok(self.data().column(name.as_ref())?.as_ref())
    }

    /// get a vector of cloned columns
    pub fn get_columns<I, S>(&self, names: I) -> CoreResult<Vec<&Series>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let res = self
            .data()
            .columns(names)?
            .into_iter()
            .map(|s| s.as_ref())
            .collect();

        Ok(res)
    }

    /// get a reference of FDataFrame's index
    pub fn index(&self) -> Option<&Series> {
        self.index_tag()
            .and_then(|it| self.data().column(it.name()).ok().map(|s| s.as_ref()))
    }

    /// get column names
    pub fn get_column_names(&self) -> Vec<&str> {
        self.data().get_column_names()
    }

    /// dataframe dtypes
    pub fn dtypes(&self) -> Vec<&ValueType> {
        self.data().dtypes().iter().map(|t| t.into()).collect_vec()
    }

    /// index check null.
    pub fn index_has_null(&self) -> Option<bool> {
        match self.index_tag() {
            Some(it) => self
                .data()
                .column(it.name.as_str())
                .ok()
                .map(|s| s.is_not_null().all()),
            None => None,
        }
    }

    /// dataframe check null columns
    pub fn has_null(&self) -> Vec<bool> {
        self.data().iter().map(|s| !s.is_not_null().all()).collect()
    }

    /// get DataFrame fields info
    pub fn fields(&self) -> Vec<FieldInfo> {
        self.data()
            .fields()
            .iter()
            .map(FieldInfo::from)
            .collect::<Vec<_>>()
    }

    /// get index field info
    pub fn index_field(&self) -> Option<FieldInfo> {
        self.index_tag()
            .map(|it| FieldInfo::from((it.name(), it.data_type())))
    }

    /// get shape
    pub fn shape(&self) -> (usize, usize) {
        self.data().shape()
    }

    /// get width
    pub fn width(&self) -> usize {
        self.data().width()
    }

    /// get height
    pub fn height(&self) -> usize {
        self.data().height()
    }

    /// take cloned rows by an indices array
    pub fn take_rows_by_idx(&self, indices: &[usize]) -> CoreResult<Fabrix> {
        let iter = indices.iter().copied();
        let data = self.data().take_iter(iter)?;

        Ok(Fabrix {
            data,
            index_tag: self.index_tag().cloned(),
        })
    }

    /// take cloned DataFrame by an index Series
    pub fn take_rows(&self, index: &Series) -> CoreResult<Fabrix> {
        match self.index_tag() {
            Some(it) => {
                let s = self.get_column(it.name.as_str())?;
                let iter = s
                    .find_indices(index)
                    .into_iter()
                    .map(|i| i as u64)
                    .collect();
                let data = self.data().take(&IdxCa::new_vec("idx", iter))?;

                Ok(Fabrix {
                    data,
                    index_tag: self.index_tag().cloned(),
                })
            }
            None => Err(inf_err()),
        }
    }

    /// slice the DataFrame along the rows
    pub fn slice(&self, offset: i64, length: usize) -> Fabrix {
        let data = self.data().slice(offset, length);

        Fabrix {
            data,
            index_tag: self.index_tag().cloned(),
        }
    }

    /// take cloned DataFrame by column names
    pub fn take_cols<I, S>(&self, cols: I) -> CoreResult<Fabrix>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let data = self.data().select(cols)?;
        Ok(Fabrix {
            data,
            index_tag: self.index_tag().cloned(),
        })
    }

    /// rechunk: aggregate all chunks to a contiguous array of memory
    pub fn rechunk(&mut self) {
        self.data.rechunk();
    }

    /// set index_tag
    pub fn set_index_tag(&mut self, index_tag: impl IntoIndexTag) -> CoreResult<&IndexTag> {
        let fields = self.data.fields();
        self.index_tag = Some(index_tag.into_index_tag(&fields)?);
        Ok(self.index_tag.as_ref().unwrap())
    }

    /// generate a new column with row count, be careful the index_tag will be overridden
    pub fn generate_index(&mut self) -> &mut Self {
        self.data.with_row_count_mut(IDX, None);
        self.index_tag = Some(IndexTag::new(0, IDX, ValueType::U32));
        self
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

    /// horizontal stack, return cloned data
    pub fn hconcat(&self, columns: Vec<Series>) -> CoreResult<Fabrix> {
        let raw_columns = columns.into_iter().map(|v| v.0).collect::<Vec<_>>();
        let data = self.data.hstack(raw_columns.as_slice())?;

        Ok(Self {
            data,
            index_tag: self.index_tag.to_owned(),
        })
    }

    /// horizontal stack, self mutation
    pub fn hconcat_mut(&mut self, columns: Vec<Series>) -> CoreResult<&mut Self> {
        let raw_columns = columns.into_iter().map(|v| v.0).collect::<Vec<_>>();
        self.data = self.data.hstack(raw_columns.as_slice())?;

        Ok(self)
    }

    // TODO:

    pub fn drop_series_by_idx() {
        unimplemented!()
    }

    pub fn drop_series_by_name() {
        unimplemented!()
    }

    pub fn insert_series() {
        unimplemented!()
    }

    /// vertical stack, return cloned data
    pub fn vconcat(&self, df: &Fabrix) -> CoreResult<Fabrix> {
        let data = self.data.vstack(df.data())?;

        Ok(Self {
            data,
            index_tag: self.index_tag.to_owned(),
        })
    }

    /// vertical concat, self mutation
    pub fn vconcat_mut(&mut self, df: &Fabrix) -> CoreResult<&mut Self> {
        self.data.vstack_mut(df.data())?;

        Ok(self)
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
                let s = self.get_column(idx.name.as_str())?;
                match s.find_index(index) {
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
                let idx = self
                    .get_column(it.name.as_str())?
                    .find_indices(&idx)
                    .into_iter()
                    .map(|i| i as u64)
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
                let idx = self.get_column(it.name.as_str())?.find_indices(index);
                let pop = self.popup_rows_by_idx(&idx)?;

                Ok(pop)
            }
            None => Err(inf_err()),
        }
    }

    // TODO:

    pub fn apply() {
        unimplemented!()
    }

    pub fn apply_at_idx() {
        unimplemented!()
    }

    pub fn iter_column(&self) -> IntoIteratorColumn {
        FabrixRefIterToColumn(self).into_iter()
    }
}

impl From<DataFrame> for Fabrix {
    fn from(df: DataFrame) -> Self {
        Self {
            data: df,
            index_tag: None,
        }
    }
}

#[derive(RefCast)]
#[repr(transparent)]
pub struct FabrixDataFrame(DataFrame);

impl FabrixDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self(df)
    }

    pub fn shape(&self) -> (usize, usize) {
        self.0.shape()
    }

    pub fn width(&self) -> usize {
        self.0.width()
    }

    pub fn height(&self) -> usize {
        self.0.height()
    }

    pub fn dtypes(&self) -> Vec<&ValueType> {
        self.0.dtypes().iter().map(|t| t.into()).collect_vec()
    }

    pub fn fields(&self) -> Vec<FieldInfo> {
        self.0
            .fields()
            .iter()
            .map(FieldInfo::from)
            .collect::<Vec<_>>()
    }

    pub fn data(&self) -> &DataFrame {
        &self.0
    }

    pub fn iter_column(&self) -> IntoIteratorColumn {
        FabrixDataFrameIterToColumn(self).into_iter()
    }
}

impl AsRef<FabrixDataFrame> for DataFrame {
    fn as_ref(&self) -> &FabrixDataFrame {
        FabrixDataFrame::ref_cast(self)
    }
}

// ================================================================================================
// IntoIteratorColumn for Fabrix & FabrixDataFrame
// ================================================================================================

/// FabrixRefIterToColumn
pub struct FabrixRefIterToColumn<'a>(&'a Fabrix);

pub struct IntoIteratorColumn<'a> {
    data_iters: std::vec::IntoIter<&'a Series>,
}

impl<'a> Iterator for IntoIteratorColumn<'a> {
    type Item = &'a Series;

    fn next(&mut self) -> Option<Self::Item> {
        self.data_iters.next()
    }
}

impl<'a> IntoIterator for FabrixRefIterToColumn<'a> {
    type Item = &'a Series;
    type IntoIter = IntoIteratorColumn<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let data_iters = self
            .0
            .data()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&Series>>()
            .into_iter();

        IntoIteratorColumn { data_iters }
    }
}

/// FabrixDataFrameIterToColumn
pub struct FabrixDataFrameIterToColumn<'a>(&'a FabrixDataFrame);

impl<'a> IntoIterator for FabrixDataFrameIterToColumn<'a> {
    type Item = &'a Series;
    type IntoIter = IntoIteratorColumn<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let data_iters = self
            .0
            .data()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&Series>>()
            .into_iter();

        IntoIteratorColumn { data_iters }
    }
}

// ================================================================================================
// PartialEq
// ================================================================================================

impl PartialEq for Fabrix {
    fn eq(&self, other: &Self) -> bool {
        if self.index_tag != other.index_tag {
            return false;
        }

        if self.shape() != other.shape() {
            return false;
        }

        for (s1, s2) in self.data.iter().zip(other.data.iter()) {
            let s1: &Series = s1.as_ref();
            let s2: &Series = s2.as_ref();
            if s1 != s2 {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod test_fabrix_dataframe {

    use crate::{date, datetime, decimal, fx, series, time, uuid, FieldInfo, ValueType};

    #[test]
    fn fx_dtypes_match_success() {
        let df = fx![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ];

        assert!(df.is_ok());

        let df = df.unwrap();

        assert_eq!(
            df.dtypes(),
            vec![&ValueType::String, &ValueType::I32, &ValueType::I32]
        );

        assert_eq!(df.get_column("names").unwrap().len(), 3);
    }

    #[test]
    fn fx_fields_match_success() {
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
    fn fx_viewing_success() {
        let df = fx![
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ];

        assert!(df.is_ok());

        let mut df = df.unwrap();
        let it = df.set_index_tag(1);
        assert!(it.is_ok(), "set index tag should work");

        let test1 = df.get_columns(["names", "val"]);
        assert!(test1.is_ok());
        assert_eq!(test1.unwrap().len(), 2);

        let test2 = df.take_rows_by_idx(&[0, 2]);
        assert!(test2.is_ok());
        assert_eq!(test2.unwrap().shape(), (2, 3));

        let test3 = df.take_cols(["names", "val"]);
        assert!(test3.is_ok());
        assert_eq!(test3.unwrap().shape(), (3, 2));

        // watch out that the default index type is u64
        let flt = series!([1u64, 3]);
        let test4 = df.take_rows(&flt);
        assert!(test4.is_ok());
        assert_eq!(test4.unwrap().shape(), (0, 3)); // 1 row, since index 3u64 doesn't exist
    }

    #[test]
    fn fx_concat_success() {
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
        assert!(res.is_ok(), "vconcat_mut should work");
    }

    #[test]
    fn fx_has_null_success() {
        let fx = fx![
            "uuid";
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
            "decimal" => [decimal!(1000, 24), decimal!(820, 23), decimal!(2100, 15)],
            "uuid" => [Some(uuid!("72b1e846-1f57-47da-bae0-9c08b0ba0fce")), None, Some(uuid!("088a052c-aa47-41c2-b3b4-6813776d0edc"))],
        ].unwrap();

        assert_eq!(fx.index_has_null(), Some(false));
        assert_eq!(
            fx.has_null(),
            vec![false, false, true, false, false, false, false, true]
        )
    }

    // TODO: test the rest of the methods
}
