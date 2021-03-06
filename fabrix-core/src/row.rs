//! Fabrix row
//!
//! This module contains the row struct, which is used to represent a row in a DataFrame.
//!
//! Methods:
//! 1. new
//! 1. from_values
//! 1. data
//! 1. index
//! 1. data_fields
//! 1. len
//!
//! Methods provided to Dataframe:
//! 1. from_rows
//! 1. from_row_values_iter
//! 1. from_row_values
//! 1. get_row_by_idx
//! 1. get_row
//! 1. append
//! 1. insert_row_by_idx
//! 1. insert_row
//! 1. insert_rows_by_idx
//! 1. insert_rows
//! 1. iter_rows

use polars::prelude::Field;
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Serialize};

use super::{cis_err, ims_err, inf_err, oob_err, util::Stepper, SeriesIterator};
use crate::{
    CoreError, CoreResult, D2Value, Fabrix, FabrixDataFrame, IndexTag, Series, Value, ValueType,
};

#[derive(Debug, Clone)]
pub struct Row {
    pub index: Option<usize>,
    pub data: Vec<Value>,
}

impl Serialize for Row {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut q = s.serialize_seq(Some(self.len()))?;

        for v in self.data() {
            q.serialize_element(v)?;
        }

        q.end()
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RowVisitor;

        impl<'de> Visitor<'de> for RowVisitor {
            type Value = Row;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("[Value]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut data = Vec::<Value>::new();

                while let Some(v) = seq.next_element()? {
                    data.push(v);
                }

                Ok(Row::new(None, data))
            }
        }

        d.deserialize_seq(RowVisitor)
    }
}

impl Row {
    /// Row constructor
    pub fn new(index: Option<usize>, data: Vec<Value>) -> Self {
        let index = index.and_then(|i| if i >= data.len() { None } else { Some(i) });
        Row { index, data }
    }

    /// Row constructor, no index
    pub fn from_values(data: Vec<Value>) -> Self {
        Row { index: None, data }
    }

    /// get data
    pub fn data(&self) -> &[Value] {
        &self.data[..]
    }

    /// get index
    pub fn index(&self) -> Option<&Value> {
        self.index.and_then(|i| self.data.get(i))
    }

    /// get index type
    pub fn index_dtype(&self) -> Option<ValueType> {
        self.index().map(ValueType::from)
    }

    /// get data field
    pub fn data_fields(&self) -> Vec<Field> {
        self.data.iter().map(|v| v.into()).collect()
    }

    /// check if the row is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// row length
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// cast
    pub fn cast(self, types: &[ValueType]) -> CoreResult<Self> {
        let types_len = types.len();
        let self_len = self.len();

        if types_len != self_len {
            return Err(CoreError::LengthMismatch(types_len, self_len));
        }

        let data = self
            .data
            .into_iter()
            .zip(types.iter())
            .map(|(v, t)| v.force_cast(t))
            .collect();

        Ok(Self {
            index: self.index,
            data,
        })
    }
}

impl Fabrix {
    /// create a DataFrame by Rows, slower than column-wise constructors.
    /// cannot build from an empty `Vec<Row>`
    pub fn from_rows(mut rows: Vec<Row>) -> CoreResult<Self> {
        if rows.is_empty() {
            return Err(cis_err("row"));
        }

        // rows length
        let m = rows.len();
        // rows width
        let n = rows.first().unwrap().len();
        let mut series = Vec::with_capacity(n);
        let index_idx = rows.first().unwrap().index;
        for j in 0..n {
            let mut buf = Vec::with_capacity(m);
            for r in rows.iter_mut() {
                if r.index != index_idx {
                    return Err(ims_err());
                }
                let mut tmp = Value::Null;
                std::mem::swap(&mut tmp, &mut r.data[j]);
                buf.push(tmp);
            }
            series.push(Series::from_values(buf, &format!("Column_{:?}", j), true)?);
        }

        match index_idx {
            Some(i) => Fabrix::from_series(series, i),
            None => Fabrix::from_series_no_index(series),
        }
    }

    /// create a DataFrame by IntoIter<Vec<Value>>, slower than column-wise constructors
    /// cannot build from an empty iterator
    ///
    /// index_col: the index column location, default is None
    pub fn from_row_values_iter(
        iter: std::vec::IntoIter<Vec<Value>>,
        index_col: Option<usize>,
        has_header: bool,
    ) -> CoreResult<Self> {
        // create a peekable iterator
        let mut iter = iter.peekable();

        if iter.peek().is_none() {
            return Err(cis_err("row"));
        }

        // length of the first row, and width of the dataframe. number of columns
        let n = iter.peek().unwrap().len();
        let mut transposed_values: D2Value = vec![vec![]; n];

        // if has header, skip the first row
        let header = if has_header {
            Some(
                iter.next()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        for row in iter {
            row.into_iter()
                .enumerate()
                .for_each(|(i, v)| transposed_values[i].push(v));
        }

        // from the `transposed_values` to a vec of series
        let series = transposed_values
            .into_iter()
            .enumerate()
            .map(|(i, v)| Series::from_values(v, &format!("Column_{:?}", i), true))
            .collect::<CoreResult<Vec<_>>>()?;

        let mut fx = match index_col {
            Some(s) => Fabrix::from_series(series, s),
            None => Fabrix::from_series_no_index(series),
        }?;

        // set header
        if let Some(h) = &header {
            fx.set_column_names(h)?;
        }

        Ok(fx)
    }

    /// create a DataFrame by D2Value, slower than column-wise constructors
    pub fn from_row_values(
        values: D2Value,
        index_col: Option<usize>,
        has_header: bool,
    ) -> CoreResult<Self> {
        let iter = values.into_iter();
        Fabrix::from_row_values_iter(iter, index_col, has_header)
    }

    /// get a row by idx. This method is slower than get a column (`self.data.get_row`).
    /// beware performance: `Series.get` is slow.
    pub fn get_row_by_idx(&self, idx: usize) -> CoreResult<Row> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        let (data, index) = (
            self.data
                .iter()
                .map(|s| Value::from(s.get(idx)))
                .collect::<Vec<_>>(),
            self.index_tag().map(|it| it.loc),
        );

        Ok(Row { index, data })
    }

    /// get a row by index. This method is slower than get a column.
    pub fn get_row(&self, index: &Value) -> CoreResult<Row> {
        match self.index_tag() {
            Some(it) => {
                let idx = self.get_column(&it.name)?.find_index(index);
                match idx {
                    Some(i) => self.get_row_by_idx(i),
                    None => Err(inf_err()),
                }
            }
            None => Err(inf_err()),
        }
    }

    /// append a row to the dataframe. dtypes of the row must be equivalent to self dtypes
    pub fn append(&mut self, row: Row) -> CoreResult<&mut Self> {
        let mut d = Fabrix::from_rows(vec![row])?;
        d.set_column_names(&self.get_column_names())?;
        self.vconcat_mut(&d)
    }

    /// insert a row into the dataframe by idx
    pub fn insert_row_by_idx(&mut self, idx: usize, row: Row) -> CoreResult<&mut Self> {
        let len = self.height();
        let mut d1 = self.slice(0, idx);
        let d2 = self.slice(idx as i64, len);

        d1.append(row)?.vconcat_mut(&d2)?;
        *self = d1;

        Ok(self)
    }

    /// insert a row into the dataframe
    pub fn insert_row(&mut self, index: &Value, row: Row) -> CoreResult<&mut Self> {
        match &self.index_tag {
            Some(it) => {
                let idx = self.get_column(&it.name)?.find_index(index);
                match idx {
                    Some(idx) => self.insert_row_by_idx(idx, row),
                    None => Err(inf_err()),
                }
            }
            None => Err(inf_err()),
        }
    }

    /// insert rows into the dataframe by idx
    pub fn insert_rows_by_idx(&mut self, idx: usize, rows: Vec<Row>) -> CoreResult<&mut Self> {
        let len = self.height();
        let mut d1 = self.slice(0, idx);
        let d2 = self.slice(idx as i64, len);
        let mut di = Fabrix::from_rows(rows)?;
        di.set_column_names(&self.get_column_names())?;

        d1.vconcat_mut(&di)?.vconcat_mut(&d2)?;
        *self = d1;

        Ok(self)
    }

    /// insert rows into the dataframe by index
    pub fn insert_rows(&mut self, index: &Value, rows: Vec<Row>) -> CoreResult<&mut Self> {
        match &self.index_tag {
            Some(it) => {
                let idx = self.get_column(&it.name)?.find_index(index);
                match idx {
                    Some(i) => self.insert_rows_by_idx(i, rows),
                    None => Err(inf_err()),
                }
            }
            None => Err(inf_err()),
        }
    }

    /// iterate through the rows of the dataframe, same as `into_iter()`
    pub fn iter_rows(&self) -> IntoIteratorRow {
        FabrixIterToRow(self).into_iter()
    }
}

// ================================================================================================
// IntoIteratorRow for Fabrix & FabrixDataFrame
// ================================================================================================

/// FabrixIterToRow
pub struct FabrixIterToRow<'a>(&'a Fabrix);

pub struct IntoIteratorRow<'a> {
    index: Option<usize>,
    data_iters: Vec<SeriesIterator<'a>>,
    stepper: Stepper,
}

impl<'a> Iterator for IntoIteratorRow<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stepper.exhausted() {
            None
        } else {
            let data = self
                .data_iters
                .iter_mut()
                .map(|v| v.next().unwrap())
                .collect::<Vec<_>>();

            self.stepper.forward();
            Some(Row::new(self.index, data))
        }
    }
}

impl<'a> IntoIterator for FabrixIterToRow<'a> {
    type Item = Row;
    type IntoIter = IntoIteratorRow<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut data_iters = Vec::with_capacity(self.0.width());
        for s in self.0.iter_column() {
            data_iters.push(s.into_iter());
        }

        IntoIteratorRow {
            index: self.0.index_tag().map(IndexTag::loc),
            data_iters,
            stepper: Stepper::new(self.0.height()),
        }
    }
}

/// FabrixDataFrameIterToRow
pub struct FabrixDataFrameIterToRow<'a>(&'a FabrixDataFrame);

impl<'a> IntoIterator for FabrixDataFrameIterToRow<'a> {
    type Item = Row;
    type IntoIter = IntoIteratorRow<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut data_iters = Vec::with_capacity(self.0.width());
        for s in self.0.iter_column() {
            data_iters.push(s.into_iter());
        }

        IntoIteratorRow {
            index: None,
            data_iters,
            stepper: Stepper::new(self.0.height()),
        }
    }
}

impl FabrixDataFrame {
    pub fn iter_rows(&self) -> IntoIteratorRow {
        FabrixDataFrameIterToRow(self).into_iter()
    }
}

#[cfg(test)]
mod test_row {

    use crate::{fx, rows, value, Fabrix, Row};

    #[test]
    fn test_from_rows() {
        let rows = rows!(
            [0, "Jacob", "A", 10],
            [1, "Sam", "A", 9],
            [2, "James", "A", 9],
        );

        let df = Fabrix::from_rows(rows);
        assert!(df.is_ok());
        assert!(df.unwrap().shape() == (3, 4));

        let rows = rows!(
            0;
            [0, "Jacob", "A", 10],
            [1, "Sam", "A", 9],
            [2, "James", "A", 9],
        );

        let df = Fabrix::from_rows(rows);
        assert!(df.is_ok());
        let df = df.unwrap();
        assert!(df.shape() == (3, 4));
    }

    #[test]
    fn test_from_vec_vec_value() {
        let vvv = vec![
            vec![value!(11), value!("Jacob"), value!("A"), value!(10)],
            vec![value!(21), value!("Sam"), value!("A"), value!(9)],
            vec![value!(31), value!("James"), value!("A"), value!(9)],
        ];

        let df = Fabrix::from_row_values(vvv, None, false);
        assert!(df.is_ok());

        let df = df.unwrap();
        assert!(df.shape() == (3, 4));
    }

    #[test]
    fn test_get_row() {
        let df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ];
        assert!(df.is_ok());

        let df = df.unwrap();
        assert_eq!(df.shape(), (3, 3));

        let test1 = df.get_row_by_idx(1).unwrap();
        assert_eq!(test1.index().unwrap(), &value!(2));
        assert_eq!(
            test1.data(),
            &[value!("Sam"), value!(2), value!(None::<i32>)]
        );

        let test2 = df.get_row(&value!(2i32)).unwrap();
        assert_eq!(test2.index().unwrap(), &value!(2));
        assert_eq!(
            test2.data(),
            &[value!("Sam"), value!(2), value!(None::<i32>)]
        );
    }

    #[test]
    fn test_df_op() {
        let mut df = fx![
            "ord";
            "names" => ["Jacob", "Sam", "James"],
            "ord" => [1, 2, 3],
            "val" => [10, 9, 8]
        ]
        .unwrap();

        let row1 = Row::new(Some(1), vec![value!("Mia"), value!(4), value!(10)]);
        let res1 = df.append(row1);
        assert!(res1.is_ok(), "append row should work");

        let row2 = Row::new(Some(1), vec![value!("Mandy"), value!(5), value!(9)]);
        let res2 = df.insert_row(&value!(2), row2);
        assert!(res2.is_ok(), "insert row should work");
        assert!(df.shape() == (5, 3));

        let rows = rows!(
            1;
            ["Jamie", 6, 9],
            ["Justin", 7, 6],
            ["Julia", 8, 8]
        );

        let res3 = df.insert_rows(&value!(5), rows);
        assert!(res3.is_ok(), "insert rows should work");

        let res4 = df.remove_row(&value!(7));
        assert!(res4.is_ok(), "remove row should work");
        assert_eq!(df.shape(), (7, 3));

        let res4 = df.remove_slice(1, 2);
        assert!(res4.is_ok(), "remove slice should work");
        assert_eq!(df.shape(), (5, 3));

        let res5 = df.remove_rows(vec![value!(2), value!(4)]);
        assert!(res5.is_ok(), "remove rows should work");
        assert_eq!(df.shape(), (2, 3));
    }

    #[test]
    fn test_df_iter() {
        let fx = fx![
            "name" => ["Jacob", "Sam", "James", "Julia"],
            "star" => [100, 99, 100, 69],
            "loc" => [2u8, 3, 1, 4]
        ]
        .unwrap();

        let mut iter = fx.iter_rows();

        let r1 = iter.next();
        assert!(r1.is_some());
        assert_eq!(
            r1.unwrap().data(),
            vec![value!("Jacob"), value!(100), value!(2u8)]
        );

        let r2 = iter.next();
        assert!(r2.is_some());
        assert_eq!(
            r2.unwrap().data(),
            vec![value!("Sam"), value!(99), value!(3u8)]
        );

        let r3 = iter.next();
        assert!(r3.is_some());
        assert_eq!(
            r3.unwrap().data(),
            vec![value!("James"), value!(100), value!(1u8)]
        );

        let r4 = iter.next();
        assert!(r4.is_some());
        assert_eq!(
            r4.unwrap().data(),
            vec![value!("Julia"), value!(69), value!(4u8)]
        );

        let r5 = iter.next();
        assert!(r5.is_none());
    }

    #[test]
    fn row_se_and_de_success() {
        use crate::{date, datetime, time, value};

        let data = vec![
            value!(date!(2020, 1, 1)),
            value!(time!(12, 0, 0)),
            value!(datetime!(2020, 1, 1, 12, 0, 0)),
        ];
        let row = Row::new(None, data);

        let foo = serde_json::to_string(&row);
        println!("{:?}", foo);

        let foo_str = "[18262,43200000000000,1577880000000000000]";

        let bar: Row = serde_json::from_str(foo_str).unwrap();

        println!("{:?}", bar);
    }
}
