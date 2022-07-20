//! Fabrix RowMap

use std::collections::BTreeMap;

use polars::datatypes::Field;
use serde::{Deserialize, Serialize};

use crate::{
    util::{cis_err, ims_err, inf_err, oob_err, Stepper},
    CoreResult, Fabrix, IndexTag, Series, SeriesIterator, SeriesRef, Value, ValueType,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rowmap {
    pub index: Option<usize>,
    pub data: BTreeMap<String, Value>,
}

impl Rowmap {
    /// RowMap constructor
    pub fn new(index: Option<usize>, data: BTreeMap<String, Value>) -> Self {
        let index = index.and_then(|i| if i >= data.len() { None } else { Some(i) });
        Rowmap { index, data }
    }

    /// Row constructor, no index
    pub fn from_values(data: Vec<(String, Value)>) -> Self {
        Rowmap {
            index: None,
            data: BTreeMap::from_iter(data.into_iter()),
        }
    }

    /// get data
    pub fn data(&self) -> &BTreeMap<String, Value> {
        &self.data
    }

    /// get index
    pub fn index(&self) -> Option<&Value> {
        self.index
            .and_then(|i| self.data.iter().nth(i).map(|t| t.1))
    }

    /// get index type
    pub fn index_dtype(&self) -> Option<ValueType> {
        self.index().map(ValueType::from)
    }

    /// get data field
    pub fn data_fields(&self) -> Vec<Field> {
        self.data.iter().map(|v| v.1.into()).collect()
    }

    /// check if the row is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// row length
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl Fabrix {
    /// create a DataFrame by RowMaps
    pub fn from_rowmaps(mut rowmaps: Vec<Rowmap>) -> CoreResult<Self> {
        if rowmaps.is_empty() {
            return Err(cis_err("rowmap"));
        }

        // rowmaps' length
        let m = rowmaps.len();
        // rowmaps' width
        let n = rowmaps.first().unwrap().len();
        let mut series = Vec::with_capacity(n);
        let index_idx = rowmaps.first().unwrap().index;
        let names = rowmaps
            .first()
            .unwrap()
            .data()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for (j, n) in names.iter().enumerate() {
            let mut buf = Vec::with_capacity(m);
            for r in rowmaps.iter_mut() {
                if r.index != index_idx {
                    return Err(ims_err());
                }
                let mut tmp = Value::Null;
                let (_, v) = r.data.iter_mut().nth(j).unwrap();
                std::mem::swap(&mut tmp, v);
                buf.push(tmp);
            }
            series.push(Series::from_values(buf, n, true)?);
        }

        match index_idx {
            Some(i) => Fabrix::from_series(series, i),
            None => Fabrix::from_series_no_index(series),
        }
    }

    pub fn get_rowmap_by_idx(&self, idx: usize) -> CoreResult<Rowmap> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        let (data, index) = (
            self.data
                .iter()
                .map(|s| (s.name().to_owned(), Value::from(s.get(idx))))
                .collect::<BTreeMap<_, _>>(),
            self.index_tag().map(|it| it.loc),
        );

        Ok(Rowmap { index, data })
    }

    pub fn get_rowmap(&self, index: &Value) -> CoreResult<Rowmap> {
        match self.index_tag() {
            Some(it) => {
                let idx = SeriesRef(self.data.column(&it.name)?).find_index(index);
                match idx {
                    Some(i) => self.get_rowmap_by_idx(i),
                    None => Err(inf_err()),
                }
            }
            None => Err(inf_err()),
        }
    }

    pub fn iter_rowmaps(&self) -> IntoIteratorRowmap {
        FabrixIterToRowmap(self).into_iter()
    }
}

pub struct FabrixIterToRowmap<'a>(&'a Fabrix);

pub struct IntoIteratorRowmap<'a> {
    index: Option<usize>,
    data_iters: Vec<(&'a str, SeriesIterator<'a>)>,
    stepper: Stepper,
}

impl<'a> Iterator for IntoIteratorRowmap<'a> {
    type Item = Rowmap;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stepper.exhausted() {
            None
        } else {
            let data = self
                .data_iters
                .iter_mut()
                .map(|(ref n, s)| (n.to_string(), s.next().unwrap()))
                .collect::<BTreeMap<_, _>>();

            self.stepper.forward();
            Some(Rowmap::new(self.index, data))
        }
    }
}

impl<'a> IntoIterator for FabrixIterToRowmap<'a> {
    type Item = Rowmap;
    type IntoIter = IntoIteratorRowmap<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut data_iters = Vec::with_capacity(self.0.width());
        for s in self.0.data.iter() {
            let iter = SeriesRef(s).into_iter();
            data_iters.push((s.name(), iter));
        }

        IntoIteratorRowmap {
            index: self.0.index_tag().map(IndexTag::loc),
            data_iters,
            stepper: Stepper::new(self.0.height()),
        }
    }
}
