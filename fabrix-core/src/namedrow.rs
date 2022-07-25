//! Fabrix RowMap

use polars::datatypes::Field;
use serde::{de::Visitor, ser::SerializeMap, Deserialize, Serialize};

use crate::{
    util::{cis_err, ims_err, inf_err, oob_err, Stepper},
    CoreError, CoreResult, Fabrix, FabrixRef, FabrixViewer, IndexTag, Series, SeriesIterator,
    SeriesRef, Value, ValueType,
};

#[derive(Debug, Clone)]
pub struct NamedRow {
    pub index: Option<usize>,
    pub data: Vec<(String, Value)>,
}

impl Serialize for NamedRow {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut m = s.serialize_map(Some(self.len()))?;

        for (key, value) in self.data() {
            m.serialize_entry(key, value)?;
        }

        m.end()
    }
}

impl<'de> Deserialize<'de> for NamedRow {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NamedRowVisitor;

        impl<'de> Visitor<'de> for NamedRowVisitor {
            type Value = NamedRow;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("{name: Value}")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut data = Vec::<(String, Value)>::new();

                while let Some(kv) = map.next_entry()? {
                    data.push(kv);
                }

                Ok(NamedRow::new(None, data))
            }
        }

        d.deserialize_map(NamedRowVisitor)
    }
}

impl NamedRow {
    /// RowMap constructor
    pub fn new(index: Option<usize>, data: Vec<(String, Value)>) -> Self {
        let index = index.and_then(|i| if i >= data.len() { None } else { Some(i) });
        NamedRow { index, data }
    }

    /// Row constructor, no index
    pub fn from_values(data: Vec<(String, Value)>) -> Self {
        NamedRow { index: None, data }
    }

    /// get data
    pub fn data(&self) -> &[(String, Value)] {
        &self.data
    }

    /// get index
    pub fn index(&self) -> Option<&Value> {
        self.index.and_then(|i| self.data.get(i).map(|t| &t.1))
    }

    /// get index type
    pub fn index_dtype(&self) -> Option<ValueType> {
        self.index().map(ValueType::from)
    }

    /// get data field
    pub fn data_fields(&self) -> Vec<Field> {
        self.data.iter().map(|v| (&v.1).into()).collect()
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
            .map(|((n, v), t)| (n, v.force_cast(t)))
            .collect();

        Ok(Self {
            index: self.index,
            data,
        })
    }
}

impl Fabrix {
    /// create a DataFrame by RowMaps
    pub fn from_named_rows(mut named_rows: Vec<NamedRow>) -> CoreResult<Self> {
        if named_rows.is_empty() {
            return Err(cis_err("rowmap"));
        }

        // rows' length
        let m = named_rows.len();
        // rows' width
        let n = named_rows.first().unwrap().len();
        let mut series = Vec::with_capacity(n);
        let index_idx = named_rows.first().unwrap().index;
        let names = named_rows
            .first()
            .unwrap()
            .data()
            .iter()
            .cloned()
            .map(|(n, _)| n)
            .collect::<Vec<_>>();

        for (j, nm) in names.iter().enumerate() {
            let mut buf = Vec::with_capacity(m);
            for r in named_rows.iter_mut() {
                if r.index != index_idx {
                    return Err(ims_err());
                }
                let mut tmp = Value::Null;
                let (_, v) = r.data.get_mut(j).unwrap();
                std::mem::swap(&mut tmp, v);
                buf.push(tmp);
            }
            series.push(Series::from_values(buf, nm, true)?);
        }

        match index_idx {
            Some(i) => Fabrix::from_series(series, i),
            None => Fabrix::from_series_no_index(series),
        }
    }

    pub fn get_named_row_by_idx(&self, idx: usize) -> CoreResult<NamedRow> {
        let len = self.height();
        if idx >= len {
            return Err(oob_err(idx, len));
        }
        let (data, index) = (
            self.data
                .iter()
                .map(|s| (s.name().to_owned(), Value::from(s.get(idx))))
                .collect::<Vec<_>>(),
            self.index_tag().map(|it| it.loc),
        );

        Ok(NamedRow::new(index, data))
    }

    pub fn get_named_row(&self, index: &Value) -> CoreResult<NamedRow> {
        match self.index_tag() {
            Some(it) => {
                let idx = SeriesRef(self.data.column(&it.name)?).find_index(index);
                match idx {
                    Some(i) => self.get_named_row_by_idx(i),
                    None => Err(inf_err()),
                }
            }
            None => Err(inf_err()),
        }
    }

    pub fn iter_named_rows(&self) -> IntoIteratorNamedRow {
        FabrixIterToNamedRow(self).into_iter()
    }
}

pub struct FabrixIterToNamedRow<'a>(&'a Fabrix);

impl<'a> FabrixIterToNamedRow<'a> {
    pub fn new(f: &'a Fabrix) -> Self {
        Self(f)
    }
}

pub struct FabrixRefIterToNamedRow<'a>(&'a FabrixRef<'a>);

impl<'a> FabrixRefIterToNamedRow<'a> {
    pub fn new(f: &'a FabrixRef) -> Self {
        Self(f)
    }
}

pub struct IntoIteratorNamedRow<'a> {
    index: Option<usize>,
    data_iters: Vec<(&'a str, SeriesIterator<'a>)>,
    stepper: Stepper,
}

impl<'a> Iterator for IntoIteratorNamedRow<'a> {
    type Item = NamedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stepper.exhausted() {
            None
        } else {
            let data = self
                .data_iters
                .iter_mut()
                .map(|(ref n, s)| (n.to_string(), s.next().unwrap()))
                .collect::<Vec<_>>();

            self.stepper.forward();
            Some(NamedRow::new(self.index, data))
        }
    }
}

impl<'a> IntoIterator for FabrixIterToNamedRow<'a> {
    type Item = NamedRow;
    type IntoIter = IntoIteratorNamedRow<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut data_iters = Vec::with_capacity(self.0.width());
        for s in self.0.data.iter() {
            let iter = SeriesRef(s).into_iter();
            data_iters.push((s.name(), iter));
        }

        IntoIteratorNamedRow {
            index: self.0.index_tag().map(IndexTag::loc),
            data_iters,
            stepper: Stepper::new(self.0.height()),
        }
    }
}

impl<'a> IntoIterator for FabrixRefIterToNamedRow<'a> {
    type Item = NamedRow;
    type IntoIter = IntoIteratorNamedRow<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut data_iters = Vec::with_capacity(self.0.width());
        for s in self.0.data.iter() {
            let iter = SeriesRef(s).into_iter();
            data_iters.push((s.name(), iter));
        }

        IntoIteratorNamedRow {
            index: self.0.index_tag().map(IndexTag::loc),
            data_iters,
            stepper: Stepper::new(self.0.height()),
        }
    }
}
