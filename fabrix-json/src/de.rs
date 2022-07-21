//! Deserialize functions

use std::borrow::Cow;
use std::collections::HashMap;

use fabrix_core::polars::prelude::{DataFrame, Series as PolarsSeries};
use fabrix_core::{Fabrix, NamedRow, Row, Series, Value, ValueType};
use serde::{
    de::{self, Visitor},
    Deserializer,
};

pub(crate) fn dataframe_column_wise_deserialize<'de, D>(d: D) -> Result<DataFrame, D::Error>
where
    D: Deserializer<'de>,
{
    struct DfVisitor;

    impl<'de> Visitor<'de> for DfVisitor {
        type Value = DataFrame;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("[{name: <name>, datatype: <dtype>, values: <values array>}]")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut polars_series = Vec::<PolarsSeries>::new();

            while let Some(e) = seq.next_element::<Series>()? {
                polars_series.push(e.0);
            }

            DataFrame::new(polars_series).map_err(de::Error::custom)
        }
    }

    d.deserialize_seq(DfVisitor)
}

pub(crate) fn dataframe_row_wise_deserialize<'de, D>(d: D) -> Result<DataFrame, D::Error>
where
    D: Deserializer<'de>,
{
    const FIELDS: &[&str] = &["values", "types"];

    struct DfVisitor;

    impl<'de> Visitor<'de> for DfVisitor {
        type Value = DataFrame;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("{types: <type array>, values: [<values map>]}")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            let mut types = Vec::<ValueType>::new();
            let mut values_set = false;
            let mut count = 0;
            while let Some(k) = map.next_key::<Cow<str>>()? {
                count += 1;
                match k.as_ref() {
                    "types" => {
                        let t = map.next_value::<Vec<ValueType>>()?;
                        types.extend(t);
                    }
                    "values" => {
                        values_set = true;
                        if count != 2 {
                            return Err(de::Error::custom("field values should behind types"));
                        }
                        break;
                    }
                    fld => return Err(de::Error::unknown_field(fld, FIELDS)),
                }
            }
            if !values_set {
                return Err(de::Error::missing_field("values"));
            }
            // TODO:

            // let values: Vec<HashMap<String, Value>> = map.next_value()?;
            // let rowmaps = values
            //     .into_iter()
            //     .map(|v| Rowmap::new(None, v))
            //     .collect::<Vec<_>>();

            // let fx = Fabrix::from_rowmaps(rowmaps).map_err(de::Error::custom)?;

            // Ok(fx.data)

            todo!()
        }
    }

    d.deserialize_map(DfVisitor)
}

pub(crate) fn dataframe_dataset_type_deserialize<'de, D>(d: D) -> Result<DataFrame, D::Error>
where
    D: Deserializer<'de>,
{
    struct DfVisitor;

    impl<'de> Visitor<'de> for DfVisitor {
        type Value = DataFrame;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("[<values array>]")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut rows = Vec::<Row>::new();

            while let Some(e) = seq.next_element::<Vec<Value>>()? {
                rows.push(Row::from_values(e));
            }

            let fx = Fabrix::from_rows(rows).map_err(de::Error::custom)?;
            Ok(fx.data)
        }
    }

    d.deserialize_seq(DfVisitor)
}
