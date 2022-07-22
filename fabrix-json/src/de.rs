//! Deserialize functions

use std::borrow::Cow;

use fabrix_core::polars::prelude::{DataFrame, Series as PolarsSeries};
use fabrix_core::{CoreResult, Fabrix, NamedRow, Row, Series, ValueType};
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
    const FIELDS: &[&str] = &["types", "values"];

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

            let values: Vec<NamedRow> = map.next_value()?;

            let named_rows = values
                .into_iter()
                .map(|nr| nr.cast(&types))
                .collect::<CoreResult<Vec<_>>>()
                .map_err(de::Error::custom)?;

            let fx = Fabrix::from_named_rows(named_rows).map_err(de::Error::custom)?;

            Ok(fx.data)
        }
    }

    d.deserialize_map(DfVisitor)
}

pub(crate) fn dataframe_dataset_type_deserialize<'de, D>(d: D) -> Result<DataFrame, D::Error>
where
    D: Deserializer<'de>,
{
    const FIELDS: &[&str] = &["names", "types", "values"];

    struct DfVisitor;

    impl<'de> Visitor<'de> for DfVisitor {
        type Value = DataFrame;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter
                .write_str("{names: <name array>, types: <type array>, values: [<values array>]}")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            let mut names = Vec::<String>::new();
            let mut types = Vec::<ValueType>::new();
            let mut values_set = false;
            let mut count = 0;
            while let Some(k) = map.next_key::<Cow<str>>()? {
                count += 1;
                match k.as_ref() {
                    "names" => {
                        let n = map.next_value::<Vec<String>>()?;
                        names.extend(n);
                    }
                    "types" => {
                        let t = map.next_value::<Vec<ValueType>>()?;
                        types.extend(t);
                    }
                    "values" => {
                        values_set = true;
                        if count != 3 {
                            return Err(de::Error::custom(
                                "field values should behind names and types",
                            ));
                        }
                        break;
                    }
                    fld => return Err(de::Error::unknown_field(fld, FIELDS)),
                }
            }
            if !values_set {
                return Err(de::Error::missing_field("values"));
            }

            let values: Vec<Row> = map.next_value()?;

            let rows = values
                .into_iter()
                .map(|r| r.cast(&types))
                .collect::<CoreResult<Vec<_>>>()
                .map_err(|_| de::Error::custom("rows error"))?;

            let mut fx = Fabrix::from_rows(rows).map_err(|_| de::Error::custom("fx error"))?;
            fx.set_column_names(&names).map_err(de::Error::custom)?;

            Ok(fx.data)
        }
    }

    d.deserialize_map(DfVisitor)
}
