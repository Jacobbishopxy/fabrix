//! Fabrix Json Executor

use fabrix_core::{polars::prelude::DataFrame, polars::prelude::Series as PolarsSeries};
use fabrix_core::{Fabrix, IndexTag, Series, SeriesRef};
use serde::{
    de::{self, Visitor},
    ser::SerializeSeq,
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::Value as JsonValue;

use crate::{JsonError, JsonResult};

pub enum JsonType {
    Column,
    Row,
    Dataset,
}

fn dataframe_column_wised_serialize<S>(fx: &DataFrame, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let series = fx.get_columns().iter().map(SeriesRef).collect::<Vec<_>>();
    let mut seq = s.serialize_seq(Some(series.len()))?;
    for e in series {
        seq.serialize_element(&e)?;
    }
    seq.end()
}

fn dataframe_column_wised_deserialize<'de, D>(d: D) -> Result<DataFrame, D::Error>
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

            DataFrame::new(polars_series)
                .map_err(|_| de::Error::invalid_length(0, &"must have at least one series"))
        }
    }

    d.deserialize_seq(DfVisitor)
}

/// redesign se/de for `DataFrame`
#[derive(Serialize, Deserialize)]
// #[serde(remote = "DataFrame")]
pub struct DataFrameDef {
    // #[serde(getter = "DataFrame::get_columns")]
    data: Vec<Series>,
}

impl From<DataFrameDef> for DataFrame {
    fn from(def: DataFrameDef) -> Self {
        let s = def.data.into_iter().map(|i| i.0).collect();
        DataFrame::new(s).unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct FabrixColumnWised {
    #[serde(
        serialize_with = "dataframe_column_wised_serialize",
        deserialize_with = "dataframe_column_wised_deserialize"
    )]
    pub data: DataFrame,
    pub index_tag: Option<IndexTag>,
}

impl From<FabrixColumnWised> for Fabrix {
    fn from(v: FabrixColumnWised) -> Self {
        Fabrix {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}

impl From<Fabrix> for FabrixColumnWised {
    fn from(v: Fabrix) -> Self {
        FabrixColumnWised {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}

#[derive(Default)]
pub struct JsonExecutor {
    pub(crate) data: Option<Fabrix>,
}

impl JsonExecutor {
    pub fn new() -> Self {
        Self { data: None }
    }

    pub fn new_with_data<T: Into<Fabrix>>(data: T) -> Self {
        Self {
            data: Some(data.into()),
        }
    }

    pub fn with_data<T: Into<Fabrix>>(&mut self, data: T) -> &mut Self {
        self.data = Some(data.into());
        self
    }

    pub fn data(&self) -> Option<&Fabrix> {
        self.data.as_ref()
    }

    pub fn from_json(&mut self, json: JsonValue, json_type: JsonType) -> JsonResult<&mut Self> {
        match json_type {
            JsonType::Column => {
                let res: Result<FabrixColumnWised, serde_json::Error> =
                    serde_json::from_value(json);
                self.data = Some(res?.into());

                Ok(self)
            }
            JsonType::Row => todo!(),
            JsonType::Dataset => todo!(),
        }
    }

    pub fn from_str(&mut self, s: &str, json_type: JsonType) -> JsonResult<&mut Self> {
        match json_type {
            JsonType::Column => {
                let res: Result<FabrixColumnWised, serde_json::Error> = serde_json::from_str(s);
                self.data = Some(res?.into());

                Ok(self)
            }
            JsonType::Row => todo!(),
            JsonType::Dataset => todo!(),
        }
    }

    pub fn to_json(&self, json_type: JsonType) -> JsonResult<JsonValue> {
        match self.data.as_ref() {
            Some(d) => match json_type {
                JsonType::Column => {
                    let res = serde_json::to_value(FabrixColumnWised::from(d.clone()))?;
                    Ok(res)
                }
                JsonType::Row => todo!(),
                JsonType::Dataset => todo!(),
            },
            None => Err(JsonError::SourceNotFound),
        }
    }

    pub fn to_string(&self, json_type: JsonType) -> JsonResult<String> {
        match self.data.as_ref() {
            Some(d) => match json_type {
                JsonType::Column => {
                    let res = serde_json::to_string(&FabrixColumnWised::from(d.clone()))?;
                    Ok(res)
                }
                JsonType::Row => todo!(),
                JsonType::Dataset => todo!(),
            },
            None => Err(JsonError::SourceNotFound),
        }
    }
}

#[cfg(test)]
mod test_ec {
    use super::*;
    use fabrix_core::{date, datetime, fx, time};

    #[test]
    fn serialize_and_deserialize_success() {
        let df = fx![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        let mut jec = JsonExecutor::new();
        jec.with_data(df.clone());

        let foo = jec.to_string(JsonType::Column);
        println!("{:?}", foo);

        let foo_str = "{\"data\":[{\"name\":\"names\",\"datatype\":\"String\",\"values\":[\"Jacob\",\"Sam\",\"Jason\"]},{\"name\":\"ord\",\"datatype\":\"I32\",\"values\":[1,2,3]},{\"name\":\"val\",\"datatype\":\"I32\",\"values\":[10,null,8]}],\"index_tag\":null}";
        assert_eq!(foo.unwrap(), foo_str);

        jec.from_str(foo_str, JsonType::Column)
            .expect("failed parsing from string");

        let bar = jec.data();
        println!("{:?}", bar);
        assert_eq!(bar.unwrap(), &df);
    }

    #[test]
    fn serialize_and_deserialize_success_r() {
        let df = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
        ]
        .unwrap();

        let mut jec = JsonExecutor::new();
        jec.with_data(df.clone());

        let foo = jec.to_string(JsonType::Column);
        println!("{:?}", foo);

        let foo_str = "{\"data\":[{\"name\":\"id\",\"datatype\":\"I32\",\"values\":[1,2,3]},{\"name\":\"name\",\"datatype\":\"String\",\"values\":[\"a\",\"b\",\"c\"]},{\"name\":\"date\",\"datatype\":\"Date\",\"values\":[18262,18263,18264]},{\"name\":\"time\",\"datatype\":\"Time\",\"values\":[43200000000000,43201000000000,43202000000000]},{\"name\":\"datetime\",\"datatype\":\"DateTime\",\"values\":[1577880000000000000,1577880001000000000,1577880002000000000]}],\"index_tag\":{\"loc\":0,\"name\":\"id\",\"data_type\":\"I32\"}}";
        assert_eq!(foo.unwrap(), foo_str);

        jec.from_str(foo_str, JsonType::Column)
            .expect("failed parsing from string");

        let bar = jec.data();

        println!("{:?}", bar);
        assert_eq!(bar.unwrap(), &df);
    }
}
