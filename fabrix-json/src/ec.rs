//! Fabrix Json Executor

use fabrix_core::Fabrix;
use serde_json::{Error as SerdeJsonError, Value as SerdeJsonValue};

use crate::{FabrixColumnWise, FabrixDataset, FabrixRowWise, JsonError, JsonResult, JsonType};

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

    pub fn from_json(
        &mut self,
        json: SerdeJsonValue,
        json_type: JsonType,
    ) -> JsonResult<&mut Self> {
        match json_type {
            JsonType::Column => {
                let res: Result<FabrixColumnWise, SerdeJsonError> = serde_json::from_value(json);
                self.data = Some(res?.into());

                Ok(self)
            }
            JsonType::Row => {
                let res: Result<FabrixRowWise, SerdeJsonError> = serde_json::from_value(json);
                self.data = Some(res?.into());

                Ok(self)
            }
            JsonType::Dataset => {
                let res: Result<FabrixDataset, SerdeJsonError> = serde_json::from_value(json);
                self.data = Some(res?.into());

                Ok(self)
            }
        }
    }

    pub fn from_str(&mut self, s: &str, json_type: JsonType) -> JsonResult<&mut Self> {
        match json_type {
            JsonType::Column => {
                let res: Result<FabrixColumnWise, SerdeJsonError> = serde_json::from_str(s);
                self.data = Some(res?.into());

                Ok(self)
            }
            JsonType::Row => {
                let res: Result<FabrixRowWise, SerdeJsonError> = serde_json::from_str(s);
                self.data = Some(res?.into());

                Ok(self)
            }
            JsonType::Dataset => {
                let res: Result<FabrixDataset, SerdeJsonError> = serde_json::from_str(s);
                self.data = Some(res?.into());

                Ok(self)
            }
        }
    }

    pub fn to_json(&self, json_type: JsonType) -> JsonResult<SerdeJsonValue> {
        match self.data.as_ref() {
            Some(d) => match json_type {
                JsonType::Column => {
                    let res = serde_json::to_value(FabrixColumnWise::from(d.clone()))?;

                    Ok(res)
                }
                JsonType::Row => {
                    let res = serde_json::to_value(FabrixRowWise::from(d.clone()))?;

                    Ok(res)
                }
                JsonType::Dataset => {
                    let res = serde_json::to_value(FabrixDataset::from(d.clone()))?;

                    Ok(res)
                }
            },
            None => Err(JsonError::SourceNotFound),
        }
    }

    pub fn to_string(&self, json_type: JsonType) -> JsonResult<String> {
        match self.data.as_ref() {
            Some(d) => match json_type {
                JsonType::Column => {
                    let res = serde_json::to_string(&FabrixColumnWise::from(d.clone()))?;

                    Ok(res)
                }
                JsonType::Row => {
                    let res = serde_json::to_string(&FabrixRowWise::from(d.clone()))?;

                    Ok(res)
                }
                JsonType::Dataset => {
                    let res = serde_json::to_string(&FabrixDataset::from(d.clone()))?;

                    Ok(res)
                }
            },
            None => Err(JsonError::SourceNotFound),
        }
    }
}

#[cfg(test)]
mod test_ec {
    use super::*;
    use fabrix_core::{bytes, date, datetime, decimal, fx, time, uuid};

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
    fn serialize_and_deserialize_column_wise_success() {
        let df = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
            "decimal" => [decimal!(1000, 24), decimal!(820, 23), decimal!(2100, 15)],
            "uuid" => [uuid!("72b1e846-1f57-47da-bae0-9c08b0ba0fce"), uuid!("31c9fe2a-4b23-4938-9b84-ae9ea1ef43f8"), uuid!("088a052c-aa47-41c2-b3b4-6813776d0edc")],
            "bytes" => [bytes!("Jacob"), bytes!("Sam"), bytes!("James")]
        ]
        .unwrap();

        let mut jec = JsonExecutor::new();
        jec.with_data(df.clone());

        let foo = jec.to_string(JsonType::Column);
        println!("{:?}", foo);

        let foo_str = "{\"data\":[{\"name\":\"id\",\"datatype\":\"I32\",\"values\":[1,2,3]},{\"name\":\"name\",\"datatype\":\"String\",\"values\":[\"a\",\"b\",\"c\"]},{\"name\":\"date\",\"datatype\":\"Date\",\"values\":[18262,18263,18264]},{\"name\":\"time\",\"datatype\":\"Time\",\"values\":[43200000000000,43201000000000,43202000000000]},{\"name\":\"datetime\",\"datatype\":\"DateTime\",\"values\":[1577880000000000000,1577880001000000000,1577880002000000000]},{\"name\":\"decimal\",\"datatype\":\"Decimal\",\"values\":[\"0.000000000000000000001000\",\"0.00000000000000000000820\",\"0.000000000002100\"]},{\"name\":\"uuid\",\"datatype\":\"Uuid\",\"values\":[\"72b1e846-1f57-47da-bae0-9c08b0ba0fce\",\"31c9fe2a-4b23-4938-9b84-ae9ea1ef43f8\",\"088a052c-aa47-41c2-b3b4-6813776d0edc\"]},{\"name\":\"bytes\",\"datatype\":\"Bytes\",\"values\":[[74,97,99,111,98],[83,97,109],[74,97,109,101,115]]}],\"index_tag\":{\"loc\":0,\"name\":\"id\",\"data_type\":\"I32\"}}";
        assert_eq!(foo.unwrap(), foo_str);

        jec.from_str(foo_str, JsonType::Column)
            .expect("failed parsing from string");

        let bar = jec.data();

        println!("{:?}", bar);
        assert_eq!(bar.unwrap(), &df);
    }

    #[test]
    fn serialize_and_deserialize_row_wise_success() {
        let df = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
            "decimal" => [decimal!(1000, 24), decimal!(820, 23), decimal!(2100, 15)],
            "uuid" => [uuid!("72b1e846-1f57-47da-bae0-9c08b0ba0fce"), uuid!("31c9fe2a-4b23-4938-9b84-ae9ea1ef43f8"), uuid!("088a052c-aa47-41c2-b3b4-6813776d0edc")],
            "bytes" => [bytes!("Jacob"), bytes!("Sam"), bytes!("James")]
        ]
        .unwrap();

        println!("{:?}", &df);

        let mut jec = JsonExecutor::new();
        jec.with_data(df.clone());

        let foo = jec.to_string(JsonType::Row);
        println!("{:?}", foo);

        let foo_str = "{\"data\":{\"types\":[\"I32\",\"String\",\"Date\",\"Time\",\"DateTime\",\"Decimal\",\"Uuid\",\"Bytes\"],\"values\":[{\"id\":1,\"name\":\"a\",\"date\":18262,\"time\":43200000000000,\"datetime\":1577880000000000000,\"decimal\":\"0.000000000000000000001000\",\"uuid\":\"72b1e846-1f57-47da-bae0-9c08b0ba0fce\",\"bytes\":[74,97,99,111,98]},{\"id\":2,\"name\":\"b\",\"date\":18263,\"time\":43201000000000,\"datetime\":1577880001000000000,\"decimal\":\"0.00000000000000000000820\",\"uuid\":\"31c9fe2a-4b23-4938-9b84-ae9ea1ef43f8\",\"bytes\":[83,97,109]},{\"id\":3,\"name\":\"c\",\"date\":18264,\"time\":43202000000000,\"datetime\":1577880002000000000,\"decimal\":\"0.000000000002100\",\"uuid\":\"088a052c-aa47-41c2-b3b4-6813776d0edc\",\"bytes\":[74,97,109,101,115]}]},\"index_tag\":{\"loc\":0,\"name\":\"id\",\"data_type\":\"I32\"}}";
        assert_eq!(foo.unwrap(), foo_str);

        jec.from_str(foo_str, JsonType::Row)
            .expect("failed parsing from string");

        let bar = jec.data();

        println!("{:?}", bar);
        assert_eq!(bar.unwrap(), &df);
    }

    #[test]
    fn serialize_and_deserialize_dataset_type_success() {
        let df = fx![
            "id";
            "id" => [1, 2, 3],
            "name" => ["a", "b", "c"],
            "date" => [date!(2020,1,1), date!(2020,1,2), date!(2020,1,3)],
            "time" => [time!(12,0,0), time!(12,0,1), time!(12,0,2)],
            "datetime" => [datetime!(2020,1,1,12,0,0), datetime!(2020,1,1,12,0,1), datetime!(2020,1,1,12,0,2)],
            "decimal" => [decimal!(1000, 24), decimal!(820, 23), decimal!(2100, 15)],
            "uuid" => [uuid!("72b1e846-1f57-47da-bae0-9c08b0ba0fce"), uuid!("31c9fe2a-4b23-4938-9b84-ae9ea1ef43f8"), uuid!("088a052c-aa47-41c2-b3b4-6813776d0edc")],
            "bytes" => [bytes!("Jacob"), bytes!("Sam"), bytes!("James")]
        ]
        .unwrap();

        println!("{:?}", &df);

        let mut jec = JsonExecutor::new();
        jec.with_data(df.clone());

        let foo = jec.to_string(JsonType::Dataset);
        println!("{:?}", foo);

        let foo_str = "{\"data\":{\"names\":[\"id\",\"name\",\"date\",\"time\",\"datetime\",\"decimal\",\"uuid\",\"bytes\"],\"types\":[\"I32\",\"String\",\"Date\",\"Time\",\"DateTime\",\"Decimal\",\"Uuid\",\"Bytes\"],\"values\":[[1,\"a\",18262,43200000000000,1577880000000000000,\"0.000000000000000000001000\",\"72b1e846-1f57-47da-bae0-9c08b0ba0fce\",[74,97,99,111,98]],[2,\"b\",18263,43201000000000,1577880001000000000,\"0.00000000000000000000820\",\"31c9fe2a-4b23-4938-9b84-ae9ea1ef43f8\",[83,97,109]],[3,\"c\",18264,43202000000000,1577880002000000000,\"0.000000000002100\",\"088a052c-aa47-41c2-b3b4-6813776d0edc\",[74,97,109,101,115]]]},\"index_tag\":{\"loc\":0,\"name\":\"id\",\"data_type\":\"I32\"}}";
        assert_eq!(foo.unwrap(), foo_str);

        jec.from_str(foo_str, JsonType::Dataset)
            .expect("failed parsing from string");

        let bar = jec.data();

        println!("{:?}", bar);
        assert_eq!(bar.unwrap(), &df);
    }
}
