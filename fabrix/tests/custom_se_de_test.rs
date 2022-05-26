//! Custom Serialize/Deserialize

use std::fmt;

use fabrix::Value;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeTuple, Serializer};

enum Eqt {
    Not,
    Equal(Value),
    NotEqual(Value),
    Greater(Value),
    GreaterEqual(Value),
    Less(Value),
    LessEqual(Value),
    In(Vec<Value>),
    Between((Value, Value)),
    Like(String),
}

impl Serialize for Eqt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self {
            Eqt::Not => serializer.serialize_str("not"),
            Eqt::Equal(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element("=")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::NotEqual(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element("!=")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::Greater(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element(">")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::GreaterEqual(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element(">=")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::Less(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element("<")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::LessEqual(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element("<=")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::In(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element("in")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::Between(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element("between")?;
                tup.serialize_element(v)?;
                tup.end()
            }
            Eqt::Like(v) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element("%")?;
                tup.serialize_element(v)?;
                tup.end()
            }
        }
    }
}

#[test]
fn test_serialize_equation() {
    let foo = Eqt::GreaterEqual(Value::String("foo".to_owned()));

    println!("{:?}", serde_json::to_string(&foo).unwrap());
}

#[derive(Debug)]
struct Duration {
    secs: u64,
    nanos: u32,
}

impl Duration {
    fn new(secs: u64, nanos: u32) -> Self {
        Duration { secs, nanos }
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Secs,
            Nanos,
        }

        // This part could also be generated independently by:
        //
        //    #[derive(Deserialize)]
        //    #[serde(field_identifier, rename_all = "lowercase")]
        //    enum Field { Secs, Nanos }
        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`secs` or `nanos`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "secs" => Ok(Field::Secs),
                            "nanos" => Ok(Field::Nanos),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct DurationVisitor;

        impl<'de> Visitor<'de> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Duration")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Duration, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let secs = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let nanos = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(Duration::new(secs, nanos))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Duration, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut secs = None;
                let mut nanos = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Secs => {
                            if secs.is_some() {
                                return Err(de::Error::duplicate_field("secs"));
                            }
                            secs = Some(map.next_value()?);
                        }
                        Field::Nanos => {
                            if nanos.is_some() {
                                return Err(de::Error::duplicate_field("nanos"));
                            }
                            nanos = Some(map.next_value()?);
                        }
                    }
                }
                let secs = secs.ok_or_else(|| de::Error::missing_field("secs"))?;
                let nanos = nanos.ok_or_else(|| de::Error::missing_field("nanos"))?;
                Ok(Duration::new(secs, nanos))
            }
        }

        const FIELDS: &'static [&'static str] = &["secs", "nanos"];
        deserializer.deserialize_struct("Duration", FIELDS, DurationVisitor)
    }
}

#[test]
fn test_deserialize_duration() {
    let duration_str = r#"
    {"secs": 1, "nanos": 2}
    "#;

    let duration: Duration = serde_json::from_str(duration_str).unwrap();

    println!("{:?}", duration);
}
