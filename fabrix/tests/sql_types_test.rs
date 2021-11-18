//! New types test case: &'static str -> db types
//!
//! Use `nom` to parse a string into a db type.

use std::{collections::HashMap, marker::PhantomData, str::FromStr};

use nom::bytes::complete::tag;
use nom::character::complete::alpha1;
use nom::sequence::{delimited, separated_pair};
use nom::IResult;

use fabrix::{DbError, Value, ValueType};

trait SqlTypeTagMarker: Sync + Send {
    fn to_str(&self) -> &str;

    fn to_dtype(&self) -> ValueType;
}

struct SqlTypeTag<T>(&'static str, PhantomData<T>)
where
    T: Into<Value>;

impl<T> SqlTypeTag<T>
where
    T: Into<Value>,
{
    fn new(name: &'static str) -> Self {
        SqlTypeTag(name, PhantomData)
    }
}

// sql type binds to `bool`
impl SqlTypeTagMarker for SqlTypeTag<bool> {
    fn to_str(&self) -> &str {
        self.0
    }

    fn to_dtype(&self) -> ValueType {
        ValueType::Bool
    }
}

type STTM = Box<dyn SqlTypeTagMarker>;

lazy_static::lazy_static! {
    // Mysql types map
    static ref MYSQL_TMAP: HashMap<&'static str, STTM> = {
        HashMap::from([
            (
                "BOOLEAN",
                Box::new(SqlTypeTag::<bool>::new("BOOLEAN")) as STTM,
            )
        ])
    };

    // Postgres types map
    static ref PG_TMAP: HashMap<&'static str, STTM> = {
        HashMap::from([
            (
                "BOOL",
                Box::new(SqlTypeTag::<bool>::new("BOOL")) as STTM,
            )
        ])
    };
}

fn get_sql_type(input: &str) -> IResult<&str, (&str, &str)> {
    let sql_type = |s| alpha1(s);
    let data_type = |s| alpha1(s);

    let ctn = separated_pair(sql_type, tag(":"), data_type);
    let mut par = delimited(tag("["), ctn, tag("]"));

    par(input)
}

// "[MYSQL:BOOLEAN]" -> MYSQL_TMAP["BOOLEAN"]
// "[PG:BOOL]" -> PG_TMAP["BOOL"]
impl FromStr for &STTM {
    type Err = DbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match get_sql_type(s) {
            Ok((_, (db_type, sql_type))) => match db_type {
                "MYSQL" => MYSQL_TMAP
                    .get(sql_type)
                    .ok_or(DbError::new_common_error("MYSQL data type not found")),
                "PG" => PG_TMAP
                    .get(sql_type)
                    .ok_or(DbError::new_common_error("PG data type not found")),
                _ => Err(DbError::new_common_error("DB type not found")),
            },
            Err(_) => Err(DbError::new_common_error("parsing error")),
        }
    }
}

#[test]
fn test_parsing() {
    let foo = "[MYSQL:BOOLEAN]";
    let foo = foo.parse::<&STTM>();

    println!("{:?}", foo.unwrap().to_dtype());

    let bar = "[PG:BOOL]";
    let bar = bar.parse::<&STTM>();

    println!("{:?}", bar.unwrap().to_dtype());
}
