//! CSV Reader
//!
//! Reading CSV files.

use polars::prelude::CsvReader;

use crate::{DataFrame, FabrixError, FabrixResult};

pub struct Reader {
    pub data: Option<DataFrame>,
}

#[cfg(test)]
mod tests_reader {
    use polars::{io::SerReader, prelude::CsvReader};

    const CSV_FILE_PATH: &str = "../mock/test.csv";

    #[test]
    fn raw_read() {
        let foo = CsvReader::from_path(CSV_FILE_PATH)
            .expect("file exists")
            .has_header(true)
            .finish();

        println!("{:?}", foo.unwrap());
    }
}
