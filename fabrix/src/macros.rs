//! Fabrix macros

/// value creation macro
#[macro_export]
macro_rules! value {
    ($val:expr) => {{
        let res: $crate::Value = $val.into();
        res
    }};
}

/// df creation macro
/// Supporting:
/// 1. dataframe with default index
/// 1. dataframe with given index
#[macro_export]
macro_rules! df {
    ($($col_name:expr => $slice:expr),+ $(,)*) => {{
        use polars::prelude::NamedFrom;

        let mut columns = vec![];
            $(
                columns.push($crate::Series::new($col_name, $slice));
            )+

        $crate::DataFrame::from_series_default_index(columns)
    }};
    ($index_name:expr; $($col_name:expr => $slice:expr),+ $(,)*) => {{
        use polars::prelude::NamedFrom;

        let mut columns = vec![];
        $(
            columns.push($crate::Series::new($col_name, $slice));
        )+

        $crate::DataFrame::from_series_with_index(columns, $index_name)
    }};
}

/// series creation macro
/// Supporting:
/// 1. series with default name
/// 1. series with given name
#[macro_export]
macro_rules! series {
    ($slice:expr) => {{
        use polars::prelude::NamedFrom;

        $crate::Series::new($crate::core::IDX, $slice)
    }};
    ($name:expr => $slice:expr) => {{
        use polars::prelude::NamedFrom;

        $crate::Series::new($name, $slice)
    }};
}

/// rows creation macro
/// Supporting:
/// 1. rows with default indices
/// 1. rows with given indices
#[macro_export]
macro_rules! rows {
    ($([$($val:expr),* $(,)*]),+ $(,)*) => {{
        let mut idx = 0u32;
        let mut buf: Vec<$crate::Row> = Vec::new();
        $({
            let mut row: Vec<$crate::Value> = Vec::new();
            $(
                row.push($crate::value!($val));
            )*
            idx += 1;
            buf.push($crate::Row::new($crate::value!(idx - 1), row));
        })+

        buf
    }};
    ($($index:expr => [$($val:expr),* $(,)*]),+ $(,)*) => {{
        let mut buf: Vec<$crate::Row> = Vec::new();
        $({
            let mut row: Vec<$crate::Value> = Vec::new();
            $(
                row.push($crate::value!($val));
            )*
            buf.push($crate::Row::new($crate::value!($index), row));
        })+

        buf
    }};
}

#[cfg(test)]
mod test_macros {

    #[test]
    fn test_value() {
        println!("{:?}", value!("Jacob"));
    }

    #[test]
    fn test_series_new() {
        // use chrono::NaiveDate;

        let series = series!([Some("Jacob"), None, Some("Sam"), Some("Jason")]);
        println!("{:?}", series);

        let series = series!("name" => ["Jacob", "Sam", "Jason"]);
        println!("{:?}", series);

        // let series = series!("date" => [
        //     NaiveDate::from_ymd(2019, 1, 1),
        //     NaiveDate::from_ymd(2019, 1, 2),
        //     NaiveDate::from_ymd(2019, 1, 3),
        // ]);
        // println!("{:?}", series);
    }

    #[test]
    fn test_df_new1() {
        let df = df![
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);
        println!("{:?}", df.data_dtypes());
        println!("{:?}", df.get_column("names").unwrap());
    }

    #[test]
    fn test_df_new2() {
        let df = df![
            "ord";
            "names" => ["Jacob", "Sam", "Jason"],
            "ord" => [1,2,3],
            "val" => [Some(10), None, Some(8)]
        ]
        .unwrap();

        println!("{:?}", df);
        println!("{:?}", df.fields());
        println!("{:?}", df.get_column("names").unwrap());
    }

    #[test]
    fn test_rows_new() {
        let rows = rows!(
            [0, "Jacob", "A", 10],
            [1, "Sam", "A", 9],
            [2, "James", "A", 9],
        );

        println!("{:?}", rows);

        let rows = rows!(
            1 => ["Jacob", "A", 10],
            2 => ["Sam", "A", 9],
            3 => ["James", "A", 9],
        );

        println!("{:?}", rows);
    }
}
