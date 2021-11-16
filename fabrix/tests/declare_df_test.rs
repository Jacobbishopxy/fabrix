use fabrix::{df, series, DateTime};

#[test]
fn declare_df() {
    let df = df![
        "ord";
        "names" => ["Jacob", "Sam", "James", "Lucas", "Mia", "Livia"],
        "ord" => [10,11,12,20,22,31],
        "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8), Some(11.2)],
        "note" => [Some("FS"), Some("OP"), Some("TEC"), None, Some("SS"), None],
        // "dt" => [
        //     DateTime(chrono::NaiveDate::from_ymd(2016, 1, 8).and_hms(9, 10, 11)),
        //     DateTime(chrono::NaiveDate::from_ymd(2017, 1, 7).and_hms(9, 10, 11)),
        //     DateTime(chrono::NaiveDate::from_ymd(2018, 1, 6).and_hms(9, 10, 11)),
        //     DateTime(chrono::NaiveDate::from_ymd(2019, 1, 5).and_hms(9, 10, 11)),
        //     DateTime(chrono::NaiveDate::from_ymd(2020, 1, 4).and_hms(9, 10, 11)),
        //     DateTime(chrono::NaiveDate::from_ymd(2020, 1, 3).and_hms(9, 10, 11)),
        // ]
    ]
    .unwrap();

    println!("{:?}", df.get_columns(&["names", "val"]).unwrap());

    // TODO: series-trait `take` method is not implemented for `ObjectType` column yet
    // in this case, "dt" column cannot be taken, panic occurs
    println!("{:?}", df.take_rows_by_idx(&[0, 2]));

    println!("{:?}", df.take_cols(&["names", "val"]).unwrap());

    let flt = series!([12, 31]);

    println!("{:?}", df.take_rows(&flt));
}
