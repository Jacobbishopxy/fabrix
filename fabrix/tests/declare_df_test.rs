//! Df test case

use fabrix::{datetime, fx, series, uuid, Uuid};

/*
Create a dataframe with specified index column

cargo test --package fabrix --test declare_df_test -- test_new_df --exact --nocapture
*/
#[test]
fn test_new_df() {
    let result_df = fx![
        "ord";
        "names" => ["Jacob", "Sam", "James", "Lucas", "Mia", "Livia"],
        "ord" => [10,11,12,20,22,31],
        "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8), Some(11.2)],
        "note" => [Some("FS"), Some("OP"), Some("TEC"), None, Some("SS"), None],
        "dt" => [
            datetime!(2016, 1, 8, 9, 10, 11),
            datetime!(2017, 1, 7, 9, 10, 11),
            datetime!(2018, 1, 6, 9, 10, 11),
            datetime!(2019, 1, 5, 9, 10, 11),
            datetime!(2020, 1, 4, 9, 10, 11),
            datetime!(2020, 1, 3, 9, 10, 11),
        ]
    ];

    assert!(result_df.is_ok());

    let df = result_df.unwrap();

    assert_eq!(df.shape(), (6, 5));
}

/*
query data from df

cargo test --package fabrix --test declare_df_test -- test_df_query --exact --nocapture
*/
#[test]
fn test_df_query() {
    let df = fx![
        "ord";
        "names" => ["Jacob", "Sam", "James", "Lucas", "Mia", "Livia"],
        "ord" => [10,11,12,20,22,31],
        "val" => [Some(10.1), None, Some(8.0), Some(9.5), Some(10.8), Some(11.2)],
        "note" => [Some("FS"), Some("OP"), Some("TEC"), None, Some("SS"), None],
        "dt" => [
            datetime!(2016, 1, 8, 9, 10, 11),
            datetime!(2017, 1, 7, 9, 10, 11),
            datetime!(2018, 1, 6, 9, 10, 11),
            datetime!(2019, 1, 5, 9, 10, 11),
            datetime!(2020, 1, 4, 9, 10, 11),
            datetime!(2020, 1, 3, 9, 10, 11),
        ]
    ]
    .unwrap();

    println!("{:?}", df.get_columns(&["names", "val"]).unwrap());

    println!("{:?}", df.take_cols(&["names", "val"]).unwrap());

    println!("{:?}", df.take_rows_by_idx(&[0, 2]));

    let flt = series!([12, 31]);
    println!("{:?}", df.take_rows(&flt));

    println!("{:?}", flt);
}

/*
The purpose of this test is to check Series consisted by custom-typed value can be taken by index.
Instead of using `series.take` method, which occurs `not implement` panic, use `series.take_iter` method.
*/
#[test]
fn test_obj_chunk() {
    use polars::prelude::{IntoSeries, ObjectChunked};

    let uuids = vec![uuid!(), uuid!(), uuid!(), uuid!(), uuid!(), uuid!()];
    let arr = ObjectChunked::<Uuid>::new_from_vec("uuids", uuids);
    let s = arr.into_series();

    let mut it = vec![0, 2].into_iter();
    println!("{:?}", s.take_iter(&mut it));
}

/*
Continue from the above test case, a chunkedArray of custom-typed value can be taken by index as well.
But explicit type annotation is required for polars `TakeIdx` enum.
*/
#[test]
fn test_obj_chunked_arr_take() {
    // use std::array::IntoIter as ArrayIntoIter;
    use std::vec::IntoIter as VecIntoIter;

    use polars::prelude::{ChunkTake, ChunkedArray, NewChunkedArray, ObjectType, TakeIdx};

    let uuids = [uuid!(), uuid!(), uuid!(), uuid!(), uuid!(), uuid!()];
    let arr = ChunkedArray::<ObjectType<Uuid>>::from_slice("uuids", &uuids);
    println!("{:?}", arr);

    // TODO: update?
    // let ti = [0usize, 2].into_iter();
    // let tk: TakeIdx<ArrayIntoIter<usize, 2>, ArrayIntoIter<Option<usize>, 2>> = TakeIdx::Iter(ti);
    // println!("{:?}", arr.take(tk));

    let ti = vec![0usize, 4].into_iter();
    let tk: TakeIdx<VecIntoIter<usize>, VecIntoIter<Option<usize>>> = TakeIdx::Iter(ti);
    println!("{:?}", arr.take(tk));
}
