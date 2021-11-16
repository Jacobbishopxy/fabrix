use fabrix::{df, series};

#[test]
fn declare_df() {
    let df = df![
        "names" => ["Jacob", "Sam", "James"],
        "ord" => [1,2,3],
        "val" => [Some(10), None, Some(8)]
    ]
    .unwrap();

    println!("{:?}", df.get_columns(&["names", "val"]).unwrap());

    println!("{:?}", df.take_rows_by_idx(&[0, 2]));

    println!("{:?}", df.take_cols(&["names", "val"]).unwrap());

    // watch out that the default index type is u64
    let flt = series!([1u64, 3]);

    println!("{:?}", df.take_rows(&flt));
}
