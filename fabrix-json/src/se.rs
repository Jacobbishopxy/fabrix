//! Serialize functions

use fabrix_core::polars::prelude::DataFrame;
use fabrix_core::{Fabrix, SeriesRef};
use serde::ser::SerializeMap;
use serde::{ser::SerializeSeq, Serializer};

pub(crate) fn dataframe_column_wise_serialize<S>(df: &DataFrame, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let series = df.get_columns().iter().map(SeriesRef).collect::<Vec<_>>();
    let mut seq = s.serialize_seq(Some(series.len()))?;
    for e in series {
        seq.serialize_element(&e)?;
    }
    seq.end()
}

pub(crate) fn dataframe_row_wise_serialize<S>(df: &DataFrame, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // TODO:
    // FabrixRef (same as `SeriesRef`)

    let fx = Fabrix {
        data: df.clone(),
        index_tag: None,
    };
    let mut m = s.serialize_map(Some(2))?;
    let types = fx.dtypes();
    m.serialize_entry("types", &types)?;

    let values = fx.iter_named_row().collect::<Vec<_>>();
    m.serialize_entry("values", &values)?;

    m.end()
}

pub(crate) fn dataframe_dataset_type_serialize<S>(df: &DataFrame, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let fx = Fabrix {
        data: df.clone(),
        index_tag: None,
    };
    let mut seq = s.serialize_seq(Some(fx.height()))?;
    for r in fx.iter_rows() {
        seq.serialize_element(r.data())?;
    }
    seq.end()
}
