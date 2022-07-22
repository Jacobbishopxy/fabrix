//! Fabrix Serialize/Deserialize types

use fabrix_core::polars::prelude::DataFrame;
use fabrix_core::{Fabrix, IndexTag};
use serde::{Deserialize, Serialize};

use crate::{
    dataframe_column_wise_deserialize, dataframe_column_wise_serialize,
    dataframe_dataset_type_deserialize, dataframe_dataset_type_serialize,
    dataframe_row_wise_deserialize, dataframe_row_wise_serialize,
};

pub enum JsonType {
    Column,
    Row,
    Dataset,
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct FabrixColumnWise {
    #[serde(
        serialize_with = "dataframe_column_wise_serialize",
        deserialize_with = "dataframe_column_wise_deserialize"
    )]
    pub data: DataFrame,
    pub index_tag: Option<IndexTag>,
}

impl From<FabrixColumnWise> for Fabrix {
    fn from(v: FabrixColumnWise) -> Self {
        Fabrix {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}

impl From<Fabrix> for FabrixColumnWise {
    fn from(v: Fabrix) -> Self {
        FabrixColumnWise {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct FabrixRowWise {
    #[serde(
        serialize_with = "dataframe_row_wise_serialize",
        deserialize_with = "dataframe_row_wise_deserialize"
    )]
    pub data: DataFrame,
    pub index_tag: Option<IndexTag>,
}

impl From<FabrixRowWise> for Fabrix {
    fn from(v: FabrixRowWise) -> Self {
        Fabrix {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}

impl From<Fabrix> for FabrixRowWise {
    fn from(v: Fabrix) -> Self {
        FabrixRowWise {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct FabrixDataset {
    #[serde(
        serialize_with = "dataframe_dataset_type_serialize",
        deserialize_with = "dataframe_dataset_type_deserialize"
    )]
    pub data: DataFrame,
    pub index_tag: Option<IndexTag>,
}

impl From<FabrixDataset> for Fabrix {
    fn from(v: FabrixDataset) -> Self {
        Fabrix {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}

impl From<Fabrix> for FabrixDataset {
    fn from(v: Fabrix) -> Self {
        FabrixDataset {
            data: v.data,
            index_tag: v.index_tag,
        }
    }
}
