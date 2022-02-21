use crate::{SellerId, SellerSet};
use anyhow::{Context, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

pub const ROW_ID_COL_NAME: &str = "_row_id";

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    derive_more::Display,
    derive_more::Constructor,
    derive_more::Deref,
    derive_more::DerefMut,
    derive_more::AsRef,
    derive_more::AsMut,
    derive_more::From,
    derive_more::Into,
)]
#[as_ref(forward)]
#[as_mut(forward)]
pub struct RowId(pub u64);

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub df: DataFrame,
    pub seller_map: HashMap<RowId, SellerSet>,
}

impl Table {
    pub fn load(
        name: impl Into<String>,
        csv_path: impl AsRef<Path>,
        row_id_path: impl AsRef<Path>,
        seller_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let mut df = CsvReader::new(File::open(csv_path)?).finish()?;
        let row_id: Vec<u64> = serde_json::from_reader(BufReader::new(File::open(row_id_path)?))?;
        df.with_column(Series::new(ROW_ID_COL_NAME, row_id))?;

        #[derive(Debug, Deserialize)]
        struct Seller {
            index: HashMap<String, RowId>,
            seller: HashMap<String, SellerId>,
        }

        let seller: Seller = serde_json::from_reader(BufReader::new(File::open(seller_path)?))?;
        let mut seller_map: HashMap<RowId, SellerSet> = HashMap::new();
        for (i_k, i_v) in seller.index {
            let s_v = *seller
                .seller
                .get(&i_k)
                .context("failed to read -seller.json")?;
            seller_map.entry(i_v).or_default().insert(s_v);
        }

        Ok(Self {
            name: name.into(),
            df,
            seller_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_data_dir;

    #[test]
    fn test_load() {
        let data_dir = test_data_dir();
        let country = Table::load(
            "country",
            data_dir.join("world/country.csv"),
            data_dir.join("world-metadata/country-index.json"),
            data_dir.join("world-metadata/country-seller.json"),
        )
        .unwrap();
        dbg!(&country.df);
        assert_eq!(country.df.shape().0, country.seller_map.len());
    }
}
