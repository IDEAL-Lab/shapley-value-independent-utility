use anyhow::{Context, Result};
use glob::glob;
use polars::prelude::*;
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    io::BufReader,
    path::Path,
};

pub fn load_df(csv_path: impl AsRef<Path>, row_id_path: impl AsRef<Path>) -> Result<DataFrame> {
    let mut df = CsvReader::new(File::open(csv_path)?).finish()?;
    let row_id: Vec<u64> = serde_json::from_reader(BufReader::new(File::open(row_id_path)?))?;
    df.with_column(Series::new("row_id", row_id))?;
    Ok(df)
}

pub fn load_seller(seller_path: impl AsRef<Path>) -> Result<HashMap<u64, BTreeSet<u64>>> {
    #[derive(Debug, Deserialize)]
    struct Seller {
        index: HashMap<String, u64>,
        seller: HashMap<String, u64>,
    }

    let seller: Seller = serde_json::from_reader(BufReader::new(File::open(seller_path)?))?;
    let mut ans: HashMap<u64, BTreeSet<u64>> = HashMap::new();
    for (i_k, i_v) in seller.index {
        let s_v = *seller
            .seller
            .get(&i_k)
            .context("failed to read -seller.json")?;
        ans.entry(i_v).or_default().insert(s_v);
    }

    Ok(ans)
}

pub fn load_dfs_in_dir(
    data_dir: impl AsRef<Path>,
    meta_dir: impl AsRef<Path>,
) -> Result<HashMap<String, (DataFrame, HashMap<u64, BTreeSet<u64>>)>> {
    let data_dir = data_dir.as_ref();
    let meta_dir = meta_dir.as_ref();
    info!("load data from {}...", data_dir.display());
    info!("load meta from {}...", meta_dir.display());
    let mut ans = HashMap::new();
    for f in glob(&data_dir.join("*.csv").to_string_lossy())? {
        let f = f?;
        let name = f.file_stem().unwrap().to_string_lossy().to_string();
        let df = load_df(f, meta_dir.join(format!("{}-index.json", name)))?;
        let seller = load_seller(meta_dir.join(format!("{}-seller.json", name)))?;
        ans.insert(name, (df, seller));
    }
    Ok(ans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_load() {
        let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
        let country = load_df(
            data_dir.join("world/country.csv"),
            data_dir.join("world-metadata/country-index.json"),
        )
        .unwrap();
        dbg!(&country);
        let country_seller =
            load_seller(data_dir.join("world-metadata/country-seller.json")).unwrap();
        assert_eq!(country.shape().0, country_seller.len());

        load_dfs_in_dir(data_dir.join("world"), data_dir.join("world-metadata")).unwrap();
    }
}
