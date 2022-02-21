use crate::{SellerSet, Table};
use anyhow::Result;
use glob::glob;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::Instant,
};

#[derive(Debug, Clone)]
pub struct DataSet {
    pub name: String,
    pub tables: HashMap<String, Table>,
    pub sellers: SellerSet,
}

impl DataSet {
    pub fn load(
        name: impl Into<String>,
        csv_dir: impl AsRef<Path>,
        meta_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let begin = Instant::now();
        let csv_dir = csv_dir.as_ref();
        let meta_dir = meta_dir.as_ref();
        info!("load csv from {}...", csv_dir.display());
        info!("load meta from {}...", meta_dir.display());

        let mut tables = HashMap::new();
        for csv_f in glob(&csv_dir.join("*.csv").to_string_lossy())? {
            let csv_f = csv_f?;
            let name = csv_f.file_stem().unwrap().to_string_lossy().to_string();
            let row_id_f = meta_dir.join(format!("{name}-index.json"));
            let seller_f = meta_dir.join(format!("{name}-seller.json"));
            let table = Table::load(name.clone(), csv_f, row_id_f, seller_f)?;
            tables.insert(name, table);
        }
        // TODO: store seller list directly
        let sellers = {
            let mut sellers = HashSet::new();
            for t in tables.values() {
                for s in t.seller_map.values() {
                    sellers.extend(s.iter().copied());
                }
            }
            SellerSet::new(sellers.into_iter().collect())
        };

        info!("done in {:?}", Instant::now() - begin);
        Ok(Self {
            name: name.into(),
            tables,
            sellers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_load() {
        let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
        let world = DataSet::load(
            "world",
            data_dir.join("world"),
            data_dir.join("world-metadata"),
        )
        .unwrap();
        dbg!(&world.sellers);
    }
}
