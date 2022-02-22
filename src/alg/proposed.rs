use crate::{
    alg::join::join, DataSet, RowId, SellerId, SellerSet, ShapleyResult, PLANS, ROW_ID_COL_NAME,
};
use anyhow::{Context, Result};
use polars::prelude::*;
use rayon::prelude::*;
use std::{collections::HashMap, mem::drop, time::Instant};

mod synthesis;
use synthesis::Synthesis;

pub fn proposed_scheme(dataset: &DataSet) -> Result<ShapleyResult> {
    info!("proposed scheme...");
    let begin = Instant::now();

    let join_df = join(
        |table_name| dataset.tables.get(table_name).map(|t| &t.df),
        PLANS
            .get(dataset.name.as_str())
            .context("cannot find join plan")?,
    )?;
    let row_id_columns: Vec<(String, Vec<RowId>)> = join_df
        .columns(
            dataset
                .tables
                .keys()
                .map(|t| format!("{}:{}", ROW_ID_COL_NAME, t)),
        )?
        .into_iter()
        .map(|column| {
            let table_name = column.name().rsplit(':').next().unwrap().to_string();
            let row_ids = column
                .u64()
                .unwrap()
                .into_iter()
                .map(|row_id| RowId::new(row_id.unwrap()))
                .collect();
            (table_name, row_ids)
        })
        .collect();
    let rows = join_df.shape().0;
    let cols = row_id_columns.len();
    drop(join_df);

    let row_id_columns_ref = &row_id_columns;
    let syntheses: Vec<_> = (0..rows)
        .into_par_iter()
        .map(move |i| {
            let seller_sets = (0..cols).map(move |j| {
                let (table_name, row_ids) = &row_id_columns_ref[j];
                let row_id = row_ids[i];
                &dataset.tables[table_name].seller_map[&row_id]
            });
            let mut syns = Synthesis::from_seller_sets(seller_sets);
            syns.minimal();
            syns
        })
        .collect();
    drop(row_id_columns);

    let total_time = Instant::now() - begin;
    let avg_time = total_time / dataset.sellers.len() as u32;
    info!("done in {:?}", total_time);
    todo!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_data_dir;

    #[test]
    fn test() {
        let data_dir = test_data_dir();
        let world = DataSet::load(
            "world",
            data_dir.join("world"),
            data_dir.join("world-metadata"),
        )
        .unwrap();
        let r = proposed_scheme(&world).unwrap();
        dbg!(&r);
        let actual = r.shapley_values.values().sum::<f64>();
        assert!(actual - 30670. < 1e-5);
    }
}
