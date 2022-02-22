use crate::{alg::join::join, DataSet, SellerSet, PLANS, ROW_ID_COL_NAME};
use anyhow::{Context, Result};
use dashmap::DashMap;
use polars::prelude::*;
use rayon::prelude::*;
use std::{borrow::Cow, collections::HashMap};

pub fn subset_utility(dataset: &DataSet, subset: &SellerSet) -> Result<f64> {
    let tables: HashMap<String, Cow<DataFrame>> = dataset
        .tables
        .par_iter()
        .map(|(table_name, table)| {
            let mask = table
                .df
                .column(ROW_ID_COL_NAME)?
                .u64()?
                .into_iter()
                .map(|row_id| {
                    let row_id = row_id.context("cannot find row_id")?.into();
                    let seller = table
                        .seller_map
                        .get(&row_id)
                        .context("cannot get seller set")?;
                    Ok(seller.intersection(&subset).next().is_some())
                })
                .collect::<Result<BooleanChunked>>()?;
            let mut df = table.df.filter(&mask)?;
            let _ = df.drop_in_place(ROW_ID_COL_NAME)?;
            Ok((table_name.to_owned(), Cow::Owned(df)))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    let df = join(
        &tables,
        PLANS
            .get(dataset.name.as_str())
            .context("cannot find join plan")?,
    )?;
    Ok(df.shape().0 as f64)
}

#[inline]
pub fn subset_utility_with_cache(
    dataset: &DataSet,
    subset: SellerSet,
    cache: &DashMap<SellerSet, f64>,
) -> Result<f64> {
    if let Some(u) = cache.get(&subset) {
        return Ok(*u);
    }

    let u = subset_utility(dataset, &subset)?;
    cache.insert(subset, u);
    Ok(u)
}
