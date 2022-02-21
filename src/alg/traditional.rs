use crate::{alg::join::join, DataSet, SellerId, SellerSet, ShapleyResult, PLANS, ROW_ID_COL_NAME};
use anyhow::{Context, Result};
use dashmap::DashMap;
use itertools::Itertools;
use polars::prelude::*;
use rayon::prelude::*;
use std::{borrow::Cow, collections::HashMap, time::Instant};

fn subset_utility(dataset: &DataSet, subset: &SellerSet) -> Result<f64> {
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
                    Ok(seller.is_subset(subset))
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
fn subset_utility_with_cache(
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

pub fn traditional_scheme(dataset: &DataSet) -> Result<ShapleyResult> {
    info!("traditional scheme...");
    let begin = Instant::now();
    let cache: DashMap<SellerSet, f64> = DashMap::new();
    let cache_ref = &cache;
    let seller_len = dataset.sellers.len();
    let shapley_values = dataset
        .sellers
        // .par_iter()
        .iter()
        .copied()
        .map(|seller| {
            let contribution = (0..seller_len - 1)
                .into_par_iter()
                .map(move |k| {
                    let (utility, count) = dataset
                        .sellers
                        .iter()
                        .copied()
                        .filter(|s| *s != seller)
                        .combinations(k)
                        .par_bridge()
                        .map(|subset| {
                            let mut subset = SellerSet(subset.into_iter().collect());
                            let utility_without_seller =
                                subset_utility_with_cache(dataset, subset.clone(), cache_ref)?;
                            subset.insert(seller);
                            let utility_with_seller =
                                subset_utility_with_cache(dataset, subset, cache_ref)?;
                            Ok((utility_with_seller - utility_without_seller, 1.))
                        })
                        .reduce(
                            || Ok((0., 0.)),
                            |a: Result<_>, b: Result<_>| {
                                let a = a?;
                                let b = b?;
                                Ok((a.0 + b.0, a.1 + b.1))
                            },
                        )?;
                    Ok(utility / count)
                })
                .reduce(
                    || Ok(0.),
                    |a: Result<_>, b: Result<_>| {
                        let a = a?;
                        let b = b?;
                        Ok(a + b)
                    },
                )?;

            Ok((seller, contribution / seller_len as f64))
        })
        .collect::<Result<HashMap<SellerId, f64>>>()?;
    let total_time = Instant::now() - begin;
    let avg_time = total_time / seller_len as u32;
    info!("done in {:?}", total_time);
    Ok(ShapleyResult {
        shapley_values,
        avg_time,
        total_time,
    })
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
        let r = traditional_scheme(&world).unwrap();
        dbg!(r);
    }
}
