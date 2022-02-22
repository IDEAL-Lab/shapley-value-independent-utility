use crate::{
    alg::subset_utility::subset_utility_with_cache, DataSet, SellerId, SellerSet, ShapleyResult,
};
use anyhow::Result;
use dashmap::DashMap;
use itertools::Itertools;
use rayon::prelude::*;
use std::{collections::HashMap, time::Instant};

pub fn traditional_scheme(dataset: &DataSet) -> Result<ShapleyResult> {
    info!("traditional scheme...");
    let begin = Instant::now();
    let cache: DashMap<SellerSet, f64> = DashMap::new();
    let cache_ref = &cache;
    let seller_len = dataset.sellers.len();
    let shapley_values = dataset
        .sellers
        .par_iter()
        .copied()
        .map(|seller| {
            info!("seller #{}", seller);
            let contribution = (0..seller_len)
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

            info!("seller #{} done", seller);
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
        ..Default::default()
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
        dbg!(&r);
        let actual = r.shapley_values.values().sum::<f64>();
        assert!(actual - 30670. < 1e-5);
    }
}
