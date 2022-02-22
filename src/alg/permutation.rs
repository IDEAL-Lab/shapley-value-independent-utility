use crate::{
    alg::subset_utility::subset_utility_with_cache, utils::merge_sv, DataSet, SellerId, SellerSet,
    ShapleyResult,
};
use anyhow::Result;
use dashmap::DashMap;
use rand::prelude::*;
use rayon::prelude::*;
use std::{collections::HashMap, time::Instant};

pub fn permutation_scheme(dataset: &DataSet, sample_size: usize) -> Result<ShapleyResult> {
    info!("permutation scheme...");
    let begin = Instant::now();
    let cache: DashMap<SellerSet, f64> = DashMap::new();
    let cache_ref = &cache;

    let mut shapley_values = (0..sample_size)
        .into_par_iter()
        .map(|i| {
            info!("sample #{}", i);
            let mut rng = thread_rng();
            let mut sellers: Vec<SellerId> = dataset.sellers.iter().copied().collect();
            sellers.shuffle(&mut rng);

            let mut last_utility = 0.;
            let mut seller_set = SellerSet::default();
            let mut ans = HashMap::new();

            for seller in sellers {
                seller_set.insert(seller);
                let subset_utility =
                    subset_utility_with_cache(dataset, seller_set.clone(), cache_ref)?;
                ans.insert(seller, subset_utility - last_utility);
                last_utility = subset_utility;
            }

            info!("sample #{} done", i);
            Ok(ans)
        })
        .reduce(
            || Ok(HashMap::new()),
            |a: Result<HashMap<SellerId, f64>>, b: Result<HashMap<SellerId, f64>>| {
                let a = a?;
                let b = b?;
                Ok(merge_sv(a, b))
            },
        )?;
    shapley_values.par_iter_mut().for_each(|(_, v)| {
        *v /= sample_size as f64;
    });

    let total_time = Instant::now() - begin;
    let avg_time = total_time / dataset.sellers.len() as u32;
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
        polars_core::POOL.install(|| {
            let data_dir = test_data_dir();
            let world = DataSet::load(
                "world",
                data_dir.join("world"),
                data_dir.join("world-metadata"),
            )
            .unwrap();
            let r = permutation_scheme(&world, 50).unwrap();
            dbg!(&r);
            let actual = r.shapley_values.values().sum::<f64>();
            assert!(actual - 30670. < 1e-5);
        });
    }
}
