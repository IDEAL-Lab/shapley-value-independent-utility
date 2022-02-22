use crate::{
    alg::subset_utility::subset_utility_with_cache, DataSet, SellerId, SellerSet, ShapleyResult,
};
use anyhow::{Error, Result};
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
        .map(|_| {
            let mut rng = thread_rng();
            let mut sellers: Vec<SellerId> = dataset.sellers.iter().copied().collect();
            sellers.shuffle(&mut rng);

            let mut last_utility = 0.;
            let mut seller_set = SellerSet::default();

            sellers
                .into_iter()
                .map(|seller| {
                    seller_set.insert(seller);
                    let subset_utility =
                        subset_utility_with_cache(dataset, seller_set.clone(), cache_ref)?;
                    let u = subset_utility - last_utility;
                    last_utility = subset_utility;
                    Ok::<_, Error>((seller, u))
                })
                .fold(
                    Ok(HashMap::new()),
                    |acc: Result<HashMap<SellerId, f64>>, x: Result<(SellerId, f64)>| {
                        let mut acc = acc?;
                        let (seller, u) = x?;
                        acc.insert(seller, u);
                        Ok(acc)
                    },
                )
        })
        .reduce(
            || Ok(HashMap::new()),
            |a: Result<HashMap<SellerId, f64>>, b: Result<HashMap<SellerId, f64>>| {
                let a = a?;
                let b = b?;
                let (to_consume, mut to_mutate) = if a.len() < b.len() { (a, b) } else { (b, a) };
                for (seller, u) in to_consume {
                    *to_mutate.entry(seller).or_default() += u;
                }
                Ok(to_mutate)
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
        let r = permutation_scheme(&world, 50).unwrap();
        dbg!(&r);
        let actual = r.shapley_values.values().sum::<f64>();
        assert!(actual - 30670. < 1e-5);
    }
}
