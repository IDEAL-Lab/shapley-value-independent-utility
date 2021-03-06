use crate::SellerId;
use anyhow::{ensure, Error, Result};
#[cfg(test)]
use std::path::PathBuf;
use std::{cmp, collections::HashMap};
use tracing_subscriber::EnvFilter;

pub fn init_tracing_subscriber(default_filter: &str) -> Result<()> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(Error::msg)
}

#[cfg(test)]
pub fn test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

#[cfg(test)]
pub fn assert_world_sv(actual: &HashMap<SellerId, f64>) {
    use once_cell::sync::Lazy;
    static EXPECT: Lazy<HashMap<SellerId, f64>> = Lazy::new(|| {
        let data = std::fs::read(test_data_dir().join("world-ground-truth.json")).unwrap();
        serde_json::from_slice(&data).unwrap()
    });

    assert_eq!(actual.len(), EXPECT.len());
    for (s, u) in actual {
        let u_e = EXPECT[s];
        assert!((u - u_e).abs() < 1e-5);
    }
}

// use the same thread pool across our crate and polars.
pub fn setup_rayon(num_threads: Option<usize>) -> Result<()> {
    if let Some(num_threads) = num_threads {
        std::env::set_var("POLARS_MAX_THREADS", format!("{}", num_threads));
        ensure!(polars_core::POOL.current_num_threads() == num_threads);
    }
    Ok(())
}

#[inline]
pub fn binom(k: usize, n: usize) -> usize {
    let k = cmp::min(k, n - k);
    let mut res = 1;
    let mut n = n;

    for d in 1..=k {
        res *= n;
        res /= d;
        n -= 1;
    }

    res
}

#[inline]
pub fn merge_sv(a: HashMap<SellerId, f64>, b: HashMap<SellerId, f64>) -> HashMap<SellerId, f64> {
    let (to_consume, mut to_mutate) = if a.len() < b.len() { (a, b) } else { (b, a) };
    for (seller, u) in to_consume {
        *to_mutate.entry(seller).or_default() += u;
    }
    to_mutate
}
