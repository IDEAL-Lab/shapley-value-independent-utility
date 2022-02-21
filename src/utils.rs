use anyhow::{Error, Result};
#[cfg(test)]
use std::path::PathBuf;
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

pub fn setup_rayon(num_threads: Option<usize>) -> Result<()> {
    let mut pool = rayon::ThreadPoolBuilder::new();
    if let Some(num_threads) = num_threads {
        pool = pool.num_threads(num_threads);
    }
    pool.build_global()?;
    Ok(())
}
