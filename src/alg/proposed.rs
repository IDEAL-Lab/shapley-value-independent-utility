use crate::{alg::join::join, DataSet, SellerId, SellerSet, ShapleyResult, PLANS};
use anyhow::{Context, Result};
use polars::prelude::*;
use rayon::prelude::*;
use std::{borrow::Cow, collections::HashMap, time::Instant};

mod synthesis;

pub fn proposed_scheme(dataset: &DataSet) -> Result<ShapleyResult> {
    info!("proposed scheme...");
    let begin = Instant::now();

    let join_df = join(
        |table_name| dataset.tables.get(table_name).map(|t| &t.df),
        PLANS
            .get(dataset.name.as_str())
            .context("cannot find join plan")?,
    )?;

    dbg!(join_df);

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
