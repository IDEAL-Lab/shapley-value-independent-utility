use crate::JoinPlan;
use anyhow::{Context, Result};
use polars::prelude::*;
use std::{borrow::Cow, collections::HashMap};

pub fn join(inputs: &HashMap<String, Cow<DataFrame>>, plan: &JoinPlan) -> Result<DataFrame> {
    let mut table = DataFrame::default();
    let init_table = inputs
        .get(plan.init_table)
        .context("cannot find init table")?
        .as_ref();

    for (i, step) in plan.steps.iter().enumerate() {
        let left_table = if i == 0 { init_table } else { &table };
        let right_table = inputs
            .get(step.table_to_join)
            .context("cannot find table to join")?
            .as_ref();
        table = left_table.join(
            right_table,
            &step.left_join_keys,
            &step.right_join_keys,
            JoinType::Inner,
            Some(format!(":{}", step.table_to_join)),
        )?;

        for (l, r) in step.left_join_keys.iter().zip(step.right_join_keys.iter()) {
            table.rename(*l, *r)?;
        }
    }

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{utils::test_data_dir, DataSet, PLANS};

    #[test]
    fn test_join() {
        let data_dir = test_data_dir();
        let world = DataSet::load(
            "world",
            data_dir.join("world"),
            data_dir.join("world-metadata"),
        )
        .unwrap();
        let tables: HashMap<String, Cow<DataFrame>> = world
            .tables
            .iter()
            .map(|(k, v)| (k.to_owned(), Cow::Borrowed(&v.df)))
            .collect();
        let r = join(&tables, &PLANS["world"]).unwrap();
        assert!(r.shape().0 > 0);
        dbg!(r);
    }
}
