use crate::JoinPlan;
use anyhow::{Context, Result};
use polars::prelude::*;
use std::collections::HashMap;

pub fn join(mut inputs: HashMap<String, LazyFrame>, plan: &JoinPlan) -> Result<DataFrame> {
    let mut table = inputs
        .remove(plan.init_table)
        .context("cannot find init table")?;

    for step in &plan.steps {
        let table2 = inputs
            .remove(step.table_to_join)
            .context("cannot find table to join")?;
        let left: Vec<_> = step.left_join_keys.iter().map(|k| col(*k)).collect();
        let right: Vec<_> = step.right_join_keys.iter().map(|k| col(*k)).collect();
        let join = table
            .join_builder()
            .with(table2)
            .how(JoinType::Inner)
            .left_on(left)
            .right_on(right)
            .suffix(format!(":{}", step.table_to_join))
            .finish();

        table = join.rename(step.left_join_keys.iter(), step.right_join_keys.iter());
    }

    Ok(table.collect()?)
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
        let tables: HashMap<String, LazyFrame> = world
            .tables
            .into_iter()
            .map(|(k, v)| (k, v.df.lazy()))
            .collect();
        let r = join(tables, &PLANS["world"]).unwrap();
        dbg!(r);
    }
}
