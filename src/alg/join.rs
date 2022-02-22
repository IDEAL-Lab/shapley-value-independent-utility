use crate::{JoinPlan, ROW_ID_COL_NAME};
use anyhow::{Context, Result};
use polars::prelude::*;

pub fn join<'a, 'b>(
    df_fn: impl Fn(&'a str) -> Option<&'a DataFrame>,
    plan: &'b JoinPlan,
) -> Result<DataFrame> {
    let mut table = DataFrame::default();
    let init_table = df_fn(plan.init_table).context("cannot find init table")?;

    for (i, step) in plan.steps.iter().enumerate() {
        let left_table = if i == 0 { init_table } else { &table };
        let right_table = df_fn(step.table_to_join).context("cannot find table to join")?;
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

    table
        .rename(
            ROW_ID_COL_NAME,
            &format!("{}:{}", ROW_ID_COL_NAME, plan.init_table),
        )
        .ok();

    table.drop_duplicates(false, None)?;

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{utils::test_data_dir, DataSet, PLANS};

    #[test]
    fn test_join() {
        polars_core::POOL.install(|| {
            let data_dir = test_data_dir();
            let world = DataSet::load(
                "world",
                data_dir.join("world"),
                data_dir.join("world-metadata"),
            )
            .unwrap();
            let r = join(
                |table_name| world.tables.get(table_name).map(|t| &t.df),
                &PLANS["world"],
            )
            .unwrap();
            assert_eq!(r.shape().0, 30670);
            dbg!(r);
        });
    }
}
