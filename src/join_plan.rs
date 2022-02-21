use std::collections::HashMap;

use once_cell::sync::Lazy;
use polars::prelude::*;

#[derive(Debug, Clone)]
pub struct JoinStep {
    pub table_to_join: &'static str,
    pub left_join_keys: Vec<&'static str>,
    pub right_join_keys: Vec<&'static str>,
}

#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub init_table: &'static str,
    pub steps: Vec<JoinStep>,
}

pub static PLANS: Lazy<HashMap<&'static str, JoinPlan>> = Lazy::new(|| {
    let mut plans = HashMap::new();
    plans.insert(
        "world",
        JoinPlan {
            init_table: "city",
            steps: vec![
                JoinStep {
                    table_to_join: "country",
                    left_join_keys: vec!["CountryCode"],
                    right_join_keys: vec!["Code"],
                },
                JoinStep {
                    table_to_join: "countrylanguage",
                    left_join_keys: vec!["Code"],
                    right_join_keys: vec!["CountryCode"],
                },
            ],
        },
    );

    plans
});
