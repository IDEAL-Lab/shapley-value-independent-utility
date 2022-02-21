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

    plans.insert(
        "tpch",
        JoinPlan {
            init_table: "region",
            steps: vec! [
                JoinStep {
                    table_to_join: "nation",
                    left_join_keys: vec!["r_regionkey"],
                    right_join_keys: vec!["n_regionkey"]
                },
                JoinStep {
                    table_to_join: "supplier",
                    left_join_keys: vec!["n_nationkey"],
                    right_join_keys: vec!["s_nationkey"],
                },
                JoinStep {
                    table_to_join: "partsupp",
                    left_join_keys: vec!["s_suppkey"],
                    right_join_keys: vec!["ps_suppkey"],
                },
                JoinStep {
                    table_to_join: "part",
                    left_join_keys: vec!["ps_partkey"],
                    right_join_keys: vec!["p_partkey"],
                },
                JoinStep {
                    table_to_join: "lineitem",
                    left_join_keys: vec!["p_partkey", "s_suppkey"],
                    right_join_keys: vec!["l_partkey", "l_suppkey"],
                },
                JoinStep {
                    table_to_join: "orders",
                    left_join_keys: vec!["l_orderkey"],
                    right_join_keys: vec!["o_orderkey"],
                },
                JoinStep {
                    table_to_join: "customer",
                    left_join_keys: vec!["o_custkey"],
                    right_join_keys: vec!["c_custkey"],
                },
            ],
        },
    );

    plans.insert(
        "ssb",
        JoinPlan {
            init_table: "lineorder",
            steps: vec![
                JoinStep {
                    table_to_join: "customer",
                    left_join_keys: vec!["lo_custkey"],
                    right_join_keys: vec!["c_custkey"],
                },
                JoinStep {
                    table_to_join: "supplier",
                    left_join_keys: vec!["lo_suppkey"],
                    right_join_keys: vec!["s_suppkey"],
                },
                JoinStep {
                    table_to_join: "part",
                    left_join_keys: vec!["lo_partkey"],
                    right_join_keys: vec!["p_partkey"],
                },
                JoinStep {
                    table_to_join: "date",
                    left_join_keys: vec!["lo_orderdate"],
                    right_join_keys: vec!["d_datekey"],
                },
            ],
        },
    );
    
    plans
});
