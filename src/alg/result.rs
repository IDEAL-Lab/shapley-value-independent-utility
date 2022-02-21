use crate::SellerId;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ShapleyResult {
    pub avg_time: Duration,
    pub total_time: Duration,
    pub shapley_values: HashMap<SellerId, f64>,
}
