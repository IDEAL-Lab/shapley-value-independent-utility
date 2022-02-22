use crate::SellerId;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ShapleyResult {
    #[serde(with = "serde_time")]
    pub avg_time: Duration,
    #[serde(with = "serde_time")]
    pub total_time: Duration,
    pub shapley_values: HashMap<SellerId, f64>,
    // following is only used by proposed.
    pub linear_count: usize,
    pub lookup_count: usize,
    pub comb_count: usize,
}

mod serde_time {
    use super::*;
    use serde::{de::Deserializer, ser::Serializer};

    pub fn serialize<S: Serializer>(t: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(t.as_secs_f64())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let t = <f64>::deserialize(d)?;
        Ok(Duration::from_secs_f64(t))
    }
}
