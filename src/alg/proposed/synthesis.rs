use crate::SellerSet;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    derive_more::Constructor,
    derive_more::Deref,
    derive_more::DerefMut,
    derive_more::AsRef,
    derive_more::AsMut,
    derive_more::From,
    derive_more::Into,
)]
#[as_ref(forward)]
#[as_mut(forward)]
pub struct Synthesis(pub HashSet<SellerSet>);

impl Synthesis {
    pub fn minimal(&mut self) {
        let mut sets: Vec<_> = self.drain().collect();
        sets.sort_unstable_by_key(|s| s.len());
        let mut skips = vec![false; sets.len()];

        for (i, s_i) in sets.iter().enumerate() {
            if skips[i] {
                continue;
            }

            for (j, s_j) in sets.iter().enumerate().skip(i + 1) {
                if skips[j] {
                    continue;
                }

                if s_i.is_subset(s_j) {
                    skips[j] = true;
                } else if s_j.is_subset(s_i) {
                    skips[i] = true;
                }
            }
        }

        for (i, s) in sets.into_iter().enumerate() {
            if !skips[i] {
                self.insert(s);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal() {
        let mut syns = Synthesis::new(
            vec![
                vec![1].into_iter().collect::<SellerSet>(),
                vec![1, 2].into_iter().collect::<SellerSet>(),
                vec![1, 3].into_iter().collect::<SellerSet>(),
                vec![4, 5, 6].into_iter().collect::<SellerSet>(),
                vec![4, 6].into_iter().collect::<SellerSet>(),
                vec![5, 6, 7, 8, 9].into_iter().collect::<SellerSet>(),
                vec![6, 8].into_iter().collect::<SellerSet>(),
                vec![10, 11].into_iter().collect::<SellerSet>(),
                vec![11, 12].into_iter().collect::<SellerSet>(),
            ]
            .into_iter()
            .collect(),
        );
        syns.minimal();
        let expect = Synthesis::new(
            vec![
                vec![1].into_iter().collect::<SellerSet>(),
                vec![4, 6].into_iter().collect::<SellerSet>(),
                vec![6, 8].into_iter().collect::<SellerSet>(),
                vec![10, 11].into_iter().collect::<SellerSet>(),
                vec![11, 12].into_iter().collect::<SellerSet>(),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(syns, expect);
    }
}
