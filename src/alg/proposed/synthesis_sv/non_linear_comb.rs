use crate::{SellerId, SellerSet};
use rayon::prelude::*;
use std::collections::HashSet;

struct Union {
    num_of_set: usize,
    max_set_id: usize,
    set: HashSet<SellerId>,
}

impl Union {
    #[inline(always)]
    fn utility(&self) -> f64 {
        let signed_flag = if self.num_of_set % 2 == 0 { -1. } else { 1. };
        signed_flag / self.set.len() as f64
    }
}

fn get_utility_of_cardinality_of_set_union(syns: &[&SellerSet]) -> f64 {
    let syns_len = syns.len();
    match syns_len {
        0 => return 0.,
        1 => return 1. / syns[0].len() as f64,
        2 => {
            return 1. / syns[0].len() as f64 + 1. / syns[1].len() as f64
                - 1. / (syns[0].union(syns[1]).count() as f64);
        }
        _ => {}
    }

    let mut unions: Vec<Union> = syns
        .into_par_iter()
        .enumerate()
        .map(|(set_id, set)| Union {
            num_of_set: 1,
            max_set_id: set_id,
            set: set.iter().copied().collect(),
        })
        .collect();
    let mut ans = unions.par_iter().map(|u| u.utility()).sum();

    while !unions.is_empty() {
        let new_unions: Vec<Union> = unions
            .par_iter()
            .flat_map(|old_u| {
                (old_u.max_set_id + 1..syns_len)
                    .into_par_iter()
                    .map(|new_set_id| {
                        let mut new_set = old_u.set.clone();
                        new_set.extend(syns[new_set_id].iter().copied());
                        Union {
                            num_of_set: old_u.num_of_set + 1,
                            max_set_id: new_set_id,
                            set: new_set,
                        }
                    })
            })
            .collect();

        ans += new_unions.par_iter().map(|u| u.utility()).sum::<f64>();
        unions = new_unions;
    }

    ans
}

pub fn cal_sv_non_linear_comb(
    syns_with_current_seller: &[&SellerSet],
    syns_without_current_seller: &[&SellerSet],
) -> f64 {
    let utility_with_current_seller =
        get_utility_of_cardinality_of_set_union(syns_with_current_seller);

    let syns_interaction_list: HashSet<SellerSet> = syns_with_current_seller
        .par_iter()
        .flat_map(|syn_with_current_seller| {
            syns_without_current_seller
                .par_iter()
                .map(|syn_without_current_seller| {
                    syn_with_current_seller
                        .union(syn_without_current_seller)
                        .copied()
                        .collect::<SellerSet>()
                })
        })
        .collect();
    let syns_interaction_list: Vec<_> = syns_interaction_list.iter().collect();
    let utility_without_current_seller =
        get_utility_of_cardinality_of_set_union(&syns_interaction_list);

    utility_with_current_seller - utility_without_current_seller
}
