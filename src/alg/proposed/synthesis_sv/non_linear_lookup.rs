use crate::{utils::binom, SellerId, SellerSet};
use rayon::prelude::*;
use std::collections::HashSet;

#[derive(Clone)]
struct Subset {
    next_id: usize,
    subset: HashSet<SellerId>,
    with_flag: bool,
}

impl Subset {
    fn utility_with_current_seller(
        &mut self,
        seller: SellerId,
        syns_with_current_seller: &[&SellerSet],
    ) -> bool {
        if self.with_flag {
            return true;
        }

        let syn_len_upper_bound = self.subset.len() + 1;
        self.with_flag = syns_with_current_seller.par_iter().any(|syn| {
            syn.len() <= syn_len_upper_bound
                && syn
                    .par_iter()
                    .all(|v| *v == seller || self.subset.contains(v))
        });
        self.with_flag
    }

    fn utility_without_current_seller(&self, syns_without_current_seller: &[&SellerSet]) -> bool {
        let syn_len_upper_bound = self.subset.len();
        syns_without_current_seller.par_iter().any(|syn| {
            syn.len() <= syn_len_upper_bound && syn.par_iter().all(|v| self.subset.contains(v))
        })
    }
}

pub fn cal_sv_non_linear_lookup(
    syns_with_current_seller: &[&SellerSet],
    syns_without_current_seller: &[&SellerSet],
    sellers: &HashSet<SellerId>,
    seller: SellerId,
) -> f64 {
    let number_of_sellers = sellers.len();
    let rest_of_sellers: Vec<_> = sellers.iter().copied().filter(|s| *s != seller).collect();
    let rest_of_sellers_len = rest_of_sellers.len();

    let mut marginal_contribution_for_current_seller = 0.;
    let mut init_subset = Subset {
        next_id: 0,
        subset: HashSet::new(),
        with_flag: false,
    };

    if init_subset.utility_with_current_seller(seller, &syns_with_current_seller) {
        // when subset is empty; number_of_sub_combination = 1 and without_flag = false
        marginal_contribution_for_current_seller += 1.;
    }

    let mut subsets: Vec<Subset> = vec![init_subset];
    let mut chosen = 1;

    while !subsets.is_empty() {
        let (marginal_contribution_in_sub_combination, new_subsets): (usize, Vec<Subset>) = subsets
            .par_iter()
            .flat_map(|old_s| {
                (old_s.next_id..rest_of_sellers_len)
                    .into_par_iter()
                    .filter_map(|next_id| {
                        let mut new_s = old_s.clone();
                        new_s.next_id = next_id + 1;
                        new_s.subset.insert(rest_of_sellers[next_id]);

                        if new_s.utility_with_current_seller(seller, &syns_with_current_seller) {
                            if new_s.utility_without_current_seller(&syns_without_current_seller) {
                                // early stop
                                return None;
                            } else {
                                return Some((1, new_s));
                            }
                        }

                        Some((0, new_s))
                    })
            })
            .fold(
                || (0, Vec::new()),
                |mut acc, input| {
                    acc.0 += input.0;
                    acc.1.push(input.1);
                    acc
                },
            )
            .reduce(
                || (0, Vec::new()),
                |mut a, mut b| -> (usize, Vec<Subset>) {
                    a.0 += b.0;
                    a.1.append(&mut b.1);
                    a
                },
            );

        if marginal_contribution_in_sub_combination != 0 {
            let number_of_sub_combination = binom(chosen, rest_of_sellers_len);
            marginal_contribution_for_current_seller +=
                marginal_contribution_in_sub_combination as f64 / number_of_sub_combination as f64;
        }

        subsets = new_subsets;
        chosen += 1;
    }

    marginal_contribution_for_current_seller / number_of_sellers as f64
}
