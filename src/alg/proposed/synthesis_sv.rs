use super::*;
use crate::{
    utils::{binom, merge_sv},
    SellerId,
};
use std::collections::HashMap;

mod non_linear_comb;
mod non_linear_lookup;

pub fn cal_sv_linear(syns: &Synthesis, count: usize, k: usize) -> HashMap<SellerId, f64> {
    let alpha = count;
    let beta = syns.len() - count;
    let sv_alpha = if alpha == 0 {
        0.
    } else {
        alpha as f64 / ((k + beta) * binom(k - 1, k + beta - 1)) as f64
    };
    let sv_beta = if beta == 0 {
        0.
    } else {
        (1. - k as f64 * sv_alpha) / beta as f64
    };
    let mut ans = HashMap::new();
    for syn in syns.iter() {
        if syn.len() == 1 {
            let id = syn.iter().next().unwrap();
            ans.insert(*id, sv_beta);
        } else {
            for id in syn.iter() {
                ans.insert(*id, sv_alpha);
            }
        }
    }
    ans
}

/// Return (shapley_value, lookup_count, comb_count).
pub fn cal_sv_non_linear(syns: &Synthesis, scale: f64) -> (HashMap<SellerId, f64>, usize, usize) {
    let sellers = syns.unique_sellers();
    sellers
        .par_iter()
        .map(|&seller| {
            let mut syns_with_current_seller = vec![];
            let mut syns_without_current_seller = vec![];

            let mut ans = HashMap::new();
            let mut lookup_count = 0;
            let mut comb_count = 0;
            for syn in syns.iter() {
                if syn.contains(&seller) {
                    syns_with_current_seller.push(syn);
                } else {
                    syns_without_current_seller.push(syn);
                }

                let number_of_pow_for_syns = if syns_with_current_seller.is_empty() {
                    syns_without_current_seller.len()
                } else if syns_without_current_seller.is_empty() {
                    syns_with_current_seller.len()
                } else {
                    syns_with_current_seller.len() * syns_without_current_seller.len()
                };

                if sellers.len() as f64 <= scale * number_of_pow_for_syns as f64 {
                    let u = non_linear_lookup::cal_sv_non_linear_lookup(
                        &syns_with_current_seller,
                        &syns_without_current_seller,
                        &sellers,
                        seller,
                    );
                    ans.insert(seller, u);
                    lookup_count += 1;
                } else {
                    let u = non_linear_comb::cal_sv_non_linear_comb(
                        &syns_with_current_seller,
                        &syns_without_current_seller,
                    );
                    ans.insert(seller, u);
                    comb_count += 1;
                }
            }
            (ans, lookup_count, comb_count)
        })
        .reduce(
            || (HashMap::new(), 0, 0),
            |a, b| (merge_sv(a.0, b.0), a.1 + b.1, a.2 + b.2),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SellerSet;

    #[test]
    fn test_linear() {
        let syns = Synthesis::new(
            vec![
                vec![0].into_iter().collect::<SellerSet>(),
                vec![3].into_iter().collect::<SellerSet>(),
                vec![1, 2, 4].into_iter().collect::<SellerSet>(),
            ]
            .into_iter()
            .collect(),
        );
        dbg!(cal_sv_linear(&syns, 1, 3));

        let syns = Synthesis::new(
            vec![
                vec![0].into_iter().collect::<SellerSet>(),
                vec![1].into_iter().collect::<SellerSet>(),
                vec![3].into_iter().collect::<SellerSet>(),
            ]
            .into_iter()
            .collect(),
        );
        dbg!(cal_sv_linear(&syns, 0, 0));
    }
}
