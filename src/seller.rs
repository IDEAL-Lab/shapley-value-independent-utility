use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    derive_more::Display,
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
pub struct SellerId(pub u64);

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
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
pub struct SellerSet(pub BTreeSet<SellerId>);

impl FromIterator<u64> for SellerSet {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        Self(iter.into_iter().map(SellerId).collect())
    }
}
