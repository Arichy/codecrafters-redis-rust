use anyhow::Result;

use crate::rdb::{Database, SortedSet, Value, ValueType};
use crate::rdb::zset::ZSet;

/// Add a member with score to a sorted set.
/// Returns the number of elements added (1 if new, 0 if score updated).
pub fn zset_add(db: &mut Database, key: &str, member: &str, score: f64) -> Result<i64> {
    match db.get_sorted_set(key.to_string())? {
        Some(sorted_set) => {
            if sorted_set.zset.add(member.to_string(), score) {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        None => {
            // Key doesn't exist or expired, create new sorted set
            db.map.remove(key);
            let mut zset = ZSet::new();
            zset.add(member.to_string(), score);
            db.map.insert(
                key.to_string(),
                Value {
                    expiry: None,
                    value: ValueType::SortedSet(SortedSet { zset }),
                },
            );
            Ok(1)
        }
    }
}

/// Get the score of a member in a sorted set.
/// Returns None if the member or key doesn't exist.
pub fn zset_score(db: &mut Database, key: &str, member: &str) -> Result<Option<f64>> {
    match db.get_sorted_set(key.to_string())? {
        Some(sorted_set) => Ok(sorted_set.zset.score(member).copied()),
        None => Ok(None),
    }
}

/// Get members in a sorted set by index range.
/// Returns empty vector if the key doesn't exist.
pub fn zset_range(db: &mut Database, key: &str, start: i32, end: i32) -> Result<Vec<String>> {
    match db.get_sorted_set(key.to_string())? {
        Some(sorted_set) => Ok(sorted_set.zset.range(start, end).into_iter().cloned().collect()),
        None => Ok(vec![]),
    }
}

/// Get the rank of a member in a sorted set.
/// Returns None if the member or key doesn't exist.
pub fn zset_rank(db: &mut Database, key: &str, member: &str) -> Result<Option<usize>> {
    match db.get_sorted_set(key.to_string())? {
        Some(sorted_set) => Ok(sorted_set.zset.rank(member)),
        None => Ok(None),
    }
}

/// Get the number of elements in a sorted set.
/// Returns 0 if the key doesn't exist.
pub fn zset_card(db: &mut Database, key: &str) -> Result<usize> {
    match db.get_sorted_set(key.to_string())? {
        Some(sorted_set) => Ok(sorted_set.zset.len()),
        None => Ok(0),
    }
}

/// Remove a member from a sorted set.
/// Returns 1 if removed, 0 if the member didn't exist.
pub fn zset_rem(db: &mut Database, key: &str, member: &str) -> Result<i64> {
    match db.get_sorted_set(key.to_string())? {
        Some(sorted_set) => {
            if sorted_set.zset.rem(member.to_string()) {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        None => Ok(0),
    }
}