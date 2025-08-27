use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    rdb::zset::skip_list::{Node, SkipList},
    utils::{index, normalize_index},
};

mod skip_list;

#[derive(Debug, Clone)]
pub struct ZSet {
    skip_list: SkipList<String>,
    hash_map: HashMap<String, f64>,
}

impl ZSet {
    pub fn new() -> Self {
        Self {
            skip_list: SkipList::new(),
            hash_map: HashMap::new(),
        }
    }

    pub fn add(&mut self, value: String, score: f64) -> bool {
        // if member existed, update the score
        if let Some(old_score) = self.hash_map.get(&value) {
            assert!(self.skip_list.delete(value.clone(), *old_score));
            assert!(self.skip_list.insert(value.clone(), score));
            (*self.hash_map.get_mut(&value).unwrap()) = score;
            false
        } else {
            // not existed, just insert
            self.hash_map.insert(value.clone(), score);
            self.skip_list.insert(value, score)
        }
    }

    pub fn rem(&mut self, value: String) -> bool {
        if let Some(score) = self.hash_map.get(&value) {
            let score = *score;
            self.hash_map.remove(&value);
            self.skip_list.delete(value, score)
        } else {
            false
        }
    }

    pub fn rank(&self, value: &str) -> Option<usize> {
        self.hash_map.get(value).and_then(|score| {
            self.skip_list
                .search(value.to_string(), *score)
                .map(|(_, rank)| rank)
        })
    }

    pub fn range(&self, start: i32, end: i32) -> Vec<&String> {
        // let values = self.skip_list.values();

        match normalize_index(self.skip_list.len(), start, end) {
            Some((start, end)) => {
                let mut result = vec![];
                for i in (start..=end) {
                    result.push(&self.skip_list[i]);
                }

                result
            }
            None => vec![],
        }
    }

    pub fn len(&self) -> usize {
        self.skip_list.len()
    }

    pub fn score(&self, member: &str) -> Option<&f64> {
        self.hash_map.get(member)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_zset() {
        let mut zset = ZSet::new();
        assert!(zset.add("member_with_score_1".to_string(), 1.0));
        assert!(zset.add("member_with_score_2".to_string(), 2.0));
        assert!(zset.add("another_member_with_score_2".to_string(), 2.0));

        assert_eq!(zset.rank("member_with_score_1"), Some(0));
        assert_eq!(zset.rank("member_with_score_2"), Some(2));
        assert_eq!(zset.rank("another_member_with_score_2"), Some(1));

        assert!(zset.rem("member_with_score_1".to_string()));
    }

    #[test]
    fn zset_range() {
        let mut zset = ZSet::new();
        assert!(zset.add("Sam-Bodden".to_string(), 8.1));
        assert!(zset.add("Royce".to_string(), 10.2));
        assert!(zset.add("Ford".to_string(), 6.0));
        assert!(zset.add("Prickett".to_string(), 14.1));
        // F S R P

        assert_eq!(zset.range(0, 2), vec!["Ford", "Sam-Bodden", "Royce"]);

        assert_eq!(zset.range(-2, -1), vec!["Royce", "Prickett"]);

        assert_eq!(zset.range(0, -3), vec!["Ford", "Sam-Bodden"]);
    }

    #[test]
    fn zset_update_score() {
        let mut zset = ZSet::new();
        assert!(zset.add("apple".to_string(), 8.1));
        assert!(!zset.add("apple".to_string(), 9.1));
    }

    #[test]
    fn zset_rank() {
        let mut zset = ZSet::new();
        zset.add("foo".to_string(), 100.0);
        zset.add("bar".to_string(), 100.0);
        zset.add("baz".to_string(), 20.0);
        zset.add("caz".to_string(), 30.1);
        zset.add("paz".to_string(), 40.2);

        assert_eq!(zset.rank("caz"), Some(1));
        assert_eq!(zset.rank("baz"), Some(0));
        assert_eq!(zset.rank("bar"), Some(3));
        assert_eq!(zset.rank("foo"), Some(4));
    }
}
