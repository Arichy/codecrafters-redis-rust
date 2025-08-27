use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt;
use std::rc::Rc;

const MAX_LEVEL: usize = 16;

#[derive(Debug)]
struct Node<T> {
    value: T,
    score: f64,
    forward: Vec<Option<Rc<RefCell<Node<T>>>>>,
}

impl<T> Node<T> {
    fn new(value: T, score: f64, level: usize) -> Self {
        Node {
            value,
            score,
            forward: vec![None; level + 1],
        }
    }
}

#[derive(Debug)]
pub struct SkipList<T> {
    header: Rc<RefCell<Node<T>>>,
    level: usize,
    length: usize,
}

impl<T> SkipList<T>
where
    T: Clone + PartialEq + fmt::Debug + Default,
{
    fn new() -> Self {
        SkipList {
            header: Rc::new(RefCell::new(Node::new(
                Default::default(),
                f64::NEG_INFINITY,
                MAX_LEVEL,
            ))),
            level: 0,
            length: 0,
        }
    }

    pub fn with_header(header_value: T) -> Self {
        SkipList {
            header: Rc::new(RefCell::new(Node::new(
                header_value,
                f64::NEG_INFINITY,
                MAX_LEVEL,
            ))),
            level: 0,
            length: 0,
        }
    }

    fn random_level() -> usize {
        let mut level = 0;
        while level < MAX_LEVEL && rand::random::<f64>() < 0.5 {
            level += 1;
        }
        level
    }

    pub fn insert(&mut self, value: T, score: f64) {
        let mut update: Vec<Rc<RefCell<Node<T>>>> = Vec::with_capacity(MAX_LEVEL + 1);
        let mut current = Rc::clone(&self.header);

        for level in (0..=self.level).rev() {
            loop {
                let current_borrow = current.borrow();
                let next_option = current_borrow.forward[level].clone();
                drop(current_borrow);

                if let Some(next) = next_option {
                    let next_score = next.borrow().score;
                    if next_score < score {
                        current = next;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            if update.len() <= level {
                update.resize(level + 1, Rc::clone(&self.header));
            }
            update[level] = Rc::clone(&current);
        }

        let new_level = Self::random_level();

        if new_level > self.level {
            for level in (self.level + 1)..=new_level {
                if update.len() <= level {
                    update.resize(level + 1, Rc::clone(&self.header));
                }
                update[level] = Rc::clone(&self.header);
            }
            self.level = new_level;
        }

        let new_node = Rc::new(RefCell::new(Node::new(value, score, new_level)));

        for level in 0..=new_level {
            if level < update.len() {
                let update_node = &update[level];
                let next = update_node.borrow().forward[level].clone();
                new_node.borrow_mut().forward[level] = next;
                update_node.borrow_mut().forward[level] = Some(Rc::clone(&new_node));
            }
        }

        self.length += 1;
    }

    pub fn search(&self, score: f64) -> Option<T> {
        let mut current = Rc::clone(&self.header);

        for level in (0..=self.level).rev() {
            loop {
                let current_borrow = current.borrow();
                let next_option = current_borrow.forward[level].clone();
                drop(current_borrow);

                if let Some(next) = next_option {
                    let next_borrow = next.borrow();
                    let next_score = next_borrow.score;

                    match next_score.partial_cmp(&score) {
                        Some(Ordering::Less) => {
                            drop(next_borrow);
                            current = next;
                        }
                        Some(Ordering::Equal) => {
                            return Some(next_borrow.value.clone());
                        }
                        _ => break,
                    }
                } else {
                    break;
                }
            }
        }
        None
    }

    pub fn delete(&mut self, score: f64) -> bool {
        let mut update: Vec<Rc<RefCell<Node<T>>>> = Vec::with_capacity(MAX_LEVEL + 1);
        let mut current = Rc::clone(&self.header);

        for level in (0..=self.level).rev() {
            loop {
                let current_borrow = current.borrow();
                let next_option = current_borrow.forward[level].clone();
                drop(current_borrow);

                if let Some(next) = next_option {
                    let next_score = next.borrow().score;
                    if next_score < score {
                        current = next;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            if update.len() <= level {
                update.resize(level + 1, Rc::clone(&self.header));
            }
            update[level] = Rc::clone(&current);
        }

        let current_borrow = current.borrow();
        let target_node = current_borrow.forward[0].clone();
        drop(current_borrow);

        if let Some(target) = target_node {
            let target_score = target.borrow().score;
            if (target_score - score).abs() < f64::EPSILON {
                for level in 0..=self.level {
                    if level < update.len() {
                        let update_node = &update[level];
                        let mut update_borrow = update_node.borrow_mut();

                        if let Some(ref node_at_level) = update_borrow.forward[level] {
                            let node_score = node_at_level.borrow().score;
                            if (node_score - score).abs() < f64::EPSILON {
                                let next = node_at_level.borrow().forward[level].clone();
                                update_borrow.forward[level] = next;
                            }
                        }
                    }
                }

                while self.level > 0 {
                    let header_borrow = self.header.borrow();
                    let has_next = header_borrow.forward[self.level].is_some();
                    drop(header_borrow);

                    if has_next {
                        break;
                    }
                    self.level -= 1;
                }

                self.length -= 1;
                return true;
            }
        }
        false
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn values(&self) -> Vec<(f64, T)> {
        let mut result = Vec::new();
        let mut current = self.header.borrow().forward[0].clone();

        while let Some(node) = current {
            let node_borrow = node.borrow();
            result.push((node_borrow.score, node_borrow.value.clone()));
            current = node_borrow.forward[0].clone();
        }

        result
    }
}

mod rand {
    use std::cell::Cell;

    thread_local! {
        static RNG_STATE: Cell<u64> = Cell::new(1);
    }

    pub fn random<T>() -> T
    where
        T: From<f64>,
    {
        RNG_STATE.with(|state| {
            let mut x = state.get();
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            state.set(x);
            T::from((x as f64) / (u64::MAX as f64))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_skip_list() {
        let skip_list: SkipList<i32> = SkipList::with_header(0);
        assert_eq!(skip_list.len(), 0);
        assert!(skip_list.is_empty());
    }

    #[test]
    fn test_insert_and_search() {
        let mut skip_list = SkipList::with_header(String::new());
        skip_list.insert("hello".to_string(), 1.0);

        assert_eq!(skip_list.len(), 1);
        assert_eq!(skip_list.search(1.0), Some("hello".to_string()));
        assert_eq!(skip_list.search(2.0), None);
    }

    #[test]
    fn test_multiple_elements() {
        let mut skip_list = SkipList::with_header(0);

        skip_list.insert(10, 1.0);
        skip_list.insert(20, 2.0);
        skip_list.insert(5, 0.5);

        assert_eq!(skip_list.len(), 3);

        let values = skip_list.values();
        assert_eq!(values[0], (0.5, 5));
        assert_eq!(values[1], (1.0, 10));
        assert_eq!(values[2], (2.0, 20));
    }

    #[test]
    fn test_delete() {
        let mut skip_list = SkipList::with_header(0);

        skip_list.insert(10, 1.0);
        skip_list.insert(20, 2.0);
        skip_list.insert(30, 3.0);

        assert_eq!(skip_list.len(), 3);
        assert!(skip_list.delete(2.0));
        assert_eq!(skip_list.len(), 2);
        assert_eq!(skip_list.search(2.0), None);

        let values = skip_list.values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], (1.0, 10));
        assert_eq!(values[1], (3.0, 30));
    }

    #[test]
    fn test_edge_cases() {
        let mut skip_list = SkipList::with_header(0);

        // Test with negative scores
        skip_list.insert(-1, -1.0);
        skip_list.insert(0, 0.0);
        skip_list.insert(1, 1.0);

        let values = skip_list.values();
        assert_eq!(values[0], (-1.0, -1));
        assert_eq!(values[1], (0.0, 0));
        assert_eq!(values[2], (1.0, 1));

        // Test deletion of non-existent element
        assert!(!skip_list.delete(5.0));
        assert_eq!(skip_list.len(), 3);
    }

    #[test]
    fn test_duplicate_scores() {
        let mut skip_list = SkipList::with_header(String::new());

        skip_list.insert("first".to_string(), 1.0);
        skip_list.insert("second".to_string(), 1.0);
        skip_list.insert("third".to_string(), 2.0);

        assert_eq!(skip_list.len(), 3);

        // Should find one of the elements with score 1.0
        let result = skip_list.search(1.0);
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert!(result_val == "first".to_string() || result_val == "second".to_string());
    }

    #[test]
    fn test_delete_all_elements() {
        let mut skip_list = SkipList::with_header(0);

        skip_list.insert(10, 1.0);
        skip_list.insert(20, 2.0);

        assert!(skip_list.delete(1.0));
        assert!(skip_list.delete(2.0));

        assert_eq!(skip_list.len(), 0);
        assert!(skip_list.is_empty());
        assert!(skip_list.values().is_empty());
    }
}
