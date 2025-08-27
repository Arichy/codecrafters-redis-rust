use std::{
    borrow::Borrow,
    cmp::Ordering,
    f64,
    fmt::{self, Display},
    ops::{Deref, DerefMut, Index},
    process::Output,
    ptr::{self, NonNull},
    sync::{Arc, Mutex},
};

const MAX_LEVEL: usize = 16;

#[derive(Debug, Clone)]
struct NodeKey<'a, T: ?Sized> {
    value: &'a T,
    score: &'a f64,
}

impl<T: PartialEq> std::cmp::PartialEq for NodeKey<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.value == other.value
    }
}

impl<T: PartialOrd> std::cmp::PartialOrd for NodeKey<'_, T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.score < other.score {
            Some(Ordering::Less)
        } else if self.score > other.score {
            Some(Ordering::Greater)
        } else {
            if self.value < other.value {
                Some(Ordering::Less)
            } else if self.value > other.value {
                Some(Ordering::Greater)
            } else {
                Some(Ordering::Equal)
            }
        }
    }
}

#[derive(Debug)]
struct ForwardPtr<T> {
    ptr: NonNull<Node<T>>,
    span: usize,
}

impl<T> Deref for ForwardPtr<T> {
    type Target = Node<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr.as_ptr() }
    }
}

impl<T> DerefMut for ForwardPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.ptr.as_mut() }
    }
}

impl<T> Clone for ForwardPtr<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            span: self.span,
        }
    }
}

impl<T> Copy for ForwardPtr<T> {}

// type ForwardPtr<T> = NonNull<Node<T>>;

#[derive(Debug, Clone)]
pub struct Node<T> {
    value: T,
    score: f64,
    forward: Vec<Option<ForwardPtr<T>>>,
}

impl<T> Node<T> {
    fn new(value: T, score: f64, level: usize) -> NonNull<Self> {
        let forward = (0..=level).map(|i| None).collect();

        unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Self {
                value,
                score,
                forward,
            })))
        }
    }

    fn key(&self) -> NodeKey<T> {
        NodeKey {
            value: &self.value,
            score: &self.score,
        }
    }

    fn value(&self) -> &T {
        &self.value
    }

    fn score(&self) -> f64 {
        self.score
    }

    fn next(&self, level: usize) -> Option<ForwardPtr<T>> {
        self.forward[level]
    }
}

impl<T: Display> fmt::Display for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.score, self.value)
    }
}

// SAFETY: The user guarantees that the SkipList, which owns the Nodes,
// is protected by a Mutex, ensuring exclusive access.
unsafe impl<T: Send> Send for Node<T> {}
unsafe impl<T: Sync> Sync for Node<T> {}

#[derive(Debug, Clone)]
pub struct SkipList<T> {
    head: NonNull<Node<T>>,
    tail: NonNull<Node<T>>,
    length: usize,
    level: usize,
}

// SAFETY: SkipList would be wrapped in LazyLock<Mutex>, so every operation would be exclusive
unsafe impl<T: Send> Send for SkipList<T> {}
unsafe impl<T: Sync> Sync for SkipList<T> {}

impl<T: Default + PartialEq + PartialOrd + fmt::Debug + fmt::Display> SkipList<T> {
    pub fn new() -> Self {
        let mut head = Node::new(T::default(), f64::NEG_INFINITY, 0);
        let tail = Node::new(T::default(), f64::INFINITY, 0);
        unsafe {
            for item in &mut (*head.as_ptr()).forward {
                *item = Some(ForwardPtr { ptr: tail, span: 1 });
            }
        }

        Self {
            head,
            tail,
            length: 0,
            level: 0,
        }
    }

    fn rand_level(&self) -> usize {
        let mut level = 1;
        while rand::random::<f64>() < 0.5 && level < MAX_LEVEL {
            level += 1;
        }
        level

        // if self.length & 1 == 0 {
        //     1
        // } else {
        //     2
        // }
    }

    pub fn search(&self, value: T, score: f64) -> Option<(NonNull<Node<T>>, usize)> {
        let mut level = self.level;
        let mut p = self.head;
        let mut current_step = 0;

        let target_key = NodeKey {
            value: &value,
            score: &score,
        };

        for i in (0..=level).rev() {
            loop {
                let next_ptr =
                    unsafe { p.as_ref().next(i) }.expect("would break before reaching tail");

                unsafe {
                    if (*next_ptr).key() < target_key {
                        current_step += next_ptr.span;
                        p = next_ptr.ptr;
                    } else {
                        break;
                    }
                }
            }
        }

        unsafe {
            let next_ptr = p.as_ref().next(0).unwrap();
            p = next_ptr.ptr;
            current_step += next_ptr.span;

            if (*p.as_ptr()).key() == target_key {
                Some((p, current_step - 1))
            } else {
                None
            }
        }
    }

    pub fn insert(&mut self, value: T, score: f64) -> bool {
        // println!("insert {:?}:{}", value, score);
        let mut update = vec![None; MAX_LEVEL + 1];
        let mut steps = vec![1; MAX_LEVEL + 1]; // span sum from HEAD to update[i]
        let mut p = self.head;

        let insert_key = NodeKey {
            value: &value,
            score: &score,
        };

        let mut step = 0;
        for i in (0..=self.level).rev() {
            loop {
                let forward_ptr =
                    unsafe { (*p.as_ptr()).next(i) }.expect("would break before reaching tail");

                unsafe {
                    let next_key = (*forward_ptr).key();
                    if next_key == insert_key {
                        return false;
                    }

                    // if score already exists, insert after existing one
                    if next_key < insert_key {
                        step += forward_ptr.span;
                        p = forward_ptr.ptr;
                    } else {
                        break;
                    }
                }
            }
            update[i] = Some(p);
            steps[i] = step;
        }

        let rank = steps[0] + 1;

        unsafe {
            p = (*p.as_ptr())
                .next(0)
                .expect("would break before reaching tail")
                .ptr
        };

        let rand_level = self.rand_level();
        let mut level = rand_level;

        if level > self.level {
            self.level += 1;
            level = self.level;
            unsafe {
                (*self.head.as_ptr()).forward.push(Some(ForwardPtr {
                    ptr: self.tail,
                    span: self.length + 1,
                }));
            }
            update[level] = Some(self.head);
            steps[level] = 0;
        }

        let mut new_node = Node::new(value, score, level);
        for i in (0..=self.level).rev() {
            p = update[i].expect("it can't be None");
            let p_node_ref = unsafe { &mut *(p.as_ptr()) };
            if i <= rand_level {
                unsafe {
                    (*new_node.as_mut()).forward[i] = Some(ForwardPtr {
                        ptr: p_node_ref.forward[i].unwrap().ptr,
                        span: steps[i] + p_node_ref.forward[i].unwrap().span + 1 - rank,
                    });

                    p_node_ref.forward[i] = Some(ForwardPtr {
                        ptr: new_node,
                        span: rank - steps[i],
                    });
                };
            } else {
                p_node_ref.forward[i].as_mut().unwrap().span += 1;
            }
        }
        // println!("new_node: {:?}", unsafe { &*new_node.as_ptr() });

        self.length += 1;

        true
    }

    pub fn delete(&mut self, value: T, score: f64) -> bool {
        // println!("delete value: {:?}", value);
        let mut update = vec![None; MAX_LEVEL + 1];
        let mut p = self.head;

        let delete_key = NodeKey {
            value: &value,
            score: &score,
        };

        for i in (0..=self.level).rev() {
            loop {
                let forward_ptr =
                    unsafe { (*p.as_ptr()).next(i) }.expect("would break before reaching tail");

                unsafe {
                    let next_key = (*forward_ptr).key();

                    if next_key < delete_key {
                        p = forward_ptr.ptr;
                    } else {
                        break;
                    }
                }
            }
            update[i] = Some(p);
        }

        unsafe {
            p = (*p.as_ptr())
                .next(0)
                .expect("The score of tail node is INFINITE so `cur` can't be tail.")
                .ptr;
        }

        if unsafe { (*p.as_ptr()).key() } == delete_key {
            for i in (0..=self.level) {
                let mut update = update[i].expect("it can't be None");

                unsafe {
                    let mut next_ptr = (*update.as_ptr()).next(i).unwrap();
                    if (*next_ptr).key() == delete_key {
                        (*update.as_ptr()).forward[i] = Some(ForwardPtr {
                            ptr: (*next_ptr).forward[i].unwrap().ptr,
                            span: (*update.as_ptr()).forward[i].unwrap().span
                                + (*next_ptr).forward[i].unwrap().span
                                - 1,
                        });
                    } else {
                        (*update.as_ptr()).forward[i].as_mut().unwrap().span -= 1;
                    }
                }
            }

            unsafe {
                let _ = Box::from_raw(p.as_ptr());
            }

            while self.level > 0 && {
                let head_next = unsafe { (*self.head.as_ptr()).forward[self.level].unwrap() };

                head_next.ptr == self.tail
            } {
                unsafe { (*self.head.as_ptr()).forward.pop() };
                self.level -= 1;
            }

            self.length -= 1;
            true
        } else {
            false
        }
    }

    pub fn values(&self) -> Vec<&T> {
        let mut values = Vec::with_capacity(self.length);

        let mut p = self.head;

        while p != self.tail {
            unsafe {
                let next = (*p.as_ptr()).next(0).unwrap();
                if next.ptr != self.tail {
                    values.push(&(*next.ptr.as_ptr()).value);
                }
                p = next.ptr;
            }
        }

        values
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.length {
            return None;
        }

        let target_step = index + 1;
        let mut p = self.head;
        let mut current_step = 0;
        for i in (0..self.level).rev() {
            loop {
                if current_step == target_step {
                    return Some(unsafe { &p.as_ref().value });
                }

                let next_ptr =
                    unsafe { p.as_ref().next(i) }.expect("would break before reaching tail");

                if current_step + next_ptr.span > target_step {
                    break;
                } else {
                    current_step += next_ptr.span;
                    p = next_ptr.ptr
                }
            }
        }

        None
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

impl<T: Default + PartialEq + PartialOrd + fmt::Debug + fmt::Display> Index<usize> for SkipList<T> {
    type Output = T;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl<T: fmt::Display> fmt::Display for SkipList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 1. Get all nodes from level 0. This defines the columns of our graph.
        let mut nodes_l0 = vec![];
        let mut current = self.head;
        loop {
            nodes_l0.push(current);
            if current == self.tail {
                break;
            }
            unsafe {
                let forward_ptr = (*current.as_ptr()).next(0).unwrap();
                current = forward_ptr.ptr;
            }
        }

        // 2. Get string representations for each node.
        let node_reprs: Vec<String> = nodes_l0
            .iter()
            .map(|&node_ptr| {
                if node_ptr == self.head {
                    "HEAD".to_string()
                } else if node_ptr == self.tail {
                    "TAIL".to_string()
                } else {
                    unsafe { (*node_ptr.as_ptr()).to_string() }
                }
            })
            .collect();

        // 3. Print each level from top to bottom.
        for i in (0..=self.level).rev() {
            // Print node line
            write!(f, "L{:<2}|", i)?;
            let mut node_on_level = self.head;
            for (l0_idx, &l0_node) in nodes_l0.iter().enumerate() {
                let repr = &node_reprs[l0_idx];
                let is_node_on_level = l0_node == node_on_level;

                if l0_idx > 0 {
                    if is_node_on_level {
                        write!(f, " -> ")?;
                    } else {
                        write!(f, "----")?;
                    }
                }

                if is_node_on_level {
                    write!(f, "{}", repr)?;
                    if node_on_level != self.tail {
                        unsafe {
                            let forward_ptr = (*node_on_level.as_ptr()).next(i).unwrap();
                            node_on_level = forward_ptr.ptr;
                        }
                    }
                } else {
                    write!(f, "{}", "-".repeat(repr.len()))?;
                }
            }
            writeln!(f)?;

            // Print spans line
            write!(f, "   |")?;
            let mut node_on_level_for_span = self.head;
            for (l0_idx, &l0_node) in nodes_l0.iter().enumerate() {
                let repr = &node_reprs[l0_idx];
                if l0_idx > 0 {
                    write!(f, "    ")?;
                }

                if l0_node == node_on_level_for_span {
                    if node_on_level_for_span != self.tail {
                        unsafe {
                            let forward_ptr = (*node_on_level_for_span.as_ptr()).next(i).unwrap();
                            let span = forward_ptr.span;
                            let span_str = format!("({})", span);
                            write!(f, "{:<width$}", span_str, width = repr.len())?;
                            node_on_level_for_span = forward_ptr.ptr;
                        }
                    } else {
                        // TAIL node has no outgoing span
                        write!(f, "{}", " ".repeat(repr.len()))?;
                    }
                } else {
                    write!(f, "{}", " ".repeat(repr.len()))?;
                }
            }
            writeln!(f)?;

            // 4. Print vertical connectors.
            if i > 0 {
                write!(f, "   |")?;
                let mut node_on_level_for_vertical = self.head;
                for (l0_idx, &l0_node) in nodes_l0.iter().enumerate() {
                    let repr = &node_reprs[l0_idx];
                    if l0_idx > 0 {
                        write!(f, "    ")?;
                    }

                    if l0_node == node_on_level_for_vertical {
                        write!(f, "|")?;
                        write!(f, "{}", " ".repeat(repr.len() - 1))?;
                        if node_on_level_for_vertical != self.tail {
                            unsafe {
                                let forward_ptr =
                                    (*node_on_level_for_vertical.as_ptr()).next(i).unwrap();
                                node_on_level_for_vertical = forward_ptr.ptr;
                            }
                        }
                    } else {
                        write!(f, "{}", " ".repeat(repr.len()))?;
                    }
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        let mut skip_list = <SkipList<String>>::new();
        skip_list.insert("a".to_string(), 1.0);
        // println!("Init: \n{}", skip_list);

        skip_list.insert("b".to_string(), 2.0);
        // println!("Init: \n{}", skip_list);

        skip_list.insert("c".to_string(), 3.0);
        // println!("Init: \n{}", skip_list);

        skip_list.insert("d".to_string(), 4.0);
        // println!("Init: \n{}", skip_list);

        assert!(skip_list.search("b".to_string(), 2.0).is_some());
        assert!(skip_list.search("e".to_string(), 1.0).is_none());

        skip_list.delete("b".to_string(), 2.0);
        // println!("After removing b: \n{}", skip_list);
        assert!(skip_list.search("b".to_string(), 2.0).is_none());

        skip_list.delete("d".to_string(), 4.0);
        // println!("After removing d: \n{}", skip_list);
        assert!(skip_list.search("d".to_string(), 4.0).is_none());
    }

    #[test]
    fn same_score() {
        let mut skip_list = <SkipList<&str>>::new();
        skip_list.insert("a", 1.0);
        skip_list.insert("c", 2.0);
        skip_list.insert("b", 2.0);

        assert_eq!(skip_list.values(), vec![&"a", &"b", &"c"])
    }

    #[test]
    fn rank() {
        let mut skip_list = <SkipList<String>>::new();
        skip_list.insert("a".to_string(), 1.0);
        skip_list.insert("c".to_string(), 2.0);
        skip_list.insert("b".to_string(), 2.0);

        assert_eq!(
            skip_list
                .search("a".to_string(), 1.0)
                .map(|(_, rank)| { rank }),
            Some(0)
        );
        assert_eq!(
            skip_list.search("b".to_string(), 2.0).map(|(_, rank)| rank),
            Some(1)
        );
        assert_eq!(
            skip_list.search("c".to_string(), 2.0).map(|(_, rank)| rank),
            Some(2)
        );
    }

    #[test]
    fn get() {
        let mut skip_list = <SkipList<String>>::new();
        skip_list.insert("a".to_string(), 1.0);
        skip_list.insert("c".to_string(), 2.0);
        skip_list.insert("b".to_string(), 2.0);

        assert_eq!(skip_list.get(1), Some(&"b".to_string()));
        assert_eq!(skip_list.get(0), Some(&"a".to_string()));
        assert_eq!(skip_list.get(2), Some(&"c".to_string()));
        assert!(skip_list.get(3).is_none())
    }
}
