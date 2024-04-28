//! A generic doubly linked list implementation.

use std::{ops::Deref, sync::{Arc, RwLock, Weak}};

#[derive(Debug)]
pub struct ListNode<T> {
    prev: Option<Weak<RwLock<ListNode<T>>>>,
    next: Option<Arc<RwLock<ListNode<T>>>>,
    value: Option<Arc<T>>,
}

impl<T> ListNode<T> {
    pub fn prev(&self) -> Option<Arc<RwLock<ListNode<T>>>> {
        if let Some(p) = self.prev.clone() {
            if let Some(p) = p.upgrade() {
                return Some(p);
            }
        }
        None
    }

    pub fn next(&self) -> Option<Arc<RwLock<ListNode<T>>>> {
        self.next.clone()
    }

    pub fn value(&self) -> Option<Arc<T>> {
        self.value.clone()
    }
}

#[derive(Debug)]
pub struct List<T> {
    head: Option<Arc<RwLock<ListNode<T>>>>,
    tail: Option<Arc<RwLock<ListNode<T>>>>,
    len: usize,
}

impl<T: PartialEq> List<T> {
    /// Create a new list.
    pub fn new() -> List<T> {
        List { head: None, tail: None, len: 0 }
    }

    /// Add a new node to the list, to head, contaning the specified 'value' 
    /// pointer as value.
    pub fn add_node_head(&mut self, value: Option<Arc<T>>) -> &mut Self {
        let node = Arc::new(RwLock::new(
            ListNode { prev: None, next: self.head.take(), value }
        ));
        if self.len == 0 {
            self.head = Some(node.clone());
            self.tail = Some(node.clone());
        } else {
            self.head = Some(node.clone());
            if let Some(p) = node.read().unwrap().next.clone() {
                p.write().unwrap().prev = Some(Arc::downgrade(&node));
            }
        }
        self.len += 1;
        self
    }

    /// Add a new node to the list, to tail, contaning the specified 'value' 
    /// pointer as value.
    pub fn add_node_tail(&mut self, value: Option<Arc<T>>) -> &mut Self {
        let node = Arc::new(RwLock::new(
            ListNode { prev: None, next: None, value }
        ));
        if self.len == 0 {
            self.head = Some(node.clone());
            self.tail = Some(node.clone());
        } else {
            if let Some(t) = self.tail.clone() {
                t.write().unwrap().next = Some(node.clone());
                node.write().unwrap().prev = Some(Arc::downgrade(&t));
                self.tail = Some(node.clone());
            }
        }
        self.len += 1;
        self
    }

    /// Remove the specified node from the specified list.
    /// The caller shouldn't hold the reference to the node. 
    /// 
    /// Assumption: the node belongs to the list.
    pub fn del_node(&mut self, node: Option<Arc<RwLock<ListNode<T>>>>) {
        if let Some(node) = node {
            if node.read().unwrap().prev.is_some() {
                if let Some(p) = node.read().unwrap().prev.clone() {
                    if let Some(p) = p.upgrade() {
                        p.write().unwrap().next = node.read().unwrap().next.clone();
                    }
                }
            } else {
                self.head = node.read().unwrap().next.clone();
            }
            if node.read().unwrap().next.is_some() {
                if let Some(n) = node.read().unwrap().next.clone() {
                    n.write().unwrap().prev = node.read().unwrap().prev.clone();
                }
            } else {
                if let Some(p) = node.read().unwrap().prev.clone() {
                    self.tail = p.upgrade();
                }
            }
            self.len -= 1;
        }
    }

    /// Returns a list iterator. After the initialization every
    /// call to `next()` will return the next element of the list.
    pub fn iter(&self, direction: IterDir) -> ListIter<T> {
        let mut iter: ListIter<T> = ListIter { next: None, direction: IterDir::AlStartHead };
        match direction {
            IterDir::AlStartHead => {
                iter.next = self.head.clone();
            },
            IterDir::AlStartTail => {
                iter.next = self.tail.clone();
                iter.direction = IterDir::AlStartTail;
            },
        }
        iter
    }

    /// Reset the iter to the head.
    pub fn rewind(&self, iter: &mut ListIter<T>) {
        iter.next = self.head.clone();
        iter.direction = IterDir::AlStartHead;
    }

    /// Reset the iter to the tail.
    pub fn rewind_tail(&self, iter: &mut ListIter<T>) {
        iter.next = self.tail.clone();
        iter.direction = IterDir::AlStartTail;
    }

    /// Search the list for a node matching a given key.
    pub fn search_key(&self, key: Option<Arc<T>>) -> Option<Arc<RwLock<ListNode<T>>>> {
        if let Some(key) = key {
            let mut iter = self.iter(IterDir::AlStartHead);
        
            while let Some(node) = iter.next() {
                if let Some(n) = node.read().unwrap().value() {
                    if n == key {
                        return Some(node.clone());
                    }
                }
            }
        }
        None
    }

    /// Return the element at the specified zero-based index
    /// where 0 is the head, 1 is the element next to head
    /// and so on. Negative integers are used in order to count
    /// from the tail, -1 is the last element, -2 the penultimante
    /// and so on. If the index is out of range None is returned.
    pub fn index(&self, mut index: isize) -> Option<Arc<RwLock<ListNode<T>>>> {
        let mut _n: Option<Arc<RwLock<ListNode<T>>>> = None;

        if index < 0 {
            index = (-index) - 1;
            _n = self.tail.clone();
            while index > 0 && _n.is_some() {
                if let Some(p) = _n.unwrap().read().unwrap().prev.clone() {
                    if let Some(p) = p.upgrade() {
                        _n = Some(p);
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
                index -= 1;
            }
        } else {
            _n = self.head.clone();
            while index > 0 && _n.is_some() {
                _n = _n.unwrap().read().unwrap().next.clone();
                index -= 1;
            }
        }
        _n
    }

    // TODO: replacing Rust macros for c macros

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn first(&self) -> Option<Arc<RwLock<ListNode<T>>>> {
        self.head.clone()
    }

    pub fn last(&self) -> Option<Arc<RwLock<ListNode<T>>>> {
        self.tail.clone()
    }
}

impl<T: PartialEq + Clone> Clone for List<T> {
    /// Duplicate the whole list.
    fn clone(&self) -> Self {
        let mut clone: List<T> = List::new();

        let mut iter = self.iter(IterDir::AlStartHead);
        while let Some(node) = iter.next() {
            if let Some(n) = node.read().unwrap().value() {
                clone.add_node_tail(Some(Arc::new(n.deref().clone())));
            }
        }
        clone
    }
}

pub struct ListIter<T> {
    next: Option<Arc<RwLock<ListNode<T>>>>,
    direction: IterDir,
}

impl<T> ListIter<T> {
    /// Return the next element of an iterator.
    /// It's valid to remove the currently returned element using 
    /// `del_node()`, but not to remove other elements.
    pub fn next(&mut self) -> Option<Arc<RwLock<ListNode<T>>>> {
        if let Some(c) = self.next.clone() {
            match self.direction {
                IterDir::AlStartHead => {
                    self.next = c.read().unwrap().next.clone();
                    Some(c.clone())
                },
                IterDir::AlStartTail => {
                    if let Some(p) = c.read().unwrap().prev.clone() {
                        if let Some(p) = p.upgrade() {
                            self.next = Some(p);
                            return Some(c.clone());
                        }
                    }
                    return None;
                },
            }
        } else {
            self.next.clone()
        }
    }
}

/// Directions for iterators.
pub enum IterDir {
    AlStartHead,
    AlStartTail,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        let mut list: List<i32> = List::new();
        list.add_node_tail(Some(Arc::new(1)))
            .add_node_tail(Some(Arc::new(2)))
            .add_node_tail(Some(Arc::new(3)))
            .add_node_tail(Some(Arc::new(4)));
        assert_eq!(format!("{:?}", list), "List { head: Some(RwLock { data: ListNode { prev: None, next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), value: Some(3) }, poisoned: false, .. }), value: Some(2) }, poisoned: false, .. }), value: Some(1) }, poisoned: false, .. }), tail: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), len: 4 }");

        let mut iter = list.iter(IterDir::AlStartTail);
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), value: Some(3) }, poisoned: false, .. }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), value: Some(3) }, poisoned: false, .. }), value: Some(2) }, poisoned: false, .. }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: None, next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), value: Some(3) }, poisoned: false, .. }), value: Some(2) }, poisoned: false, .. }), value: Some(1) }, poisoned: false, .. }");
        }
        assert!(iter.next().is_none());

        let mut iter = list.iter(IterDir::AlStartHead);
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: None, next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), value: Some(3) }, poisoned: false, .. }), value: Some(2) }, poisoned: false, .. }), value: Some(1) }, poisoned: false, .. }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), value: Some(3) }, poisoned: false, .. }), value: Some(2) }, poisoned: false, .. }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: Some((Weak)), next: Some(RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }), value: Some(3) }, poisoned: false, .. }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RwLock { data: ListNode { prev: Some((Weak)), next: None, value: Some(4) }, poisoned: false, .. }");
        }
        assert!(iter.next().is_none());

        let mut iter = list.iter(IterDir::AlStartHead);
        while let Some(node) = iter.next() {
            list.del_node(Some(node));
        }
        assert_eq!(list.len(), 0);
    }
}
