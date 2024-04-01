//! A generic doubly linked list implementation.

use std::{cell::RefCell, ops::Deref, rc::{Rc, Weak}};

#[derive(Debug)]
pub struct ListNode<T> {
    prev: Option<Weak<RefCell<ListNode<T>>>>,
    next: Option<Rc<RefCell<ListNode<T>>>>,
    value: Option<Rc<T>>,
}

impl<T> ListNode<T> {
    pub fn prev(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        if let Some(p) = self.prev.clone() {
            if let Some(p) = p.upgrade() {
                return Some(p);
            }
        }
        None
    }

    pub fn next(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        self.next.clone()
    }

    pub fn value(&self) -> Option<Rc<T>> {
        self.value.clone()
    }
}

#[derive(Debug)]
pub struct List<T> {
    head: Option<Rc<RefCell<ListNode<T>>>>,
    tail: Option<Rc<RefCell<ListNode<T>>>>,
    len: usize,
}

impl<T: PartialEq> List<T> {
    /// Create a new list.
    /// 
    /// TODO: AlFreeList()
    pub fn new() -> List<T> {
        List { head: None, tail: None, len: 0 }
    }

    /// Add a new node to the list, to head, contaning the specified 'value' 
    /// pointer as value.
    pub fn add_node_head(&mut self, value: Option<Rc<T>>) -> &mut Self {
        let node = Rc::new(RefCell::new(
            ListNode { prev: None, next: self.head.take(), value }
        ));
        if self.len == 0 {
            self.head = Some(node.clone());
            self.tail = Some(node.clone());
        } else {
            self.head = Some(node.clone());
            if let Some(p) = node.deref().borrow().next.clone() {
                p.deref().borrow_mut().prev = Some(Rc::downgrade(&node));
            }
        }
        self.len += 1;
        self
    }

    /// Add a new node to the list, to tail, contaning the specified 'value' 
    /// pointer as value.
    pub fn add_node_tail(&mut self, value: Option<Rc<T>>) -> &mut Self {
        let node = Rc::new(RefCell::new(
            ListNode { prev: None, next: None, value }
        ));
        if self.len == 0 {
            self.head = Some(node.clone());
            self.tail = Some(node.clone());
        } else {
            if let Some(t) = self.tail.clone() {
                t.deref().borrow_mut().next = Some(node.clone());
                node.deref().borrow_mut().prev = Some(Rc::downgrade(&t));
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
    pub fn del_node(&mut self, node: Option<Rc<RefCell<ListNode<T>>>>) {
        if let Some(node) = node {
            if node.deref().borrow().prev.is_some() {
                if let Some(p) = node.deref().borrow().prev.clone() {
                    if let Some(p) = p.upgrade() {
                        p.deref().borrow_mut().next = node.deref().borrow().next.clone();
                    }
                }
            } else {
                self.head = node.deref().borrow().next.clone();
            }
            if node.deref().borrow().next.is_some() {
                if let Some(n) = node.deref().borrow().next.clone() {
                    n.deref().borrow_mut().prev = node.deref().borrow().prev.clone();
                }
            } else {
                if let Some(p) = node.deref().borrow().prev.clone() {
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
    pub fn search_key(&self, key: Option<Rc<T>>) -> Option<Rc<RefCell<ListNode<T>>>> {
        if let Some(key) = key {
            let mut iter = self.iter(IterDir::AlStartHead);
        
            while let Some(node) = iter.next() {
                if let Some(n) = node.deref().borrow().value() {
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
    pub fn index(&self, mut index: isize) -> Option<Rc<RefCell<ListNode<T>>>> {
        let mut n: Option<Rc<RefCell<ListNode<T>>>> = None;

        if index < 0 {
            index = (-index) - 1;
            n = self.tail.clone();
            while index > 0 && n.is_some() {
                if let Some(p) = n.unwrap().deref().borrow().prev.clone() {
                    if let Some(p) = p.upgrade() {
                        n = Some(p);
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
                index -= 1;
            }
        } else {
            n = self.head.clone();
            while index > 0 && n.is_some() {
                n = n.unwrap().deref().borrow().next.clone();
                index -= 1;
            }
        }
        n
    }

    // TODO: replacing Rust macros for c macros

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn first(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        self.head.clone()
    }

    pub fn last(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        self.tail.clone()
    }
}

impl<T: PartialEq + Clone> Clone for List<T> {
    /// Duplicate the whole list.
    fn clone(&self) -> Self {
        let mut clone: List<T> = List::new();

        let mut iter = self.iter(IterDir::AlStartHead);
        while let Some(node) = iter.next() {
            if let Some(n) = node.deref().borrow().value() {
                clone.add_node_tail(Some(Rc::new(n.deref().clone())));
            }
        }
        clone
    }
}

pub struct ListIter<T> {
    next: Option<Rc<RefCell<ListNode<T>>>>,
    direction: IterDir,
}

impl<T> ListIter<T> {
    /// Return the next element of an iterator.
    /// It's valid to remove the currently returned element using 
    /// `del_node()`, but not to remove other elements.
    pub fn next(&mut self) -> Option<Rc<RefCell<ListNode<T>>>> {
        if let Some(c) = self.next.clone() {
            match self.direction {
                IterDir::AlStartHead => {
                    self.next = c.deref().borrow().next.clone();
                    Some(c.clone())
                },
                IterDir::AlStartTail => {
                    if let Some(p) = c.deref().borrow().prev.clone() {
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
        list.add_node_tail(Some(Rc::new(1)))
            .add_node_tail(Some(Rc::new(2)))
            .add_node_tail(Some(Rc::new(3)))
            .add_node_tail(Some(Rc::new(4)));
        assert_eq!(format!("{:?}", list), "List { head: Some(RefCell { value: ListNode { prev: None, next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), value: Some(3) } }), value: Some(2) } }), value: Some(1) } }), tail: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), len: 4 }");

        let mut iter = list.iter(IterDir::AlStartTail);
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), value: Some(3) } }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), value: Some(3) } }), value: Some(2) } }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: None, next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), value: Some(3) } }), value: Some(2) } }), value: Some(1) } }");
        }
        assert!(iter.next().is_none());

        let mut iter = list.iter(IterDir::AlStartHead);
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: None, next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), value: Some(3) } }), value: Some(2) } }), value: Some(1) } }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), value: Some(3) } }), value: Some(2) } }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: Some((Weak)), next: Some(RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }), value: Some(3) } }");
        }
        if let Some(node) = iter.next() {
            assert_eq!(format!("{:?}", node), "RefCell { value: ListNode { prev: Some((Weak)), next: None, value: Some(4) } }");
        }
        assert!(iter.next().is_none());

        let mut iter = list.iter(IterDir::AlStartHead);
        while let Some(node) = iter.next() {
            list.del_node(Some(node));
        }
        assert_eq!(list.len(), 0);
    }
}
