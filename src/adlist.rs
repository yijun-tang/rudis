//! A generic doubly linked list implementation.

use std::{borrow::{Borrow, BorrowMut}, cell::RefCell, ops::Deref, rc::{Rc, Weak}};

pub struct ListNode<T> {
    prev: Option<Weak<RefCell<ListNode<T>>>>,
    next: Option<Rc<RefCell<ListNode<T>>>>,
    value: Option<Box<T>>,
}

impl<T> ListNode<T> {
    
    pub fn prev(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        todo!()
    }

    pub fn next(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        todo!()
    }

    pub fn value(&self) -> Option<Box<T>> {
        todo!()
    }

}

pub struct List<T, K> {
    head: Option<Rc<RefCell<ListNode<T>>>>,
    tail: Option<Rc<RefCell<ListNode<T>>>>,
    len: usize,
    dup: Option<Box<dyn Fn(Option<Rc<List<T, K>>>) -> Option<List<T, K>>>>,
    free: Option<Box<dyn Fn(Option<Rc<List<T, K>>>)>>,
    match_: Option<Box<dyn Fn(Option<Rc<List<T, K>>>, Option<Rc<K>>) -> usize>>,
}

impl<T, K> List<T, K> {
    /// Create a new list.
    /// 
    /// TODO: AlFreeList()
    pub fn new() -> List<T, K> {
        List { head: None, tail: None, len: 0, dup: None, free: None, match_: None }
    }

    /// Add a new node to the list, to head, contaning the specified 'value' 
    /// pointer as value.
    pub fn add_node_head(&mut self, value: Option<Box<T>>) -> &mut Self {
        let node = Rc::new(RefCell::new(
            ListNode { prev: None, next: self.head.take(), value }
        ));
        if self.tail.is_none() {
            self.head = Some(node.clone());
            self.tail = Some(node.clone());
        } else {
            self.head = Some(node.clone());
            if let Some(p) = node.deref().borrow().next.clone() {
                p.deref().borrow_mut().prev = Some(Rc::downgrade(&node));
            }
        }
        self
    }

    pub fn add_node_tail(&mut self, value: Option<Box<T>>) {
        
        todo!()
    }

    pub fn del_node(&mut self, node: Option<Rc<RefCell<ListNode<T>>>>) {

        todo!()
    }

    pub fn iter(&self, direction: IterDir) -> ListIter<T> {

        todo!()
    }

    // TODO: Clone or Copy for listDup

    pub fn search_key(&self, key: Option<Box<K>>) -> Option<Rc<RefCell<ListNode<T>>>> {

        todo!()
    }

    pub fn index(&self, index: usize) -> Option<Rc<RefCell<ListNode<T>>>> {

        todo!()
    }


    // functions for macros in c

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn first(&self) -> Option<Rc<RefCell<ListNode<T>>>> {

        todo!()
    }

    pub fn last(&self) -> Option<Rc<RefCell<ListNode<T>>>> {
        
        todo!()
    }

    // TODO function pointers SETTER/GETTER macros


}

pub struct ListIter<T> {
    next: Option<Rc<RefCell<ListNode<T>>>>,
    direction: IterDir,
}

impl<T> ListIter<T> {
    pub fn next(&mut self) -> Option<Rc<RefCell<ListNode<T>>>> {

        todo!()
    }

    // TODO: listReleaseIterator() may need addtional steps

    pub fn rewind(&mut self) {

        todo!()
    }

    pub fn rewind_tail(&mut self) {

        todo!()
    }

}

/// Directions for iterators.
pub enum IterDir {
    AL_START_HEAD,
    AL_START_TAIL,
}
