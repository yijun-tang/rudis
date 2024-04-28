use std::{cmp::Ordering, collections::HashMap, sync::{Arc, RwLock, Weak}};
use rand::Rng;
use super::obj::{compare_string_objects, RedisObject};

const SKIPLIST_MAXLEVEL: usize = 32;

#[derive(Clone)]
pub struct SkipList {
    header: Arc<RwLock<SkipListNode>>,
    tail: Option<Arc<RwLock<SkipListNode>>>,
    length: usize,
    level: usize,
}

impl SkipList {
    pub fn new() -> SkipList {
        SkipList {
            header: Arc::new(RwLock::new(SkipListNode::new(SKIPLIST_MAXLEVEL, 0f64, None))),
            tail: None,
            length: 0,
            level: 1,
        }
    }

    pub fn insert(&mut self, score: f64, obj: Arc<RedisObject>) {
        let mut update: Vec<Option<Arc<RwLock<SkipListNode>>>> = Vec::with_capacity(SKIPLIST_MAXLEVEL);
        for _ in 0..SKIPLIST_MAXLEVEL { update.push(None); }
        let mut rank: Vec<usize> = Vec::with_capacity(SKIPLIST_MAXLEVEL);
        for _ in 0..SKIPLIST_MAXLEVEL { rank.push(0); }

        let mut x = self.header.clone();
        for i in (0..self.level).rev() {
            // store rank that is crossed to reach the insert position
            if i == self.level - 1 {
                rank[i] = 0;
            } else {
                rank[i] = rank[i + 1];
            }

            // TODO: so ugly
            // Step forward on level i
            while x.read().unwrap().forward[i].is_some() &&
                (x.read().unwrap().forward[i].as_ref().unwrap().read().unwrap().score < score ||
                    (x.read().unwrap().forward[i].as_ref().unwrap().read().unwrap().score == score &&
                    compare_string_objects(x.read().unwrap().forward[i].as_ref().unwrap().read().unwrap().obj.as_ref().unwrap().as_ref(), obj.as_ref()) == Ordering::Less)) {
                
                if i > 0 {
                    rank[i] += x.read().unwrap().forward[i].as_ref().unwrap().read().unwrap().span[i - 1];
                } else {
                    rank[i] += 1;
                }
                let n = x.read().unwrap().forward[i].as_ref().unwrap().clone();
                x = n;
            }
            update[i] = Some(x.clone());
        }

        // we assume the key is not already inside, since we allow duplicated
        // scores, and the re-insertion of score and redis object should never
        // happpen since the caller of zslInsert() should test in the hash table
        // if the element is already inside or not.
        let level = self.randome_level();
        if level > self.level {         // higher new levels initialization
            for i in self.level..level {
                rank[i] = 0;
                update[i] = Some(self.header.clone());
                update[i].as_mut().unwrap().write().unwrap().span[i - 1] = self.length;
            }
            self.level = level;
        }

        x = Arc::new(RwLock::new(SkipListNode::new(level as usize, score, Some(obj))));
        for i in 0..level {
            x.write().unwrap().forward[i] = update[i].as_ref().unwrap().read().unwrap().forward[i].clone();
            update[i].as_mut().unwrap().write().unwrap().forward[i] = Some(x.clone());

            // update span covered by update[i] as x is inserted here
            if i > 0 {
                x.write().unwrap().span[i - 1] = update[i].as_ref().unwrap().read().unwrap().span[i - 1] - (rank[0] - rank[i]);
                update[i].as_mut().unwrap().write().unwrap().span[i - 1] = rank[0] - rank[i] + 1;
            }
        }

        // increment span for untouched levels
        for i in level..self.level {
            update[i].as_mut().unwrap().write().unwrap().span[i - 1] += 1;
        }

        if update[0].as_ref().unwrap().read().unwrap().obj.is_none() {
            x.write().unwrap().backward = None;
        } else {
            x.write().unwrap().backward = Some(Arc::downgrade(update[0].as_ref().unwrap()));
        }
        // update the backward pointers
        if x.read().unwrap().forward[0].is_some() {
            x.read().unwrap().forward[0].as_ref().unwrap().write().unwrap().backward = Some(Arc::downgrade(&x));
        } else {
            self.tail = Some(x.clone());
        }
        self.length += 1;
    }

    pub fn delete(&mut self, score: f64, obj: Arc<RedisObject>) -> bool {
        let mut update: Vec<Option<Arc<RwLock<SkipListNode>>>> = Vec::with_capacity(SKIPLIST_MAXLEVEL);
        for _ in 0..SKIPLIST_MAXLEVEL { update.push(None); }

        let mut x = self.header.clone();
        for i in (0..self.level).rev() {
            while x.read().unwrap().forward[i].is_some() &&
                (x.read().unwrap().forward[i].as_ref().unwrap().read().unwrap().score < score ||
                    (x.read().unwrap().forward[i].as_ref().unwrap().read().unwrap().score == score &&
                    compare_string_objects(x.read().unwrap().forward[i].as_ref().unwrap().read().unwrap().obj.as_ref().unwrap().as_ref(), obj.as_ref()) == Ordering::Less)) {
                
                let n = x.read().unwrap().forward[i].as_ref().unwrap().clone();
                x = n;
            }
            update[i] = Some(x.clone());
        }

        let next_r = {
            let x_r = x.read().unwrap();
            x_r.forward[0].clone()
        };
        match next_r {
            Some(next) => {
                if next.read().unwrap().score == score && compare_string_objects(next.read().unwrap().obj.as_ref().unwrap().as_ref(), obj.as_ref()) == Ordering::Equal {
                    self.delete_node(next, &mut update);
                    return true;
                }
            },
            None => {},
        }
        false
    }
    fn delete_node(&mut self, x: Arc<RwLock<SkipListNode>>, update: &mut Vec<Option<Arc<RwLock<SkipListNode>>>>) {
        for i in 0..self.level {
            let next_n = update[i].as_ref().unwrap().read().unwrap().forward[i].clone();
            match next_n {
                Some(next) => {
                    if next.read().unwrap().score == x.read().unwrap().score && 
                        compare_string_objects(next.read().unwrap().obj.as_ref().unwrap().as_ref(), x.read().unwrap().obj.as_ref().unwrap().as_ref()) == Ordering::Equal {
                        
                        if i > 0 {
                            update[i].as_mut().unwrap().write().unwrap().span[i - 1] += x.read().unwrap().span[i - 1] - 1;
                        }
                        update[i].as_mut().unwrap().write().unwrap().forward[i] = x.read().unwrap().forward[i].clone();
                        continue;
                    }
                },
                None => {},
            }

            update[i].as_mut().unwrap().write().unwrap().span[i - 1] -= 1;
        }

        // update the backward pointers
        if x.read().unwrap().forward[0].is_some() {
            x.read().unwrap().forward[0].as_ref().unwrap().write().unwrap().backward = x.read().unwrap().backward.clone();
        } else {
            match x.read().unwrap().backward.as_ref() {
                Some(pre) => {
                    self.tail = pre.upgrade();
                },
                None => { self.tail = None; },
            }
        }

        while self.level > 1 && self.header.read().unwrap().forward[self.level - 1].is_none() {
            self.level -= 1;
        }
        self.length -= 1;
    }

    /// Finds an element by its rank. The rank argument needs to be 1-based.
    pub fn get_ele_by_rank(&self, rank: usize) -> Option<Arc<RwLock<SkipListNode>>> {
        let mut traversed = 0usize;
        let mut x = self.header.clone();

        for i in (0..self.level).rev() {
            while x.read().unwrap().forward[i].is_some() {
                let steps = match i > 0 {
                    true => x.read().unwrap().span[i - 1],
                    false => 1,
                };

                if traversed + steps > rank { break; }
                traversed += steps;
                let n = x.read().unwrap().forward[i].as_ref().unwrap().clone();
                x = n;
            }

            if traversed == rank {
                return Some(x);
            }
        }
        None
    }

    /// Find the first node having a score equal or greater than the specified one.
    /// Returns None if there is no match.
    pub fn first_with_score(&self, score: f64) -> Option<Arc<RwLock<SkipListNode>>> {
        let mut x = self.header.clone();
        for i in (0..self.level).rev() {
            while x.read().unwrap().forward[i].is_some() {
                let next = x.read().unwrap().forward[i].clone().unwrap();
                if next.read().unwrap().score < score {
                    x = next;
                    continue;
                }
                break;
            }
        }
        return x.read().unwrap().forward[0].clone();
    }

    /// Delete all the elements with score between min and max from the skiplist.
    /// Min and mx are inclusive, so a score >= min || score <= max is deleted.
    /// Note that this function takes the reference to the hash table view of the
    /// sorted set, in order to remove the elements from the hash table too.
    pub fn delete_range_by_score(&mut self, min: f64, max: f64, dict: &mut HashMap<RedisObject, f64>) -> usize {
        let mut update: Vec<Option<Arc<RwLock<SkipListNode>>>> = Vec::with_capacity(SKIPLIST_MAXLEVEL);
        for _ in 0..SKIPLIST_MAXLEVEL { update.push(None); }

        let mut x = self.header.clone();
        for i in (0..self.level).rev() {
            while x.read().unwrap().forward[i].is_some() {
                let next = x.read().unwrap().forward[i].clone();
                if next.clone().unwrap().read().unwrap().score < min {
                    x = next.unwrap();
                    continue;
                }
                break;
            }
            update[i] = Some(x.clone());
        }

        let mut x = x.read().unwrap().forward[0].clone();
        let mut removed = 0;
        while x.is_some() {
            let node_r = x.clone().unwrap();
            if node_r.read().unwrap().score > max {
                break;
            }

            let next = node_r.read().unwrap().forward(0);
            self.delete_node(node_r.clone(), &mut update);
            dict.remove(node_r.read().unwrap().obj.clone().unwrap().as_ref());
            removed += 1;
            x = next;
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.length
    }
    pub fn tail(&self) -> Option<Arc<RwLock<SkipListNode>>> {
        self.tail.clone()
    }
    pub fn header(&self, level: usize) -> Option<Arc<RwLock<SkipListNode>>>  {
        self.header.read().unwrap().forward[level].clone()
    }

    /// The probability of stepping upward is 1/4.
    fn randome_level(&self) -> usize {
        let mut rand_gen = rand::thread_rng();
        let mut level = 1;
        while rand_gen.gen_ratio(1, 4) {
            level += 1;
        }
        level
    }
}

pub struct SkipListNode {
    forward: Vec<Option<Arc<RwLock<SkipListNode>>>>,
    backward: Option<Weak<RwLock<SkipListNode>>>,
    span: Vec<usize>,
    score: f64,
    obj: Option<Arc<RedisObject>>,
}

impl SkipListNode {
    pub fn new(level: usize, score: f64, obj: Option<Arc<RedisObject>>) -> SkipListNode {
        let mut forward: Vec<Option<Arc<RwLock<SkipListNode>>>> = Vec::with_capacity(level);
        for _ in 0..level { forward.push(None); }
        let mut span: Vec<usize> = Vec::with_capacity(level - 1);
        for _ in 0..(level - 1) { span.push(0); }
        SkipListNode {
            forward,
            backward: None,
            span,
            score: score,
            obj,
        }
    }

    pub fn obj(&self) -> Option<Arc<RedisObject>> {
        self.obj.clone()
    }

    pub fn score(&self) -> f64 {
        self.score
    }

    pub fn backward(&self) -> Option<Arc<RwLock<SkipListNode>>>  {
        match self.backward.clone() {
            Some(pre) => pre.upgrade(),
            None => None,
        }
    }

    pub fn forward(&self, level: usize) -> Option<Arc<RwLock<SkipListNode>>> {
        self.forward[level].clone()
    }
}
