use std::{cmp::Ordering, collections::{hash_set::{Intersection, Iter}, HashMap, HashSet, LinkedList}, hash::{Hash, RandomState}, ops::Deref, sync::{Arc, RwLock}};
use once_cell::sync::Lazy;
use super::skiplist::SkipList;


/// 
/// Redis Objects.
///  


/// Our shared "common" objects
/// 
pub static CRLF: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("\r\n".to_string()) }))
});
pub static OK: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("+OK\r\n".to_string()) }))
});
pub static ERR: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("-ERR\r\n".to_string()) }))
});
pub static EMPTY_BULK: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("$0\r\n\r\n".to_string()) }))
});
pub static C_ZERO: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String(":0\r\n".to_string()) }))
});
pub static C_ONE: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String(":1\r\n".to_string()) }))
});
pub static NULL_BULK: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("$-1\r\n".to_string()) }))
});
pub static NULL_MULTI_BULK: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("*-1\r\n".to_string()) }))
});
pub static EMPTY_MULTI_BULK: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("*0\r\n".to_string()) }))
});
pub static PONG: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("+PONG\r\n".to_string()) }))
});
pub static QUEUED: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("+QUEUED\r\n".to_string()) }))
});
pub static WRONG_TYPE_ERR: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("-ERR Operation against a key holding the wrong kind of value\r\n".to_string()) }))
});
pub static NO_KEY_ERR: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("-ERR no such key\r\n".to_string()) }))
});
pub static SYNTAX_ERR: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("-ERR syntax error\r\n".to_string()) }))
});
pub static SAME_OBJECT_ERR: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("-ERR source and destination objects are the same\r\n".to_string()) }))
});
pub static OUT_OF_RANGE_ERR: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("-ERR index out of range\r\n".to_string()) }))
});
pub static SPACE: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String(" ".to_string()) }))
});
pub static COLON: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String(":".to_string()) }))
});
pub static PLUS: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("+".to_string()) }))
});
pub static SELECT0: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 0\r\n".to_string()) }))
});
pub static SELECT1: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 1\r\n".to_string()) }))
});
pub static SELECT2: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 2\r\n".to_string()) }))
});
pub static SELECT3: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 3\r\n".to_string()) }))
});
pub static SELECT4: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 4\r\n".to_string()) }))
});
pub static SELECT5: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 5\r\n".to_string()) }))
});
pub static SELECT6: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 6\r\n".to_string()) }))
});
pub static SELECT7: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 7\r\n".to_string()) }))
});
pub static SELECT8: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 8\r\n".to_string()) }))
});
pub static SELECT9: Lazy<Arc<RwLock<RedisObject>>> = Lazy::new(|| {
    Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::String("select 9\r\n".to_string()) }))
});


/// Object types
#[derive(Clone, Eq)]
pub enum RedisObject {
    String {
        ptr: StringStorageType,
    },
    List {
        l: ListStorageType,
    },
    Set {
        s: SetStorageType,
    },
    ZSet {
        zs: ZSetStorageType,
    },
}
impl RedisObject {
    /// type code for dumping
    pub fn type_code(&self) -> u8 {
        match self {
            RedisObject::String { ptr: _ } => 0,
            RedisObject::List { l: _ } => 1,
            RedisObject::Set { s: _ } => 2,
            RedisObject::ZSet { zs: _ } => 3,
        }
    }

    pub fn is_string(&self) -> bool {
        match self {
            Self::String { ptr: _ } => true,
            _ => false,
        }
    }

    pub fn as_key(&self) -> &str {
        self.string().unwrap().string().unwrap()
    }

    pub fn string(&self) -> Option<&StringStorageType> {
        match self {
            Self::String {ptr} => { Some(ptr) },
            _ => { None },
        }
    }

    pub fn string_mut(&mut self) -> Option<&mut StringStorageType> {
        match self {
            Self::String { ptr } => { Some(ptr) },
            _ => { None },
        }
    }

    pub fn is_list(&self) -> bool {
        match self {
            Self::List { l: _ } => true,
            _ => false,
        }
    }

    pub fn list(&self) -> Option<&ListStorageType> {
        match self {
            Self::List { l } => { Some(l) },
            _ => { None },
        }
    }

    pub fn list_mut(&mut self) -> Option<&mut ListStorageType> {
        match self {
            Self::List { l } => { Some(l) },
            _ => { None },
        }
    }

    pub fn is_set(&self) -> bool {
        match self {
            Self::Set { s: _ } => true,
            _ => false,
        }
    }

    pub fn set_mut(&mut self) -> Option<&mut SetStorageType> {
        match self {
            Self::Set { s } => { Some(s) },
            _ => { None },
        }
    }

    pub fn set(&self) -> Option<&SetStorageType> {
        match self {
            Self::Set { s } => { Some(s) },
            _ => { None },
        }
    }

    pub fn is_zset(&self) -> bool {
        match self {
            Self::ZSet { zs: _ } => true,
            _ => false,
        }
    }

    pub fn zset_mut(&mut self) -> Option<&mut ZSetStorageType> {
        match self {
            Self::ZSet { zs } => { Some(zs) },
            _ => { None },
        }
    }

    pub fn zset(&self) -> Option<&ZSetStorageType> {
        match self {
            Self::ZSet { zs } => { Some(zs) },
            _ => { None },
        }
    }

    /// Get a decoded version of an encoded object (returned as a new object).
    /// If the object is already raw-encoded just increment the ref count.
    pub fn get_decoded(&self) -> RedisObject {
        match &self {
            Self::String { ptr } => {
                match ptr {
                    StringStorageType::Integer(n) => {
                        RedisObject::String { ptr: StringStorageType::String(n.to_string()) }
                    },
                    _ => { self.clone() },
                }
            },
            _ => { self.clone() },
        }
    }
}
impl PartialEq for RedisObject {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String { ptr: l_ptr }, Self::String { ptr: r_ptr }) => l_ptr == r_ptr,
            _ => false,
        }
    }
}
impl Hash for RedisObject {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::String { ptr } => {
                match ptr {
                    StringStorageType::String(s) => { s.hash(state) },
                    _ => { assert!(false, "impossible code"); }
                }
            },
            _ => { assert!(false, "impossible code"); },
        }
    }
}

#[derive(Clone, Eq)]
pub enum StringStorageType {
    String(String),     // raw string
    Integer(isize),     // encoded as integer
} 
impl StringStorageType {
    pub fn string(&self) -> Option<&str> {
        match self {
            Self::String(s) => { Some(s) },
            _ => { None }
        }
    }

    pub fn set_string(&mut self, str: &str) -> bool {
        match self {
            Self::String(s) => {
                s.clear();
                s.push_str(str);
                true
            },
            _ => { false }
        }
    }
}
impl PartialEq for StringStorageType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::String(l0), Self::Integer(r0)) => l0.eq(&r0.to_string()),
            (Self::Integer(l0), Self::String(r0)) => r0.eq(&l0.to_string()),
        }
    }
}
#[derive(Clone, Eq)]
pub enum ListStorageType {
    LinkedList(LinkedList<RedisObject>),
}
impl ListStorageType {
    pub fn push_front(&mut self, obj: Arc<RwLock<RedisObject>>) {
        match self {
            Self::LinkedList(l) => {
                l.push_front(obj.read().unwrap().clone());
            },
        }
    }
    pub fn push_back(&mut self, obj: Arc<RwLock<RedisObject>>) {
        match self {
            Self::LinkedList(l) => {
                l.push_back(obj.read().unwrap().clone());
            },
        }
    }
    pub fn pop_front(&mut self) -> Option<RedisObject> {
        match self {
            Self::LinkedList(l) => {
                l.pop_front()
            },
        }
    }
    pub fn pop_back(&mut self) -> Option<RedisObject> {
        match self {
            Self::LinkedList(l) => {
                l.pop_back()
            },
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::LinkedList(l) => {
                l.len()
            },
        }
    }
    // TODO: lazy loading
    pub fn range(&self, start: i32, end: i32) -> Vec<RedisObject> {
        match self {
            Self::LinkedList(l) => {
                let mut skip = 0usize;
                if start > 0 { skip = (start - 1) as usize; }
                let size = (end - start + 1) as usize;
                let v: Vec<RedisObject> = l.iter().cloned()
                                                .skip(skip)
                                                .take(size)
                                                .collect();
                v
            },
        }
    }
    pub fn retain_range(&mut self, start: i32, end: i32) {
        match self {
            Self::LinkedList(l) => {
                let len = l.len() - ((start + end) as usize);
                let skip = start as usize;
                let mut v: LinkedList<RedisObject> = l.iter().cloned()
                                                .skip(skip)
                                                .take(len)
                                                .collect();
                l.clear();
                l.append(&mut v);
            },
        }
    }
    pub fn index(&self, index: i32) -> Option<RedisObject> {
        match self {
            Self::LinkedList(l) => {
                l.iter().cloned().nth(index as usize)
            },
        }
    }
    pub fn set(&mut self, index: i32, obj: Arc<RwLock<RedisObject>>) -> bool {
        if 0 <= index && index < self.len() as i32 {
            let mut new_l: LinkedList<RedisObject> = LinkedList::new();
            match self {
                Self::LinkedList(l) => {
                    let mut first_part: LinkedList<RedisObject> = l.iter().cloned()
                                                                        .take(index as usize)
                                                                        .collect();
                    new_l.append(&mut first_part);
                    new_l.push_back(obj.read().unwrap().clone());
                    let mut second_part: LinkedList<RedisObject> = l.iter().cloned()
                                                                        .skip(index as usize + 1)
                                                                        .collect();
                    new_l.append(&mut second_part);
                    l.clear();
                    l.append(&mut new_l);
                },
            }
            return true;
        }
        false
    }
    pub fn remove_head(&mut self, n: i32, obj: Arc<RwLock<RedisObject>>) -> i32 {
        let mut remaining: LinkedList<RedisObject> = LinkedList::new();
        let mut removed = 0;
        match self {
            Self::LinkedList(l) => {
                let mut iter = l.iter();
                while let Some(e) = iter.next() {
                    if eq_string_objects(e, &obj) {
                        removed += 1;
                        if n > 0 && removed == n { break; }
                    } else {
                        remaining.push_back(e.clone());
                    }
                }
                while let Some(e) = iter.next() {
                    remaining.push_back(e.clone());
                }
                l.clear();
                l.append(&mut remaining);
                removed
            },
        }
    }
    pub fn remove_tail(&mut self, n: i32, obj: Arc<RwLock<RedisObject>>) -> i32 {
        let mut remaining: LinkedList<RedisObject> = LinkedList::new();
        let mut removed = 0;
        match self {
            Self::LinkedList(l) => {
                let mut iter = l.iter().rev();
                while let Some(e) = iter.next() {
                    if eq_string_objects(e, &obj) {
                        removed += 1;
                        if n > 0 && removed == n { break; }
                    } else {
                        remaining.push_front(e.clone());
                    }
                }
                while let Some(e) = iter.next() {
                    remaining.push_front(e.clone());
                }
                l.clear();
                l.append(&mut remaining);
                removed
            },
        }
    }
}
impl PartialEq for ListStorageType {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}
#[derive(Clone, Eq)]
pub enum SetStorageType {
    HashSet(HashSet<RedisObject>)
}
impl SetStorageType {
    pub fn insert(&mut self, obj: Arc<RwLock<RedisObject>>) -> bool {
        match self {
            Self::HashSet(s) => {
                s.insert(obj.read().unwrap().clone())
            },
        }
    }

    pub fn remove(&mut self, obj: Arc<RwLock<RedisObject>>) -> bool {
        match self {
            Self::HashSet(s) => {
                s.remove(obj.read().unwrap().deref())
            },
        }
    }

    pub fn get_random_key(&self) -> Option<Arc<RwLock<RedisObject>>> {
        match self {
            Self::HashSet(s) => {
                // TODO: random
                s.iter().nth(0).map(|e| Arc::new(RwLock::new(e.clone())))
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::HashSet(s) => {
                s.len()
            },
        }
    }

    pub fn contains(&self, obj: Arc<RwLock<RedisObject>>) -> bool {
        match self {
            Self::HashSet(s) => {
                s.contains(obj.read().unwrap().deref())
            },
        }
    }
    pub fn contains2(&self, obj: &RedisObject) -> bool {
        match self {
            Self::HashSet(s) => {
                s.contains(obj)
            },
        }
    }

    pub fn inter<'a>(&'a self, other: &'a SetStorageType) -> Intersection<RedisObject, RandomState> {
        match (self, other) {
            (Self::HashSet(l), Self::HashSet(r)) => {
                l.intersection(r)
            },
        }
    }

    pub fn iter(&self) -> Iter<RedisObject> {
        match self {
            Self::HashSet(s) => {
                s.iter()
            },
        }
    }
}
impl PartialEq for SetStorageType {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}
#[derive(Clone)]
pub enum ZSetStorageType {
    SkipList(HashMap<RedisObject, f64>, SkipList)
}
impl ZSetStorageType {
    pub fn dict(&self) -> &HashMap<RedisObject, f64> {
        match self {
            Self::SkipList(d, _) => d
        }
    }

    pub fn dict_mut(&mut self) -> &mut HashMap<RedisObject, f64> {
        match self {
            Self::SkipList(d, _) => d
        }
    }

    pub fn skiplist(&self) -> &SkipList {
        match self {
            Self::SkipList(_, s) => s
        }
    }

    pub fn skiplist_mut(&mut self) -> &mut SkipList {
        match self {
            Self::SkipList(_, s) => s
        }
    }

    pub fn delete_range_by_score(&mut self, min: f64, max: f64) -> usize {
        match self {
            Self::SkipList(d, s) => s.delete_range_by_score(min, max, d)
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::SkipList(_, s) => s.len()
        }
    }
}
impl PartialEq for ZSetStorageType {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}
impl Eq for ZSetStorageType {}

pub fn try_object_sharing(obj: Arc<RwLock<RedisObject>>) {
    todo!()
}

/// Try to encode a string object in order to save space
pub fn try_object_encoding(obj: Arc<RwLock<RedisObject>>) -> Arc<RwLock<RedisObject>> {
    // It's not save to encode shared objects: shared objects can be shared
    // everywhere in the "object space" of Redis. Encoded objects can only
    // appear as "values" (and not, for instance, as keys)
    if Arc::strong_count(&obj) > 1 {
        return obj;
    }

    // Currently we try to encode only strings
    // TODO: redis assert

    match obj.read().unwrap().string() {
        Some(str_storage) => {
            match str_storage {
                StringStorageType::String(s) => {
                    match is_string_representable_as_int(s) {
                        Ok(encoded) => { 
                            return Arc::new(RwLock::new(RedisObject::String { ptr: StringStorageType::Integer(encoded) }));
                        },
                        Err(_) => {},
                    }
                },
                StringStorageType::Integer(_) => {},
            }
        },
        None => {},
    }
    obj
}

/// Check if the string 's' can be represented by a `isize` integer
/// (that is, is a number that fits into `isize` without any other space or
/// character before or after the digits).
/// 
/// If so, the function returns encoded integer of the string s. 
/// Otherwise error string is returned.
fn is_string_representable_as_int(s: &str) -> Result<isize, String> {
    let mut _i = 0isize;
    match s.parse() {
        Ok(v) => { _i = v; },
        Err(e) => { return Err(e.to_string()); },
    }

    // If the number converted back into a string is not identical
    // then it's not possible to encode the string as integer
    if !_i.to_string().eq(s) {
        return Err("failed to encode".to_string());
    }
    Ok(_i)
}

/// Compare two string objects via strcmp() or alike.
/// Note that the objects may be integer-encoded. In such a case we
/// use snprintf() to get a string representation of the numbers on the stack
/// and compare the strings, it's much faster than calling getDecodedObject().
/// 
/// Important note: if objects are not integer encoded, but binary-safe strings,
/// sdscmp() from sds.c will apply memcmp() so this function ca be considered
/// binary safe.
/// 
/// NOTE: USING get_decoded() FOR SIMPLICITY
pub fn eq_string_objects(obj1: &RedisObject, obj2: &Arc<RwLock<RedisObject>>) -> bool {
    match obj1 {
        RedisObject::String { ptr: _ } => {},
        _ => { return false; }
    }
    match obj2.read().unwrap().string() {
        Some(_) => {},
        None => { return false; }
    }
    obj1.get_decoded().string().unwrap().string().unwrap()
        .eq(obj2.read().unwrap().get_decoded().string().unwrap().string().unwrap())
}
// TODO: replicated
pub fn compare_string_objects(obj1: &RedisObject, obj2: &RedisObject) -> Ordering {
    match obj1 {
        RedisObject::String { ptr: _ } => {},
        _ => { assert!(false, "impossible code"); }
    }
    match obj2.string() {
        Some(_) => {},
        None => { assert!(false, "impossible code"); }
    }
    obj1.get_decoded().string().unwrap().string().unwrap()
        .cmp(obj2.get_decoded().string().unwrap().string().unwrap())
}
