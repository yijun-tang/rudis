use std::{borrow::Borrow, collections::LinkedList, sync::{Arc, RwLock}};
use once_cell::sync::Lazy;


/// 
/// Redis Objects.
///  


/// Our shared "common" objects
/// 
pub static CRLF: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("\r\n".to_string()) })
});
pub static OK: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("+OK\r\n".to_string()) })
});
pub static ERR: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("-ERR\r\n".to_string()) })
});
pub static EMPTY_BULK: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("$0\r\n\r\n".to_string()) })
});
pub static C_ZERO: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String(":0\r\n".to_string()) })
});
pub static C_ONE: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String(":1\r\n".to_string()) })
});
pub static NULL_BULK: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("$-1\r\n".to_string()) })
});
pub static NULL_MULTI_BULK: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("*-1\r\n".to_string()) })
});
pub static EMPTY_MULTI_BULK: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("*0\r\n".to_string()) })
});
pub static PONG: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("+PONG\r\n".to_string()) })
});
pub static QUEUED: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("+QUEUED\r\n".to_string()) })
});
pub static WRONG_TYPE_ERR: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("-ERR Operation against a key holding the wrong kind of value\r\n".to_string()) })
});
pub static NO_KEY_ERR: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("-ERR no such key\r\n".to_string()) })
});
pub static SYNTAX_ERR: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("-ERR syntax error\r\n".to_string()) })
});
pub static SAME_OBJECT_ERR: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("-ERR source and destination objects are the same\r\n".to_string()) })
});
pub static OUT_OF_RANGE_ERR: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("-ERR index out of range\r\n".to_string()) })
});
pub static SPACE: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String(" ".to_string()) })
});
pub static COLON: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String(":".to_string()) })
});
pub static PLUS: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("+".to_string()) })
});
pub static SELECT0: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 0\r\n".to_string()) })
});
pub static SELECT1: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 1\r\n".to_string()) })
});
pub static SELECT2: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 2\r\n".to_string()) })
});
pub static SELECT3: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 3\r\n".to_string()) })
});
pub static SELECT4: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 4\r\n".to_string()) })
});
pub static SELECT5: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 5\r\n".to_string()) })
});
pub static SELECT6: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 6\r\n".to_string()) })
});
pub static SELECT7: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 7\r\n".to_string()) })
});
pub static SELECT8: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 8\r\n".to_string()) })
});
pub static SELECT9: Lazy<Arc<RedisObject>> = Lazy::new(|| {
    Arc::new(RedisObject::String { ptr: StringStorageType::String("select 9\r\n".to_string()) })
});


/// Object types
#[derive(Clone)]
pub enum RedisObject {
    String {
        ptr: StringStorageType,
    },
    List {
        l: ListStorageType,
    },
    Set,
    ZSet,
    Hash,
}
impl RedisObject {
    pub fn as_key(&self) -> &str {
        self.string().unwrap().string().unwrap()
    }

    pub fn string(&self) -> Option<&StringStorageType> {
        match self {
            Self::String {ptr} => { Some(ptr) },
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

#[derive(Clone)]
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
}
#[derive(Clone)]
pub enum ListStorageType {
    LinkedList(Arc<RwLock<LinkedList<Arc<RedisObject>>>>),
}
impl ListStorageType {
    pub fn push_front(&self, obj: Arc<RedisObject>) {
        match self {
            Self::LinkedList(l) => {
                l.write().unwrap().push_front(obj);
            },
        }
    }
    pub fn push_back(&self, obj: Arc<RedisObject>) {
        match self {
            Self::LinkedList(l) => {
                l.write().unwrap().push_back(obj);
            },
        }
    }
    pub fn pop_front(&self) -> Option<Arc<RedisObject>> {
        match self {
            Self::LinkedList(l) => {
                l.write().unwrap().pop_front()
            },
        }
    }
    pub fn pop_back(&self) -> Option<Arc<RedisObject>> {
        match self {
            Self::LinkedList(l) => {
                l.write().unwrap().pop_back()
            },
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::LinkedList(l) => {
                l.read().unwrap().len()
            },
        }
    }
    // TODO: lazy loading
    pub fn range(&self, start: i32, end: i32) -> Vec<Arc<RedisObject>> {
        match self {
            Self::LinkedList(l) => {
                let mut skip = 0usize;
                if start > 0 { skip = (start - 1) as usize; }
                let size = (end - start + 1) as usize;
                let v: Vec<Arc<RedisObject>> = l.read().unwrap().iter()
                                                .skip(skip)
                                                .take(size)
                                                .map(|e| e.clone()).collect();
                v
            },
        }
    }
    pub fn retain_range(&self, start: i32, end: i32) {
        match self {
            Self::LinkedList(l) => {
                let len = self.len() - ((start + end) as usize);
                let skip = start as usize;
                let mut v: LinkedList<Arc<RedisObject>> = l.read().unwrap().iter()
                                                .skip(skip)
                                                .take(len)
                                                .map(|e| e.clone()).collect();
                let mut l_w = l.write().unwrap();
                l_w.clear();
                l_w.append(&mut v);
            },
        }
    }
    pub fn index(&self, index: i32) -> Option<Arc<RedisObject>> {
        match self {
            Self::LinkedList(l) => {
                l.read().unwrap().iter().nth(index as usize).map(|e| e.clone())
            },
        }
    }
    pub fn set(&self, index: i32, obj: Arc<RedisObject>) -> bool {
        if 0 <= index && index < self.len() as i32 {
            let mut new_l: LinkedList<Arc<RedisObject>> = LinkedList::new();
            match self {
                Self::LinkedList(l) => {
                    let mut first_part: LinkedList<Arc<RedisObject>> = l.read().unwrap().iter()
                                                                        .take(index as usize)
                                                                        .map(|e| e.clone())
                                                                        .collect();
                    new_l.append(&mut first_part);
                    new_l.push_back(obj);
                    let mut second_part: LinkedList<Arc<RedisObject>> = l.read().unwrap().iter()
                                                                        .skip(index as usize + 1)
                                                                        .map(|e| e.clone())
                                                                        .collect();
                    new_l.append(&mut second_part);
                    let mut l_w = l.write().unwrap();
                    l_w.clear();
                    l_w.append(&mut new_l);
                },
            }
            return true;
        }
        false
    }
    pub fn remove_head(&self, n: i32, obj: Arc<RedisObject>) -> i32 {
        let mut remaining: LinkedList<Arc<RedisObject>> = LinkedList::new();
        let mut removed = 0;
        match self {
            Self::LinkedList(l) => {
                {
                    let l_r = l.read().unwrap();
                    let mut iter = l_r.iter();
                    while let Some(e) = iter.next() {
                        if compare_string_objects(e, &obj) {
                            removed += 1;
                            if n > 0 && removed == n { break; }
                        } else {
                            remaining.push_back(e.clone());
                        }
                    }
                    while let Some(e) = iter.next() {
                        remaining.push_back(e.clone());
                    }
                }
                let mut l_w = l.write().unwrap();
                l_w.clear();
                l_w.append(&mut remaining);
                removed
            },
        }
    }
    pub fn remove_tail(&self, n: i32, obj: Arc<RedisObject>) -> i32 {
        let mut remaining: LinkedList<Arc<RedisObject>> = LinkedList::new();
        let mut removed = 0;
        match self {
            Self::LinkedList(l) => {
                {
                    let l_r = l.read().unwrap();
                    let mut iter = l_r.iter().rev();
                    while let Some(e) = iter.next() {
                        if compare_string_objects(e, &obj) {
                            removed += 1;
                            if n > 0 && removed == n { break; }
                        } else {
                            remaining.push_front(e.clone());
                        }
                    }
                    while let Some(e) = iter.next() {
                        remaining.push_front(e.clone());
                    }
                }
                let mut l_w = l.write().unwrap();
                l_w.clear();
                l_w.append(&mut remaining);
                removed
            },
        }
    }
}

pub fn try_object_sharing(obj: Arc<RedisObject>) {
    todo!()
}

/// Try to encode a string object in order to save space
pub fn try_object_encoding(obj: Arc<RedisObject>) -> Arc<RedisObject> {
    // It's not save to encode shared objects: shared objects can be shared
    // everywhere in the "object space" of Redis. Encoded objects can only
    // appear as "values" (and not, for instance, as keys)
    if Arc::strong_count(&obj) > 1 {
        return obj;
    }

    // Currently we try to encode only strings
    // TODO: redis assert

    match obj.borrow() {
        RedisObject::String { ptr } => {
            match ptr {
                StringStorageType::String(s) => {
                    match is_string_representable_as_int(s) {
                        Ok(encoded) => { 
                            return Arc::new(RedisObject::String { ptr: StringStorageType::Integer(encoded) });
                        },
                        Err(_) => {},
                    }
                },
                StringStorageType::Integer(_) => {},
            }
        },
        _ => {},
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
    let mut i = 0isize;
    match s.parse() {
        Ok(v) => { i = v; },
        Err(e) => { return Err(e.to_string()); },
    }

    // If the number converted back into a string is not identical
    // then it's not possible to encode the string as integer
    if !i.to_string().eq(s) {
        return Err("failed to encode".to_string());
    }
    Ok(i)
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
fn compare_string_objects(obj1: &Arc<RedisObject>, obj2: &Arc<RedisObject>) -> bool {
    match obj1.borrow() {
        RedisObject::String { ptr: _ } => {},
        _ => { return false; }
    }
    match obj2.borrow() {
        RedisObject::String { ptr: _ } => {},
        _ => { return false; }
    }
    if Arc::ptr_eq(obj1, obj2) {
        return true;
    }
    obj1.get_decoded().string().unwrap().string().unwrap()
        .eq(obj2.get_decoded().string().unwrap().string().unwrap())
}
