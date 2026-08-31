use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub struct CachedObject {
    pub status: u16,
    pub body: Bytes,
}

impl CachedObject {
    pub fn len(&self) -> u64 {
        self.body.len() as u64
    }

    pub fn cacheable(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

#[derive(Clone, Debug)]
pub enum FetchOutcome {
    Object(CachedObject),
    Error,
}

#[derive(Debug)]
struct LruInner {
    map: HashMap<String, CachedObject>,
    order: Vec<String>,
    bytes: u64,
    max_bytes: u64,
    max_object_bytes: u64,
    evictions: u64,
}

impl LruInner {
    fn get(&mut self, key: &str) -> Option<CachedObject> {
        if self.map.contains_key(key) {
            self.touch(key);
            self.map.get(key).cloned()
        } else {
            None
        }
    }

    fn touch(&mut self, key: &str) {
        self.order.retain(|k| k != key);
        self.order.push(key.to_string());
    }

    fn evict_one(&mut self) {
        if let Some(oldest) = self.order.first().cloned() {
            self.order.remove(0);
            if let Some(obj) = self.map.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(obj.len());
                self.evictions += 1;
            }
        }
    }

    fn insert(&mut self, key: String, obj: CachedObject) -> u64 {
        if obj.len() > self.max_object_bytes || obj.len() > self.max_bytes || !obj.cacheable() {
            return 0;
        }

        if let Some(prev) = self.map.remove(&key) {
            self.bytes = self.bytes.saturating_sub(prev.len());
            self.order.retain(|k| k != &key);
        }

        let before = self.evictions;
        while self.bytes + obj.len() > self.max_bytes && !self.order.is_empty() {
            self.evict_one();
        }

        if self.bytes + obj.len() > self.max_bytes {
            return self.evictions - before;
        }

        self.bytes += obj.len();
        self.order.push(key.clone());
        self.map.insert(key, obj);
        self.evictions - before
    }

    fn keys(&self) -> Vec<String> {
        self.order.clone()
    }
}

#[derive(Clone)]
pub struct ByteLru {
    inner: Arc<Mutex<LruInner>>,
}

impl ByteLru {
    pub fn new(max_bytes: u64, max_object_bytes: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LruInner {
                map: HashMap::new(),
                order: Vec::new(),
                bytes: 0,
                max_bytes: max_bytes.max(1),
                max_object_bytes: max_object_bytes.max(1),
                evictions: 0,
            })),
        }
    }

    pub fn get(&self, key: &str) -> Option<CachedObject> {
        self.inner.lock().expect("cache").get(key)
    }

    pub fn insert(&self, key: String, obj: CachedObject) -> u64 {
        self.inner.lock().expect("cache").insert(key, obj)
    }

    pub fn bytes(&self) -> u64 {
        self.inner.lock().expect("cache").bytes
    }

    pub fn keys(&self) -> Vec<String> {
        self.inner.lock().expect("cache").keys()
    }

    pub fn max_object_bytes(&self) -> u64 {
        self.inner.lock().expect("cache").max_object_bytes
    }
}

pub struct LeaderGuard {
    key: String,
    tx: broadcast::Sender<FetchOutcome>,
    inflight: Arc<Mutex<HashMap<String, broadcast::Sender<FetchOutcome>>>>,
    completed: AtomicBool,
}

impl LeaderGuard {
    pub fn complete(self, outcome: FetchOutcome) {
        let _ = self.tx.send(outcome);
        self.completed.store(true, Ordering::SeqCst);
        self.inflight.lock().expect("singleflight").remove(&self.key);
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::SeqCst) {
            let _ = self.tx.send(FetchOutcome::Error);
            self.inflight.lock().expect("singleflight").remove(&self.key);
        }
    }
}

pub enum Flight {
    Leader(LeaderGuard),
    Waiter(broadcast::Receiver<FetchOutcome>),
}

#[derive(Clone, Default)]
pub struct SingleFlight {
    inflight: Arc<Mutex<HashMap<String, broadcast::Sender<FetchOutcome>>>>,
}

impl SingleFlight {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn join(&self, key: &str) -> Flight {
        let mut map = self.inflight.lock().expect("singleflight");
        if let Some(tx) = map.get(key) {
            return Flight::Waiter(tx.subscribe());
        }
        let (tx, _rx) = broadcast::channel(1);
        map.insert(key.to_string(), tx.clone());
        Flight::Leader(LeaderGuard {
            key: key.to_string(),
            tx,
            inflight: self.inflight.clone(),
            completed: AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_oldest_when_over_capacity() {
        let lru = ByteLru::new(10, 10);
        lru.insert(
            "a".into(),
            CachedObject {
                status: 200,
                body: Bytes::from(vec![1; 6]),
            },
        );
        lru.insert(
            "b".into(),
            CachedObject {
                status: 200,
                body: Bytes::from(vec![2; 6]),
            },
        );
        assert!(lru.get("a").is_none());
        assert!(lru.get("b").is_some());
        assert_eq!(lru.bytes(), 6);
    }

    #[test]
    fn refuses_objects_over_max() {
        let lru = ByteLru::new(100, 4);
        let evicted = lru.insert(
            "big".into(),
            CachedObject {
                status: 200,
                body: Bytes::from(vec![1; 8]),
            },
        );
        assert_eq!(evicted, 0);
        assert!(lru.get("big").is_none());
    }

    #[test]
    fn does_not_cache_5xx() {
        let lru = ByteLru::new(100, 100);
        lru.insert(
            "err".into(),
            CachedObject {
                status: 500,
                body: Bytes::from_static(b"nope"),
            },
        );
        assert!(lru.get("err").is_none());
    }
}
