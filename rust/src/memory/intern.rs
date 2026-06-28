use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::RwLock;

use boxcar::Vec as BoxcarVec;

pub struct InternTable {
    fwd: RwLock<HashMap<String, u16>>,
    rev: BoxcarVec<String>,
    next: AtomicU16,
}

impl InternTable {
    pub fn new() -> Self {
        let rev = BoxcarVec::new();
        InternTable {
            fwd: RwLock::new(HashMap::new()),
            rev,
            next: AtomicU16::new(0),
        }
    }

    pub fn prefill(&self, strings: &[&str]) {
        let mut fwd = self.fwd.write().unwrap();
        for &s in strings {
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            fwd.insert(s.to_string(), id);
            self.rev.push(s.to_string());
        }
    }

    pub fn intern(&self, s: &str) -> u16 {
        {
            let fwd = self.fwd.read().unwrap();
            if let Some(&id) = fwd.get(s) {
                return id;
            }
        }
        let mut fwd = self.fwd.write().unwrap();
        if let Some(&id) = fwd.get(s) {
            return id;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        fwd.insert(s.to_string(), id);
        self.rev.push(s.to_string());
        id
    }

    pub fn lookup(&self, id: u16) -> Option<&str> {
        self.rev.get(id as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> u16 {
        self.next.load(Ordering::Relaxed)
    }
}

impl Default for InternTable {
    fn default() -> Self {
        Self::new()
    }
}
