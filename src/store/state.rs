use dashmap::DashMap;

use crate::protobuf::redis_service::Entry;

pub struct State {
    entries: DashMap<String, Entry>,
}

impl State {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }
}

impl State {
    /// Get the entry associated with the key.
    pub fn get(&self, key: impl AsRef<str>) -> Option<Entry> {
        self.entries
            .get(key.as_ref())
            .map(|entry| entry.value().clone())
    }

    /// Returns old entry if there was one.
    pub fn set(&self, key: String, entry: Entry) -> Option<Entry> {
        self.entries.insert(key, entry)
    }

    /// Remove the entry associated with the key.
    pub fn remove(&self, key: impl AsRef<str>) -> Option<Entry> {
        self.entries.remove(key.as_ref()).map(|(_, entry)| entry)
    }
}
