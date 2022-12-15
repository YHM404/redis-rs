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

#[cfg(test)]
mod test {
    use crate::{protobuf::redis_service::Entry, store::state::State};

    #[test]
    fn test_set_entry() {
        let state = State::new();

        let old_entry = state.set("key_1".to_string(), Entry::new(vec![0, 1, 2]));
        assert!(old_entry.is_none());

        let old_entry = state.set("key_2".to_string(), Entry::new(vec![3, 4, 5]));
        assert!(old_entry.is_none());

        let old_entry = state.set("key_1".to_string(), Entry::new(vec![]));
        assert_eq!(old_entry, Some(Entry::new(vec![0, 1, 2])));

        let old_entry = state.set("key_2".to_string(), Entry::new(vec![]));
        assert_eq!(old_entry, Some(Entry::new(vec![3, 4, 5])));
    }

    #[test]
    fn test_get_entry() {
        let state = State::new();

        state.set("key_1".to_string(), Entry::new(vec![0, 1, 2]));
        state.set("key_2".to_string(), Entry::new(vec![3, 4, 5]));

        let entry = state.get("key_1");
        assert_eq!(entry, Some(Entry::new(vec![0, 1, 2])));

        let entry = state.get("key_2");
        assert_eq!(entry, Some(Entry::new(vec![3, 4, 5])));

        let entry = state.get("key_3");
        assert!(entry.is_none());
    }

    #[test]
    fn test_remove_entry() {
        let state = State::new();

        state.set("key_1".to_string(), Entry::new(vec![0, 1, 2]));

        let old_entry = state.remove("key_1");
        assert_eq!(old_entry, Some(Entry::new(vec![0, 1, 2])));

        let old_entry = state.remove("key_1");
        assert!(old_entry.is_none());

        let old_entry = state.remove("key_2");
        assert!(old_entry.is_none());
    }
}
