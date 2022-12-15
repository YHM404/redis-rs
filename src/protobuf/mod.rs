use self::redis_service::Entry;

pub mod redis_service {
    include!("redis_service.rs");
}

impl Entry {
    pub fn new(value: Vec<u8>) -> Self {
        Entry { value }
    }
}
