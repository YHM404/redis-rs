use std::ops::Range;

use self::{proxy_service::SlotRange, redis_service::Entry};

pub mod proxy_service;
pub mod redis_service;

impl Entry {
    pub fn new(value: Vec<u8>) -> Self {
        Entry { value }
    }
}

impl From<SlotRange> for Range<usize> {
    fn from(range: SlotRange) -> Self {
        Range {
            start: range.start as usize,
            end: range.end as usize,
        }
    }
}
