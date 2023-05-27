use std::ops::Range;

use crate::SLOTS_LENGTH;

use self::{
    common::{slot::State, Slot, SlotsMapping},
    proxy_service::SlotRange,
    redis_service::Entry,
};

pub mod common;
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

impl SlotsMapping {
    pub fn init() -> Self {
        Self {
            slots: vec![Slot::init(); SLOTS_LENGTH],
        }
    }
}

impl Slot {
    fn init() -> Self {
        Self {
            id: Default::default(),
            state: State::Unallocated.into(),
        }
    }
}
