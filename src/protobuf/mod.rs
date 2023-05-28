use std::ops::Range;

use anyhow::{anyhow, Context};
use url::Url;

use crate::SLOTS_LENGTH;

use self::{
    common::{endpoint::Scheme, slot::State, Endpoint, Slot, SlotsMapping},
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
        let slots = (0..SLOTS_LENGTH).into_iter().fold(
            Vec::with_capacity(SLOTS_LENGTH),
            |mut slots, id| {
                slots.push(Slot::init(id as u32));
                slots
            },
        );

        SlotsMapping { slots }
    }
}

impl Slot {
    fn init(id: u32) -> Self {
        Self {
            id,
            state: State::Unallocated.into(),
            ..Default::default()
        }
    }
}

impl ToString for Endpoint {
    fn to_string(&self) -> String {
        format!(
            "{}://{}/{}",
            self.scheme().as_str_name().to_lowercase(),
            self.host,
            self.port,
        )
    }
}

impl TryFrom<Url> for Endpoint {
    type Error = anyhow::Error;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            _ => return Err(anyhow!("不支持的scheme")),
        };

        Ok(Endpoint {
            scheme: scheme.into(),
            host: url.host_str().context("host为空")?.to_string(),
            port: url.port().context("port为空")? as u32,
        })
    }
}
