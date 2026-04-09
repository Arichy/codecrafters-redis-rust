use std::{
    collections::{
        btree_map::{BTreeMap, Entry},
        BTreeSet, HashMap, VecDeque,
    },
    ops::{Range, RangeBounds},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Ok};
use bytes::{Buf, Bytes};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub mod zset;

use crate::{
    message::{Array, BulkString, Message, SimpleError},
    rdb::zset::ZSet,
};

#[derive(Debug, Clone)]
pub struct RDB {
    pub header: String,
    pub metadata: BTreeMap<String, String>,
    pub databases: Vec<Database>,
    pub eof: [u8; 8],
}

fn split_at_two(byte: u8) -> (u8, u8) {
    ((byte & 0b1100_0000) >> 6, (byte & 0b0011_1111))
}

pub fn parse_encoded_size(bytes: &mut Bytes) -> usize {
    let first_byte = bytes[0];
    let (first_two, rest_six) = split_at_two(bytes[0]);
    match first_two {
        0b00 => {
            bytes.advance(1);
            rest_six as usize
        }
        0b01 => {
            bytes.advance(1);
            let size = u16::from_be_bytes([rest_six, bytes[0]]) as usize;
            bytes.advance(1);
            size
        }
        0b10 => {
            bytes.advance(1);
            let size = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
            bytes.advance(4);

            size
        }
        _ => {
            unreachable!()
        }
    }
}

pub fn parse_encoded_string(bytes: &mut Bytes) -> anyhow::Result<String> {
    let first_byte = bytes[0];
    let (first_two, _) = split_at_two(bytes[0]);
    let string = match first_two {
        0x00 | 0x01 | 0x10 => {
            let size = parse_encoded_size(bytes);
            let string =
                String::from_utf8(bytes.slice(..size).to_vec()).context("Invalid UTF-8 string.")?;

            bytes.advance(string.len());

            string
        }
        0b11 => {
            bytes.advance(1);
            match first_byte {
                0xc0 => {
                    let string = u8::from_le_bytes([bytes[0]]).to_string();
                    bytes.advance(1);
                    string
                }
                0xc1 => {
                    let string = u16::from_le_bytes([bytes[0], bytes[1]]).to_string();
                    bytes.advance(2);
                    string
                }
                0xc2 => {
                    let string =
                        u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string();
                    bytes.advance(4);
                    string
                }
                _ => {
                    unreachable!()
                }
            }
        }
        _ => {
            unreachable!()
        }
    };

    Ok(string)
}

impl RDB {
    pub fn new() -> Self {
        Self {
            header: Default::default(),
            metadata: Default::default(),
            databases: vec![Database {
                index: 0,
                map: BTreeMap::new(),
            }],
            eof: Default::default(),
        }
    }

    pub fn from_bytes(bytes: &mut Bytes) -> anyhow::Result<Self> {
        // parse header section
        let magic_string = String::from_utf8(bytes.slice(..5).to_vec()).context("Magic string")?;
        bytes.advance(5);
        let version = String::from_utf8(bytes.slice(..4).to_vec()).context("Header section")?;
        bytes.advance(4);
        let header = format!("{magic_string}{version}");

        // parse metadata section
        let mut metadata = BTreeMap::new();
        while bytes[0] == 0xfa {
            bytes.advance(1);
            let key = parse_encoded_string(bytes)?;
            let value = parse_encoded_string(bytes)?;
            metadata.insert(key, value);
        }

        // println!("finished metadata: {:?} ,{:?}", metadata, bytes.as_ref());

        // parse database section
        let mut databases = vec![];
        while bytes[0] == 0xfe {
            bytes.advance(1);
            let database_index = parse_encoded_size(bytes);
            assert_eq!(bytes[0], 0xfb);
            bytes.advance(1);
            let hashtable_size = parse_encoded_size(bytes);
            let hashtable_with_expiry_size = parse_encoded_size(bytes);
            let mut database_map = BTreeMap::new();

            while database_map.len() < hashtable_size {
                let expiry = match bytes[0] {
                    0xfd => {
                        bytes.advance(1);
                        let expire_ts_in_seconds =
                            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                        bytes.advance(4);

                        Some((expire_ts_in_seconds as u64) * 1000)
                    }
                    0xfc => {
                        bytes.advance(1);
                        let expire_ts_in_milliseconds = u64::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7],
                        ]);
                        bytes.advance(8);

                        Some(expire_ts_in_milliseconds)
                    }
                    _ => None,
                };

                let value_type = match bytes[0] {
                    0b00 => "string",
                    _ => {
                        todo!()
                    }
                };
                bytes.advance(1);

                let k = parse_encoded_string(bytes).context("Invalid key.")?;
                let v = parse_encoded_string(bytes).context("Invalid value.")?;

                let v = match value_type {
                    "string" => Value {
                        expiry,
                        value: ValueType::StringValue(StringValue { string: v }),
                    },
                    _ => {
                        todo!()
                    }
                };
                database_map.insert(k, v);
            }

            databases.push(Database {
                index: database_index,
                map: database_map,
            });
        }

        // parse EOF section
        assert_eq!(bytes[0], 0xff);
        bytes.advance(1);
        let eof = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        bytes.advance(8);

        Ok(Self {
            header,
            metadata,
            databases,
            eof,
        })
    }

    pub fn to_bytes(&self) -> Bytes {
        // empty file for test
        let file_str = "52 45 44 49 53 30 30 31 31 fa 09 72 65 64 69 73
2d 76 65 72 05 37 2e 32 2e 30 fa 0a 72 65 64 69
73 2d 62 69 74 73 c0 40 fa 05 63 74 69 6d 65 c2
6d 08 bc 65 fa 08 75 73 65 64 2d 6d 65 6d c2 b0
c4 10 00 fa 08 61 6f 66 2d 62 61 73 65 c0 00 ff
f0 6e 3b fe c0 ff 5a a2";
        let file_bytes: Vec<_> = file_str
            .replace("\n", " ")
            .split(" ")
            .map(|s| u8::from_str_radix(s, 16).unwrap())
            .collect();

        Bytes::from(file_bytes)
    }

    pub fn get_db_mut(&mut self, index: usize) -> anyhow::Result<&mut Database> {
        self.databases
            .iter_mut()
            .find(|db| db.index == index)
            .context(format!("db {index} does not exist."))
    }

    pub fn get_db(&self, index: usize) -> anyhow::Result<&Database> {
        self.databases
            .iter()
            .find(|db| db.index == index)
            .context(format!("db {index} does not exist."))
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    pub index: usize,
    pub map: BTreeMap<String, Value>,
}

impl Database {
    pub fn validate_key(&self, key: &str) -> bool {
        self.map.get(key).is_none()
    }

    pub fn validate_stream_id(&self, key: &str, id: impl Into<StreamId>) -> bool {
        let id: StreamId = id.into();
        match self.map.get(key) {
            Some(Value {
                expiry: _,
                value: ValueType::StreamValue(StreamValue { stream }),
            }) => {
                if let Some(last) = stream.last() {
                    let last_id: StreamId = last.get("id").unwrap().parse().unwrap();
                    id > last_id
                } else {
                    id > StreamId { ts: 0, seq: 0 }
                }
            }
            _ => id > StreamId { ts: 0, seq: 0 },
        }
    }

    pub fn generate_stream_id(&self, key: &str, ts: Option<i64>) -> anyhow::Result<StreamId> {
        let ts = ts
            .or_else(|| Some(chrono::Utc::now().timestamp_millis()))
            .unwrap();

        match self.map.get(key) {
            Some(Value {
                expiry: _,
                value: ValueType::StreamValue(StreamValue { stream }),
            }) => {
                if let Some(last) = stream.last() {
                    let last_id: StreamId = last.get("id").unwrap().parse().unwrap();
                    if ts == last_id.ts {
                        Ok(StreamId {
                            ts,
                            seq: last_id.seq + 1,
                        })
                    } else if ts < last_id.ts {
                        Err(anyhow::anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
                    } else {
                        Ok(StreamId { ts, seq: 0 })
                    }
                } else {
                    if ts < 0 {
                        Err(anyhow::anyhow!(format!("ts must be greater than 0",)))
                    } else {
                        Ok({
                            StreamId {
                                ts,
                                seq: if ts == 0 { 1 } else { 0 },
                            }
                        })
                    }
                }
            }
            _ => {
                if ts < 0 {
                    Err(anyhow::anyhow!(format!("ts must be greater than 0",)))
                } else {
                    Ok({
                        StreamId {
                            ts,
                            seq: if ts == 0 { 1 } else { 0 },
                        }
                    })
                }
            }
        }
    }

    pub fn get_sorted_set(&mut self, key: String) -> anyhow::Result<Option<&mut SortedSet>> {
        match self.map.entry(key) {
            Entry::Occupied(mut entry) => {
                if entry.get().is_expired() {
                    entry.remove();
                    Ok(None)
                } else {
                    let zset_value = entry.into_mut();
                    let ValueType::SortedSet(sorted_set) = &mut zset_value.value else {
                        return Err(SimpleError {
                            string: "Wrong kind of value".to_string(),
                        }
                        .into());
                    };

                    Ok(Some(sorted_set))
                }
            }
            Entry::Vacant(_) => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Value {
    pub expiry: Option<u64>,
    pub value: ValueType,
}

impl Value {
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            let current_ts = chrono::Utc::now().timestamp_millis() as u64;

            expiry < current_ts
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub enum ValueType {
    StringValue(StringValue),
    StreamValue(StreamValue),
    ListValue(ListValue),
    SortedSet(SortedSet),
}

#[derive(Debug, Clone)]
pub struct StringValue {
    pub string: String,
}

#[derive(Debug, Clone)]

pub struct StreamValue {
    pub stream: Vec<BTreeMap<String, String>>,
}

impl StreamValue {
    pub fn get_message_by_record(record: &BTreeMap<String, String>) -> Message {
        let mut entries = vec![];
        let mut id = None;

        record.iter().for_each(|(k, v)| {
            if k.as_str() == "id" {
                id = Some(Message::BulkString(BulkString {
                    length: v.len() as isize,
                    string: v.to_string(),
                }));
            } else {
                entries.push(Message::BulkString(BulkString {
                    length: k.len() as isize,
                    string: k.to_string(),
                }));
                entries.push(Message::BulkString(BulkString {
                    length: v.len() as isize,
                    string: v.to_string(),
                }));
            }
        });

        Message::Array(Array {
            items: vec![id.unwrap(), Message::Array(Array { items: entries })],
        })
    }

    pub fn get_by_range(&self, range: impl RangeBounds<usize>) -> Vec<Message> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => self.stream.len(),
        };
        let slice = &self.stream[start..end];

        slice
            .iter()
            .map(|record| Self::get_message_by_record(record))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub ts: i64,
    pub seq: usize,
}

impl StreamId {
    pub fn parse_range_str(input: impl AsRef<str>) -> anyhow::Result<(i64, Option<usize>)> {
        let input = input.as_ref();

        let splits: Vec<_> = input.split("-").collect();

        let ts: i64 = splits[0].parse().context("Invalid ts part.")?;

        if splits.len() < 2 {
            return Ok((ts, None));
        }

        let seq = splits[1];
        if seq == "*" {
            return Ok((ts, None));
        }

        let seq: usize = seq.parse().context("Invalid seq part.")?;
        return Ok((ts, Some(seq)));
    }

    // pub fn parse_add_str(input: impl AsRef<str>) -> anyhow::Result<(i64, Option<usize>)> {

    // }
}

impl Into<String> for StreamId {
    fn into(self) -> String {
        format!("{}-{}", self.ts.to_string(), self.seq.to_string())
    }
}

impl FromStr for StreamId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let splits: Vec<_> = s.split("-").collect();
        if splits.len() != 2 {
            return Err(anyhow::Error::msg("Invalid stream id string."));
        }

        let ts = splits[0].parse().context("ts part must be a u64 string")?;
        let seq = splits[1]
            .parse()
            .context("ts part must be a usize string")?;

        Ok(Self { ts, seq })
    }
}

#[derive(Debug, Clone)]
pub struct ListValue {
    pub list: VecDeque<String>,
}

#[derive(Debug, Clone)]
pub struct SortedSet {
    pub zset: ZSet,
}
