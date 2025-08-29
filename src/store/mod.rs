use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use anyhow::Result;
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::rdb::{Database, RDB, Value, ValueType, StringValue, ListValue, StreamValue, StreamId, SortedSet};
use crate::rdb::zset::ZSet;

pub struct Store {
    rdb: Arc<RwLock<RDB>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            rdb: Arc::new(RwLock::new(RDB::new())),
        }
    }
    
    pub async fn load_from_rdb(&self, content: &[u8]) -> Result<()> {
        let mut bytes = Bytes::from(content.to_vec());
        let mut rdb = RDB::from_bytes(&mut bytes)?;
        
        // Ensure database 0 exists
        if !rdb.databases.iter().any(|db| db.index == 0) {
            rdb.databases.push(crate::rdb::Database {
                index: 0,
                map: std::collections::BTreeMap::new(),
            });
        }
        
        let mut store_rdb = self.rdb.write().await;
        *store_rdb = rdb;
        Ok(())
    }
    
    pub async fn get(&self, db_index: usize, key: &str) -> Result<Option<Value>> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        
        Ok(db.map.get(key).cloned().filter(|v| !v.is_expired()))
    }
    
    pub async fn set(&self, db_index: usize, key: String, value: Value) -> Result<()> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        db.map.insert(key, value);
        Ok(())
    }
    
    pub async fn delete(&self, db_index: usize, key: &str) -> Result<bool> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        Ok(db.map.remove(key).is_some())
    }
    
    pub async fn exists(&self, db_index: usize, key: &str) -> Result<bool> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        Ok(db.map.contains_key(key) && !db.map[key].is_expired())
    }
    
    pub async fn keys(&self, db_index: usize, pattern: &str) -> Result<Vec<String>> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        
        // For now, just return all keys (pattern matching can be added later)
        Ok(db.map.keys()
            .filter(|k| {
                if let Some(v) = db.map.get(*k) {
                    !v.is_expired()
                } else {
                    false
                }
            })
            .cloned()
            .collect())
    }
    
    pub async fn get_string(&self, db_index: usize, key: &str) -> Result<Option<String>> {
        if let Some(value) = self.get(db_index, key).await? {
            match &value.value {
                ValueType::StringValue(StringValue { string }) => Ok(Some(string.clone())),
                _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
            }
        } else {
            Ok(None)
        }
    }
    
    pub async fn set_string(&self, db_index: usize, key: String, value: String, expiry: Option<u64>) -> Result<()> {
        self.set(db_index, key, Value {
            expiry,
            value: ValueType::StringValue(StringValue { string: value }),
        }).await
    }
    
    pub async fn incr(&self, db_index: usize, key: &str) -> Result<i64> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.map.get_mut(key) {
            Some(value) => {
                if let ValueType::StringValue(StringValue { string }) = &value.value {
                    let current = string.parse::<i64>()
                        .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
                    let next = current.checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
                    
                    value.value = ValueType::StringValue(StringValue {
                        string: next.to_string(),
                    });
                    Ok(next)
                } else {
                    Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value"))
                }
            }
            None => {
                db.map.insert(key.to_string(), Value {
                    expiry: None,
                    value: ValueType::StringValue(StringValue {
                        string: "1".to_string(),
                    }),
                });
                Ok(1)
            }
        }
    }
    
    pub async fn get_type(&self, db_index: usize, key: &str) -> Result<String> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        
        Ok(match db.map.get(key) {
            Some(value) if !value.is_expired() => match &value.value {
                ValueType::StringValue(_) => "string",
                ValueType::ListValue(_) => "list",
                ValueType::StreamValue(_) => "stream",
                ValueType::SortedSet(_) => "zset",
            },
            _ => "none",
        }.to_string())
    }
    
    // List operations
    pub async fn lpush(&self, db_index: usize, key: &str, values: Vec<String>) -> Result<usize> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        let list = db.map.entry(key.to_string()).or_insert(Value {
            expiry: None,
            value: ValueType::ListValue(ListValue {
                list: VecDeque::new(),
            }),
        });
        
        match &mut list.value {
            ValueType::ListValue(list_value) => {
                for value in values.into_iter().rev() {
                    list_value.list.push_front(value);
                }
                Ok(list_value.list.len())
            }
            _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
        }
    }
    
    pub async fn rpush(&self, db_index: usize, key: &str, values: Vec<String>) -> Result<usize> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        let list = db.map.entry(key.to_string()).or_insert(Value {
            expiry: None,
            value: ValueType::ListValue(ListValue {
                list: VecDeque::new(),
            }),
        });
        
        match &mut list.value {
            ValueType::ListValue(list_value) => {
                list_value.list.extend(values);
                Ok(list_value.list.len())
            }
            _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
        }
    }
    
    pub async fn lrange(&self, db_index: usize, key: &str, start: i32, end: i32) -> Result<Vec<String>> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        
        match db.map.get(key) {
            Some(value) if !value.is_expired() => match &value.value {
                ValueType::ListValue(list_value) => {
                    let len = list_value.list.len();
                    let result: Vec<String> = if let Some((start_idx, end_idx)) = crate::utils::normalize_index(len, start, end) {
                        list_value.list.iter()
                            .skip(start_idx)
                            .take(end_idx - start_idx + 1)
                            .cloned()
                            .collect()
                    } else {
                        vec![]
                    };
                    Ok(result)
                }
                _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
            },
            _ => Ok(vec![]),
        }
    }
    
    pub async fn llen(&self, db_index: usize, key: &str) -> Result<usize> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        
        match db.map.get(key) {
            Some(value) if !value.is_expired() => match &value.value {
                ValueType::ListValue(list_value) => Ok(list_value.list.len()),
                _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
            },
            _ => Ok(0),
        }
    }
    
    pub async fn lpop(&self, db_index: usize, key: &str, count: usize) -> Result<Vec<String>> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.map.get_mut(key) {
            Some(value) if !value.is_expired() => match &mut value.value {
                ValueType::ListValue(list_value) => {
                    let count = count.min(list_value.list.len());
                    let popped: Vec<_> = list_value.list.drain(..count).collect();
                    Ok(popped)
                }
                _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
            },
            _ => Ok(vec![]),
        }
    }
    
    // Stream operations
    pub async fn xadd(&self, db_index: usize, key: &str, id: StreamId, fields: Vec<(String, String)>) -> Result<StreamId> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        // Check if the ID is valid
        if id <= (StreamId { ts: 0, seq: 0 }) {
            return Err(anyhow::anyhow!("ERR The ID specified in XADD must be greater than 0-0"));
        }
        
        let mut stream_data = BTreeMap::from([("id".to_string(), id.clone().into())]);
        for (k, v) in fields {
            stream_data.insert(k, v);
        }
        
        match db.map.get_mut(key) {
            Some(value) if !value.is_expired() => match &mut value.value {
                ValueType::StreamValue(stream_value) => {
                    if let Some(last) = stream_value.stream.last() {
                        let last_id: StreamId = last.get("id").unwrap().parse()?;
                        if id <= last_id {
                            return Err(anyhow::anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
                        }
                    }
                    stream_value.stream.push(stream_data);
                    Ok(id)
                }
                _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
            },
            _ => {
                db.map.insert(key.to_string(), Value {
                    expiry: None,
                    value: ValueType::StreamValue(StreamValue {
                        stream: vec![stream_data],
                    }),
                });
                Ok(id)
            }
        }
    }
    
    pub async fn generate_stream_id(&self, db_index: usize, key: &str, ts: Option<i64>) -> Result<StreamId> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        db.generate_stream_id(key, ts)
    }
    
    pub async fn xrange(&self, db_index: usize, key: &str, start: StreamId, end: StreamId) -> Result<Vec<BTreeMap<String, String>>> {
        let rdb = self.rdb.read().await;
        let db = rdb.get_db(db_index)?;
        
        match db.map.get(key) {
            Some(value) if !value.is_expired() => match &value.value {
                ValueType::StreamValue(stream_value) => {
                    let result: Vec<_> = stream_value.stream.iter()
                        .filter(|entry| {
                            let id: StreamId = entry.get("id").unwrap().parse().unwrap();
                            id >= start && id <= end
                        })
                        .cloned()
                        .collect();
                    Ok(result)
                }
                _ => Err(anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")),
            },
            _ => Ok(vec![]),
        }
    }
    
    // Sorted set operations
    pub async fn zadd(&self, db_index: usize, key: &str, member: String, score: f64) -> Result<bool> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.get_sorted_set(key.to_string())? {
            Some(sorted_set) => {
                Ok(sorted_set.zset.add(member, score))
            }
            None => {
                let mut zset = ZSet::new();
                let added = zset.add(member, score);
                db.map.insert(key.to_string(), Value {
                    expiry: None,
                    value: ValueType::SortedSet(SortedSet { zset }),
                });
                Ok(added)
            }
        }
    }
    
    pub async fn zrank(&self, db_index: usize, key: &str, member: &str) -> Result<Option<usize>> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.get_sorted_set(key.to_string())? {
            Some(sorted_set) => Ok(sorted_set.zset.rank(member)),
            None => Ok(None),
        }
    }
    
    pub async fn zrange(&self, db_index: usize, key: &str, start: i32, end: i32) -> Result<Vec<String>> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.get_sorted_set(key.to_string())? {
            Some(sorted_set) => Ok(sorted_set.zset.range(start, end).iter().map(|s| s.to_string()).collect()),
            None => Ok(vec![]),
        }
    }
    
    pub async fn zcard(&self, db_index: usize, key: &str) -> Result<usize> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.get_sorted_set(key.to_string())? {
            Some(sorted_set) => Ok(sorted_set.zset.len()),
            None => Ok(0),
        }
    }
    
    pub async fn zscore(&self, db_index: usize, key: &str, member: &str) -> Result<Option<f64>> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.get_sorted_set(key.to_string())? {
            Some(sorted_set) => Ok(sorted_set.zset.score(member).copied()),
            None => Ok(None),
        }
    }
    
    pub async fn zrem(&self, db_index: usize, key: &str, member: String) -> Result<bool> {
        let mut rdb = self.rdb.write().await;
        let db = rdb.get_db_mut(db_index)?;
        
        match db.get_sorted_set(key.to_string())? {
            Some(sorted_set) => Ok(sorted_set.zset.rem(member)),
            None => Ok(false),
        }
    }
    
    pub async fn get_rdb(&self) -> tokio::sync::RwLockReadGuard<'_, RDB> {
        self.rdb.read().await
    }
}