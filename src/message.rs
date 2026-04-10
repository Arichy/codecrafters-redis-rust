use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::codec::{Decoder, Encoder};
use futures_util::stream::SplitSink;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::rdb::RDB;

pub type MessageWriter = Arc<Mutex<SplitSink<Framed<TcpStream, MessageFramer>, Message>>>;

#[derive(Debug, Clone)]
pub enum Message {
    Array(Array),
    NullArray,
    SimpleString(SimpleString),
    BulkString(BulkString),
    NullBulkString,
    Integer(Integer),
    SimpleError(SimpleError),
    RDB(RDB),
}

impl Message {
    pub fn length(&self) -> anyhow::Result<usize> {
        let mut message_framer = MessageFramer;
        let mut bytes = BytesMut::new();
        message_framer
            .encode(self.clone(), &mut bytes)
            .context("Error: encoding to get length.")?;
        Ok(bytes.len())
    }

    pub fn new_bulk_string(string: String) -> Self {
        Self::BulkString(BulkString { string })
    }

    pub fn new_array(items: Vec<Message>) -> Self {
        Self::Array(Array { items })
    }

    pub fn new_simple_string(string: impl Into<String>) -> Self {
        Self::SimpleString(SimpleString { string: string.into() })
    }

    pub fn new_integer(value: i64) -> Self {
        Self::Integer(Integer { value })
    }

    pub fn new_error(string: impl Into<String>) -> Self {
        Self::SimpleError(SimpleError { string: string.into() })
    }

    pub fn as_bulk_str(&self) -> Option<&str> {
        match self {
            Message::BulkString(BulkString { string }) => Some(string),
            Message::NullBulkString => None,
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Message>> {
        match self {
            Message::Array(Array { items }) => Some(items),
            Message::NullArray => None,
            _ => None,
        }
    }

    pub fn as_simple_str(&self) -> Option<&str> {
        match self {
            Message::SimpleString(SimpleString { string }) => Some(string),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Message::Integer(Integer { value }) => Some(*value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Array {
    pub items: Vec<Message>,
}

#[derive(Debug, Clone)]
pub struct BulkString {
    pub string: String,
}

#[derive(Debug, Clone)]
pub struct SimpleString {
    pub string: String,
}

#[derive(Debug, Clone)]
pub struct Integer {
    pub value: i64,
}

#[derive(Debug, Clone, Error)]
#[error("{string}")]
pub struct SimpleError {
    pub string: String,
}

impl SimpleError {
    pub fn new_wrongtype() -> Self {
        Self {
            string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        }
    }
}

pub struct MessageFramer;
const MAX: usize = 1 << 16;

impl Decoder for MessageFramer {
    type Error = std::io::Error;
    type Item = Message;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // println!("{src:?}");
        if src.is_empty() {
            return Ok(None);
        }

        let start_char = src[0] as char;
        src.advance(1);

        match start_char {
            '*' => {
                let new_line_pos = src
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .context("Frame does not have have \\r\\n.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                let count_string_bytes = &src[0..new_line_pos];
                let count = String::from_utf8(count_string_bytes.to_vec());
                if count.is_err() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Count is not a number."),
                    ));
                }
                let count = count.unwrap().parse::<i32>();
                if count.is_err() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Count is not a number."),
                    ));
                }
                let count = count.unwrap();
                src.advance(new_line_pos + 2);

                // Null array: *-1\r\n
                if count == -1 {
                    return Ok(Some(Message::NullArray));
                }

                let mut items = vec![];
                for _ in 0..count {
                    let item = self
                        .decode(src)
                        .expect("decode array item failed")
                        .context("item is none")
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                    items.push(item);
                }

                Ok(Some(Message::Array(Array { items })))
            }

            '+' => {
                let new_line_pos = src
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .context("Simple string does not have \\r\\n.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                let string = String::from_utf8(src[..new_line_pos].to_vec())
                    .context("Simple string is not valid UTF-8.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                src.advance(new_line_pos + 2);

                Ok(Some(Message::SimpleString(SimpleString { string })))
            }

            '-' => {
                let new_line_pos = src
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .context("Simple string does not have \\r\\n.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                let string = String::from_utf8(src[..new_line_pos].to_vec())
                    .context("Simple string is not valid UTF-8.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                src.advance(new_line_pos + 2);

                Ok(Some(Message::SimpleError(SimpleError { string })))
            }

            '$' => {
                let new_line_pos = src
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .context("Length does not have \\r\\n.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                let length_string_bytes = &src[0..new_line_pos];
                let length_str = String::from_utf8(length_string_bytes.to_vec());
                if length_str.is_err() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Length is not valid UTF-8.",
                    ));
                }
                let length_str = length_str.unwrap();
                let length = length_str.parse::<isize>();
                if length.is_err() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Length is not a number.",
                    ));
                }
                let length = length.unwrap();
                src.advance(new_line_pos + 2);

                // Null bulk string: $-1\r\n
                if length < 0 {
                    return Ok(Some(Message::NullBulkString));
                }

                let length = length as usize;
                let is_bulk_string = {
                    if src.len() >= length + 2 {
                        src[length] == b'\r' && src[length + 1] == b'\n'
                    } else {
                        false
                    }
                };

                if is_bulk_string {
                    let string = String::from_utf8(src[..length].to_vec())
                        .context("Bulk string is not valid UTF-8.")
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    src.advance(length + 2);
                    Ok(Some(Message::BulkString(BulkString { string })))
                } else {
                    let mut bytes = Bytes::from(src[..length].to_vec());
                    src.advance(length);
                    Ok(Some(Message::RDB(
                        RDB::from_bytes(&mut bytes)
                            .context("Invalid RDB format")
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
                    )))
                }
            }

            ':' => {
                let new_line_pos = src
                    .windows(2)
                    .position(|window| window == b"\r\n")
                    .context("Length does not have \\r\\n.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let int_string = String::from_utf8(src[..new_line_pos].to_vec())
                    .context("Bulk string is not valid UTF-8.")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let int_value = int_string
                    .parse()
                    .context("Invalid int string")
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                Ok(Some(Message::Integer(Integer { value: int_value })))
            }

            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unknown start: {other}"),
                ));
            }
        }
    }
}

impl Encoder<Message> for MessageFramer {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
            Message::Array(Array { items }) => {
                let count = items.len();
                let count_str = count.to_string();
                let count_line: String = format!("*{count_str}\r\n");

                dst.extend_from_slice(count_line.as_bytes());
                for item in items {
                    let _ = self.encode(item, dst);
                }
            }
            Message::NullArray => {
                dst.extend_from_slice(b"*-1\r\n");
            }
            Message::SimpleString(SimpleString { string }) => {
                let simple_string = format!("+{string}\r\n");
                dst.extend_from_slice(simple_string.as_bytes());
            }
            Message::BulkString(BulkString { string }) => {
                let length = string.len();
                let bulk_string = format!("${length}\r\n{string}\r\n");
                dst.extend_from_slice(bulk_string.as_bytes());
            }
            Message::NullBulkString => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Message::RDB(rdb) => {
                let bytes = rdb.to_bytes();
                dst.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                dst.extend_from_slice(bytes.as_ref());
                // No trailing \r\n - tester expects raw RDB bytes only
            }

            Message::Integer(Integer { value }) => {
                let int_string = format!(":{}\r\n", value.to_string());
                dst.extend_from_slice(int_string.as_bytes());
            }

            Message::SimpleError(SimpleError { string }) => {
                let simple_error = format!("-{string}\r\n");
                dst.extend_from_slice(simple_error.as_bytes());
            }

            _ => {}
        }

        Ok(())
    }
}
