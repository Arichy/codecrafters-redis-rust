use anyhow::Result;
use crate::message::{Message, Array, SimpleError};
use crate::commands::CommandContext;
use crate::rdb::{StreamId, StreamValue};
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "XRANGE requires 3 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let start_str = params[1];
    let end_str = params[2];
    let db_index = *ctx.selected_db_index.read().await;
    
    // Parse start
    let start = if start_str == "-" {
        StreamId { ts: 0, seq: 0 }
    } else {
        let (ts, seq_opt) = StreamId::parse_range_str(start_str)?;
        StreamId { ts, seq: seq_opt.unwrap_or(0) }
    };
    
    // Parse end
    let end = if end_str == "+" {
        StreamId { ts: i64::MAX, seq: usize::MAX }
    } else {
        let (ts, seq_opt) = StreamId::parse_range_str(end_str)?;
        StreamId { ts, seq: seq_opt.unwrap_or(usize::MAX) }
    };
    
    let entries = ctx.store.xrange(db_index, key, start, end).await?;
    
    let items: Vec<Message> = entries
        .into_iter()
        .map(|entry| StreamValue::get_message_by_record(&entry))
        .collect();
    
    Ok(Some(Message::Array(Array { items })))
}