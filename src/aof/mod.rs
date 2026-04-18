use std::path::PathBuf;

use anyhow::Result;
use bytes::BytesMut;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tokio_util::codec::Encoder;

use crate::message::{Message, MessageFramer};

#[derive(Debug, Clone)]
pub struct AOF {
    pub dir: PathBuf,
    pub appendonly: bool,
    pub appenddirname: String,
    pub appendfilename: String,
    pub appendfsync: String,
}

impl AOF {
    pub fn full_appendonlly_dir(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(PathBuf::from(&self.appenddirname))
    }

    pub fn manifest_path(&self) -> PathBuf {
        self.full_appendonlly_dir()
            .join(PathBuf::from(format!("{}.manifest", self.appendfilename)))
    }

    pub async fn get_current_aof_file(&self) -> Result<PathBuf> {
        let manifest_filename = self.manifest_path();

        let content = tokio::fs::read_to_string(manifest_filename).await?;
        let mut lines = content.lines();

        let target = lines
            .find_map(|line| {
                let mut segments = line.split_whitespace();
                let record_type = segments.next().unwrap();
                let filename = segments.next().unwrap();
                segments.next(); // "seq"
                let seq = segments.next().unwrap();
                segments.next(); // "type"
                let type_str = segments.next().unwrap();

                if type_str == "i" {
                    Some(filename)
                } else {
                    None
                }
            })
            .unwrap();

        Ok(self.full_appendonlly_dir().join(target))
    }

    pub async fn write_to_current_aof_file(&self, message: Message) -> Result<()> {
        let current_aof_file = dbg!(self.get_current_aof_file().await?);
        let mut file = OpenOptions::new()
            .append(true)
            .write(true)
            .open(current_aof_file)
            .await?;
        let mut mf = MessageFramer;
        let mut bytes = BytesMut::new();
        mf.encode(message, &mut bytes);

        file.write_all(&bytes).await?;
        if self.appendfsync == "fsync" {
            file.flush().await?;
        }
        Ok(())
    }
}
