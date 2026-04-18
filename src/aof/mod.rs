use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AOF {
    pub dir: PathBuf,
    pub appendonly: bool,
    pub appenddirname: String,
    pub appendfilename: String,
    pub appendfsync: String,
}
