use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

use crate::{commands::CommandContext, message::Message};

#[derive(Debug, Clone)]
pub struct User {
    name: String,
    passwords: Vec<String>,
}

pub async fn acl(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let subcommand = &args[0].to_lowercase();

    match subcommand.as_str() {
        "whoami" => whoami(ctx, args).await,
        "getuser" => getuser(ctx, args).await,
        "setuser" => setuser(ctx, args).await,
        other => Err(anyhow!("Unsupport subcommand: {other}")),
    }
}

async fn whoami(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    Ok(Some(Message::new_bulk_string("default".to_string())))
}

async fn getuser(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let user = &args[1];

    let passwords = match ctx.server.users.get(user) {
        Some(user) => user.value().passwords.clone(),
        None => vec![],
    };

    let flags = if passwords.is_empty() {
        vec![Message::new_bulk_string("nopass".to_string())]
    } else {
        vec![]
    };

    Ok(Some(Message::new_array(vec![
        Message::new_bulk_string("flags".to_string()),
        Message::new_array(flags),
        Message::new_bulk_string("passwords".to_string()),
        Message::new_array(
            passwords
                .into_iter()
                .map(|password| Message::new_bulk_string(password))
                .collect(),
        ),
    ])))
}

async fn setuser(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let username = &args[1];
    let rule = &args[2];

    if rule.starts_with('>') {
        let new_password = &rule[1..];

        let mut entry = ctx
            .server
            .users
            .entry(username.to_string())
            .or_insert_with(|| User {
                name: username.to_string(),
                passwords: vec![],
            });

        entry
            .value_mut()
            .passwords
            .push(hash_password(new_password));
        Ok(Some(Message::new_simple_string("OK")))
    } else {
        Err(anyhow!("Unsupported rule {rule}"))
    }
}

pub async fn auth(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let username = &args[0];
    let password = &args[1];

    match ctx.server.users.get(username) {
        Some(user) => {
            let password = hash_password(password);
            if user.passwords.is_empty() || user.passwords.contains(&password) {
                Ok(Some(Message::new_simple_string("OK".to_string())))
            } else {
                Err(anyhow!(
                    "WRONGPASS invalid username-password pair or user is disabled."
                ))
            }
        }
        None => Err(anyhow!(
            "WRONGPASS invalid username-password pair or user is disabled."
        )),
    }
}

fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();

    hasher.update(password);

    let result = hasher.finalize();

    hex::encode(result)
}
