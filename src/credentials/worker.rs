use std::{env, path::PathBuf};

use crate::{DEFAULT_CREDENTIALS, commands::CredentialCommand};

use super::CredentialStore;

pub(crate) async fn run_command(
    command: CredentialCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(
        env::var("SINGULARITY_CREDENTIALS_PATH")
            .unwrap_or_else(|_| DEFAULT_CREDENTIALS.to_string()),
    );
    let mut store = CredentialStore::load(&path).await?;
    match command {
        CredentialCommand::Create(collector_id) => {
            let token = store
                .create(&collector_id)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::AlreadyExists, error))?;
            store.save().await?;
            println!("collector_id={collector_id}");
            println!("token={token}");
            println!("store this token securely; it will not be shown again");
        }
        CredentialCommand::Rotate(collector_id) => {
            let token = store
                .rotate(&collector_id)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::NotFound, error))?;
            store.save().await?;
            println!("collector_id={collector_id}");
            println!("token={token}");
            println!("the previous token is no longer valid");
        }
        CredentialCommand::Revoke(collector_id) => {
            store
                .revoke(&collector_id)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::NotFound, error))?;
            store.save().await?;
            println!("revoked collector_id={collector_id}");
        }
        CredentialCommand::List => {
            for (collector_id, record) in &store.records {
                let status = if record.revoked_at.is_some() {
                    "revoked"
                } else {
                    "active"
                };
                println!("{collector_id}\t{status}");
            }
        }
    }
    Ok(())
}
