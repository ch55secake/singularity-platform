pub(super) enum CredentialCommand {
    Create(String),
    Rotate(String),
    Revoke(String),
    List,
}

pub(super) enum Command {
    Credentials(CredentialCommand),
    MetadataWorker,
    MetadataParser,
    ReplayWorker,
    AssetsSync,
    DiscordBot,
}

pub(super) fn parse_command(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<Command>, String> {
    let Some(command) = args.next() else {
        return Ok(None);
    };
    if command == "metadata-worker" {
        if args.next().is_some() {
            return Err("unexpected metadata worker argument".to_string());
        }
        return Ok(Some(Command::MetadataWorker));
    }
    if command == "metadata-parser" {
        if args.next().is_some() {
            return Err("unexpected metadata parser argument".to_string());
        }
        return Ok(Some(Command::MetadataParser));
    }
    if command == "replay-worker" {
        if args.next().is_some() {
            return Err("unexpected replay worker argument".to_string());
        }
        return Ok(Some(Command::ReplayWorker));
    }
    if command == "assets-sync" {
        if args.next().is_some() {
            return Err("unexpected assets sync argument".to_string());
        }
        return Ok(Some(Command::AssetsSync));
    }
    if command == "discord-bot" {
        if args.next().is_some() {
            return Err("unexpected discord bot argument".to_string());
        }
        return Ok(Some(Command::DiscordBot));
    }
    if command != "credentials" && command != "credential" {
        return Err("unknown command".to_string());
    }
    let subcommand = args
        .next()
        .ok_or_else(|| "missing credential command".to_string())?;
    let parsed = match subcommand.as_str() {
        "create" => CredentialCommand::Create(parse_collector_id(&mut args)?),
        "rotate" => CredentialCommand::Rotate(parse_collector_id(&mut args)?),
        "revoke" => CredentialCommand::Revoke(parse_collector_id(&mut args)?),
        "list" => CredentialCommand::List,
        _ => return Err("unknown credential command".to_string()),
    };
    if args.next().is_some() {
        return Err("unexpected credential command argument".to_string());
    }
    Ok(Some(Command::Credentials(parsed)))
}

fn parse_collector_id(args: &mut impl Iterator<Item = String>) -> Result<String, String> {
    let collector_id = args
        .next()
        .ok_or_else(|| "missing collector id".to_string())?;
    if valid_collector_id(&collector_id) {
        Ok(collector_id)
    } else {
        Err("collector id must be 1-64 ASCII letters, numbers, '.', '_' or '-'".to_string())
    }
}

fn valid_collector_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".-_".contains(character))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_worker_commands() {
        let command = parse_command(["replay-worker".to_string()].into_iter())
            .expect("worker command parses");

        assert!(matches!(command, Some(Command::ReplayWorker)));
    }

    #[test]
    fn parses_credential_commands() {
        let command = parse_command(
            [
                "credentials".to_string(),
                "rotate".to_string(),
                "collector-a".to_string(),
            ]
            .into_iter(),
        )
        .expect("credential command parses");

        assert!(matches!(
            command,
            Some(Command::Credentials(CredentialCommand::Rotate(id))) if id == "collector-a"
        ));
    }

    #[test]
    fn rejects_unexpected_command_arguments() {
        let error = parse_command(["replay-worker".to_string(), "extra".to_string()].into_iter())
            .err()
            .expect("extra argument is rejected");

        assert_eq!(error, "unexpected replay worker argument");
    }
}
