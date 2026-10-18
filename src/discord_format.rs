//! Shared formatting helpers for Discord embeds.

/// Embed side colours for the two teams.
pub(crate) const TEAM_COLOURS: [u32; 2] = [0xE8A33A, 0x3D7BD9];

/// Removes control characters, escapes Discord markdown, and caps text to an
/// embed-safe length. Used for player-provided names and asset labels.
pub(crate) fn safe_embed_text(text: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    let escaped = text
        .chars()
        .filter(|character| !character.is_control())
        .map(|character| {
            if matches!(character, '\\' | '*' | '_' | '~' | '`' | '|' | '>') {
                format!("\\{character}")
            } else {
                character.to_string()
            }
        })
        .collect::<Vec<_>>();
    let length = escaped
        .iter()
        .map(|part| part.chars().count())
        .sum::<usize>();
    if length <= limit {
        return escaped.concat();
    }

    let mut output = String::new();
    let mut used = 0;
    for part in escaped {
        let part_length = part.chars().count();
        if used + part_length > limit.saturating_sub(1) {
            break;
        }
        output.push_str(&part);
        used += part_length;
    }
    output.push('…');
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_markdown_and_drops_control_characters() {
        assert_eq!(safe_embed_text("A_*`\nB", 20), "A\\_\\*\\`B");
    }

    #[test]
    fn truncates_without_leaving_a_partial_escape() {
        assert_eq!(safe_embed_text("ab*cdef", 4), "ab…");
        assert_eq!(safe_embed_text("abcdef", 4), "abc…");
    }
}
