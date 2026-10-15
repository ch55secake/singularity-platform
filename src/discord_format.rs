//! Coloured, aligned tables for the Discord posts.
//!
//! Discord colours text inside ```` ```ansi ```` code blocks on desktop. On
//! mobile the block is shown without colours but stays aligned, so every table
//! still reads as columns.

/// Embed side colours for the two teams.
pub(crate) const TEAM_COLOURS: [u32; 2] = [0xE8A33A, 0x3D7BD9];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tone {
    Plain,
    Good,
    Middling,
    Bad,
    Muted,
    Friend,
    Highlight,
    Stack(char),
}

impl Tone {
    fn code(self) -> &'static str {
        match self {
            Tone::Plain => "",
            Tone::Good => "32",
            Tone::Middling => "33",
            Tone::Bad => "31",
            Tone::Muted => "30",
            Tone::Friend => "1;36",
            Tone::Highlight => "1;33",
            Tone::Stack(letter) => match letter {
                'A' => "1;35",
                'B' => "1;34",
                'C' => "1;33",
                'D' => "1;32",
                _ => "1;37",
            },
        }
    }
}

/// One column: heading, width and alignment.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Column {
    pub(crate) heading: &'static str,
    pub(crate) width: usize,
    pub(crate) right: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cell {
    pub(crate) text: String,
    pub(crate) tone: Tone,
}

impl Cell {
    pub(crate) fn new(text: impl Into<String>, tone: Tone) -> Self {
        Self {
            text: text.into(),
            tone,
        }
    }

    pub(crate) fn plain(text: impl Into<String>) -> Self {
        Self::new(text, Tone::Plain)
    }

    pub(crate) fn empty() -> Self {
        Self::plain("")
    }
}

/// Green at 55% and up, red at 45% and below, grey for tiny samples.
pub(crate) fn rate_tone(rate: f64, sample: u32) -> Tone {
    if sample < 3 {
        Tone::Muted
    } else if rate >= 0.55 {
        Tone::Good
    } else if rate <= 0.45 {
        Tone::Bad
    } else {
        Tone::Middling
    }
}

/// Renders rows under a grey heading inside an `ansi` code block.
pub(crate) fn table(columns: &[Column], rows: &[Vec<Cell>]) -> String {
    let mut out = String::from("```ansi\n");
    let heading = columns
        .iter()
        .map(|column| fit(column.heading, column.width, column.right))
        .collect::<Vec<_>>()
        .join(" ");
    out.push_str(&paint(heading.trim_end(), Tone::Muted));
    out.push('\n');
    for row in rows {
        let line = columns
            .iter()
            .zip(row)
            .map(|(column, cell)| paint(&fit(&cell.text, column.width, column.right), cell.tone))
            .collect::<Vec<_>>()
            .join(" ");
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out.push_str("```");
    out
}

fn paint(text: &str, tone: Tone) -> String {
    let code = tone.code();
    if code.is_empty() || text.trim().is_empty() {
        text.to_string()
    } else {
        format!("\u{1b}[{code}m{text}\u{1b}[0m")
    }
}

/// Pads or cuts text to exactly `width` characters. Backticks and control
/// characters are dropped so a player name cannot break out of the block.
fn fit(text: &str, width: usize, right: bool) -> String {
    let clean = text
        .chars()
        .filter(|character| !character.is_control() && *character != '`')
        .collect::<String>();
    let count = clean.chars().count();
    let cut = if count > width {
        let mut cut = clean
            .chars()
            .take(width.saturating_sub(1))
            .collect::<String>();
        cut.push('…');
        cut
    } else {
        clean
    };
    let padding = " ".repeat(width.saturating_sub(cut.chars().count()));
    if right {
        format!("{padding}{cut}")
    } else {
        format!("{cut}{padding}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COLUMNS: [Column; 2] = [
        Column {
            heading: "Hero",
            width: 6,
            right: false,
        },
        Column {
            heading: "WR",
            width: 4,
            right: true,
        },
    ];

    #[test]
    fn pads_cuts_and_colours_cells() {
        let rows = vec![
            vec![Cell::plain("Haze"), Cell::new("56%", Tone::Good)],
            vec![Cell::plain("Deadman Danny"), Cell::empty()],
        ];
        assert_eq!(
            table(&COLUMNS, &rows),
            "```ansi\n\u{1b}[30mHero     WR\u{1b}[0m\nHaze   \u{1b}[32m 56%\u{1b}[0m\nDeadm…\n```"
        );
    }

    #[test]
    fn names_cannot_break_the_code_block() {
        assert_eq!(fit("a```b\n", 4, false), "ab  ");
    }

    #[test]
    fn rates_need_a_sample() {
        assert_eq!(rate_tone(1.0, 2), Tone::Muted);
        assert_eq!(rate_tone(0.6, 10), Tone::Good);
        assert_eq!(rate_tone(0.5, 10), Tone::Middling);
        assert_eq!(rate_tone(0.4, 10), Tone::Bad);
    }
}
