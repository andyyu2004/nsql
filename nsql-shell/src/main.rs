use std::borrow::Cow;

use nsql::{MaterializedQueryOutput, Nsql, RedbStorageEngine};
use nu_ansi_term::{Color, Style};
use reedline::{
    default_vi_insert_keybindings, default_vi_normal_keybindings, DefaultHinter, FileBackedHistory,
    KeyCode, KeyModifiers, PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus,
    PromptViMode, Reedline, ReedlineEvent, Signal, Vi,
};
use tabled::builder::Builder;
use tabled::Table;

fn main() -> nsql::Result<()> {
    let mut ikb = default_vi_insert_keybindings();
    ikb.add_binding(KeyModifiers::CONTROL, KeyCode::Char('f'), ReedlineEvent::HistoryHintComplete);

    let mut line_editor = Reedline::create()
        .with_edit_mode(Box::new(Vi::new(ikb, default_vi_normal_keybindings())))
        .with_history(Box::new(FileBackedHistory::with_file(500, "/tmp/nsql-history.txt".into())?))
        .with_hinter(Box::new(
            DefaultHinter::default().with_style(Style::new().italic().fg(Color::DarkGray)),
        ));

    let prompt = NsqlPrompt {};

    let nsql = Nsql::<RedbStorageEngine>::open("/tmp/test.db")?;
    let (conn, state) = nsql.connect();

    loop {
        let sig = line_editor.read_line(&prompt)?;
        match sig {
            Signal::Success(buffer) => match conn.query(&state, &buffer) {
                Ok(output) => println!("{}", tabulate(output)),
                Err(e) => println!("{}", e),
            },
            Signal::CtrlC => continue,
            Signal::CtrlD => break Ok(()),
        }
    }
}

fn tabulate(output: MaterializedQueryOutput) -> Table {
    let mut builder = Builder::default();
    for tuple in output.tuples {
        builder.push_record(tuple.values().map(|v| v.to_string()));
    }
    builder.build()
}

pub static DEFAULT_VI_INSERT_PROMPT_INDICATOR: &str = "> ";
pub static DEFAULT_VI_NORMAL_PROMPT_INDICATOR: &str = "〉";
pub static DEFAULT_MULTILINE_INDICATOR: &str = "::: ";

#[derive(Clone)]
pub struct NsqlPrompt;

impl reedline::Prompt for NsqlPrompt {
    fn render_prompt_left(&self) -> Cow<str> {
        Cow::Borrowed("nsql")
    }

    fn render_prompt_right(&self) -> Cow<str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, edit_mode: PromptEditMode) -> Cow<str> {
        match edit_mode {
            PromptEditMode::Default | PromptEditMode::Emacs => {
                DEFAULT_VI_NORMAL_PROMPT_INDICATOR.into()
            }
            PromptEditMode::Vi(vi_mode) => match vi_mode {
                PromptViMode::Normal => DEFAULT_VI_NORMAL_PROMPT_INDICATOR.into(),
                PromptViMode::Insert => DEFAULT_VI_INSERT_PROMPT_INDICATOR.into(),
            },
            PromptEditMode::Custom(str) => format!("({str})").into(),
        }
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<str> {
        Cow::Borrowed(DEFAULT_MULTILINE_INDICATOR)
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<str> {
        let prefix = match history_search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        // NOTE: magic strings, given there is logic on how these compose I am not sure if it
        // is worth extracting in to static constant
        Cow::Owned(format!("({}reverse-search: {}) ", prefix, history_search.term))
    }
}
