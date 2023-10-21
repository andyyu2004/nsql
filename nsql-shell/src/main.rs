use std::borrow::Cow;
use std::io::Read;
use std::path::{Path, PathBuf};

use argh::FromArgs;
use nsql::{LmdbStorageEngine, Nsql};
use nu_ansi_term::{Color, Style};
use reedline::{
    default_vi_insert_keybindings, default_vi_normal_keybindings, DefaultHinter, DefaultValidator,
    FileBackedHistory, KeyCode, KeyModifiers, PromptEditMode, PromptHistorySearch,
    PromptHistorySearchStatus, PromptViMode, Reedline, ReedlineEvent, Signal, ValidationResult, Vi,
};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Debug, FromArgs)]
#[argh(description = "nsql")]
struct Args {
    /// execute command and quit
    #[argh(option, short = 'c')]
    cmd: Option<String>,

    /// execute commands from file and quit (use - for stdin)
    #[argh(option, short = 'f')]
    file: Option<PathBuf>,

    /// path to log file
    #[argh(option)]
    log_file: Option<PathBuf>,

    /// suppress all output except errors
    #[argh(switch)]
    silent: bool,

    #[argh(positional)]
    path: PathBuf,
}

struct Validator;

impl reedline::Validator for Validator {
    fn validate(&self, line: &str) -> reedline::ValidationResult {
        if matches!(DefaultValidator.validate(line), ValidationResult::Incomplete) {
            return ValidationResult::Incomplete;
        }

        if line.trim_end().ends_with(';') {
            return ValidationResult::Complete;
        }

        ValidationResult::Incomplete

        // the following isn't smart enough
        // match nsql::parse(line) {
        //     Ok(_) => ValidationResult::Complete,
        //     Err(_) => ValidationResult::Incomplete,
        // }
    }
}

fn main() -> nsql::Result<()> {
    let args: Args = argh::from_env();

    if let Some(log_file) = args.log_file {
        let file = &*Box::leak(Box::new(std::fs::File::create(log_file)?));
        let filter =
            EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=INFO"));

        tracing_subscriber::Registry::default()
            .with(
                tracing_tree::HierarchicalLayer::new(2)
                    .with_writer(move || file)
                    .with_filter(filter),
            )
            .init();
    }

    let nsql = Nsql::<LmdbStorageEngine>::open(&args.path)?;
    let (conn, state) = nsql.connect();

    if let Some(cmd) = &args.cmd {
        let output = conn.query(&state, cmd)?;
        if !args.silent {
            println!("{output}")
        }
    }

    if let Some(file) = &args.file {
        let sql = if file.as_path() == Path::new("-") {
            let mut buffer = String::new();
            std::io::stdin().read_to_string(&mut buffer)?;
            buffer
        } else {
            std::fs::read_to_string(file)?
        };

        let output = conn.query(&state, &sql)?;
        if !args.silent {
            println!("{output}")
        }
    }

    if args.file.is_some() || args.cmd.is_some() {
        return Ok(());
    }

    let mut ikb = default_vi_insert_keybindings();
    ikb.add_binding(KeyModifiers::CONTROL, KeyCode::Char('f'), ReedlineEvent::HistoryHintComplete);

    let mut line_editor = Reedline::create()
        .with_edit_mode(Box::new(Vi::new(ikb, default_vi_normal_keybindings())))
        .with_history(Box::new(FileBackedHistory::with_file(
            500,
            dirs::data_dir().unwrap_or_else(|| "/tmp".into()).join("nsql/nsql_history"),
        )?))
        .with_validator(Box::new(Validator))
        .with_hinter(Box::new(
            DefaultHinter::default().with_style(Style::new().italic().fg(Color::DarkGray)),
        ));

    let prompt = NsqlPrompt {};

    loop {
        let sig = line_editor.read_line(&prompt)?;
        match sig {
            Signal::Success(buffer) => match conn.query(&state, &buffer) {
                Ok(output) => println!("{output}"),
                Err(e) => println!("{}", e),
            },
            Signal::CtrlC => continue,
            Signal::CtrlD => break Ok(()),
        }
    }
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
