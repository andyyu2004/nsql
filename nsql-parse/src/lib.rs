
pub use sqlparser::ast;
use sqlparser::dialect::{self, Dialect};
use sqlparser::parser::Parser;
pub use sqlparser::parser::ParserError as Error;

pub type Result<T> = std::result::Result<T, Error>;

static DIALIECT: &'static (dyn Dialect + Send + Sync) = &dialect::GenericDialect {};

fn parser(sql: &str) -> Result<Parser<'static>> {
    Parser::new(DIALIECT).try_with_sql(sql)
}

pub fn parse(sql: &str) -> Result<ast::Statement> {
    parser(sql)?.parse_statement()
}

pub fn parse_statements(sql: &str) -> Result<Vec<ast::Statement>> {
    parser(sql)?.parse_statements()
}
