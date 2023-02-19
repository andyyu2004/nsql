pub use sqlparser::ast::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};

pub type Result<T> = std::result::Result<T, ParserError>;

static DIALIECT: PostgreSqlDialect = PostgreSqlDialect {};

fn parser(sql: &str) -> Result<Parser<'static>> {
    Parser::new(&DIALIECT).try_with_sql(sql)
}

pub fn parse(sql: &str) -> Result<Statement> {
    parser(sql)?.parse_statement()
}

pub fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    parser(sql)?.parse_statements()
}
