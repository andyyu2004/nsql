use std::str::FromStr;
use std::sync::atomic;

use anyhow::bail;
use ir::Value;
use nsql_util::atomic::AtomicEnum;

#[derive(Debug, Default)]
pub struct SessionConfig {
    explain_output: AtomicEnum<ExplainOutput>,
}

impl SessionConfig {
    #[inline]
    pub fn explain_output(&self) -> ExplainOutput {
        self.explain_output.load(atomic::Ordering::Acquire)
    }

    pub fn set(&self, name: &str, value: Value) -> anyhow::Result<()> {
        match ConfigurationOption::from_str(name)? {
            ConfigurationOption::ExplainOutput => {
                let s = match value {
                    Value::Text(s) => s,
                    _ => bail!("value for `explain_output` must be of type text"),
                };
                let explain_output = ExplainOutput::from_str(&s)?;
                self.explain_output.store(explain_output, atomic::Ordering::Release);
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum ExplainOutput {
    #[default]
    Physical,
    Pipeline,
    Logical,
    All,
}

impl FromStr for ExplainOutput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "physical" => Ok(ExplainOutput::Physical),
            "pipeline" => Ok(ExplainOutput::Pipeline),
            "logical" => Ok(ExplainOutput::Logical),
            "all" => Ok(ExplainOutput::All),
            _ => bail!(
                "invalid value for explain output, must be one of `physical`, `pipeline`, `logical`, or `all`"
            ),
        }
    }
}

impl From<u8> for ExplainOutput {
    #[inline]
    fn from(val: u8) -> Self {
        match val {
            0 => ExplainOutput::Physical,
            1 => ExplainOutput::Pipeline,
            2 => ExplainOutput::Logical,
            3 => ExplainOutput::All,
            _ => panic!("invalid explain output value"),
        }
    }
}

impl From<ExplainOutput> for u8 {
    #[inline]
    fn from(val: ExplainOutput) -> Self {
        val as u8
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ConfigurationOption {
    ExplainOutput,
}

impl FromStr for ConfigurationOption {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "explain_output" => Ok(ConfigurationOption::ExplainOutput),
            _ => bail!("unknown configuration option `{s}`"),
        }
    }
}
