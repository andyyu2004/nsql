use std::sync::OnceLock;

use anyhow::Result;
use nsql_core::LogicalType;
use nsql_derive::{FromTuple, IntoTuple};
use nsql_storage::tuple::{IntoTuple, Tuple};
use nsql_storage::value::{CastError, FromValue, Value};
use nsql_storage_engine::StorageEngine;

use crate::{
    BootstrapColumn, BootstrapSequence, Catalog, ExecutionMode, Name, Namespace, Oid, SystemEntity,
    SystemTableView, Table, TransactionContext, TransactionLocalCatalogCaches,
};

pub(crate) mod column;
pub(crate) mod function;
pub(crate) mod index;
pub(crate) mod namespace;
pub(crate) mod operator;
pub(crate) mod sequence;
pub(crate) mod table;
