use nsql_storage::ColumnStorageInfo;

use super::*;
use crate::SystemEntityPrivate;

impl Table {
    mk_consts![
        NAMESPACE,
        TABLE,
        ATTRIBUTE,
        INDEX,
        FUNCTION,
        OPERATOR,
        SEQUENCE,
        NAMESPACE_NAME_UNIQUE_INDEX,
        TABLE_NAME_UNIQUE_INDEX,
        ATTRIBUTE_NAME_UNIQUE_INDEX,
        FUNCTION_NAME_ARGS_UNIQUE_INDEX,
        OPERATOR_NAME_FN_UNIQUE_INDEX,
        NAMESPACE_OID_SEQ
    ];
}

pub(super) struct BootstrapTable {
    pub oid: Oid<Table>,
    pub name: &'static str,
    pub columns: Vec<BootstrapColumn>,
    pub indexes: Vec<BootstrapIndex>,
}

pub struct BootstrapColumn {
    pub(crate) ty: LogicalType,
    pub(crate) name: &'static str,
    pub(crate) is_primary_key: bool,
    pub(crate) identity: ColumnIdentity,
    pub(crate) default_expr: Expr,
    pub(crate) seq: Option<BootstrapSequence>,
}

impl From<BootstrapColumn> for ColumnStorageInfo {
    #[inline]
    fn from(col: BootstrapColumn) -> Self {
        ColumnStorageInfo {
            name: col.name.into(),
            logical_type: col.ty.clone(),
            is_primary_key: col.is_primary_key,
        }
    }
}

pub(crate) struct BootstrapSequence {
    pub table: Oid<Table>,
    pub name: &'static str,
}

pub(super) struct BootstrapIndex {
    pub table: Oid<Table>,
    pub name: &'static str,
    pub kind: IndexKind,
    pub column_names: Vec<&'static str>,
}

pub(super) fn bootstrap_data() -> Box<[BootstrapTable]> {
    vec![
        BootstrapTable {
            oid: Table::NAMESPACE,
            name: "nsql_namespace",
            columns: Namespace::bootstrap_column_info(),
            indexes: vec![BootstrapIndex {
                table: Table::NAMESPACE_NAME_UNIQUE_INDEX,
                name: "nsql_namespace_name_index",
                kind: IndexKind::Unique,
                column_names: vec!["name"],
            }],
        },
        BootstrapTable {
            oid: Table::TABLE,
            name: "nsql_table",
            columns: Table::bootstrap_column_info(),
            indexes: vec![BootstrapIndex {
                table: Table::TABLE_NAME_UNIQUE_INDEX,
                name: "nsql_table_namespace_name_index",
                kind: IndexKind::Unique,
                column_names: vec!["namespace", "name"],
            }],
        },
        BootstrapTable {
            oid: Table::ATTRIBUTE,
            name: "nsql_attribute",
            columns: Column::bootstrap_column_info(),
            indexes: vec![BootstrapIndex {
                table: Table::ATTRIBUTE_NAME_UNIQUE_INDEX,
                name: "nsql_attribute_table_name_index",
                kind: IndexKind::Unique,
                column_names: vec!["table", "name"],
            }],
        },
        BootstrapTable {
            oid: Table::INDEX,
            name: "nsql_index",
            columns: Index::bootstrap_column_info(),
            indexes: vec![],
        },
        BootstrapTable {
            oid: Table::SEQUENCE,
            name: "nsql_sequence",
            columns: Sequence::bootstrap_column_info(),
            indexes: vec![],
        },
        BootstrapTable {
            oid: Table::FUNCTION,
            name: "nsql_function",
            columns: Function::bootstrap_column_info(),
            indexes: vec![BootstrapIndex {
                table: Table::FUNCTION_NAME_ARGS_UNIQUE_INDEX,
                name: "nsql_function_namespace_name_args_index",
                kind: IndexKind::Unique,
                column_names: vec!["namespace", "name", "args"],
            }],
        },
        BootstrapTable {
            oid: Table::OPERATOR,
            name: "nsql_operator",
            columns: Operator::bootstrap_column_info(),
            indexes: vec![BootstrapIndex {
                table: Table::OPERATOR_NAME_FN_UNIQUE_INDEX,
                name: "nsql_operator_namespace_name_function_index",
                kind: IndexKind::Unique,
                // unsure if this is a reasonable unique index?
                // postgres stores the types of the operator and has a unique index on the left and right types
                // but we just infer the type of the operator from it's function so we have a
                // constraint on the function instead.
                column_names: vec!["namespace", "name", "function"],
            }],
        },
    ]
    .into()
}
