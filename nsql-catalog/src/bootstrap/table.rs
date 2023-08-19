use super::*;

impl Table {
    mk_consts![
        NAMESPACE,
        TABLE,
        ATTRIBUTE,
        INDEX,
        FUNCTION,
        OPERATOR,
        NAMESPACE_NAME_UNIQUE_INDEX,
        TABLE_NAME_UNIQUE_INDEX,
        ATTRIBUTE_NAME_UNIQUE_INDEX,
        FUNCTION_NAME_ARGS_UNIQUE_INDEX,
        OPERATOR_NAME_LEFT_RIGHT_UNIQUE_INDEX
    ];
}

pub(super) struct BootstrapTable {
    pub oid: Oid<Table>,
    pub name: &'static str,
    pub columns: Vec<ColumnStorageInfo>,
    pub indexes: Vec<BootstrapIndex>,
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
            columns: Namespace::bootstrap_table_storage_info().columns,
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
            columns: Table::bootstrap_table_storage_info().columns,
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
            columns: Column::bootstrap_table_storage_info().columns,
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
            columns: Index::bootstrap_table_storage_info().columns,
            indexes: vec![],
        },
        BootstrapTable {
            oid: Table::FUNCTION,
            name: "nsql_function",
            columns: Function::bootstrap_table_storage_info().columns,
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
            columns: Operator::bootstrap_table_storage_info().columns,
            indexes: vec![BootstrapIndex {
                table: Table::OPERATOR_NAME_LEFT_RIGHT_UNIQUE_INDEX,
                name: "nsql_operator_namespace_name_l_r_index",
                kind: IndexKind::Unique,
                column_names: vec!["namespace", "name", "left", "right"],
            }],
        },
    ]
    .into()
}
