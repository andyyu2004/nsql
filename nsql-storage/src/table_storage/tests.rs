use nsql_core::schema::{Attribute, LogicalType};
use rust_decimal::Decimal;

use super::*;
use crate::tuple::{Literal, Value};

#[test]
fn serde_tuple_page() -> Result<()> {
    nsql_test::start(async {
        let schema = Arc::new(Schema::new(vec![
            Attribute::new("a", LogicalType::Bool),
            Attribute::new("b", LogicalType::Decimal),
            Attribute::new("c", LogicalType::Bool),
        ]));

        let ctx = TupleDeserializationContext { schema };
        let mut page = HeapTuplePage::default();
        page.insert(Tuple::from(vec![
            Value::Literal(Literal::Bool(true)),
            Value::Literal(Literal::Decimal(Decimal::new(42, 17))),
            Value::Literal(Literal::Bool(false)),
        ]))
        .await?;

        let mut buf = vec![];
        page.serialize(&mut buf).await?;

        let actual = HeapTuplePage::deserialize_with(&ctx, &mut &buf[..]).await?;
        assert_eq!(page, actual);

        Ok(())
    })
}
