use rust_decimal::Decimal;

use super::*;
use crate::tuple::{AttributeSpec, Literal, PhysicalType, Schema, Value};

#[derive(Debug, PartialEq)]
struct TestSchema {
    attributes: Vec<TestAttribute>,
}

impl Schema for TestSchema {
    fn attributes(&self) -> Box<dyn ExactSizeIterator<Item = &dyn AttributeSpec> + '_> {
        Box::new(self.attributes.iter().map(|attr| attr as &dyn AttributeSpec))
    }
}

#[derive(Debug, PartialEq)]
struct TestAttribute {
    ty: PhysicalType,
}

impl AttributeSpec for TestAttribute {
    fn physical_type(&self) -> &PhysicalType {
        &self.ty
    }
}

#[test]
fn serde_tuple_page() -> Result<()> {
    nsql_test::start(async {
        let schema = &TestSchema {
            attributes: vec![
                TestAttribute { ty: PhysicalType::Bool },
                TestAttribute { ty: PhysicalType::Decimal },
                TestAttribute { ty: PhysicalType::Bool },
            ],
        };
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
