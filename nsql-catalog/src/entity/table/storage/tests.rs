use nsql_core::Oid;
use nsql_storage::tuple::FlatTuple;
use nsql_storage::value::Value;

use crate::Function;

#[test]
// we currently rely on this behaviour
fn serialization_of_tuple_is_the_same_as_vec_of_references() {
    let oid = Oid::<Function>::new(1);
    let v = Value::Oid(oid.untyped());
    let a = nsql_rkyv::to_bytes(&vec![&v]);
    let b = nsql_rkyv::to_bytes(&FlatTuple::new(vec![v]));
    assert_eq!(a.as_slice(), b.as_slice());
}
