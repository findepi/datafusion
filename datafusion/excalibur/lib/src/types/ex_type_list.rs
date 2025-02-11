use crate::types::ex_type::ExType;
use datafusion_expr::{ColumnarValue, TypeSignatureClass};
use std::sync::Arc;

pub trait ExTypeList {
    fn type_signature() -> Vec<TypeSignatureClass>;
}

impl ExTypeList for () {
    fn type_signature() -> Vec<TypeSignatureClass> {
        vec![]
    }
}

impl<Head, Tail> ExTypeList for (Head, Tail)
where
    Head: ExType,
    Tail: ExTypeList,
{
    fn type_signature() -> Vec<TypeSignatureClass> {
        let mut signature =
            vec![TypeSignatureClass::Native(Arc::new(Head::logical_type()))];
        signature.extend(Tail::type_signature());
        signature
    }
}
