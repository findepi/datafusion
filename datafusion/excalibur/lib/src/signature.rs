use crate::bridge::ExcaliburScalarUdf;
use crate::types::ex_type::ExType;
use crate::types::ex_type_list::ExTypeList;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{Signature, Volatility};

pub fn create_excalibur_signature<T>() -> ExcaliburSignature
where
    T: ExcaliburScalarUdf,
    T::ArgumentRustTypes: ExTypeList,
    T::ReturnRustType: ExType,
{
    ExcaliburSignature {
        signature: Signature::coercible(
            T::ArgumentRustTypes::type_signature(),
            Volatility::Immutable,
        ),
        return_type: T::ReturnRustType::data_type(),
    }
}

pub struct ExcaliburSignature {
    // For now just constant signature
    signature: Signature,
    return_type: DataType,
}

impl ExcaliburSignature {
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    pub fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
}
