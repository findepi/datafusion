use crate::reader::ExArrayReader;
use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_uint64_array;
use datafusion_common::types::NativeType;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use std::any::type_name;

pub trait ExType {
    type ArrayReaderType: ExArrayReader<ValueType = Self>;
    type ScalarReaderType: ExArrayReader<ValueType = Self>;

    fn logical_type() -> NativeType {
        Self::data_type().into()
    }

    fn data_type() -> DataType;

    fn read_array(array: ArrayRef) -> Result<Self::ArrayReaderType>;

    fn read_scalar(scalar: ScalarValue) -> Result<Self::ScalarReaderType>;
}

impl ExType for u64 {
    type ArrayReaderType = UInt64Array;
    type ScalarReaderType = Option<u64>;

    fn data_type() -> DataType {
        DataType::UInt64
    }

    fn read_array(array: ArrayRef) -> Result<Self::ArrayReaderType> {
        Ok(as_uint64_array(&array)?
            // shallow clone of the array
            .clone())
    }

    fn read_scalar(scalar: ScalarValue) -> Result<Self::ScalarReaderType> {
        if let ScalarValue::UInt64(value) = scalar {
            Ok(value)
        } else {
            Err(DataFusionError::Internal(format!(
                "Could not cast scalar {:?} value to {}",
                scalar,
                type_name::<Self>()
            )))
        }
    }
}
