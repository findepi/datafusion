use crate::builder::ExArrayBuilder;
use crate::reader::ExArrayReader;
use arrow::array::{
    Array, ArrayAccessor, ArrowPrimitiveType, Int64Array, PrimitiveArray,
    PrimitiveBuilder,
};
use arrow::datatypes::Int64Type;

impl<T> ExArrayReader for PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    type ValueType = T::Native;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        self.value(position)
    }
}
