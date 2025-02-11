use crate::builder::{ExArrayBuilder, ExFullResultType};
use arrow::array::{ArrayRef, PrimitiveBuilder};
use arrow::datatypes::{ArrowPrimitiveType, Int64Type, UInt64Type};
use datafusion_common::Result;
use std::sync::Arc;

macro_rules! arrayable_primitive {
    ($primitive_type:ty) => {
        impl ExFullResultType for ((), <$primitive_type as ArrowPrimitiveType>::Native) {
            type BuilderType = PrimitiveBuilder<$primitive_type>;

            fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
                Self::BuilderType::with_capacity(result_cardinality)
            }
        }
    };
}

// TODO
// arrayable_primitive!(Int64Type);
// arrayable_primitive!(UInt64Type);

impl ExFullResultType for ((), u64) {
    type BuilderType = PrimitiveBuilder<UInt64Type>;

    fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
        Self::BuilderType::with_capacity(number_rows)
    }
}

impl<T> ExArrayBuilder for PrimitiveBuilder<T>
where
    T: ArrowPrimitiveType,
{
    type OutArg = ();
    type Return = T::Native;

    fn get_out_arg(&mut self, _position: usize) -> () {
        ()
    }

    fn append(&mut self, _out_arg: (), fn_ret: T::Native) -> Result<()> {
        self.append_value(fn_ret);
        Ok(())
    }

    fn append_null(&mut self) -> Result<()> {
        self.append_null();
        Ok(())
    }

    fn build(mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.finish()))
    }
}
