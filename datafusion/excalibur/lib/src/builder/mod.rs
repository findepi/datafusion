mod option;
mod primitive;
mod result;

use arrow::array::ArrayRef;
use datafusion_common::Result;

pub trait ExFullResultType {
    type BuilderType: ExArrayBuilder;
    fn builder_with_capacity(number_rows: usize) -> Self::BuilderType;
}

pub trait ExArrayBuilder {
    type OutArg;
    type Return;

    fn get_out_arg(&mut self, position: usize) -> Self::OutArg;

    fn append(&mut self, out_arg: Self::OutArg, fn_ret: Self::Return) -> Result<()>;

    fn append_null(&mut self) -> Result<()>;

    fn build(self) -> Result<ArrayRef>;
}
