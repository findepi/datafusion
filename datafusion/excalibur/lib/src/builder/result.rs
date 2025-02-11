use super::{ExArrayBuilder, ExFullResultType};
use arrow::array::{ArrayRef, PrimitiveBuilder};
use arrow::datatypes::{ArrowPrimitiveType, UInt64Type};
use datafusion_common::Result;
use std::sync::Arc;

// impl<T> ExFullResultType for ((), Result<T>)
// where
//     ((), T): ExFullResultType,
// {
//     type BuilderType =
//         ResultBuilderWithResultSupport<<((), T) as ExFullResultType>::BuilderType>;
//
//     fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
//         Self::BuilderType {
//             delegate: <((), T) as ExFullResultType>::builder_with_capacity(
//                 number_rows,
//             ),
//         }
//     }
// }
//
// struct ResultBuilderWithResultSupport<Delegate>
// where
//     Delegate: ExArrayBuilder,
// {
//     delegate: Delegate,
// }
//
// impl<Delegate> ExArrayBuilder for ResultBuilderWithResultSupport<Delegate>
// where
//     Delegate: ExArrayBuilder,
// {
//     type OutArgRustType = Delegate::OutArgRustType;
//     type ReturnRustType = Result<Delegate::ReturnRustType>;
//
//     fn get_out_arg(&mut self, position: usize) -> Self::OutArgRustType {
//         self.delegate.get_out_arg(position)
//     }
//
//     fn append(
//         &mut self,
//         out_arg: Self::OutArgRustType,
//         fn_ret: Self::ReturnRustType,
//     ) -> Result<()> {
//         self.delegate.append(out_arg, fn_ret?)
//     }
//
//     fn append_null(&mut self) -> Result<()> {
//         self.delegate.append_null()
//     }
//
//     fn build(self) -> Result<ArrayRef> {
//         self.delegate.build()
//     }
// }
