use crate::builder::{ExArrayBuilder, ExFullResultType};
use arrow::array::ArrayRef;
use datafusion_common::Result;

// impl<T> ExFullResultType for ((), Option<T>)
// where
//     ((), T): ExFullResultType,
// {
//     type BuilderType =
//         ResultBuilderWithOptionSupport<<((), T) as ExFullResultType>::BuilderType>;
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
// struct ResultBuilderWithOptionSupport<Delegate>
// where
//     Delegate: ExArrayBuilder,
// {
//     delegate: Delegate,
// }
//
// impl<Delegate> ExArrayBuilder for ResultBuilderWithOptionSupport<Delegate>
// where
//     Delegate: ExArrayBuilder,
// {
//     type OutArgRustType = Delegate::OutArgRustType;
//     type ReturnRustType = Option<Delegate::ReturnRustType>;
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
//         if fn_ret.is_some() {
//             self.delegate.append(out_arg, fn_ret.unwrap())
//         } else {
//             self.append_null()
//         }
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
