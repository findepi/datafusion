use crate::builder::ExFullResultType;
use crate::invoke::ApplyList;
use crate::types::ex_type::ExType;
use crate::types::ex_type_list::ExTypeList;

///! Contract between the macro and the library

pub trait ExcaliburScalarUdf
// where
//     Self::ArgumentRustTypes: ExTypeList,
//     Self::ArgumentRustTypes: ApplyList,
//     Self::ReturnRustType: ExType,
//     (Self::OutArgRustType, Self::ReturnRustType): ExFullResultType,
{
    // for example "my_function"
    const SQL_NAME: &'static str;
    // for example 2 for my_function(a, b)
    const RUST_ARGUMENT_COUNT: u8;
    // for example 2 for my_function(a, b). This currently needs to be te same as RUST_ARGUMENT_COUNT
    const SQL_ARGUMENT_COUNT: u8;

    // for example (i32, (u64, ()) for my_function(a: i32, b: u64)
    // excludes the out arg
    type ArgumentRustTypes;
    // T for `&mut T` passed to the function or () is there is no out argument
    type OutArgRustType;
    // for example i32 for my_function(..) -> i32
    type ReturnRustType;

    fn invoke(
        regular_args: Self::ArgumentRustTypes,
        out_arg: &mut Self::OutArgRustType,
    ) -> Self::ReturnRustType;
}
