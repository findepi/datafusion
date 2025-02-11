use crate::bridge::ExcaliburScalarUdf;
use crate::builder::{ExArrayBuilder, ExFullResultType};
use crate::reader::ExArrayReader;
use crate::types::ex_type::ExType;
use crate::types::ex_type_list::ExTypeList;
use arrow::array::{Array, ArrayRef, Int64Array};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_expr::ScalarFunctionArgs;
use datafusion_expr::ScalarUDFImpl;
use datafusion_expr::Signature;
use std::collections::VecDeque;

pub fn excalibur_invoke<T>(args: ScalarFunctionArgs) -> Result<ColumnarValue>
where
    T: ExcaliburScalarUdf,
    //T::ArgumentRustTypes: ExTypeList,
    T::ArgumentRustTypes: ApplyList,
    (T::OutArgRustType, T::ReturnRustType): ExFullResultType<
        BuilderType: ExArrayBuilder<
            OutArg = T::OutArgRustType,
            Return = T::ReturnRustType,
        >,
    >,
{
    let number_rows = args.number_rows;
    let mut args = args.args;
    assert_eq!(args.len(), T::SQL_ARGUMENT_COUNT as usize);
    let args = VecDeque::from(args);

    // handle no args

    let mut builder =
        <(T::OutArgRustType, T::ReturnRustType) as ExFullResultType>::builder_with_capacity(
            number_rows,
        );

    // T::ArgumentRustTypes::apply(args, &mut |arg_tuple_reader: &dyn ExArrayReader<
    //     ValueType = T::ArgumentRustTypes,
    // >| {
    //     for position in 0..number_rows {
    //         if arg_tuple_reader.is_valid(position) {
    //             let args = arg_tuple_reader.get(position);
    //             let mut out_arg: T::OutArgRustType = builder.get_out_arg(position);
    //             let result = T::invoke(args, &mut out_arg);
    //             builder.append(out_arg, result)?;
    //         } else {
    //             builder.append_null()?;
    //         }
    //     }
    //     Ok(())
    // })?;

    T::ArgumentRustTypes::apply(
        args,
        number_rows,
        // valid
        |_position: usize| true,
        // apply
        |_position, args, out_arg| T::invoke(args, out_arg),
        &mut builder,
    )?;

    let array = builder.build()?;
    Ok(ColumnarValue::Array(array))
}

pub trait ApplyList {
    // fn apply<F>(args: VecDeque<ColumnarValue>, f: F) -> Result<()>
    // where
    //     F: FnMut(&dyn ExArrayReader<ValueType = Self>) -> Result<()>;

    fn apply<Builder, Valid, Invoke>(
        args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: Fn(usize, Self, &mut Builder::OutArg) -> Builder::Return;
}

impl<Head, Tail> ApplyList for (Head, Tail)
where
    Head: ExType,
    Tail: ApplyList,
{
    fn apply<Builder, Valid, Invoke>(
        mut args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: Fn(usize, Self, &mut Builder::OutArg) -> Builder::Return,
    {
        let arg = args.pop_front().unwrap();
        match arg {
            ColumnarValue::Array(array) => {
                let reader = Head::read_array(array)?;
                Tail::apply(
                    args,
                    number_rows,
                    |position| valid(position) && reader.is_valid(position),
                    |position, tail_args, out_arg| {
                        let head_arg: Head = reader.get(position);
                        invoke(position, (head_arg, tail_args), out_arg)
                    },
                    builder,
                )
            }
            ColumnarValue::Scalar(scalar) => {
                let reader = Head::read_scalar(scalar)?;
                Tail::apply(
                    args,
                    number_rows,
                    |position| valid(position) && reader.is_valid(position),
                    |position, tail_args, out_arg| {
                        invoke(position, (reader.get(position), tail_args), out_arg)
                    },
                    builder,
                )
            }
        }
    }
}

impl ApplyList for () {
    fn apply<Builder, Valid, Invoke>(
        args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: Fn(usize, Self, &mut Builder::OutArg) -> Builder::Return,
    {
        assert!(args.is_empty());
        for position in 0..number_rows {
            if valid(position) {
                let mut out_arg: Builder::OutArg = builder.get_out_arg(position);
                let result = invoke(position, (), &mut out_arg);
                builder.append(out_arg, result)?;
            } else {
                builder.append_null()?;
            }
        }
        Ok(())
    }

    // fn apply<F>(args: VecDeque<ColumnarValue>, mut f: F) -> Result<()>
    // where
    //     F: FnMut(&dyn ExArrayReader<ValueType = Self>) -> Result<()>,
    // {
    //     assert!(args.is_empty());
    //     f(&NullReader {})
    // }
}

struct ReaderPair<'a, First, Second>
// where
//     First: ExArrayReader,
//     Second: ExArrayReader,
{
    first: &'a First,
    second: &'a Second,
}

impl<First, Second> ExArrayReader for ReaderPair<'_, First, Second>
where
    First: ExArrayReader,
    Second: ExArrayReader,
{
    type ValueType = (First::ValueType, Second::ValueType);

    fn is_valid(&self, position: usize) -> bool {
        self.first.is_valid(position) && self.second.is_valid(position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        (self.first.get(position), self.second.get(position))
    }
}

// //
// // ==================================== ARGUMENTS ====================================
// //
// trait ArgApplicabilitySupport {
//     fn apply<F>(reverse_args: Vec<ColumnarValue>, continuation: &mut F) -> Result<()>
//     where
//         F: FnMut(&dyn ExArrayReader<ValueType = Self>) -> Result<()>;
// }
//
// impl ArgApplicabilitySupport for () {
//     fn apply<F>(args: &[ColumnarValue], continuation: &mut F) -> Result<()>
//     where
//         F: FnMut(&dyn ExArrayReader<ValueType = Self>) -> Result<()>,
//     {
//         assert_eq!(args.len(), 0);
//         continuation(&NullReader {})
//     }
// }

// sentinel
struct NullReader {}
impl ExArrayReader for NullReader {
    type ValueType = ();

    fn is_valid(&self, position: usize) -> bool {
        true
    }

    fn get(&self, position: usize) -> Self::ValueType {
        ()
    }
}

// impl<Tail: ArgApplicabilitySupport> ArgApplicabilitySupport for (i64, Tail) {
//     fn apply<F>(args: &[ColumnarValue], continuation: &mut F) -> Result<()>
//     where
//         F: FnMut(&dyn ExArrayReader<ValueType = Self>) -> Result<()>,
//     {
//         match &args[0] {
//             ColumnarValue::Array(array) => {
//                 let array = array.as_any().downcast_ref::<Int64Array>().ok_or(
//                     DataFusionError::Execution(format!("Wrong array type: {:?}", array)),
//                 )?;
//                 //continuation(array, )
//                 Tail::apply(&args[1..], &mut |tail_reader| {
//                     //continuation(&(array, tail_reader));
//                     continuation(&ReaderPair {
//                         first: array,
//                         second: tail_reader,
//                     })
//                 })
//             }
//             scalar @ ColumnarValue::Scalar(scalar_value) => {
//                 // match scalar_value {
//                 //     ScalarValue::Int64(value) => {
//                 //         continuation(value, &args[1..])
//                 //     }
//                 //     _ => Err(DataFusionError::Execution(format!("Wrong scalar type: {:?}", scalar))),
//                 // }
//                 todo!()
//             }
//         }
//     }
// }
