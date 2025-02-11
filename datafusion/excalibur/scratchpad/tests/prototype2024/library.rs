use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, Int64Array, PrimitiveBuilder,
};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

pub trait Invocable {
    const ARGUMENT_COUNT: u8;
    type ArgumentTypeList;
    type OutArgType;
    type FormalReturnType;

    fn invoke(
        &self,
        regular_args: Self::ArgumentTypeList,
        out_arg: &mut Self::OutArgType,
    ) -> Self::FormalReturnType;
}

pub fn lifted_invoke<F>(f: &F, args: &[ColumnarValue]) -> Result<ColumnarValue>
where
    F: Invocable,
    F::ArgumentTypeList: ArgApplicabilitySupport,
    (F::OutArgType, F::FormalReturnType): VectorizableResultType<
        BuilderType: VectorBuilder<
            OutArgType = F::OutArgType,
            FormalReturnType = F::FormalReturnType,
        >,
    >,
{
    assert_eq!(args.len(), F::ARGUMENT_COUNT as usize);

    // handle all-array case first
    let result_cardinality = result_cardinality(args);
    let result_cardinality = result_cardinality.unwrap(); // 😎

    // handle no args

    let mut builder = <(F::OutArgType, F::FormalReturnType) as VectorizableResultType>::builder_with_capacity(result_cardinality);

    F::ArgumentTypeList::apply(args, &mut |arg_tuple_reader| {
        for position in 0..result_cardinality {
            if arg_tuple_reader.is_valid(position) {
                let args = arg_tuple_reader.get(position);
                let mut out_arg: F::OutArgType = builder.get_out_arg(position);
                let result = f.invoke(args, &mut out_arg);
                builder.append(out_arg, result)?;
            } else {
                builder.append_null()?;
            }
        }
        Ok(())
    })?;

    let array = builder.build()?;
    Ok(ColumnarValue::Array(array))
}

fn result_cardinality(args: &[ColumnarValue]) -> Option<usize> {
    args.iter().find_map(|columnar_value| match columnar_value {
        ColumnarValue::Array(array) => Some(array.len()),
        ColumnarValue::Scalar(_) => None,
    })
}

//
// ==================================== ARGUMENTS ====================================
//
trait ArgApplicabilitySupport {
    fn apply<F>(args: &[ColumnarValue], continuation: &mut F) -> Result<()>
    where
        F: FnMut(&dyn VectorReader<ValueType = Self>) -> Result<()>;
}

impl ArgApplicabilitySupport for () {
    fn apply<F>(args: &[ColumnarValue], continuation: &mut F) -> Result<()>
    where
        F: FnMut(&dyn VectorReader<ValueType = Self>) -> Result<()>,
    {
        assert_eq!(args.len(), 0);
        continuation(&NullReader {})
    }
}
// sentinel
struct NullReader {}
impl VectorReader for NullReader {
    type ValueType = ();

    fn is_valid(&self, position: usize) -> bool {
        true
    }

    fn get(&self, position: usize) -> Self::ValueType {
        ()
    }
}

impl<Tail: ArgApplicabilitySupport> ArgApplicabilitySupport for (i64, Tail) {
    fn apply<F>(args: &[ColumnarValue], continuation: &mut F) -> Result<()>
    where
        F: FnMut(&dyn VectorReader<ValueType = Self>) -> Result<()>,
    {
        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = array.as_any().downcast_ref::<Int64Array>().ok_or(
                    DataFusionError::Execution(format!("Wrong array type: {:?}", array)),
                )?;
                //continuation(array, )
                Tail::apply(&args[1..], &mut |tail_reader| {
                    //continuation(&(array, tail_reader));
                    continuation(&ReaderPair {
                        first: array,
                        second: tail_reader,
                    })
                })
            }
            scalar @ ColumnarValue::Scalar(scalar_value) => {
                // match scalar_value {
                //     ScalarValue::Int64(value) => {
                //         continuation(value, &args[1..])
                //     }
                //     _ => Err(DataFusionError::Execution(format!("Wrong scalar type: {:?}", scalar))),
                // }
                todo!()
            }
        }
    }
}

trait VectorReader /*: Sized*/ {
    type ValueType;
    fn is_valid(&self, position: usize) -> bool;
    /// may panic on invalid position
    fn get(&self, position: usize) -> Self::ValueType;
}

struct ReaderPair<'a, A, B> {
    first: &'a dyn VectorReader<ValueType = A>,
    second: &'a dyn VectorReader<ValueType = B>,
}
impl<A, B> VectorReader for ReaderPair<'_, A, B> {
    type ValueType = (A, B);

    fn is_valid(&self, position: usize) -> bool {
        self.first.is_valid(position) && self.second.is_valid(position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        (self.first.get(position), self.second.get(position))
    }
}

// abomination TODO is tuple VectorReader screwed up?
impl<T: VectorReader> VectorReader for &T {
    type ValueType = T::ValueType;

    fn is_valid(&self, position: usize) -> bool {
        (*self).is_valid(position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        (*self).get(position)
    }
}

impl VectorReader for Int64Array {
    type ValueType = i64;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        self.value(position)
    }
}

// Scalar
impl VectorReader for Option<i64> {
    type ValueType = i64;

    fn is_valid(&self, _position: usize) -> bool {
        self.is_some()
    }

    fn get(&self, _position: usize) -> Self::ValueType {
        self.unwrap()
    }
}

//
// ==================================== RESULTS ====================================
//

trait VectorizableResultType {
    type BuilderType: VectorBuilder;
    fn builder_with_capacity(result_cardinality: usize) -> Self::BuilderType /*VectorBuilder<OutArgType, FormalReturnType>*/;
}
trait VectorBuilder {
    type OutArgType: Sized;
    type FormalReturnType: Sized;
    fn get_out_arg(&mut self, position: usize) -> Self::OutArgType;
    fn append(
        &mut self,
        out_arg: Self::OutArgType,
        fn_ret: Self::FormalReturnType,
    ) -> Result<()>;
    fn append_null(&mut self) -> Result<()>;
    fn build(self) -> Result<ArrayRef>;
}
// trait Allocatable {
//     fn new(result_cardinality: usize) -> Self;
// }

// generated with a macro, like ArrowPrimitiveType
impl VectorizableResultType for ((), i64) {
    type BuilderType = PrimitiveBuilder<Int64Type>;

    fn builder_with_capacity(result_cardinality: usize) -> Self::BuilderType {
        Self::BuilderType::with_capacity(result_cardinality)
    }
}

impl<T> VectorBuilder for PrimitiveBuilder<T>
where
    T: ArrowPrimitiveType,
{
    type OutArgType = ();
    type FormalReturnType = T::Native;

    fn get_out_arg(&mut self, _position: usize) -> () {
        ()
    }

    fn append(&mut self, _out_arg: (), fn_ret: Self::FormalReturnType) -> Result<()> {
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

impl<T> VectorizableResultType for ((), Result<T>)
where
    ((), T): VectorizableResultType,
{
    type BuilderType =
        ResultBuilderWithResultSupport<<((), T) as VectorizableResultType>::BuilderType>;

    fn builder_with_capacity(result_cardinality: usize) -> Self::BuilderType {
        Self::BuilderType {
            delegate: <((), T) as VectorizableResultType>::builder_with_capacity(
                result_cardinality,
            ),
        }
    }
}

struct ResultBuilderWithResultSupport<Delegate>
where
    Delegate: VectorBuilder,
{
    delegate: Delegate,
}
impl<Delegate> VectorBuilder for ResultBuilderWithResultSupport<Delegate>
where
    Delegate: VectorBuilder,
{
    type OutArgType = Delegate::OutArgType;
    type FormalReturnType = Result<Delegate::FormalReturnType>;

    fn get_out_arg(&mut self, position: usize) -> Self::OutArgType {
        self.delegate.get_out_arg(position)
    }

    fn append(
        &mut self,
        out_arg: Self::OutArgType,
        fn_ret: Self::FormalReturnType,
    ) -> Result<()> {
        self.delegate.append(out_arg, fn_ret?)
    }

    fn append_null(&mut self) -> Result<()> {
        self.delegate.append_null()
    }

    fn build(self) -> Result<ArrayRef> {
        self.delegate.build()
    }
}

impl<T> VectorizableResultType for ((), Option<T>)
where
    ((), T): VectorizableResultType,
{
    type BuilderType =
        ResultBuilderWithOptionSupport<<((), T) as VectorizableResultType>::BuilderType>;

    fn builder_with_capacity(result_cardinality: usize) -> Self::BuilderType {
        Self::BuilderType {
            delegate: <((), T) as VectorizableResultType>::builder_with_capacity(
                result_cardinality,
            ),
        }
    }
}

struct ResultBuilderWithOptionSupport<Delegate>
where
    Delegate: VectorBuilder,
{
    delegate: Delegate,
}
impl<Delegate> VectorBuilder for ResultBuilderWithOptionSupport<Delegate>
where
    Delegate: VectorBuilder,
{
    type OutArgType = Delegate::OutArgType;
    type FormalReturnType = Option<Delegate::FormalReturnType>;

    fn get_out_arg(&mut self, position: usize) -> Self::OutArgType {
        self.delegate.get_out_arg(position)
    }

    fn append(
        &mut self,
        out_arg: Self::OutArgType,
        fn_ret: Self::FormalReturnType,
    ) -> Result<()> {
        if fn_ret.is_some() {
            self.delegate.append(out_arg, fn_ret.unwrap())
        } else {
            self.append_null()
        }
    }

    fn append_null(&mut self) -> Result<()> {
        self.delegate.append_null()
    }

    fn build(self) -> Result<ArrayRef> {
        self.delegate.build()
    }
}
