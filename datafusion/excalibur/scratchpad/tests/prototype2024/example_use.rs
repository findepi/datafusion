use super::library::{lifted_invoke, Invocable};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, Signature};
use std::any::Any;
use std::sync::Arc;

// hand-written
#[derive(Debug)] // TODO or maybe inject this? or somehow else generate it?
#[derive(Default)] // TODO or maybe inject this? or somehow else generate it?
pub struct MyGcd {}
// hand-written
impl MyGcd {
    // hand-written
    pub fn call(x: i64, y: i64) -> Result<i64> {
        datafusion::functions::math::gcd::compute_gcd(x, y)
    }
}

// generated (syntactically derived)
impl datafusion::logical_expr::ScalarUDFImpl for MyGcd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "MyGcd" // just the struct name, should not matter
    }

    fn signature(&self) -> &Signature {
        todo!()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        todo!()
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        lifted_invoke::<MyGcd>(self, args)
    }
}

// generated (syntactically derived)
impl Invocable for MyGcd {
    const ARGUMENT_COUNT: u8 = 2;
    type ArgumentTypeList = (i64, (i64, ()));
    type OutArgType = ();
    type FormalReturnType = Result<i64>;

    fn invoke(
        &self,
        regular_args: Self::ArgumentTypeList,
        _out_arg: &mut Self::OutArgType,
    ) -> Self::FormalReturnType {
        let (a, (b, ())) = regular_args;
        MyGcd::call(a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test() {
        println!("Hello, world!");

        let args: Vec<ColumnarValue> = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![0, 3, 25, -16]))), // x
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![0, -2, 15, 8]))),  // y
        ];

        let scalar_function = MyGcd::default();
        let result = ScalarUDFImpl::invoke(&scalar_function, &args).expect("call failed");
        let result = match result {
            ColumnarValue::Array(r) => r,
            ColumnarValue::Scalar(_) => panic!("want a vector"),
        };
        let ints = as_int64_array(&result).expect("failed to initialize function gcd");

        assert_eq!(ints.len(), 4);
        assert_eq!(ints.value(0), 0);
        assert_eq!(ints.value(1), 1);
        assert_eq!(ints.value(2), 5);
        assert_eq!(ints.value(3), 8);
    }
}
