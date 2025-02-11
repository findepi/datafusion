use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::ScalarValue;
use datafusion_excalibur_macros::excalibur_function;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, Signature, TypeSignatureClass, Volatility,
};
use std::sync::Arc;

#[excalibur_function]
fn add_one(a: u64) -> u64 {
    a + 1
}

#[test]
fn simple_function_signature() {
    let udf = add_one_udf();
    assert_eq!(udf.name(), "add_one");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![TypeSignatureClass::Native(Arc::new(NativeType::UInt64))],
            Volatility::Immutable
        )
    );
    let return_type = udf.return_type(&[DataType::UInt64]).unwrap();
    assert_eq!(return_type, DataType::UInt64);
}

#[test]
fn simple_function_invoke_array() {
    let udf = add_one_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(UInt64Array::from(vec![
        1000, 2000, 3000, 4000, 5000,
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 5,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &UInt64Array::from(vec![1001, 2001, 3001, 4001, 5001])
    );
}

#[test]
fn simple_function_invoke_scalar() {
    let udf = add_one_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::UInt64(Some(1000)))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &UInt64Array::from(vec![1001]));
}
