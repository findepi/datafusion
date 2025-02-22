// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Decimal128Array, Int64Array};
use arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::ScalarValue;
use datafusion_common::{exec_datafusion_err, Result};
use datafusion_excalibur::Decimal;
use datafusion_excalibur_macros::excalibur_function;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, Signature, TypeSignatureClass, Volatility,
};
use datafusion_expr_common::signature::Coercion;
use std::sync::Arc;

#[excalibur_function]
fn to_whole_decimal(s: &str) -> Result<Decimal<i128>> {
    let unscaled_value = s
        .parse::<i128>()
        .map_err(|e| exec_datafusion_err!("Failed to parse string as integer: {}", e))?;
    Ok(Decimal {
        unscaled_value,
        precision: 38,
        scale: 0,
    })
}

#[test]
fn test_function_signature() {
    let udf = to_whole_decimal_udf();
    assert_eq!(udf.name(), "to_whole_decimal");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    // TODO this is bogus
                    NativeType::Decimal(0, 0)
                ))),
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    // TODO this is bogus
                    NativeType::Decimal(0, 0)
                ))),
            ],
            Volatility::Immutable
        )
    );
    let return_type = udf
        .return_type(&[DataType::Int32, DataType::UInt32])
        .unwrap();
    assert_eq!(return_type, DataType::Int64);
}

#[test]
fn test_invoke_array() {
    let udf = to_whole_decimal_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(Decimal128Array::from(vec![
            1000, 2, 3000, -4, 5000,
        ]))),
        ColumnarValue::Array(Arc::new(Decimal128Array::from(vec![5, 111, 3000, 0, 13]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 5,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &Int64Array::from(vec![1005, 113, 6000, -4, 5013])
    );
}

#[test]
fn test_invoke_array_with_nulls() {
    let udf = to_whole_decimal_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(Decimal128Array::from(vec![
            None,
            Some(2),
            Some(3000),
            Some(-4),
            Some(5000),
        ]))),
        ColumnarValue::Array(Arc::new(Decimal128Array::from(vec![
            Some(5),
            Some(111),
            None,
            Some(0),
            Some(13),
        ]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 5,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &Int64Array::from(vec![None, Some(113), None, Some(-4), Some(5013)])
    );
}

#[test]
fn test_invoke_scalar() {
    let udf = to_whole_decimal_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(-3), 10, 0)),
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(55), 10, 0)),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &Int64Array::from(vec![52]));
}

#[test]
fn test_invoke_scalar_null() {
    let udf = to_whole_decimal_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(-3), 10, 0)),
        ColumnarValue::Scalar(ScalarValue::Decimal128(None, 10, 0)),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &Int64Array::from(vec![None]));
}
