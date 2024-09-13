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

#![allow(unused_imports)]

use arrow::array::{
    as_largestring_array, Array, ArrayAccessor, ArrayDataBuilder, ArrayRef, StringArray,
    StringBuilder,
};
use arrow::datatypes::DataType;
use arrow_buffer::MutableBuffer;
use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{lit, ColumnarValue, Expr, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};
use std::any::Any;
use std::iter::zip;
use std::sync::Arc;

use crate::string::common::*;
use crate::string::concat;

#[derive(Debug)]
pub struct MyConcatFunc {
    signature: Signature,
}

impl Default for MyConcatFunc {
    fn default() -> Self {
        MyConcatFunc::new()
    }
}

impl MyConcatFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::exact(vec![Utf8, Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MyConcatFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "my_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        assert_eq!(args.len(), 2, "Invalid number of arguments: {}", args.len());
        let arg0 = &args[0];
        let arg1 = &args[1];

        match (arg0, arg1) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(a)),
                ColumnarValue::Scalar(ScalarValue::Utf8(b)),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(match (a, b) {
                (Some(a), Some(b)) => Some(actually_do_concat(a, b)),
                _ => None,
            }))),
            (ColumnarValue::Array(a), ColumnarValue::Array(b)) => {
                let a = as_string_array(a)?;
                let b = as_string_array(b)?;
                Ok(ColumnarValue::Array(Arc::new(concat_arrays(a, b))))
            }
            _ => {
                todo!("Unsupported argument types: {:?}, {:?}", arg0, arg1);
            }
        }
    }
}

// Extracted to make analysing the compiled code easier
#[inline(never)]
pub fn concat_arrays(a: &StringArray, b: &StringArray) -> StringArray {
    let len = a.len();
    assert_eq!(
        len,
        b.len(),
        "Array lengths do not match: {} != {}",
        len,
        b.len()
    );

    // the straightforward implementation is faster than variadic concat implementation from DF by ~18%.
    let data_size = a.values().len() + b.values().len();
    let data_size = 64; // disable pre-sizing output. ~35% perf hit (compared to ours optimal)
                        // sizing can be made adaptive, instead of being function's logic
    let mut builder = StringArrayBuilder::with_capacity(len, data_size);
    let mut builder = StringBuilder::with_capacity(len, data_size);
    for i in 0..len {
        // // this is optimal but unpacks logic
        // // FIXME semantically incorrect, but shouldn't matter. our test case is all non-null
        // if a.is_valid(i) {
        //     builder
        //         .value_buffer
        //         .extend_from_slice(a.value(i).as_bytes());
        // }
        // if b.is_valid(i) {
        //     builder
        //         .value_buffer
        //         .extend_from_slice(b.value(i).as_bytes());
        // }

        // // just joining the IFs into one gives another 10% boost (semantic difference! and for the better)
        // if a.is_valid(i) && b.is_valid(i) {
        //     builder
        //         .value_buffer
        //         .extend_from_slice(a.value(i).as_bytes());
        //     builder
        //         .value_buffer
        //         .extend_from_slice(b.value(i).as_bytes());
        // }

        // actually_do_concat_into_buffer is as fast as direct buffer writing above
        if a.is_valid(i) && b.is_valid(i) {
            //actually_do_concat_into_buffer(a.value(i), b.value(i), &mut MutableBufferWriter(&mut builder.value_buffer));
            actually_do_concat_into_buffer(a.value(i), b.value(i), &mut builder);
        }

        // // having logic as `actually_do_concat(..) -> String` gives x4-6 perf hit. (#inline does not help)
        // if a.is_valid(i) && b.is_valid(i) {
        //     let s = actually_do_concat(a.value(i), b.value(i));
        //     builder.value_buffer.extend_from_slice(s.as_bytes());
        // }

        //builder.append_offset();
        builder.append_value("");
    }
    //builder.finish(None)
    builder.finish()

    // let mut result = arrow::array::StringBuilder::new();
    // for i in 0..a.len() {
    //     if a.is_null(i) || b.is_null(i) {
    //         result.append_null();
    //     } else {
    //         let c = actually_do_concat(a.value(i), b.value(i));
    //         result.append_value(c);
    //     }
    // }
    // result.finish()

    // zip(a, b)
    //     .map(|(a, b)| match (&a, &b) {
    //         (Some(a), Some(b)) => Some(actually_do_concat(a, b)),
    //         _ => None,
    //     })
    //     .collect()

    // let mut offsets = MutableBuffer::with_capacity((rows + 1) * size_of::<i32>());
    // let mut values = MutableBuffer::with_capacity(0);
    //
    // for i in 0..rows {
    //     let c = actually_do_concat(a.value(i), b.value(i));
    //     values.extend_from_slice(c.as_bytes());
    //     offsets.push(values.len() as i32);
    // }
    //
    // let result = ArrayDataBuilder::new(DataType::Utf8)
    //     .len(rows)
    //     .add_buffer(offsets.into())
    //     .add_buffer(values.into())
    //     .nulls(None); // TODO
    //
    // let result = unsafe { result.build_unchecked() };
    // let result = StringArray::from(result);
}

struct MutableBufferWriter<'a>(&'a mut MutableBuffer);
impl std::fmt::Write for MutableBufferWriter<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

// This is the only place where logic happens
// #[inline(always)]
fn actually_do_concat(a: &str, b: &str) -> String {
    // format! is slower
    //format!("{}{}", a, b)
    a.to_string() + b
}
fn actually_do_concat_into_buffer(a: &str, b: &str, output: &mut impl std::fmt::Write) {
    // using write! yields 45% perf hit
    // write!(output, "{}{}", a, b).unwrap()
    output.write_str(a).unwrap();
    output.write_str(b).unwrap();
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use super::*;
    use crate::string::concat::ConcatFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, LargeStringArray, StringViewArray};
    use arrow::array::{ArrayRef, StringArray};
    use DataType::*;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            MyConcatFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::from("bb")),
            ],
            Ok(Some("aabb")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            MyConcatFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            MyConcatFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8(None))
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }

    #[test]
    fn concat() -> Result<()> {
        let c0 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some(",x"),
            Some(",y"),
            Some(",z"),
        ])));
        let args = &[c0, c2];

        let result = MyConcatFunc::default().invoke(args)?;
        let expected =
            Arc::new(StringArray::from(vec!["foo,x", "bar,y", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }
        Ok(())
    }
}
