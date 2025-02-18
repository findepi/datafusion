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

use crate::arg_type::{ExArgType, ExFindImplementation};
use crate::reader::{ExArrayReader, ExArrayReaderConsumer};
use arrow::array::{Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::types::NativeType;
use datafusion_common::ScalarValue;
use datafusion_common::{internal_err, Result};
use datafusion_expr::ColumnarValue;
use std::ptr::NonNull;

impl ExFindImplementation for dyn AsRef<str> {
    type Type<'a> = RefStrArgType;
}

pub struct RefStrArgType;

impl ExArgType for RefStrArgType {
    type StackType<'a> = &'a str;

    fn logical_type() -> NativeType {
        NativeType::String
    }

    fn decode(
        arg: ColumnarValue,
        consumer: impl for <'a> ExArrayReaderConsumer<ValueType<'a> = Self::StackType<'a>>,
    ) -> Result<()> {
        match arg {
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8 => consumer.consume(as_string_array(&array)?),
                DataType::Utf8View => consumer.consume(as_string_view_array(&array)?),
                dt => internal_err!("Expected string array, got {:?}", dt),
            },

            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
                consumer.consume(&ScalarString(value))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => {
                consumer.consume(&ScalarString(value))
            }

            ColumnarValue::Scalar(scalar) => {
                internal_err!("Expected string scalar, got {:?}", scalar)
            }
        }
    }
}

// TODO implement this in terms of GenericByteArray

impl<'a> ExArrayReader<'a> for &'a StringArray {
    type ValueType = &'a str;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(&self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        self.value(position)
    }
}

impl<'a> ExArrayReader<'a> for &'a StringViewArray {
      type ValueType = &'a str;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(&self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        (self.value(position).into())
    }
}

struct ScalarString(Option<String>);

impl<'a> ExArrayReader<'a> for &'a ScalarString {
   type ValueType = &'a str;

    fn is_valid(&self, _position: usize) -> bool {
        self.0.is_some()
    }

    fn get(&self, _position: usize) -> Self::ValueType {
        (self.0.as_deref().unwrap().into())
    }
}
