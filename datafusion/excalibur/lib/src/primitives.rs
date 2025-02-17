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

#[macro_export]
macro_rules! primitive_type {
    ($native_type:ty, $dt_option_name:ident, $array_type:ty, $as_array:ident) => {
        impl ExArgType for $native_type {
            fn logical_type() -> datafusion_common::types::NativeType {
                arrow::datatypes::DataType::$dt_option_name.into()
            }

            fn decode(
                arg: datafusion_expr::ColumnarValue,
                consumer: impl $crate::reader::ExArrayReaderConsumer<ValueType = Self>,
            ) -> Result<()> {
                use datafusion_expr::ColumnarValue::*;
                use datafusion_common::ScalarValue;
                match arg {
                    Array(array) => {
                        consumer.consume($as_array(&array)?)
                    }
                    Scalar(scalar) => {
                        if let ScalarValue::$dt_option_name(value) = scalar {
                            consumer.consume(value)
                        } else {
                            datafusion_common::internal_err!(
                                "Expected {} scalar, got: {:?}",
                                stringify!($native_type),
                                scalar
                            )
                        }
                    }
                }
            }
        }

        impl ExRetType for $native_type {
            fn data_type() -> DataType {
                DataType::$dt_option_name
            }
        }
    };
}
