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

use crate::types::arg_type::{ExArgType, ExArrayReaderConsumer};
use crate::types::ret_type::ExRetType;
use arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_boolean_array, as_int16_array, as_int32_array, as_int64_array, as_int8_array,
    as_uint16_array, as_uint32_array, as_uint64_array, as_uint8_array,
};
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;

macro_rules! primitive_type {
    ($native_type:ty, $dt_option_name:ident, $array_type:ty, $as_array:ident) => {
        impl ExArgType for $native_type {
            // type ArrayReaderType = $array_type;
            // type ScalarReaderType = Option<$native_type>;

            fn logical_type() -> NativeType {
                DataType::$dt_option_name.into()
            }

            // fn read_array(array: ArrayRef) -> Result<Self::ArrayReaderType> {
            //     Ok($as_array(&array)?
            //         // shallow clone of the array
            //         .clone())
            // }
            //
            // fn read_scalar(scalar: ScalarValue) -> Result<Self::ScalarReaderType> {
            //     if let ScalarValue::$dt_option_name(value) = scalar {
            //         Ok(value)
            //     } else {
            //         Err(DataFusionError::Internal(format!(
            //             "Could not cast scalar {:?} value to {} scalar",
            //             scalar,
            //             stringify!($native_type)
            //         )))
            //     }
            // }

            fn decode(
                arg: ColumnarValue,
                consumer: impl ExArrayReaderConsumer<ValueType = Self>,
            ) -> Result<()> {
                match arg {
                    ColumnarValue::Array(array) => {
                        let cast_array = $as_array(&array)?
                            // shallow clone of the array
                            .clone();
                        consumer.consume(cast_array)
                    }
                    ColumnarValue::Scalar(scalar) => {
                        if let ScalarValue::$dt_option_name(value) = scalar {
                            consumer.consume(value)
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "Could not cast scalar {:?} value to {} scalar",
                                scalar,
                                stringify!($native_type)
                            )))
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

primitive_type!(i8, Int8, Int8Array, as_int8_array);
primitive_type!(i16, Int16, Int16Array, as_int16_array);
primitive_type!(i32, Int32, Int32Array, as_int32_array);
primitive_type!(i64, Int64, Int64Array, as_int64_array);

primitive_type!(u8, UInt8, UInt8Array, as_uint8_array);
primitive_type!(u16, UInt16, UInt16Array, as_uint16_array);
primitive_type!(u32, UInt32, UInt32Array, as_uint32_array);
primitive_type!(u64, UInt64, UInt64Array, as_uint64_array);

primitive_type!(bool, Boolean, BooleanArray, as_boolean_array);
