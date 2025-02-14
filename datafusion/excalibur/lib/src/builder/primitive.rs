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

use crate::builder::{ExArrayBuilder, ExFullResultType};
use arrow::array::{ArrayRef, PrimitiveBuilder};
use arrow::datatypes::{ArrowPrimitiveType, Int64Type, UInt64Type, Int32Type, UInt32Type, Int16Type, UInt16Type, Int8Type, UInt8Type};
use datafusion_common::Result;
use std::sync::Arc;

macro_rules! primitive_result_type {
    ($native_type:ty, $arrow_primitive_type:ty) => {
        impl ExFullResultType for ((), $native_type) {
            type BuilderType = PrimitiveBuilder<$arrow_primitive_type>;

            fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
                Self::BuilderType::with_capacity(number_rows)
            }
        }
    };
}

primitive_result_type!(i8, Int8Type);
primitive_result_type!(i16, Int16Type);
primitive_result_type!(i32, Int32Type);
primitive_result_type!(i64, Int64Type);

primitive_result_type!(u8, UInt8Type);
primitive_result_type!(u16, UInt16Type);
primitive_result_type!(u32, UInt32Type);
primitive_result_type!(u64, UInt64Type);

impl<T> ExArrayBuilder for PrimitiveBuilder<T>
where
    T: ArrowPrimitiveType,
{
    type OutArg = ();
    type Return = T::Native;

    fn get_out_arg(&mut self, _position: usize) {}

    fn append(&mut self, _out_arg: (), fn_ret: T::Native) -> Result<()> {
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
