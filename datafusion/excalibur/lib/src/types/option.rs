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

use crate::reader::ExArrayReader;
use crate::types::arg_type::ExArgType;
use crate::types::ret_type::ExRetType;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::ScalarValue;

impl<T> ExArgType for Option<T>
where
    T: ExArgType,
{
    type ArrayReaderType = NullableReader<T::ArrayReaderType>;
    type ScalarReaderType = NullableReader<T::ScalarReaderType>;

    fn logical_type() -> NativeType {
        T::logical_type()
    }

    fn read_array(array: ArrayRef) -> datafusion_common::Result<Self::ArrayReaderType> {
        Ok(NullableReader {
            delegate: T::read_array(array)?,
        })
    }

    fn read_scalar(
        scalar: ScalarValue,
    ) -> datafusion_common::Result<Self::ScalarReaderType> {
        Ok(NullableReader {
            delegate: T::read_scalar(scalar)?,
        })
    }
}

pub struct NullableReader<Delegate> {
    delegate: Delegate,
}

impl<Delegate> ExArrayReader for NullableReader<Delegate>
where
    Delegate: ExArrayReader,
{
    type ValueType = Option<Delegate::ValueType>;

    fn is_valid(&self, _position: usize) -> bool {
        true
    }

    fn get(&self, position: usize) -> Self::ValueType {
        if self.delegate.is_valid(position) {
            Some(self.delegate.get(position))
        } else {
            None
        }
    }
}

impl<T> ExRetType for Option<T>
where
    T: ExRetType,
{
    fn data_type() -> DataType {
        T::data_type()
    }
}
