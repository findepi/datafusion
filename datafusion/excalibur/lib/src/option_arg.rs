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

use crate::arg_type::{ExArgType};
use crate::reader::{ExArrayReader, ExArrayReaderConsumer};
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;

impl<T> ExArgType for Option<T>
where
    T: ExArgType,
{
    fn logical_type() -> NativeType {
        T::logical_type()
    }

    fn decode(
        arg: ColumnarValue,
        consumer: impl ExArrayReaderConsumer<ValueType = Self>,
    ) -> Result<()> {
        let consumer = NullableConsumer { delegate: consumer };
        T::decode(arg, consumer)
    }
}

struct NullableConsumer<Delegate> {
    delegate: Delegate,
}

impl<T, Delegate> ExArrayReaderConsumer for NullableConsumer<Delegate>
where
    Delegate: ExArrayReaderConsumer<ValueType = Option<T>>,
{
    type ValueType = T;

    fn consume<AR>(self, reader: AR) -> Result<()>
    where
        AR: ExArrayReader<ValueType = Self::ValueType>,
    {
        let NullableConsumer { delegate } = self;

        let reader = NullableReader { delegate: reader };
        delegate.consume(reader)
    }
}

struct NullableReader<Delegate> {
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

// Generic reader for scalar values
impl<T> ExArrayReader for Option<T>
where
    T: Copy,
{
    type ValueType = T;

    fn is_valid(&self, _position: usize) -> bool {
        self.is_some()
    }

    fn get(&self, _position: usize) -> Self::ValueType {
        self.unwrap()
    }
}
