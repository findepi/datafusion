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





use crate::types::arg_type::{ExArgType, FindExArgType};
use arrow::array::{Array, ArrayRef, StringArray};
use datafusion_common::cast::as_string_array;
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use std::any::type_name;
use std::ptr::NonNull;
use crate::reader::ExArrayReader;

impl FindExArgType for dyn AsRef<str> {
    type Type = ImplAsRefStr;
}

pub struct ImplAsRefStr(NonNull<str>);

impl AsRef<str> for ImplAsRefStr {
    fn as_ref(&self) -> &str {
        unsafe { self.0.as_ref() }
    }
}

impl ExArgType for ImplAsRefStr {
    type ArrayReaderType = StringArray;
    type ScalarReaderType = ScalarString;

    fn logical_type() -> NativeType {
        NativeType::String
    }

    fn read_array(array: ArrayRef) -> Result<Self::ArrayReaderType> {
        Ok(as_string_array(&array)?
            // shallow clone of the array
            .clone())
    }

    fn read_scalar(scalar: ScalarValue) -> Result<Self::ScalarReaderType> {
        if let ScalarValue::Utf8(value) = scalar {
            Ok(ScalarString(value))
        } else {
            Err(DataFusionError::Internal(format!(
                "Could not cast scalar {:?} value to Utf8 scalar",
                scalar,
            )))
        }
    }
}


// TODO implement this in terms of GenericByteArray

impl ExArrayReader for StringArray {
    type ValueType = ImplAsRefStr;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(&self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        ImplAsRefStr(self.value(position).into())
    }
}

pub struct ScalarString(Option<String>);

impl ExArrayReader for ScalarString {
    type ValueType = ImplAsRefStr;

    fn is_valid(&self, position: usize) -> bool {
        self.0.is_some()
    }

    fn get(&self, position: usize) -> Self::ValueType {
        ImplAsRefStr(self.0.as_deref().unwrap().into())
    }
}