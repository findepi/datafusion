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

use crate::r#type::Type;
use crate::type_name::TypeName;
use crate::TypeNameRef;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Debug, EnumIter, PartialEq, Eq, Hash, Clone)]
pub enum BuiltinPrimitiveType {
    /// The type of bare NULL, corresponds to [`DataType::Null`].
    Unknown,
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
}

// TODO use flat array
static BUILTIN_TYPES: OnceLock<Vec<Arc<BuiltinPrimitiveType>>> = OnceLock::new();

impl BuiltinPrimitiveType {
    // private
    fn basename(&self) -> &str {
        match self {
            BuiltinPrimitiveType::Unknown => "unknown",
            BuiltinPrimitiveType::Boolean => "boolean",
            BuiltinPrimitiveType::Int8 => "int8",
            BuiltinPrimitiveType::Int16 => "in16",
            BuiltinPrimitiveType::Int32 => "int32",
            BuiltinPrimitiveType::Int64 => "int64",
            BuiltinPrimitiveType::UInt8 => "uint8",
            BuiltinPrimitiveType::UInt16 => "uint16",
            BuiltinPrimitiveType::UInt32 => "uint32",
            BuiltinPrimitiveType::UInt64 => "uint64",
            BuiltinPrimitiveType::Float16 => "float16",
            BuiltinPrimitiveType::Float32 => "float32",
            BuiltinPrimitiveType::Float64 => "float64",
        }
    }

    pub(crate) fn all() -> &'static [Arc<BuiltinPrimitiveType>] {
        BUILTIN_TYPES.get_or_init(|| {
            BuiltinPrimitiveType::iter()
                .map(|builtin| Arc::new(builtin.clone()))
                .collect()
        })
    }
}

// TODO use flat array
static BUILTIN_TYPE_NAMES: OnceLock<HashMap<BuiltinPrimitiveType, TypeNameRef>> =
    OnceLock::new();

impl Type for BuiltinPrimitiveType {
    fn name(&self) -> &TypeNameRef {
        let map = BUILTIN_TYPE_NAMES.get_or_init(|| {
            let mut map = HashMap::new();
            for builtin in BuiltinPrimitiveType::iter() {
                let type_name = Arc::new(TypeName::from_basename(builtin.basename()));
                map.insert(builtin, type_name);
            }
            map
        });
        &map[self]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
