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

use crate::{Type, TypeName, TypeNameRef, TypeParameter, TypeRef};
use std::any::Any;
use std::sync::Arc;

pub struct MapType {
    pub key_type: TypeRef,
    pub value_type: TypeRef,
    type_name: TypeNameRef,
}

impl MapType {
    pub const BASENAME: &'static str = "map";
    pub fn new(key_type: TypeRef, value_type: TypeRef) -> Self {
        let type_name = Arc::new(TypeName::new(
            Self::BASENAME,
            vec![
                TypeParameter::Type(key_type.name().clone()),
                TypeParameter::Type(value_type.name().clone()),
            ],
        ));
        Self {
            key_type,
            value_type,
            type_name,
        }
    }
}

impl Type for MapType {
    fn name(&self) -> &TypeNameRef {
        &self.type_name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
