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

use crate::array::ArrayType;
use crate::{
    BuiltinPrimitiveType, MapType, Type, TypeNameRef, TypeParameter, TypeRef,
};
use datafusion_common::{DataFusionError, Result};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

// TODO doesn't belong to the types crate, move to main maybe?
pub trait TypeProvider {
    fn resolve(
        &self,
        type_name: &TypeNameRef,
        resolved_parameter_types: &[Option<TypeRef>],
    ) -> Result<TypeRef>;

    fn common_super_type(&self, a: &TypeRef, b: &TypeRef) -> Result<Option<TypeRef>>;
}

// TODO doesn't belong to the types crate, move to main maybe?
pub struct TypeManager {
    provider: HashMap<String, Arc<dyn TypeProvider>>,
}

impl TypeManager {
    pub fn new() -> Self {
        Self {
            provider: HashMap::new(),
        }
    }

    pub fn create_with_builtin_types() -> Self {
        let mut type_manager = TypeManager::new();
        BuiltinTypeProvider::create_and_register(&mut type_manager)
            .expect("Registering built-in types in empty TypeManager should not fail");
        type_manager
    }

    pub fn register_provider(
        &mut self,
        basename: impl Into<String>,
        resolver: &Arc<dyn TypeProvider>,
    ) -> Result<()> {
        let basename = basename.into();
        match self.provider.entry(basename.clone()) {
            Entry::Occupied(_) => Err(DataFusionError::Configuration(format!(
                "Resolver for basename '{}' is already registered",
                basename
            ))),
            Entry::Vacant(v) => {
                v.insert(resolver.clone());
                Ok(())
            }
        }
    }

    pub fn resolve(&self, type_name: &TypeNameRef) -> Result<TypeRef> {
        match self.provider.get(type_name.basename()) {
            None => Err(DataFusionError::Configuration(format!(
                "No resolver for basename '{}' is registered",
                type_name.basename()
            ))),
            Some(resolver) => {
                let mut resolved_parameter_types = Vec::new();
                for parameter in type_name.parameters() {
                    resolved_parameter_types.push(match parameter {
                        TypeParameter::Type(parameter_type_name) => {
                            Some(self.resolve(parameter_type_name)?)
                        }
                        TypeParameter::Number(_) => None,
                    });
                }
                resolver.resolve(type_name, &resolved_parameter_types)
            }
        }
    }

    fn common_super_type(&self, a: &TypeRef, b: &TypeRef) -> Result<Option<TypeRef>> {
        // TODO
        Ok(None)
    }
}

pub struct BuiltinTypeProvider {}

impl BuiltinTypeProvider {
    // private
    pub fn create_and_register(type_manager: &mut TypeManager) -> Result<()> {
        let resolver = &(Arc::new(BuiltinTypeProvider {}) as Arc<dyn TypeProvider>);
        for builtin in BuiltinPrimitiveType::all() {
            type_manager.register_provider(builtin.name().basename(), resolver)?;
        }
        type_manager.register_provider(ArrayType::BASENAME, resolver)?;
        type_manager.register_provider(MapType::BASENAME, resolver)?;
        Ok(())
    }
}

impl TypeProvider for BuiltinTypeProvider {
    fn resolve(
        &self,
        type_name: &TypeNameRef,
        resolved_parameter_types: &[Option<TypeRef>],
    ) -> Result<TypeRef> {
        assert_eq!(type_name.parameters().len(), resolved_parameter_types.len());
        if type_name.parameters().is_empty() {
            for builtin in BuiltinPrimitiveType::all() {
                if *builtin.name() == *type_name {
                    return Ok(builtin.clone());
                }
            }
        }
        let parameters = type_name.parameters();
        if type_name.basename() == ArrayType::BASENAME && parameters.len() == 1 {
            if let Some(element_type) = &resolved_parameter_types[0] {
                return Ok(Arc::new(ArrayType::new(element_type.clone())));
            }
        }
        if type_name.basename() == MapType::BASENAME && parameters.len() == 2 {
            if let (Some(key_type), Some(value_type)) =
                (&resolved_parameter_types[0], &resolved_parameter_types[1])
            {
                return Ok(Arc::new(MapType::new(key_type.clone(), value_type.clone())));
            }
        }
        Err(DataFusionError::Configuration(format!(
            "Unknown type name: {}",
            type_name
        )))
    }

    fn common_super_type(&self, a: &TypeRef, b: &TypeRef) -> Result<Option<TypeRef>> {
        let a_builtin = a.as_any().downcast_ref::<BuiltinPrimitiveType>();
        let b_builtin = b.as_any().downcast_ref::<BuiltinPrimitiveType>();
        // TODO
        Ok(None)
    }
}
