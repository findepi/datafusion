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

mod bridge;
mod builder;
mod invoke;
mod reader;
mod scalar_udf;
mod signature;
mod string;
mod types;

// Not public API.
#[doc(hidden)]
pub mod __private {
    // Re-exports used by the macros.

    pub use crate::bridge::ExcaliburScalarUdf;
    pub use crate::scalar_udf::create_excalibur_scalar_udf;
    pub use crate::types::arg_type::FindExArgType;
    pub use datafusion_expr::ScalarUDFImpl;
}
