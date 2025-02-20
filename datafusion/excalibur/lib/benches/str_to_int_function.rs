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

extern crate criterion;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_excalibur_macros::excalibur_function;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDF};
use datafusion_expr_common::columnar_value::ColumnarValue;
use helper::gen_string_array;
use std::sync::Arc;

mod helper;

#[excalibur_function(name = "excalibur_character_length")]
fn character_length(s: &str) -> i32 {
    s.chars().count() as i32
}

fn benchmark_character_length(c: &mut Criterion) {
    let standard_impl = datafusion_functions::unicode::character_length();
    let excalibur_impl = Arc::new(ScalarUDF::new_from_shared_impl(
        excalibur_character_length_udf(),
    ));

    // All benches are single batch run with 8192 rows
    let n_rows = 8192;
    // Single global group makes criterion draw all charts on single HTML page in the report
    let mut c = c.benchmark_group("the group");
    for (nulls, str_len) in [(false, 8), (true, 8), (true, 32), (true, 128), (true, 4096)]
    {
        for input_string_view in [false, true] {
            for ascii in [true, false] {
                let input = gen_string_array(
                    n_rows,
                    str_len,
                    if nulls { 0.1 } else { 0.0 },
                    if ascii { 0.0 } else { 0.5 },
                    input_string_view,
                );
                let group = format!(
                    "{:7} {:8} {:10}",
                    if nulls { "nulls" } else { "non-null" },
                    if ascii { "ascii" } else { "unicode" },
                    if input_string_view {
                        "StringView"
                    } else {
                        "String"
                    },
                );
                for (name, func) in [
                    ("standard 🏢", &standard_impl),
                    ("excalibur 🗡️", &excalibur_impl),
                ] {
                    c.bench_function(
                        BenchmarkId::new(format!("{group}/{name}"), &str_len),
                        |b| b.iter(|| black_box(func.invoke_with_args(to_args(&input)))),
                    );
                }
            }
        }
    }
}

fn to_args(array: &ArrayRef) -> ScalarFunctionArgs<'_> {
    ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(array))],
        number_rows: array.len(),
        return_type: &DataType::Int32,
    }
}

criterion_group!(benches, benchmark_character_length);
criterion_main!(benches);
