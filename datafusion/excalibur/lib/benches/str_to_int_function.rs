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
use criterion::{black_box, criterion_group, criterion_main, Criterion};
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

fn criterion_benchmark(c: &mut Criterion) {
    let standard_impl = datafusion_functions::unicode::character_length();
    let excalibur_impl = Arc::new(ScalarUDF::new_from_shared_impl(excalibur_character_length_udf()));

    // All benches are single batch run with 8192 rows
    let n_rows = 8192;
    for str_len in [8, 32, 128, 4096] {
        // StringArray ASCII only
        let input = gen_string_array(n_rows, str_len, 0.1, 0.0, false);
        let mut group = c.benchmark_group(format!("ascii   {str_len} String"));
        group.bench_function("standard_impl", |b| {
            b.iter(|| black_box(standard_impl.invoke_with_args(to_args(&input))))
        });
        group.bench_function("excalibur_impl", |b| {
            b.iter(|| black_box(excalibur_impl.invoke_with_args(to_args(&input))))
        });
        group.finish();

        // StringArray UTF8
        let input = gen_string_array(n_rows, str_len, 0.1, 0.5, false);
        let mut group = c.benchmark_group(format!("unicode {str_len} String"));
        group.bench_function("standard_impl", |b| {
            b.iter(|| black_box(standard_impl.invoke_with_args(to_args(&input))))
        });
        group.bench_function("excalibur_impl", |b| {
            b.iter(|| black_box(excalibur_impl.invoke_with_args(to_args(&input))))
        });
        group.finish();

        // StringViewArray ASCII only
        let input = gen_string_array(n_rows, str_len, 0.1, 0.0, false);
        let mut group = c.benchmark_group(format!("ascii   {str_len} StringView"));
        group.bench_function("standard_impl", |b| {
            b.iter(|| black_box(standard_impl.invoke_with_args(to_args(&input))))
        });
        group.bench_function("excalibur_impl", |b| {
            b.iter(|| black_box(excalibur_impl.invoke_with_args(to_args(&input))))
        });
        group.finish();

        // StringViewArray UTF8
        let input = gen_string_array(n_rows, str_len, 0.1, 0.5, false);
        let mut group = c.benchmark_group(format!("unicode {str_len} StringView"));
        group.bench_function("standard_impl", |b| {
            b.iter(|| black_box(standard_impl.invoke_with_args(to_args(&input))))
        });
        group.bench_function("excalibur_impl", |b| {
            b.iter(|| black_box(excalibur_impl.invoke_with_args(to_args(&input))))
        });
        group.finish();
    }
}

fn to_args(array: &ArrayRef) -> ScalarFunctionArgs<'_> {
    ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(array))],
        number_rows: array.len(),
        return_type: &DataType::Int32,
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
