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
use arrow::array::{StringArray, StringViewArray};
use arrow::datatypes::DataType;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_excalibur_macros::excalibur_function;
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDF};
use rand::distributions::Alphanumeric;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;

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

/// gen_arr(4096, 128, 0.1, 0.1, true) will generate a StringViewArray with
/// 4096 rows, each row containing a string with 128 random characters.
/// around 10% of the rows are null, around 10% of the rows are non-ASCII.
fn gen_string_array(
    n_rows: usize,
    str_len_chars: usize,
    null_density: f32,
    utf8_density: f32,
    is_string_view: bool, // false -> StringArray, true -> StringViewArray
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(42);
    let rng_ref = &mut rng;

    let corpus = "łęk 東京都 🗡️🔥".chars().collect::<Vec<_>>();

    let mut output_string_vec: Vec<Option<String>> = Vec::with_capacity(n_rows);
    for _ in 0..n_rows {
        let rand_num = rng_ref.gen::<f32>(); // [0.0, 1.0)
        if rand_num < null_density {
            output_string_vec.push(None);
        } else if rand_num < null_density + utf8_density {
            // Generate random UTF8 string
            let mut generated_string = String::with_capacity(str_len_chars);
            for _ in 0..str_len_chars {
                let char = corpus[rng_ref.gen_range(0..corpus.len())];
                generated_string.push(char);
            }
            output_string_vec.push(Some(generated_string));
        } else {
            // Generate random ASCII-only string
            let value = rng_ref
                .sample_iter(&Alphanumeric)
                .take(str_len_chars)
                .collect();
            let value = String::from_utf8(value).unwrap();
            output_string_vec.push(Some(value));
        }
    }

    if is_string_view {
        let string_view_array: StringViewArray = output_string_vec.into_iter().collect();
        Arc::new(string_view_array)
    } else {
        let string_array: StringArray = output_string_vec.into_iter().collect();
        Arc::new(string_array)
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
