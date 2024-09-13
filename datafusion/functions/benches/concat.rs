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

use arrow::array::ArrayRef;
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

fn create_args(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array1 = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    let array2 = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    vec![
        ColumnarValue::Array(Arc::clone(&array1) as ArrayRef),
        ColumnarValue::Array(Arc::clone(&array2) as ArrayRef),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    use datafusion_expr::ScalarUDFImpl;
    let concat = datafusion_functions::string::concat::ConcatFunc::default();
    let my_concat = datafusion_functions::string::my_concat::MyConcatFunc::default();

    for size in [1024, 4096, 8192] {
        let args = create_args(size, 32);
        let mut group = c.benchmark_group("benchmark_group");

        group.bench_with_input(
            BenchmarkId::new("concat", size),
            &args,
            |b, args| b.iter(|| criterion::black_box(/**/ my_concat.invoke(args).unwrap())),
        );

        // group.bench_with_input(
        //     BenchmarkId::new("my_concat", size),
        //     &args,
        //     |b, args| b.iter(|| criterion::black_box(my_concat.invoke(args).unwrap())),
        // );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
