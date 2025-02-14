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

use crate::bridge::ExcaliburScalarUdf;
use crate::builder::{ExArrayBuilder, ExFullResultType};
use crate::reader::ExArrayReader;
use crate::types::arg_type::ExArgType;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_expr::ScalarFunctionArgs;
use std::collections::VecDeque;

pub fn excalibur_invoke<T>(args: ScalarFunctionArgs) -> Result<ColumnarValue>
where
    T: ExcaliburScalarUdf,
    T::ArgumentRustTypes: ApplyList,
    (T::OutArgRustType, T::ReturnRustType): ExFullResultType<
        BuilderType: ExArrayBuilder<
            OutArg = T::OutArgRustType,
            Return = T::ReturnRustType,
        >,
    >,
{
    let number_rows = args.number_rows;
    let args = args.args;
    assert_eq!(args.len(), T::SQL_ARGUMENT_COUNT as usize);
    let args = VecDeque::from(args);

    let mut builder =
        <(T::OutArgRustType, T::ReturnRustType) as ExFullResultType>::builder_with_capacity(
            number_rows,
        );

    T::ArgumentRustTypes::apply(
        args,
        number_rows,
        // valid
        |_position: usize| true,
        // apply
        |_position, args, out_arg| T::invoke(args, out_arg),
        &mut builder,
    )?;

    let array = builder.build()?;
    Ok(ColumnarValue::Array(array))
}

pub trait ApplyList {
    fn apply<Builder, Valid, Invoke>(
        args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: Fn(usize, Self, &mut Builder::OutArg) -> Builder::Return;
}

impl<Head, Tail> ApplyList for (Head, Tail)
where
    Head: ExArgType,
    Tail: ApplyList,
{
    fn apply<Builder, Valid, Invoke>(
        mut args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: Fn(usize, Self, &mut Builder::OutArg) -> Builder::Return,
    {
        let arg = args.pop_front().unwrap();
        match arg {
            ColumnarValue::Array(array) => {
                let reader = Head::read_array(array)?;
                Tail::apply(
                    args,
                    number_rows,
                    |position| valid(position) && reader.is_valid(position),
                    |position, tail_args, out_arg| {
                        let head_arg: Head = reader.get(position);
                        invoke(position, (head_arg, tail_args), out_arg)
                    },
                    builder,
                )
            }
            ColumnarValue::Scalar(scalar) => {
                let reader = Head::read_scalar(scalar)?;
                Tail::apply(
                    args,
                    number_rows,
                    |position| valid(position) && reader.is_valid(position),
                    |position, tail_args, out_arg| {
                        invoke(position, (reader.get(position), tail_args), out_arg)
                    },
                    builder,
                )
            }
        }
    }
}

impl ApplyList for () {
    fn apply<Builder, Valid, Invoke>(
        args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: Fn(usize, Self, &mut Builder::OutArg) -> Builder::Return,
    {
        assert!(args.is_empty());
        for position in 0..number_rows {
            if valid(position) {
                let mut out_arg: Builder::OutArg = builder.get_out_arg(position);
                let result = invoke(position, (), &mut out_arg);
                builder.append(out_arg, result)?;
            } else {
                builder.append_null()?;
            }
        }
        Ok(())
    }
}
