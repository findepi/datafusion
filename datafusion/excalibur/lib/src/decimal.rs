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

use crate::__private::ExInstantiable;
use crate::arg_type::ExArgType;
use crate::reader::{ExArrayReader, ExArrayReaderConsumer};
use arrow::array::{ArrowPrimitiveType, Decimal128Array, PrimitiveArray};
use arrow::datatypes::{Decimal128Type, DecimalType};
use datafusion_common::cast::as_decimal128_array;
use datafusion_common::internal_err;
use datafusion_common::types::NativeType;
use datafusion_expr_common::columnar_value::ColumnarValue;

// We could use `arrow::datatypes::DecimalType` here, but that would unnecessarily
// tie the usage to Arrow.
pub struct Decimal<Native> {
    pub unscaled_value: Native,
    pub precision: u8,
    pub scale: i8,
}

impl ExInstantiable for Decimal<i128> {
    type StackType<'a> = Self;
}

impl ExArgType for Decimal<i128> {
    fn logical_type() -> NativeType {
        // This should be generic, a pattern over NativeType, so that
        // we allow accepting e.g. decimal(p, 0) or any decimal(p, s) without any
        // argument coercion.
        // This requires that
        // * functions declare patterns of accepted types.
        // * The patterns should also allow dependent return types, e.g.
        //   times_10: decimal(p, s) -> decimal(p, s-1).
        //   The expr submodule can be used for that.
        // * DataFusion Signature handling to plug that. TypeSignature::UserDefined may be
        //   an escape hatch, but we also need to capture the return type
        //   and the type variables.
        //
        // For now, some obviously bogus type is returned.
        NativeType::Decimal(0, 0)
    }

    fn decode(
        arg: ColumnarValue,
        consumer: impl for<'a> ExArrayReaderConsumer<ValueType<'a> = Self::StackType<'a>>,
    ) -> datafusion_common::Result<()> {
        use datafusion_common::ScalarValue;
        use datafusion_expr::ColumnarValue::*;
        match arg {
            Array(array) => {
                let decimal_array = as_decimal128_array(&array)?;
                consumer.consume(DecimalReader {
                    delegate: decimal_array,
                    precision: decimal_array.precision(),
                    scale: decimal_array.scale(),
                })
            }
            Scalar(scalar) => {
                if let ScalarValue::Decimal128(value, precision, scale) = scalar {
                    consumer.consume(DecimalReader {
                        delegate: value,
                        precision,
                        scale,
                    })
                } else {
                    internal_err!(
                        "Expected {} scalar, got: {:?}",
                        stringify!($native_type),
                        scalar
                    )
                }
            }
        }
    }
}

struct DecimalReader<Delegate> {
    delegate: Delegate,
    precision: u8,
    scale: i8,
}

impl<'a, Delegate> ExArrayReader<'_> for DecimalReader<Delegate>
where
    Delegate: ExArrayReader<'a, ValueType = i128>,
{
    type ValueType = Decimal<i128>;

    fn assert_number_rows(&self, number_rows: usize) {
        self.delegate.assert_number_rows(number_rows);
    }

    fn is_valid(&self, position: usize) -> bool {
        self.delegate.is_valid(position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        Self::ValueType {
            unscaled_value: self.delegate.get(position),
            precision: self.precision,
            scale: self.scale,
        }
    }
}
