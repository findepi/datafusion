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
use arrow::array::{Array, ArrowPrimitiveType, Decimal128Array, PrimitiveArray};
use arrow::datatypes::{Decimal128Type, DecimalType};
use datafusion_common::cast::as_decimal128_array;
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_common::{internal_err, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;

// We could use `arrow::datatypes::DecimalType` here, but that would unnecessarily
// tie the usage to Arrow.
pub struct Decimal<Native: DecimalNativeType> {
    pub unscaled_value: Native,
    pub precision: u8,
    pub scale: i8,
}

// seal and adapt
trait DecimalNativeType: Sized {
    type ArrowDecimalType: ArrowPrimitiveType + DecimalType;

    fn cast_decimal_array(
        array: &dyn Array,
    ) -> Result<&PrimitiveArray<Self::ArrowDecimalType>>;

    fn cast_scalar(scalar: ScalarValue) -> Result<(Option<Self>, u8, i8)>;
}
impl DecimalNativeType for i128 {
    type ArrowDecimalType = Decimal128Type;

    fn cast_decimal_array(
        array: &dyn Array,
    ) -> Result<&PrimitiveArray<Self::ArrowDecimalType>> {
        as_decimal128_array(array)
    }

    fn cast_scalar(scalar: ScalarValue) -> Result<(Option<Self>, u8, i8)> {
        if let ScalarValue::Decimal128(value, precision, scale) = scalar {
            Ok((value, precision, scale))
        } else {
            internal_err!("Expected Decimal128 scalar, got: {:?}", scalar)
        }
    }
}

impl<Native: DecimalNativeType> ExInstantiable for Decimal<Native> {
    type StackType<'a> = Self;
}

impl<Native: DecimalNativeType> ExArgType for Decimal<Native>
where
    for<'a> DecimalReader<&'a PrimitiveArray<Native::ArrowDecimalType>>:
        ExArrayReader<'a, ValueType = Decimal<Native>>,
    for<'a> DecimalReader<Option<Native>>: ExArrayReader<'a, ValueType = Decimal<Native>>,
{
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
    ) -> Result<()> {
        use datafusion_common::ScalarValue;
        use datafusion_expr::ColumnarValue::*;
        match arg {
            Array(array) => {
                let decimal_array = Native::cast_decimal_array(&array)?;
                consumer.consume(DecimalReader {
                    delegate: decimal_array,
                    precision: decimal_array.precision(),
                    scale: decimal_array.scale(),
                })
            }
            Scalar(scalar) => {
                let (value, precision, scale) = Native::cast_scalar(scalar)?;
                consumer.consume(DecimalReader {
                    delegate: value,
                    precision,
                    scale,
                })
            }
        }
    }
}

struct DecimalReader<Delegate> {
    delegate: Delegate,
    precision: u8,
    scale: i8,
}

impl<'a, Delegate> ExArrayReader<'a> for DecimalReader<Delegate>
where
    Delegate: ExArrayReader<'a>,
    Delegate::ValueType: DecimalNativeType,
{
    type ValueType = Decimal<Delegate::ValueType>;

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
