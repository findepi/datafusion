mod bridge;
mod builder;
mod invoke;
mod reader;
mod scalar_udf;
mod signature;
mod types;

// Not public API.
#[doc(hidden)]
pub mod __private {
    // Re-exports used by the macros.

    // pub use crate::bridge::ExcaliburScalarUdfImpl;
    pub use crate::scalar_udf::create_excalibur_scalar_udf;
    // pub use crate::invoke::excalibur_invoke;
    pub use crate::bridge::ExcaliburScalarUdf;
    // pub use crate::meta::ExcaliburSignature;
    // pub use arrow::datatypes::DataType;
    // pub use datafusion_common::Result;
    // pub use datafusion_expr::ColumnarValue;
    // pub use datafusion_expr::ScalarFunctionArgs;
    pub use datafusion_expr::ScalarUDFImpl;
    // pub use datafusion_expr::Signature;
}
