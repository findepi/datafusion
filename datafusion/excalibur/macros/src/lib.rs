mod attr;
mod derive;
mod input;
mod strings;

use attr::EFAttributes;
use input::InputFnInfo;
use proc_macro::TokenStream;
use syn::{parse_macro_input, Error, ItemFn};

#[proc_macro_attribute]
pub fn excalibur_function(attributes: TokenStream, input: TokenStream) -> TokenStream {
    // parse attributes
    let mut parsed_attributes = EFAttributes::default();
    let attribute_parser = syn::meta::parser(|meta| parsed_attributes.parse(meta));
    parse_macro_input!(attributes with attribute_parser);
    // the original input should be output unchanged
    let original_input = input.clone();
    // derive
    let input_fn = parse_macro_input!(input as ItemFn);
    let input_fn_info = InputFnInfo::try_from(input_fn).unwrap();
    TokenStream::from_iter([
        derive::derive(parsed_attributes, input_fn_info)
            .unwrap_or_else(Error::into_compile_error)
            .into(),
        original_input,
    ])
}
