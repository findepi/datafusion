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

use crate::attr::EFAttributes;
use crate::input::{InputFnInfo, NameType};
use crate::strings::to_camel_case;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::token::Token;
use syn::{parse_quote, Error, Result, TraitBoundModifier, Type, TypeTuple};

pub fn derive(attributes: EFAttributes, input: InputFnInfo) -> Result<TokenStream> {
    let orig_rust_function_name = &input.name;
    let sql_function_name = sql_function_name(&attributes, &input);
    let udf_factory_function_name = format_ident!("{}_udf", sql_function_name);

    let imports = common_imports();

    let (impl_struct_name, struct_definition) =
        struct_definition(&input, &sql_function_name)?;

    let function_doc = format!("Factory method for a ScalarUDFImpl based on the function [`{orig_rust_function_name}`]");
    let factory_function = quote! {
        #[doc = #function_doc]
        #[allow(unused_qualifications)]
        #[automatically_derived]
        pub fn #udf_factory_function_name() -> ::std::sync::Arc<dyn ::datafusion_excalibur::__private::ScalarUDFImpl> {
            #imports
            #struct_definition
            static INSTANCE: LazyLock<Arc<dyn ScalarUDFImpl>> =
                LazyLock::new(|| create_excalibur_scalar_udf::<#impl_struct_name>());
            Arc::clone(INSTANCE.deref())
        }
    };
    Ok(factory_function)
}

fn common_imports() -> TokenStream {
    // Imports for everything but the outermost function signature. Keep them sorted.
    let imports = quote! {
        use ::datafusion_excalibur::__private::ExcaliburScalarUdf;
        use ::datafusion_excalibur::__private::FindExArgType;
        use ::datafusion_excalibur::__private::ScalarUDFImpl;
        use ::datafusion_excalibur::__private::create_excalibur_scalar_udf;
        use ::std::ops::Deref;
        use ::std::sync::Arc;
        use ::std::sync::LazyLock;
    };
    imports
}

fn struct_definition(
    input: &InputFnInfo,
    sql_function_name: &str,
) -> Result<(Ident, TokenStream)> {
    let orig_rust_function_name = &input.name;
    let impl_struct_name = format_ident!("{}", to_camel_case(sql_function_name));
    let rust_arg_count = input.args.len() as u8;
    let (rust_arg_type_list, destruct_args, invoke_args) =
        input.args.iter().enumerate().try_rfold(
            (
                force_type::<Type>(parse_quote! { () }),
                quote! { () },
                quote! {},
            ),
            |(type_list, destruct_args, invoke_args), (pos, arg)| -> Result<_> {
                let (impl_type, destruct, invoke) = implement_arg(arg)?;
                Ok((
                    force_type::<Type>(parse_quote! { (#impl_type, #type_list) }),
                    quote! { (#destruct, #destruct_args) },
                    quote! { #invoke, #invoke_args },
                ))
            },
        )?;
    let rust_return_type = &input.return_ty;

    let struct_definition = quote! {
        struct #impl_struct_name {}

        impl ExcaliburScalarUdf for #impl_struct_name {
            const SQL_NAME: &'static str = #sql_function_name;
            const RUST_ARGUMENT_COUNT: u8 = #rust_arg_count;
            const SQL_ARGUMENT_COUNT: u8 = #rust_arg_count; // TODO out args not supported yet
            type ArgumentRustTypes = #rust_arg_type_list;
            type OutArgRustType = (); // TODO out args not supported yet
            type ReturnRustType = #rust_return_type;

            fn invoke(
                regular_args: Self::ArgumentRustTypes,
                out_arg: &mut Self::OutArgRustType,
            ) -> Self::ReturnRustType {
                // TODO real invoke body
                let #destruct_args = regular_args;
                #orig_rust_function_name(#invoke_args)
            }
        }
    };
    Ok((impl_struct_name, struct_definition))
}

fn sql_function_name(attributes: &EFAttributes, input: &InputFnInfo) -> String {
    if let Some(name) = &attributes.name {
        name.to_owned()
    } else {
        input.name.to_string()
    }
}

fn new_arg_name(name: &Ident, pos: usize) -> Ident {
    let base_name = name.to_string();
    let sep = if base_name.ends_with(|c: char| c.is_ascii_digit()) {
        "_"
    } else {
        ""
    };
    format_ident!("{}{}{}", base_name, sep, pos)
}

fn implement_arg(arg: &NameType) -> Result<(Type, TokenStream, TokenStream)> {
    let ty = &arg.ty;
    let arg_name = &arg.name;
    match ty {
        Type::ImplTrait(impl_trait) => {
            if impl_trait.bounds.len() == 1 {
                if let syn::TypeParamBound::Trait(trait_bound) = &impl_trait.bounds[0] {
                    if trait_bound.lifetimes.is_none() {
                        if let TraitBoundModifier::None = trait_bound.modifier {
                            let trait_path = &trait_bound.path;
                            let impl_type: Type =
                                parse_quote! {<dyn #trait_path as FindExArgType>::Type };
                            return Ok((
                                impl_type,
                                quote! { #arg_name },
                                quote! { #arg_name },
                            ));
                        }
                    }
                }
            }
        }
        Type::Path(_) => {
            return Ok((ty.to_owned(), quote! { #arg_name }, quote! { #arg_name }));
        }
        _ => {}
    }
    Err(Error::new(
        ty.span(),
        "Function argument has unsupported type for use with Excalibur",
    ))
}

fn force_type<T>(val: T) -> T {
    val
}
