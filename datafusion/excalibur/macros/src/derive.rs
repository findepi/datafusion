use crate::attr::EFAttributes;
use crate::input::{InputFnInfo, NameType};
use crate::strings::to_camel_case;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::{Result, Type, TypeTuple};

pub fn derive(attributes: EFAttributes, input: InputFnInfo) -> Result<TokenStream> {
    let orig_rust_function_name = &input.name;
    let sql_function_name = sql_function_name(&attributes, &input);
    let udf_factory_function_name = format_ident!("{}_udf", sql_function_name);

    let imports = common_imports();

    let (impl_struct_name, struct_definition) =
        struct_definition(&input, &sql_function_name);

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
        // use ::datafusion_excalibur::__private::ColumnarValue;
        // use ::datafusion_excalibur::__private::DataType;
        use ::datafusion_excalibur::__private::ExcaliburScalarUdf;
        use ::datafusion_excalibur::__private::create_excalibur_scalar_udf;
        // use ::datafusion_excalibur::__private::ExcaliburScalarUdfImpl;
        // use ::datafusion_excalibur::__private::ExcaliburSignature;
        // use ::datafusion_excalibur::__private::Result;
        // use ::datafusion_excalibur::__private::ScalarFunctionArgs;
        use ::datafusion_excalibur::__private::ScalarUDFImpl;
        // use ::datafusion_excalibur::__private::Signature;
        // use ::datafusion_excalibur::__private::excalibur_invoke;
        // use ::std::any::Any;
        use ::std::ops::Deref;
        use ::std::sync::Arc;
        use ::std::sync::LazyLock;
    };
    imports
}

fn struct_definition(
    input: &InputFnInfo,
    sql_function_name: &str,
) -> (Ident, TokenStream) {
    let orig_rust_function_name = &input.name;
    let impl_struct_name = format_ident!("{}", to_camel_case(sql_function_name));
    let rust_arg_count = input.args.len() as u8;
    let rust_arg_type_list = to_type_list(
        input
            .args
            .iter()
            .map(|arg| arg.ty.to_owned())
            .collect::<Vec<_>>(),
    );
    let rust_return_type = &input.return_ty;

    let (destruct_args, invoke_args, _) = input.args.iter().rfold(
        (quote! { () }, quote! {}, input.args.len()),
        |(destruct_args, invoke_args, pos), arg| {
            let arg = new_arg_name(&arg.name, pos - 1);
            (
                quote! { (#arg, #destruct_args) },
                quote! { #arg, #invoke_args },
                pos - 1,
            )
        },
    );

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
    (impl_struct_name, struct_definition)
}

fn sql_function_name(attributes: &EFAttributes, input: &InputFnInfo) -> String {
    if let Some(name) = &attributes.name {
        name.to_owned()
    } else {
        input.name.to_string()
    }
}

fn to_type_list(types: Vec<Type>) -> Type {
    types.iter().rfold(
        Type::Tuple(TypeTuple {
            paren_token: Default::default(),
            elems: Default::default(),
        }),
        |acc, ty| {
            Type::Tuple(TypeTuple {
                paren_token: Default::default(),
                elems: [ty.to_owned(), acc].into_iter().collect(),
            })
        },
    )
}

fn new_arg_name(name: &Ident, pos: usize) -> Ident {
    let base_name = name.to_string();
    let sep = base_name
        .ends_with(|c: char| c.is_ascii_digit())
        .then(|| "_")
        .unwrap_or("");
    format_ident!("{}{}{}", base_name, sep, pos)
}

/*
// this function has a name derived as "{scalar_function}_udf"
pub fn my_add_udf() -> &'static dyn ScalarUDFImpl {

    impl VectorizedScalar for MyAddUdfImpl {
        // there are two arguments
        const ARGUMENT_COUNT: u8 = 2;
        // the second argument has #[sql_type] attribute
        const ARGUMENT_SQL_TYPES: &'static [Option<&'static str>] =
            &[None, Some("uint32")];
        type ArgumentRustTypeList = (u32, (u32, ()));
        type OutArgRustType = (); // does not use out parameter
        type ReturnRustType = Result<u32>;

        fn invoke(regular_args: Self::ArgumentRustTypeList, _out: &mut Self::OutArgRustType) -> Self::ReturnRustType {
            // unpack the arguments
            let (a, (b, ())) = regular_args;
            // call the vectorized function
            my_add(a, b)
        }
    }

    // this encapsulates argument and return type handling
    static SIGNATURE: std::sync::LazyLock<Box<dyn VectorizedSignature>> =
        std::sync::LazyLock::new(|| compile_signature::<MyAddUdfImpl>());

}

 */
