use proc_macro2::Ident;
use syn::spanned::Spanned;
use syn::{Error, FnArg, ItemFn, Pat, Result, ReturnType, Type};

pub struct InputFnInfo {
    pub name: Ident,
    pub args: Vec<NameType>,
    pub return_ty: Type,
}

pub struct NameType {
    pub name: Ident,
    pub ty: Type,
}

impl InputFnInfo {
    /// Validate the input and capture the necessary information
    pub fn try_from(input_fn: ItemFn) -> Result<Self> {
        let sig = input_fn.sig;

        if sig.asyncness.is_some() {
            return Err(Error::new(
                sig.span(),
                "Function cannot be async for use with Excalibur",
            ));
        }
        if sig.variadic.is_some() {
            return Err(Error::new(
                sig.span(),
                "Function cannot be variadic for use with Excalibur",
            ));
        }

        let args = sig
            .inputs
            .iter()
            .map(|arg| {
                if let FnArg::Typed(typed) = arg {
                    if let Pat::Ident(ref ident) = *typed.pat {
                        if typed.attrs.is_empty() {
                            return Ok(NameType {
                                name: ident.ident.clone(),
                                ty: (*typed.ty).clone(),
                            });
                        }
                    }
                }
                Err(Error::new(
                    arg.span(),
                    "Unsupported function argument (name, type or attributes) for use with Excalibur",
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let ReturnType::Type(_, return_ty) = sig.output else {
            return Err(Error::new(
                sig.output.span(),
                "Function needs a return type for use with Excalibur",
            ));
        };

        Ok(InputFnInfo {
            name: sig.ident,
            args,
            return_ty: (*return_ty).to_owned(),
        })
    }
}
