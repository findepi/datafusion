use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{Error, LitStr};

#[derive(Default)]
pub struct EFAttributes {
    pub name: Option<String>,
}

impl EFAttributes {
    pub fn parse(&mut self, meta: ParseNestedMeta) -> syn::Result<()> {
        match meta.path.get_ident().map(syn::Ident::to_string).as_deref() {
            Some("name") => {
                let value: LitStr = meta.value()?.parse()?;
                self.name = Some(value.value());
                Ok(())
            }
            _ => Err(Error::new(meta.path.span(), "Unknown attribute")),
        }
    }
}
