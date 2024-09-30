use datafusion_common::{DataFusionError, Result};
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeName {
    basename: String,
    name: String,
    parameters: Vec<TypeParameter>,
}

pub type TypeNameRef = Arc<TypeName>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeParameter {
    Type(TypeNameRef),
    Number(i128),
}

impl TypeName {
    pub fn basename(&self) -> &str {
        &self.basename
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn parameters(&self) -> &[TypeParameter] {
        &self.parameters
    }

    pub fn new(basename: impl Into<String>, parameters: Vec<TypeParameter>) -> Self {
        // TODO assert parses, no braces, etc.
        let basename = basename.into();
        let name = FormatName {
            basename: &basename,
            parameters: &parameters,
        }
        .to_string();
        Self {
            basename,
            name,
            parameters,
        }
    }
    pub fn from_basename(basename: impl Into<String>) -> Self {
        Self::new(basename, vec![])
    }
}

impl Display for TypeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl FromStr for TypeName {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        // impl and add tests
        todo!()
    }
}

impl Display for TypeParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeParameter::Type(type_name) => f.write_str(type_name.name()),
            TypeParameter::Number(number) => f.write_str(&number.to_string()),
        }
    }
}

// helper
struct FormatName<'a> {
    basename: &'a str,
    parameters: &'a [TypeParameter],
}
impl Display for FormatName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.basename)?;
        if !self.parameters.is_empty() {
            f.write_str("(")?;
            let mut first = true;
            for parameter in self.parameters {
                if !first {
                    f.write_str(", ")?;
                }
                Display::fmt(parameter, f)?;
                first = false;
            }
            f.write_str(")")?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string() {
        assert_eq!(TypeName::from_basename("bool").to_string(), "bool");
        assert_eq!(TypeName::from_basename("unknown").to_string(), "unknown");
        assert_eq!(
            TypeName::new(
                "array",
                vec![type_parameter(TypeName::from_basename("bool"))]
            )
            .to_string(),
            "array(bool)"
        );

        assert_eq!(
            TypeName::new(
                "decimal",
                vec![numeric_parameter(38), numeric_parameter(10)]
            )
            .to_string(),
            "decimal(38, 10)"
        );
    }

    fn type_parameter(type_name: TypeName) -> TypeParameter {
        TypeParameter::Type(Arc::new(type_name))
    }
    fn numeric_parameter(value: i128) -> TypeParameter {
        TypeParameter::Number(value)
    }
}
