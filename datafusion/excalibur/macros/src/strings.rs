pub fn to_camel_case(s: impl AsRef<str>) -> String {
    use convert_case::{Case, Casing};
    s.as_ref().to_case(Case::UpperCamel)
}
