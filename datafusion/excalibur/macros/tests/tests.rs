#[test]
fn test_macro_errors() {
    let test_cases = trybuild::TestCases::new();
    test_cases.compile_fail("tests/ui/errs/*.rs");
}
