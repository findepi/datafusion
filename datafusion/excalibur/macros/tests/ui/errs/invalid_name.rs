use datafusion_excalibur_macros::excalibur_function;

#[excalibur_function(name = unquoted_name)]
fn add_one(a: u64) -> u64 {
    a + 1
}

// not a string
#[excalibur_function(name = 123)]
fn add_two(a: u64) -> u64 {
    a + 2
}

// expected by trybuild
fn main() {}