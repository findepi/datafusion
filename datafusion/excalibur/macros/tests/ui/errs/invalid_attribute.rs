use datafusion_excalibur_macros::excalibur_function;

#[excalibur_function(hallucinated_attribute = "my_function")]
fn add_one(a: u64) -> u64 {
    a + 1
}

// expected by trybuild
fn main() {}