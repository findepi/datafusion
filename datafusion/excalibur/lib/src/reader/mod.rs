mod option;
mod primitive;

pub trait ExArrayReader {
    type ValueType;

    fn is_valid(&self, position: usize) -> bool;

    /// may panic on invalid position
    fn get(&self, position: usize) -> Self::ValueType;
}
