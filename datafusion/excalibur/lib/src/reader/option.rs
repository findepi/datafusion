use crate::reader::ExArrayReader;

impl<T> ExArrayReader for Option<T>
where
    T: Copy,
{
    type ValueType = T;

    fn is_valid(&self, _position: usize) -> bool {
        self.is_some()
    }

    fn get(&self, _position: usize) -> Self::ValueType {
        self.unwrap()
    }
}
