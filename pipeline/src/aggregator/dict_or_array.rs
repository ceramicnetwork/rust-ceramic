use arrow::array::{Array as _, BinaryArray, Int32Array};

#[derive(Clone)]
pub enum DictionaryOrArray<'a> {
    Dictionary {
        keys: &'a Int32Array,
        values: &'a BinaryArray,
    },
    Array(&'a BinaryArray),
}
impl<'a> DictionaryOrArray<'a> {
    pub fn value(&self, i: usize) -> Option<&'a [u8]> {
        match self {
            DictionaryOrArray::Dictionary { keys, values } => keys
                .is_valid(i)
                .then(|| values.value(keys.value(i) as usize)),
            DictionaryOrArray::Array(arr) => arr.is_valid(i).then(|| arr.value(i)),
        }
    }
}
