///A builder struct for commands where you set multiple values at once. It utilizes
///references to ensure that it does not copy any of the data given to it. It supports
///the classic builder-pattern, as well as a mutable pattern.
///# Example
///```
///# use darkredis::MSetBuilder;
/// //Builder-style
///let mut builder = MSetBuilder::new().set(b"example-key", b"some-value");
///
/// // Mutable style
///builder.append(b"some-other-key", b"some-value");
///```
#[derive(Debug, Default)]
pub struct MSetBuilder<'a> {
    inner: Vec<&'a [u8]>,
}

impl<'a> MSetBuilder<'a> {
    ///Create a new instance.
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    pub(crate) fn build(&'a self) -> impl Iterator<Item = &'a &'a [u8]> {
        self.inner.iter()
    }

    ///Add `key` to be set to `value`, mutable style.
    #[inline]
    pub fn append<K, V>(&'a mut self, key: &'a K, value: &'a V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.push(key.as_ref());
        self.inner.push(value.as_ref());
    }

    ///Add `key` to be set to `value`, builder-style.
    #[inline]
    pub fn set<K, V>(mut self, key: &'a K, value: &'a V) -> Self
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.push(key.as_ref());
        self.inner.push(value.as_ref());
        self
    }
}
