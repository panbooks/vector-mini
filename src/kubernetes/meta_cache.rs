use std::collections::HashSet;

use kube::core::ObjectMeta;

#[derive(Default)]
pub struct MetaCache {
    pub cache: HashSet<MetaDescribe>,
}

impl MetaCache {
    pub fn new() -> Self {
        Self {
            cache: HashSet::new(),
        }
    }
    pub fn store(&mut self, meta_desc: MetaDescribe) {
        self.cache.insert(meta_desc);
    }
    pub fn delete(&mut self, meta_desc: &MetaDescribe) {
        self.cache.remove(meta_desc);
    }
    pub fn contains(&self, meta_desc: &MetaDescribe) -> bool {
        self.cache.contains(meta_desc)
    }
    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct MetaDescribe {
    name: String,
    namespace: String,
}

impl MetaDescribe {
    pub fn from_meta(meta: &ObjectMeta) -> Self {
        let name = meta.name.clone().unwrap_or_default();
        let namespace = meta.namespace.clone().unwrap_or_default();

        Self { name, namespace }
    }
}

