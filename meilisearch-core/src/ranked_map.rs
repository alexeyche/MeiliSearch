use std::io::{Read, Write};

use chashmap::CHashMap;
use meilisearch_schema::FieldId;
use serde::ser::{Serialize, Serializer};
use serde::de::{Deserialize, Deserializer, Visitor, MapAccess};
use std::fmt;

use crate::{DocumentId, Number};

#[derive(Debug, Default, Clone)]
pub struct RankedMap(CHashMap<(DocumentId, FieldId), Number>);

impl RankedMap {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn insert(&mut self, document: DocumentId, field: FieldId, number: Number) {
        self.0.insert((document, field), number);
    }

    pub fn remove(&mut self, document: DocumentId, field: FieldId) {
        self.0.remove(&(document, field));
    }

    pub fn get(&self, document: DocumentId, field: FieldId) -> Option<Number> {
        self.0.get(&(document, field)).cloned()
    }

    pub fn read_from_bin<R: Read>(reader: R) -> bincode::Result<RankedMap> {
        bincode::deserialize_from(reader).map(RankedMap)
    }

    pub fn write_to_bin<W: Write>(&self, writer: W) -> bincode::Result<()> {
        bincode::serialize_into(writer, &self.0)
    }
}

impl Serialize for RankedMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (k, v) in self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de> Visitor<'de> for RankedMap {
    
    type Value = RankedMap;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a very special map")
    }

    // Deserialize MyMap from an abstract "map" provided by the
    // Deserializer. The MapAccess input is a callback provided by
    // the Deserializer to let us see each entry in the map.
    fn visit_map<M>(self, mut access: M) -> Result<RankedMap, M::Error>
    where
        M: MapAccess<'de>,
    {   let mut map = CHashMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }

        Ok(RankedMap(map))
    }
}

impl<'de> Deserialize<'de> for RankedMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RankedMap::default())
    }
}

// impl PartialEq for RankedMap {
//     fn eq(&self, other: &Self) -> bool {
//         self.0 == other.0
//     }
// }
