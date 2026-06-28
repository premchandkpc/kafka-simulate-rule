use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceEntry {
    pub id: u16,
    pub name: String,
}

impl ServiceEntry {
    pub fn new(id: u16, name: &str) -> Self {
        ServiceEntry {
            id,
            name: name.to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceTable {
    entries: Vec<ServiceEntry>,
    index: HashMap<String, u16>,
}

impl ServiceTable {
    pub fn new() -> Self {
        ServiceTable {
            entries: Vec::new(),
            index: HashMap::new(),
        }
    }

    pub fn add(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.index.get(name) {
            return id;
        }
        let id = self.entries.len() as u16;
        self.entries.push(ServiceEntry::new(id, name));
        self.index.insert(name.to_string(), id);
        id
    }

    pub fn get(&self, id: u16) -> &ServiceEntry {
        &self.entries[id as usize]
    }

    pub fn get_by_name(&self, name: &str) -> Option<&ServiceEntry> {
        self.index.get(name).map(|&id| &self.entries[id as usize])
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[ServiceEntry] {
        &self.entries
    }
}

impl Default for ServiceTable {
    fn default() -> Self {
        Self::new()
    }
}
