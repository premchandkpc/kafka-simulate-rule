use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConstantPool {
    entries: Vec<String>,
    index: HashMap<String, u16>,
}

impl ConstantPool {
    pub fn new() -> Self {
        ConstantPool {
            entries: Vec::new(),
            index: HashMap::new(),
        }
    }

    pub fn add(&mut self, s: &str) -> u16 {
        if let Some(&id) = self.index.get(s) {
            return id;
        }
        let id = self.entries.len() as u16;
        self.entries.push(s.to_string());
        self.index.insert(s.to_string(), id);
        id
    }

    pub fn get(&self, id: u16) -> &str {
        &self.entries[id as usize]
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[String] {
        &self.entries
    }
}

impl Default for ConstantPool {
    fn default() -> Self {
        Self::new()
    }
}
