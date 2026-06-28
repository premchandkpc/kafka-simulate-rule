#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MapKV {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MapExpr {
    pub operations: Vec<MapKV>,
}

impl MapExpr {
    pub fn new() -> Self {
        MapExpr {
            operations: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &str, value: &str) {
        self.operations.push(MapKV {
            key: key.to_string(),
            value: value.to_string(),
        });
    }
}

impl Default for MapExpr {
    fn default() -> Self {
        Self::new()
    }
}
