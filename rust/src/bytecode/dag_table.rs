#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DAGNode {
    pub service_id: u16,
    pub layer: u8,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DAGTable {
    pub nodes: Vec<DAGNode>,
    pub layers: Vec<Vec<u16>>,
    pub terminal_nodes: Vec<u16>,
}

impl DAGTable {
    pub fn new() -> Self {
        DAGTable {
            nodes: Vec::new(),
            layers: Vec::new(),
            terminal_nodes: Vec::new(),
        }
    }
}

impl Default for DAGTable {
    fn default() -> Self {
        Self::new()
    }
}
