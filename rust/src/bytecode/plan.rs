use super::instruction::Instruction;
use super::consts::ConstantPool;
use super::services::ServiceTable;
use super::dag_table::DAGTable;
use super::mapexpr::MapExpr;
use super::opcode::{ChunkMode, RetryStrategy};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryConfig {
    pub max_attempts: u8,
    pub strategy: RetryStrategy,
    pub fixed_ms: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkConfig {
    pub count: u8,
    pub mode: ChunkMode,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionPlan {
    pub rule_id: [u8; 64],
    pub version: u64,
    pub instr_count: u32,
    pub instructions: Vec<Instruction>,
    pub const_pool: ConstantPool,
    pub services: ServiceTable,
    pub dag_tables: Vec<DAGTable>,
    pub map_exprs: Vec<MapExpr>,
    pub retry_configs: Vec<RetryConfig>,
    pub chunk_configs: Vec<ChunkConfig>,
}

impl ExecutionPlan {
    pub fn new(rule_id: &str) -> Self {
        let mut rid = [0u8; 64];
        let bytes = rule_id.as_bytes();
        let len = bytes.len().min(63);
        rid[..len].copy_from_slice(&bytes[..len]);

        ExecutionPlan {
            rule_id: rid,
            version: 1,
            instr_count: 0,
            instructions: Vec::new(),
            const_pool: ConstantPool::new(),
            services: ServiceTable::new(),
            dag_tables: Vec::new(),
            map_exprs: Vec::new(),
            retry_configs: Vec::new(),
            chunk_configs: Vec::new(),
        }
    }

    pub fn add_instr(&mut self, instr: Instruction) {
        self.instructions.push(instr);
        self.instr_count = self.instructions.len() as u32;
    }
}
