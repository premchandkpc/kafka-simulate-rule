use std::fmt;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum OpCode {
    Next = 0,
    Parallel = 1,
    Collect = 2,
    Fallback = 3,
    Gate = 4,
    Split = 5,
    Map = 6,
    Emit = 7,
    Drop = 8,
    Buffer = 9,
    Key = 10,
    Retry = 11,
    Pipe = 12,
    Timeout = 13,
    Async = 14,
    Chunk = 15,
    Dag = 16,
    Jmp = 17,
    Label = 18,
    SvcArg = 19,
    RetryData = 20,
    JumpOffset = 21,
}

impl OpCode {
    pub fn from_u8(v: u8) -> Option<OpCode> {
        match v {
            0 => Some(OpCode::Next),
            1 => Some(OpCode::Parallel),
            2 => Some(OpCode::Collect),
            3 => Some(OpCode::Fallback),
            4 => Some(OpCode::Gate),
            5 => Some(OpCode::Split),
            6 => Some(OpCode::Map),
            7 => Some(OpCode::Emit),
            8 => Some(OpCode::Drop),
            9 => Some(OpCode::Buffer),
            10 => Some(OpCode::Key),
            11 => Some(OpCode::Retry),
            12 => Some(OpCode::Pipe),
            13 => Some(OpCode::Timeout),
            14 => Some(OpCode::Async),
            15 => Some(OpCode::Chunk),
            16 => Some(OpCode::Dag),
            17 => Some(OpCode::Jmp),
            18 => Some(OpCode::Label),
            19 => Some(OpCode::SvcArg),
            20 => Some(OpCode::RetryData),
            21 => Some(OpCode::JumpOffset),
            _ => None,
        }
    }
}

impl fmt::Display for OpCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GateOp {
    Eq = 0,
    Ne = 1,
    Gt = 2,
    Lt = 3,
    Gte = 4,
    Lte = 5,
    Contains = 6,
}

impl GateOp {
    pub fn from_str(s: &str) -> Option<GateOp> {
        match s {
            "==" => Some(GateOp::Eq),
            "!=" => Some(GateOp::Ne),
            ">" => Some(GateOp::Gt),
            "<" => Some(GateOp::Lt),
            ">=" => Some(GateOp::Gte),
            "<=" => Some(GateOp::Lte),
            "contains" => Some(GateOp::Contains),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChunkMode {
    Sequential = 0,
    Parallel = 1,
}

impl ChunkMode {
    pub fn from_str(s: &str) -> Option<ChunkMode> {
        match s {
            "seq" => Some(ChunkMode::Sequential),
            "par" => Some(ChunkMode::Parallel),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RetryStrategy {
    Exponential = 0,
    Linear = 1,
    Fixed = 2,
}

impl RetryStrategy {
    pub fn from_str(s: &str) -> Option<RetryStrategy> {
        match s {
            "exp" => Some(RetryStrategy::Exponential),
            "linear" => Some(RetryStrategy::Linear),
            "fixed" => Some(RetryStrategy::Fixed),
            _ => None,
        }
    }
}
