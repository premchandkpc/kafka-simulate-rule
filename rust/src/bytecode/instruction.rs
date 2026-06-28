use super::opcode::OpCode;

#[repr(C)]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Instruction {
    pub op: OpCode,
    pub flags: u8,
    pub a: u16,
    pub b: u16,
    pub c: u16,
}

const _: () = assert!(std::mem::size_of::<Instruction>() == 8);

impl Instruction {
    pub fn new(op: OpCode, flags: u8, a: u16, b: u16, c: u16) -> Self {
        Instruction { op, flags, a, b, c }
    }

    pub fn next(target_id: u16, timeout_ms: u64) -> Self {
        let hi = (timeout_ms >> 16) as u16;
        let lo = (timeout_ms & 0xFFFF) as u16;
        Instruction::new(OpCode::Next, 0, target_id, hi, lo)
    }

    pub fn parallel(count: u8, first_svc: u16) -> Self {
        Instruction::new(OpCode::Parallel, 0, u16::from(count), first_svc, 0)
    }

    pub fn collect() -> Self {
        Instruction::new(OpCode::Collect, 0, 0, 0, 0)
    }

    pub fn fallback(target_id: u16) -> Self {
        Instruction::new(OpCode::Fallback, 0, target_id, 0, 0)
    }

    pub fn gate(field_const: u16, gate_op: u8, value_const: u16) -> Self {
        Instruction::new(OpCode::Gate, gate_op, field_const, value_const, 0)
    }

    pub fn jmp(ip_offset: u16) -> Self {
        Instruction::new(OpCode::Jmp, 0, ip_offset, 0, 0)
    }

    pub fn label() -> Self {
        Instruction::new(OpCode::Label, 0, 0, 0, 0)
    }

    pub fn svc_arg(svc_id: u16) -> Self {
        Instruction::new(OpCode::SvcArg, 0, svc_id, 0, 0)
    }

    pub fn retry_data(max_attempts: u8, strategy: u8, fixed_ms: u32) -> Self {
        let hi = (fixed_ms >> 16) as u16;
        let lo = (fixed_ms & 0xFFFF) as u16;
        let flags = (max_attempts as u16) | ((strategy as u16) << 8);
        Instruction::new(OpCode::RetryData, 0, flags as u16, hi, lo)
    }

    pub fn jump_offset(offset: u16) -> Self {
        Instruction::new(OpCode::JumpOffset, 0, offset, 0, 0)
    }

    pub fn emit(count: u8, first_svc: u16) -> Self {
        Instruction::new(OpCode::Emit, 0, u16::from(count), first_svc, 0)
    }

    pub fn timeout(ms: u64) -> Self {
        let hi = (ms >> 16) as u16;
        let lo = (ms & 0xFFFF) as u16;
        Instruction::new(OpCode::Timeout, 0, hi, lo, 0)
    }

    pub fn map(expr_id: u16) -> Self {
        Instruction::new(OpCode::Map, 0, expr_id, 0, 0)
    }

    pub fn set_key(field_const: u16) -> Self {
        Instruction::new(OpCode::Key, 0, field_const, 0, 0)
    }

    pub fn chunk(count: u8, mode: u8) -> Self {
        Instruction::new(OpCode::Chunk, 0, u16::from(count), u16::from(mode), 0)
    }

    pub fn drop() -> Self {
        Instruction::new(OpCode::Drop, 0, 0, 0, 0)
    }

    pub fn buffer(n: u8) -> Self {
        Instruction::new(OpCode::Buffer, 0, u16::from(n), 0, 0)
    }

    pub fn async_svc(target_id: u16, timeout_ms: u64) -> Self {
        let hi = (timeout_ms >> 16) as u16;
        let lo = (timeout_ms & 0xFFFF) as u16;
        Instruction::new(OpCode::Async, 0, target_id, hi, lo)
    }

    pub fn dag(dag_table_id: u16) -> Self {
        Instruction::new(OpCode::Dag, 0, dag_table_id, 0, 0)
    }

    pub fn has_retry(&self) -> bool {
        (self.flags & 0x01) != 0
    }

    pub fn gate_op(&self) -> u8 {
        self.flags
    }

    pub fn timeout_ms(&self) -> u64 {
        ((self.b as u64) << 16) | (self.c as u64)
    }
}
