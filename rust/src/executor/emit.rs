use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;

pub fn exec_emit(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
) -> Result<(), String> {
    let first_svc = instr.b as usize;
    let count = instr.a as u8;

    for offset in 0..count as usize {
        let svc_idx = first_svc + offset;
        caller(plan.services.entries()[svc_idx as usize].id, body, 0)?;
    }
    Ok(())
}
