use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use std::time::Duration;

pub fn exec_emit(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8]) -> Result<Vec<u8>, String>,
) {
    let first_svc = instr.b as usize;
    let count = instr.a as u8;

    for offset in 0..count as usize {
        let svc_idx = first_svc + offset;
        let svc = plan.services.get(svc_idx as u16);
        let body_copy = body.to_vec();
        let svc_id = svc.id;
        std::thread::spawn(move || {
            let _ = caller(svc_id, &body_copy);
        });
    }
}
