use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use crate::executor::helpers;

pub fn exec_parallel<'a>(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    let count = instr.a as u8;
    let first_svc = instr.b as usize;

    let mut results = Vec::with_capacity(count as usize);
    for offset in 0..count as usize {
        let svc_id = plan.services.entries()[first_svc + offset].id;
        results.push(caller(svc_id, body, 0));
    }

    let mut parts = Vec::with_capacity(results.len());
    for result in results {
        match result {
            Ok(resp) => parts.push(resp),
            Err(e) => return Err(e),
        }
    }

    let part_refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    Ok(helpers::merge_json_array(&part_refs, arena))
}

#[allow(dead_code)]
pub fn exec_collect<'a>(
    parallel_result: &[u8],
    _instr: &Instruction,
    _plan: &ExecutionPlan,
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    Ok(arena.alloc_copy(parallel_result))
}
