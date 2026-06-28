use rayon::prelude::*;

use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use crate::bytecode::opcode::OpCode;
use crate::executor::helpers;

pub fn exec_parallel(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    arena: &crate::memory::arena::Arena,
) -> Result<&mut [u8], String> {
    let count = instr.a as u8;
    let first_svc = instr.b as usize;

    let results: Vec<Result<Vec<u8>, String>> = (0..count as usize)
        .into_par_iter()
        .map(|offset| {
            let svc_id = plan.services.entries()[first_svc + offset].id;
            caller(svc_id, body, 0)
        })
        .collect();

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

pub fn exec_collect(
    parallel_result: &[u8],
    _instr: &Instruction,
    _plan: &ExecutionPlan,
    arena: &crate::memory::arena::Arena,
) -> Result<&mut [u8], String> {
    // parallel result is already a JSON array from exec_parallel
    Ok(arena.alloc_copy(parallel_result))
}
