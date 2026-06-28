use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use crate::executor::helpers;

pub fn exec_jmp_if_false(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    arena: &crate::memory::arena::Arena,
    skip_instructions: &mut usize,
) {
    let field_path = plan.const_pool.get(instr.a);
    let compare_val_str = plan.const_pool.get(instr.b);
    let gate_op = instr.flags;

    let field_val = helpers::extract_json_field(body, field_path, arena);
    let passes = match field_val {
        Some(val) => helpers::compare_values(val, gate_op, compare_val_str),
        None => false,
    };

    if !passes {
        *skip_instructions = 2;
    }
}
