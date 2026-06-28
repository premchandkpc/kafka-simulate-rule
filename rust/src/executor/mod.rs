pub mod helpers;
pub mod gate;
pub mod emit;
pub mod next;
pub mod parallel;
pub mod dag;
pub mod map;
pub mod chunk;

use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use crate::bytecode::opcode::OpCode;

pub struct VM<'a> {
    pub ip: usize,
    pub plan: &'a ExecutionPlan,
    pub last_response: Vec<u8>,
    pub arena: crate::memory::arena::Arena,
    pub caller: &'a dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
}

impl<'a> VM<'a> {
    pub fn new(
        plan: &'a ExecutionPlan,
        body: &[u8],
        arena: crate::memory::arena::Arena,
        caller: &'a dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Self {
        VM {
            ip: 0,
            plan,
            last_response: body.to_vec(),
            arena,
            caller,
            failed: false,
            errors: Vec::new(),
            hop_count: 0,
        }
    }

    pub fn run(&mut self) -> Result<(), String> {
        self.ip = 0;
        while self.ip < self.plan.instructions.len() {
            let instr = &self.plan.instructions[self.ip];
            self.ip += 1;
            self.dispatch(instr)?;
        }
        Ok(())
    }

    fn dispatch(&mut self, instr: &Instruction) -> Result<(), String> {
        match instr.op {
            OpCode::Next => {
                let resp = next::exec_next(
                    &self.last_response,
                    instr,
                    self.plan,
                    self.caller,
                    false,
                )?;
                self.last_response = resp;
                self.hop_count += 1;
                Ok(())
            }
            OpCode::Async => {
                let _resp = next::exec_next(
                    &self.last_response,
                    instr,
                    self.plan,
                    self.caller,
                    true,
                )?;
                // async: do NOT update last_response (body unchanged)
                self.hop_count += 1;
                Ok(())
            }
            OpCode::Parallel => {
                let result = parallel::exec_parallel(
                    &self.last_response,
                    instr,
                    self.plan,
                    self.caller,
                    &self.arena,
                )?;
                // Store parallel result; Collect will assign it
                self.last_response = result.to_vec();
                // Don't increment hop_count here; Collect will
                Ok(())
            }
            OpCode::Collect => {
                // parallel result already in last_response (merged JSON array)
                self.hop_count += 1;
                Ok(())
            }
            OpCode::Fallback => {
                if self.failed {
                    self.failed = false;
                    let resp = next::exec_next(
                        &self.last_response,
                        instr,
                        self.plan,
                        self.caller,
                        false,
                    )?;
                    self.last_response = resp;
                    self.hop_count += 1;
                }
                Ok(())
            }
            OpCode::Gate => {
                gate::exec_jmp_if_false(
                    &self.last_response,
                    instr,
                    self.plan,
                    &self.arena,
                    &mut 0,
                );
                Ok(())
            }
            OpCode::Emit => {
                emit::exec_emit(
                    &self.last_response,
                    instr,
                    self.plan,
                    self.caller,
                );
                Ok(())
            }
            OpCode::Drop => {
                self.ip = self.plan.instructions.len();
                Ok(())
            }
            OpCode::Map => {
                let result = map::exec_map(
                    &self.last_response,
                    instr,
                    self.plan,
                    &self.arena,
                )?;
                self.last_response = result.to_vec();
                Ok(())
            }
            OpCode::Key | OpCode::Split => {
                Ok(())
            }
            OpCode::Retry => {
                Ok(())
            }
            OpCode::Buffer => {
                Err("Buffer must be handled at engine level".to_string())
            }
            OpCode::Timeout => {
                Ok(())
            }
            OpCode::Chunk => {
                Ok(())
            }
            OpCode::Dag => {
                let result = dag::exec_dag(
                    &self.last_response,
                    instr,
                    self.plan,
                    self.caller,
                    &self.arena,
                )?;
                self.last_response = result.to_vec();
                self.hop_count += 1;
                Ok(())
            }
            OpCode::Jmp => {
                self.ip = instr.a as usize;
                Ok(())
            }
            OpCode::Pipe => {
                Ok(())
            }
            OpCode::Label => {
                Ok(())
            }
            OpCode::SvcArg | OpCode::RetryData | OpCode::JumpOffset => {
                Err(format!("bare argument opcode at ip={}", self.ip - 1))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{lexer, parser, optimizer};
    use crate::bytecode::plan::ExecutionPlan;

    fn compile_dsl(dsl: &str) -> ExecutionPlan {
        use crate::dsl::compiler::Compiler;
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = optimizer::Optimizer::new();
        let optimized = opt.optimize(&pipeline);
        let compiler = Compiler::new(&[]);
        compiler.compile(&optimized, "test").unwrap()
    }

    fn mock_caller(_svc_id: u16, body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Ok(body.to_vec())
    }

    fn mock_failing_caller(_svc_id: u16, _body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Err("mock failure".to_string())
    }

    #[test]
    fn test_vm_simple_next() {
        let plan = compile_dsl("n:validate");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"hello", arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.hop_count, 1);
    }

    #[test]
    fn test_vm_chain() {
        let plan = compile_dsl("n:a n:b n:c");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"hello", arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.hop_count, 3);
    }

    #[test]
    fn test_vm_drop_halt() {
        let plan = compile_dsl("n:a d n:b");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"hello", arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.hop_count, 1);
    }

    #[test]
    fn test_vm_fallback_after_failure() {
        let plan = compile_dsl("n:a f:b");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"hello", arena, &mock_failing_caller);
        vm.run().unwrap();
        assert!(vm.failed);
    }

    #[test]
    fn test_vm_async() {
        let plan = compile_dsl("a:svc e:analytics");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"hello", arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.hop_count, 1);
    }

    #[test]
    fn test_vm_parallel_collect() {
        let plan = compile_dsl("p:a,b c");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"{\"x\":1}", arena, &mock_caller);
        vm.run().unwrap();
        assert!(vm.hop_count > 0);
    }

    #[test]
    fn test_vm_gate_true() {
        let dsl = "g:x==1 n:svc";
        let plan = compile_dsl(dsl);
        let body = b"{\"x\":1}";
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, body, arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.hop_count, 1);
    }

    #[test]
    fn test_vm_emit() {
        let plan = compile_dsl("e:a,b,c");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"hello", arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.hop_count, 0);
    }

    #[test]
    fn test_vm_dag() {
        let dsl = "dag:{A:[B,C],D:[A]} e:audit";
        let plan = compile_dsl(dsl);
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"{\"x\":1}", arena, &mock_caller);
        vm.run().unwrap();
        assert!(vm.hop_count > 0);
    }

    #[test]
    fn test_vm_map() {
        let dsl = "m:.x n:svc";
        let plan = compile_dsl(dsl);
        let body = b"{\"x\":42}";
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, body, arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.hop_count, 1);
    }

    #[test]
    fn test_vm_full_pipeline() {
        let dsl = "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics";
        let plan = compile_dsl(dsl);
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, b"{\"type\":\"ORDER\"}", arena, &mock_caller);
        vm.run().unwrap();
        assert!(vm.hop_count > 0);
    }
}
