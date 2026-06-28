pub mod chunk;
pub mod context;
pub mod dag;
pub mod emit;
pub mod expr;
pub mod gate;
pub mod helpers;
pub mod map;
pub mod next;
pub mod parallel;

use std::sync::Arc;

use crate::bytecode::instruction::Instruction;
use crate::bytecode::opcode::OpCode;
use crate::bytecode::plan::ExecutionPlan;

pub struct VM<'a> {
    pub ip: usize,
    pub plan: &'a ExecutionPlan,
    pub last_response: Vec<u8>,
    pub arena: crate::memory::arena::Arena,
    pub caller: Arc<dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync + 'a>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
    pub ctx: context::ExecutionContext,
}

impl<'a> VM<'a> {
    pub fn new<F>(
        plan: &'a ExecutionPlan,
        body: &[u8],
        arena: crate::memory::arena::Arena,
        caller: F,
    ) -> Self
    where
        F: Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync + 'a,
    {
        VM {
            ip: 0,
            plan,
            last_response: body.to_vec(),
            arena,
            caller: Arc::new(caller),
            failed: false,
            errors: Vec::new(),
            hop_count: 0,
            ctx: context::ExecutionContext::new(),
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
        let caller = self.caller.clone();
        match instr.op {
            OpCode::Next => self.op_next(instr, &*caller, false),
            OpCode::Async => self.op_next(instr, &*caller, true),
            OpCode::Parallel => self.op_parallel(instr, &*caller),
            OpCode::Collect => self.op_collect(),
            OpCode::Fallback => self.op_fallback(instr, &*caller),
            OpCode::Gate => self.op_gate(instr),
            OpCode::Emit => self.op_emit(instr, &*caller),
            OpCode::Drop => self.op_drop(),
            OpCode::Map => self.op_map(instr),
            OpCode::Dag => self.op_dag(instr, &*caller),
            OpCode::Jmp => self.op_jmp(instr),
            OpCode::Key | OpCode::Split => Ok(()),
            OpCode::Retry => Ok(()),
            OpCode::Buffer => Err("Buffer must be handled at engine level".to_string()),
            OpCode::Timeout => Ok(()),
            OpCode::Chunk => Ok(()),
            OpCode::Pipe => Ok(()),
            OpCode::Label => Ok(()),
            OpCode::SvcArg | OpCode::RetryData | OpCode::JumpOffset => Ok(()),
        }
    }

    fn op_next(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
        is_async: bool,
    ) -> Result<(), String> {
        match next::exec_next(&self.last_response, instr, self.plan, caller, is_async) {
            Ok(resp) => {
                self.last_response = resp;
                self.hop_count += 1;
                Ok(())
            }
            Err(e) => {
                self.failed = true;
                self.errors.push(e);
                Ok(())
            }
        }
    }

    fn op_parallel(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        let result =
            parallel::exec_parallel(&self.last_response, instr, self.plan, caller, &self.arena)?;
        self.last_response = result.to_vec();
        Ok(())
    }

    fn op_collect(&mut self) -> Result<(), String> {
        self.hop_count += 1;
        Ok(())
    }

    fn op_fallback(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        if self.failed {
            self.failed = false;
            match next::exec_next(&self.last_response, instr, self.plan, caller, false) {
                Ok(resp) => {
                    self.last_response = resp;
                    self.hop_count += 1;
                }
                Err(e) => {
                    self.failed = true;
                    self.errors.push(e);
                }
            }
        }
        Ok(())
    }

    fn op_gate(&mut self, instr: &Instruction) -> Result<(), String> {
        let mut skip = 0usize;
        gate::exec_jmp_if_false(&self.last_response, instr, self.plan, &self.arena, &mut skip);
        Ok(())
    }

    fn op_emit(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        emit::exec_emit(&self.last_response, instr, self.plan, caller)
    }

    fn op_drop(&mut self) -> Result<(), String> {
        self.ip = self.plan.instructions.len();
        Ok(())
    }

    fn op_map(&mut self, instr: &Instruction) -> Result<(), String> {
        let result = map::exec_map(&self.last_response, instr, self.plan, &self.arena)?;
        self.last_response = result.to_vec();
        Ok(())
    }

    fn op_dag(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        let result = dag::exec_dag(&self.last_response, instr, self.plan, caller, &self.arena)?;
        self.last_response = result.to_vec();
        self.hop_count += 1;
        Ok(())
    }

    fn op_jmp(&mut self, instr: &Instruction) -> Result<(), String> {
        self.ip = instr.a as usize;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::plan::ExecutionPlan;
    use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};

    fn compile_dsl(dsl: &str) -> ExecutionPlan {
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
