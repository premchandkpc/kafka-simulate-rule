# Execution Context

Add `ExecutionContext` — a struct threaded through the VM that carries MessageID, CorrelationID, TraceID, Partition, Offset, Headers, Deadline, Tenant, RetryCount, and local variables. The VM should operate on the context, not directly on the body.

**Files affected:**
- `rust/src/executor/mod.rs`
- `rust/src/executor/context.rs` (new)
- `rust/src/bytecode/plan.rs`

**Verification:** Existing tests pass unchanged. New tests verify context fields propagate through Next, Parallel, Gate, Map, Emit operations.
