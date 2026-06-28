# Glossary

| Term | Definition |
|------|------------|
| **Bytecode** | Immutable, verifiable, portable representation of a compiled FlowRule program. File extension `.flow`. |
| **Circuit Breaker** | Reliability pattern that prevents calls to a failing target by transitioning through closed → open → half-open states. |
| **Collect** | Synchronization point that waits for all branches of a parallel block to complete. |
| **Compilation** | The process of transforming DSL source text → AST → optimized IR → bytecode. |
| **Constant Pool** | A section in the bytecode module containing all string literals, URLs, field paths, and operators referenced by instructions. |
| **Control Plane** | The subsystem responsible for compilation, validation, deployment, and lifecycle management of rules. |
| **Credit** | Unit of backpressure. Each target has a credit balance. Debits on send, credits on response. Block when zero. |
| **Dead Letter Queue (DLQ)** | Durable store for messages that could not be delivered after exhausting retry policy. |
| **Drop** | Instruction that terminates message processing without delivery. Used in filtering pipelines. |
| **DSL** | Domain-Specific Language for expressing routing programs. Compact, declarative, one token per operation. |
| **Emit** | Instruction for fire-and-forget event publication. No acknowledgment expected. |
| **Execution Plan** | The in-memory representation of a compiled program before bytecode encoding. |
| **Fallback** | Alternative target invoked when the primary target fails after exhausting retry policy. |
| **Flow** | Synonym for a compiled rule program. Also the `.flow` file extension. |
| **Gate** | Content-based conditional branch. Evaluates a field against an operator and value. Routes to targets if condition passes. |
| **Hop** | One unit of message delivery from one target to another. Counted for observability. |
| **Instruction** | A single operation in the bytecode stream. 16 bytes fixed-size: 1 byte opcode, 1 byte flags, 3×4 byte arguments. |
| **Intern Table** | String interning table for frequently used header and field names. Reduces allocation. |
| **Map** | Instruction that transforms a message by extracting or reshaping fields. |
| **Module** | A complete compiled rule: header, constant pool, target lists, instruction stream, map expressions, metadata. |
| **Next** | Instruction that delivers the message to a single target with optional timeout and retry. |
| **Opcode** | Numeric identifier for an instruction type (NOP=0, NEXT=1, ..., JUMP_IFN=16). |
| **Parallel** | Instruction that fans out a message to multiple targets concurrently. |
| **Partition Key** | A value derived from the message used for partitioning, ordering, or routing decisions. |
| **Pipe** | Instruction that marks the end of a gate-true branch. Skips to the next pipe or end. |
| **Plugin** | WASM-compiled extension that executes at defined attachment points (GATE, MAP, PRE_CALL, POST_CALL). |
| **Retry** | Policy specifying number of delivery attempts and backoff strategy for a Next instruction. |
| **Rule** | A named, versioned, compiled program that processes messages matching its conditions. |
| **Rule Table** | Atomic snapshot of all active rules. Swapped atomically on hot-reload. |
| **Runtime** | The subsystem that loads modules, manages execution contexts, and provides lifecycle, scheduling, and resource management. |
| **Scheduler** | Component that assigns messages to workers, manages concurrency, and enforces backpressure. |
| **Section** | A named region in the bytecode format: const pool, target lists, instructions, map exprs, rule metadata, debug. |
| **Target** | A named destination (URL, service, queue) that receives messages. Resolved during compilation. |
| **Target List** | An ordered list of constant pool indices used by PARALLEL and EMIT instructions. |
| **Timeout** | Maximum wall-clock time allowed for a single Next call. |
| **Transport** | A protocol adapter (HTTP, gRPC, Kafka) that sends and receives messages on behalf of the runtime. |
| **VM** | The virtual machine that dispatches bytecode instructions. Stateless, deterministic, embeddable. |
| **WASM** | WebAssembly — portable binary format for plugin execution. Sandboxed, language-independent. |
