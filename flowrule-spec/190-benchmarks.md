# Benchmark Specification

## 1. Purpose

Benchmarks define performance baselines, prevent regressions, and validate that implementation meets the non-functional requirements.

## 2. Benchmark Environment

All benchmarks run on:
- CPU: Apple M5 (reference), Intel Xeon (CI)
- Go version: 1.26+
- No competing load
- 10 iterations minimum, 100 recommended
- Results averaged after 3 warm-up runs

## 3. Compilation Benchmarks

### 3.1 Lexer
```
BenchmarkLex/simple-next    1M   112 ns/op    0 B/op    0 allocs/op
BenchmarkLex/full-pipeline  1M   340 ns/op    0 B/op    0 allocs/op
BenchmarkLex/gate-complex   1M   220 ns/op    0 B/op    0 allocs/op
```

### 3.2 Parser
```
BenchmarkParse/simple-next    1M   302 ns/op    0 B/op    0 allocs/op
BenchmarkParse/full-pipeline  1M   890 ns/op    0 B/op    0 allocs/op
```

### 3.3 Compiler (DSL → ExecutionPlan)
```
BenchmarkCompile/simple-next    1M   185 ns/op    0 B/op    0 allocs/op
BenchmarkCompile/full-pipeline  1M   550 ns/op    0 B/op    0 allocs/op
```

### 3.4 Bytecode Encoder
```
BenchmarkEncode/simple   1M   200 ns/op    0 B/op    0 allocs/op
BenchmarkEncode/complex  1M   600 ns/op    0 B/op    0 allocs/op
```

### 3.5 Bytecode Decoder
```
BenchmarkDecode/simple   1M   180 ns/op    0 B/op    0 allocs/op
BenchmarkDecode/complex  1M   500 ns/op    0 B/op    0 allocs/op
```

## 4. Execution Benchmarks

### 4.1 VM Dispatch
```
BenchmarkVM/NEXT             1M   95 ns/op    0 B/op    0 allocs/op
BenchmarkVM/GATE-pass        1M   180 ns/op   0 B/op    0 allocs/op
BenchmarkVM/GATE-skip        1M   160 ns/op   0 B/op    0 allocs/op
BenchmarkVM/PARALLEL-2      500K   2 μs/op    0 B/op    0 allocs/op
BenchmarkVM/COLLECT         500K   80 ns/op   0 B/op    0 allocs/op
BenchmarkVM/FALLBACK         1M   100 ns/op   0 B/op    0 allocs/op
BenchmarkVM/EMIT             1M   95 ns/op   0 B/op    0 allocs/op
BenchmarkVM/MAP              1M   200 ns/op   0 B/op    0 allocs/op
BenchmarkVM/JUMP             1M   50 ns/op    0 B/op    0 allocs/op
```

### 4.2 Full Pipeline
```
BenchmarkPipeline/simple-next-noop    1M    1 μs/op    0 B/op    0 allocs/op
BenchmarkPipeline/gate-pass-next      1M    1.5 μs/op  0 B/op    0 allocs/op
BenchmarkPipeline/parallel-3-collect 500K   3 μs/op    0 B/op    0 allocs/op
```

## 5. Memory Benchmarks

### 5.1 Arena
```
BenchmarkArenaAlloc/64      10M   8 ns/op    0 B/op    0 allocs/op
BenchmarkArenaAlloc/256     10M   8 ns/op    0 B/op    0 allocs/op
BenchmarkArenaAlloc/1024    10M   10 ns/op   0 B/op    0 allocs/op
```

### 5.2 Slab Pool
```
BenchmarkSlabGetPut/small   10M   25 ns/op   0 B/op    0 allocs/op
BenchmarkSlabGetPut/medium  10M   30 ns/op   0 B/op    0 allocs/op
BenchmarkSlabGetPut/large   10M   40 ns/op   0 B/op    0 allocs/op
```

### 5.3 Intern Table
```
BenchmarkIntern/existing    10M   15 ns/op   0 B/op    0 allocs/op
BenchmarkIntern/new          1M   50 ns/op   0 B/op    0 allocs/op
BenchmarkLookup             10M   8 ns/op    0 B/op    0 allocs/op
```

## 6. Transport Benchmarks

```
BenchmarkHTTPCall/localhost  10K   5 ms/op   1 KB/op    5 allocs/op
BenchmarkHTTPCall/timeout    10K   30 ms/op  512 B/op   3 allocs/op
```

## 7. Benchmark Comparison

All benchmarks include a baseline file (committed to repo). CI compares PR branches against baseline:
- Regression >5%: warning
- Regression >10%: failure
- Improvement >10%: update baseline

## 8. Benchmark Categories

- **Speed**: ns/op (nanoseconds per operation)
- **Allocations**: B/op (bytes per operation)
- **Alloc count**: allocs/op (allocations per operation)
- **Throughput**: ops/s (operations per second, for integration benchmarks)

## 9. Zero-Allocation Targets

The following operations must achieve zero heap allocations (0 B/op, 0 allocs/op):
- Arena Alloc (within buffer)
- Slab Get (when pool has free entries)
- Intern Table Lookup
- Intern Table Intern (existing entry)
- VM Dispatch (NEXT, GATE, JUMP, JUMP_IF, JUMP_IFN, DROP)
- Gate Evaluation (all operators)
- Bytecode Decode (constant pool access)
- Target List Resolution
