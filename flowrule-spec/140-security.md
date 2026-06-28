# Security Specification

## 1. Principles

- **Sandbox by default**: Plugins have zero host access unless explicitly granted.
- **Encryption in transit**: All transports use TLS 1.3 minimum.
- **Immutable execution**: Compiled bytecode is never modified at runtime.
- **No eval**: No runtime code generation or string-to-code paths.
- **No reflection**: All dispatch is static, all interfaces are explicit.
- **Least privilege**: Each subsystem has only the capabilities it needs.

## 2. Plugin Security

### 2.1 WASM Sandbox
- No file system access
- No network access
- No environment variable access
- No system call access
- Linear memory is isolated per instance
- Instruction budget: future implementation

### 2.2 Plugin Verification
- ABI version check before instantiation
- Module hash verification (optional)
- Capability declaration (future)
- Plugin signature verification (future)

## 3. Transport Security

### 3.1 TLS
- Minimum: TLS 1.2
- Preferred: TLS 1.3
- Cipher suites: modern AEAD (AES-GCM, ChaCha20-Poly1305)
- Certificate validation: required in production
- mTLS: optional, configurable

### 3.2 gRPC
- TLS via credentials
- Optional mutual authentication
- No plaintext in production

## 4. Bytecode Integrity

### 4.1 Verification
- Magic bytes check
- Version compatibility
- Section bounds validation
- Constant pool index validation
- Jump target bounds validation
- Optional SHA-256 checksum

### 4.2 Immutability
- `.flow` files are never modified after creation
- Hot reload creates new files, never modifies existing
- No self-modifying code support

## 5. Resource Protection

### 5.1 Timeouts
- Plugin execution: 5s
- NEXT call: configurable (default 30s)
- Overall message: configurable (inherited from caller context)

### 5.2 Limits
- Plugin memory: 64KB linear memory
- Worker count: bounded goroutine pool
- Credit balance: per-target upper bound
- Queue depth: per-worker channel capacity
- Message body: configurable (default 1MB)

## 6. Threat Model

| Threat | Mitigation |
|--------|------------|
| Malicious plugin | WASM sandbox, no host access, timeout |
| Bytecode injection | Verification, checksum, version check |
| DoS via resource exhaustion | Credit system, worker pool, timeouts |
| TLS downgrade | Enforce minimum TLS 1.2 |
| Configuration tamper | File permissions, verification |
| Memory exhaustion | Per-message arena reset, slab pools |
