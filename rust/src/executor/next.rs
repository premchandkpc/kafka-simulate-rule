use std::time::Duration;
use rand::Rng;
use rayon::prelude::*;

use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::{ExecutionPlan, RetryConfig, RetryStrategy};
use crate::bytecode::opcode::ChunkMode;

pub fn exec_next(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    async_ack: bool,
) -> Result<Vec<u8>, String> {
    let svc_id = instr.a;
    let timeout_ms = instr.timeout_ms();
    let has_retry = instr.has_retry();

    if has_retry {
        let retry_cfg = find_retry_config(instr, plan);
        exec_with_retry(svc_id, body, timeout_ms, &retry_cfg, caller, async_ack)
    } else {
        caller(svc_id, body, timeout_ms)
    }
}

fn exec_with_retry(
    svc_id: u16,
    body: &[u8],
    timeout_ms: u64,
    retry_cfg: &RetryConfig,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    async_ack: bool,
) -> Result<Vec<u8>, String> {
    let max = retry_cfg.max_attempts as usize + 1;
    let mut last_err = None;

    for attempt in 0..max {
        if attempt > 0 {
            let delay = match retry_cfg.strategy {
                RetryStrategy::Exponential => {
                    let base = Duration::from_millis(100 * (1u64 << (attempt - 1)));
                    let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..50));
                    base.min(Duration::from_secs(10)) + jitter
                }
                RetryStrategy::Linear => {
                    Duration::from_millis(100 * attempt as u64)
                }
                RetryStrategy::Fixed => {
                    Duration::from_millis(retry_cfg.fixed_ms as u64)
                }
            };
            std::thread::sleep(delay);
        }

        match caller(svc_id, body, timeout_ms) {
            Ok(resp) => {
                if async_ack {
                    return Ok(Vec::new());
                }
                return Ok(resp);
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| "all retries exhausted".to_string()))
}

fn find_retry_config(instr: &Instruction, plan: &ExecutionPlan) -> RetryConfig {
    let cfg_idx = instr.c as usize;
    if cfg_idx < plan.retry_configs.len() {
        plan.retry_configs[cfg_idx].clone()
    } else {
        RetryConfig {
            max_attempts: 3,
            strategy: RetryStrategy::Exponential,
            fixed_ms: 0,
        }
    }
}

pub fn exec_chunked_call(
    svc_id: u16,
    body: &[u8],
    count: u8,
    mode: ChunkMode,
    timeout_ms: u64,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
) -> Result<Vec<u8>, String> {
    let chunk_size = body.len().div_ceil(count as usize);
    let chunk_id = uuid::Uuid::new_v4().to_string();

    match mode {
        ChunkMode::Sequential => {
            for (i, chunk) in body.chunks(chunk_size).enumerate() {
                let _headers = format_chunk_headers(&chunk_id, i, count);
                caller(svc_id, chunk, timeout_ms)?;
            }
            Ok(Vec::new())
        }
        ChunkMode::Parallel => {
            let chunks: Vec<&[u8]> = body.chunks(chunk_size).collect();
            let results: Vec<Result<Vec<u8>, String>> = chunks
                .par_iter()
                .enumerate()
                .map(|(i, chunk)| {
                    let _headers = format_chunk_headers(&chunk_id, i, count);
                    caller(svc_id, chunk, timeout_ms)
                })
                .collect();
            results.into_iter().collect::<Result<Vec<_>, _>>()?;
            Ok(Vec::new())
        }
    }
}

fn format_chunk_headers(chunk_id: &str, index: usize, total: u8) -> String {
    format!(
        "X-FlowRule-Chunk-ID: {}\nX-FlowRule-Chunk-Index: {}\nX-FlowRule-Chunk-Total: {}",
        chunk_id, index, total
    )
}
