use std::collections::HashMap;
use rayon::prelude::*;

use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use crate::executor::helpers;

pub fn exec_dag(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    arena: &crate::memory::arena::Arena,
) -> Result<&mut [u8], String> {
    let dag_id = instr.a as usize;
    let dag = &plan.dag_tables[dag_id];
    let mut layer_results: HashMap<u16, Vec<u8>> = HashMap::new();

    for layer in &dag.layers {
        let layer_svc_ids = layer.clone();
        let results: Vec<(u16, Result<Vec<u8>, String>)> = layer_svc_ids
            .par_iter()
            .map(|&svc_id| {
                let resp = caller(svc_id, body, 0);
                (svc_id, resp)
            })
            .collect();

        for (svc_id, result) in results {
            match result {
                Ok(resp) => { layer_results.insert(svc_id, resp); }
                Err(e) => return Err(format!("dag layer service {}: {}", svc_id, e)),
            }
        }
    }

    // Merge terminal node responses into JSON object keyed by service name
    let merged = merge_dag_results(&dag.terminal_nodes, &layer_results, plan, arena);
    Ok(merged)
}

fn merge_dag_results(
    terminal_nodes: &[u16],
    results: &HashMap<u16, Vec<u8>>,
    plan: &ExecutionPlan,
    arena: &crate::memory::arena::Arena,
) -> &mut [u8] {
    let mut entries = Vec::new();
    for &svc_id in terminal_nodes {
        if let Some(resp) = results.get(&svc_id) {
            let svc_name = plan.services.get(svc_id).name.clone();
            let entry = format!("\"{}\":{}", svc_name, String::from_utf8_lossy(resp));
            entries.push(entry);
        }
    }

    let joined = entries.join(",");
    let result = format!("{{{}}}", joined);
    arena.alloc_copy(result.as_bytes())
}
