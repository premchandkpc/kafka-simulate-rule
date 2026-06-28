use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use super::expr;

pub fn exec_map<'a>(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    let expr = plan.const_pool.get(instr.a);

    if expr.is_empty() {
        return Ok(arena.alloc_copy(body));
    }

    if expr.contains('=') {
        let result = expr::eval_map_expression(expr, body)?;
        return Ok(arena.alloc_copy(&result));
    }

    if let Some(stripped) = expr.strip_prefix('.') {
        let parts: Vec<&str> = stripped.split('.').collect();
        let body_str = std::str::from_utf8(body).map_err(|e| format!("invalid utf8: {}", e))?;
        let mut current: serde_json::Value =
            serde_json::from_str(body_str).map_err(|e| format!("invalid json: {}", e))?;

        for part in &parts {
            if *part == "[]" {
                match current {
                    serde_json::Value::Array(ref arr) => {
                        if let Some(first) = arr.first() {
                            let s = first.to_string();
                            return Ok(arena.alloc_copy(s.as_bytes()));
                        }
                        return Ok(arena.alloc_copy(b"null"));
                    }
                    _ => return Ok(arena.alloc_copy(b"null")),
                }
            } else if *part == "*" {
                match current {
                    serde_json::Value::Object(ref map) => {
                        let vals: Vec<String> = map.values().map(|v| v.to_string()).collect();
                        let result = format!("[{}]", vals.join(","));
                        return Ok(arena.alloc_copy(result.as_bytes()));
                    }
                    serde_json::Value::Array(ref arr) => {
                        let vals: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
                        let result = format!("[{}]", vals.join(","));
                        return Ok(arena.alloc_copy(result.as_bytes()));
                    }
                    _ => return Ok(arena.alloc_copy(b"null")),
                }
            } else {
                match current {
                    serde_json::Value::Object(ref map) => {
                        current = map.get(*part).cloned().unwrap_or(serde_json::Value::Null);
                    }
                    _ => return Ok(arena.alloc_copy(b"null")),
                }
            }
        }

        let result = current.to_string();
        Ok(arena.alloc_copy(result.as_bytes()))
    } else {
        Ok(arena.alloc_copy(body))
    }
}
