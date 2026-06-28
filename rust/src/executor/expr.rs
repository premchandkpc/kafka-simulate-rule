use std::time::SystemTime;

#[derive(Debug, Clone)]
enum Expr {
    Field(String),
    FnCall {
        name: String,
        args: Vec<Expr>,
    },
    Concat(Vec<Expr>),
    Literal(String),
    Number(f64),
    Raw(String),
}

fn parse_expr(input: &str) -> Result<Expr, String> {
    let input = input.trim();

    if input.starts_with('"') && input.ends_with('"') {
        return Ok(Expr::Literal(input[1..input.len() - 1].to_string()));
    }
    if input.starts_with('\'') && input.ends_with('\'') {
        return Ok(Expr::Literal(input[1..input.len() - 1].to_string()));
    }
    if let Ok(n) = input.parse::<f64>() {
        return Ok(Expr::Number(n));
    }

    if let Some(open) = input.find('(') {
        if input.ends_with(')') {
            let name = input[..open].trim().to_string();
            let args_str = &input[open + 1..input.len() - 1];
            let args = if args_str.trim().is_empty() {
                Vec::new()
            } else {
                parse_args(args_str)?
            };
            return Ok(Expr::FnCall { name, args });
        }
    }

    if input.contains('+') && !input.starts_with('+') {
        let parts: Vec<&str> = input.split('+').collect();
        if parts.iter().all(|p| !p.contains('(') && !p.contains(')')) {
            let mut exprs = Vec::new();
            for part in parts {
                exprs.push(parse_expr(part.trim())?);
            }
            return Ok(Expr::Concat(exprs));
        }
    }

    if is_valid_field(input) {
        return Ok(Expr::Field(input.to_string()));
    }

    Ok(Expr::Raw(input.to_string()))
}

fn parse_args(input: &str) -> Result<Vec<Expr>, String> {
    let mut args = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let bytes = input.as_bytes();

    for i in 0..input.len() {
        if bytes[i] == b'(' {
            depth += 1;
        }
        if bytes[i] == b')' {
            depth -= 1;
        }
        if (bytes[i] == b',' && depth == 0) || i == input.len() - 1 {
            let end = if i == input.len() - 1 { i + 1 } else { i };
            let part = input[start..end].trim();
            if !part.is_empty() {
                args.push(parse_expr(part)?);
            }
            start = i + 1;
        }
    }
    Ok(args)
}

fn is_valid_field(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
}

fn eval_expr(expr: &Expr, body: &serde_json::Value) -> Result<serde_json::Value, String> {
    match expr {
        Expr::Field(path) => resolve_field(body, path),
        Expr::FnCall { name, args } => {
            let evaluated: Result<Vec<serde_json::Value>, String> = args
                .iter()
                .map(|a| eval_expr(a, body))
                .collect();
            let evaluated = evaluated?;
            let str_owned: Vec<String> = evaluated.iter().map(value_to_string).collect();
            let refs: Vec<&str> = str_owned.iter().map(|s| s.as_str()).collect();
            call_builtin(name, &refs)
        }
        Expr::Concat(parts) => {
            let mut result = String::new();
            for part in parts {
                let val = eval_expr(part, body)?;
                result.push_str(&value_to_string(&val));
            }
            Ok(serde_json::Value::String(result))
        }
        Expr::Literal(s) => Ok(serde_json::Value::String(s.clone())),
        Expr::Number(n) => Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(*n).unwrap_or(serde_json::Number::from(0)),
        )),
        Expr::Raw(s) => Ok(serde_json::Value::String(s.clone())),
    }
}

fn value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".to_string(),
        _ => v.to_string(),
    }
}

fn resolve_field(body: &serde_json::Value, path: &str) -> Result<serde_json::Value, String> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = body.clone();
    for part in parts {
        match current {
            serde_json::Value::Object(ref map) => {
                current = map.get(part).cloned().unwrap_or(serde_json::Value::Null);
            }
            _ => return Ok(serde_json::Value::Null),
        }
    }
    Ok(current)
}

fn set_field(
    body: &mut serde_json::Value,
    path: &str,
    value: serde_json::Value,
) -> Result<(), String> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return Err("empty path".to_string());
    }
    if parts.len() == 1 {
        if let serde_json::Value::Object(ref mut map) = body {
            map.insert(parts[0].to_string(), value);
            return Ok(());
        }
        return Err("root is not an object".to_string());
    }
    let mut current = body;
    for i in 0..parts.len() - 1 {
        match current {
            serde_json::Value::Object(ref mut map) => {
                current = map
                    .entry(parts[i].to_string())
                    .or_insert(serde_json::Value::Object(serde_json::Map::new()));
            }
            _ => return Err(format!("cannot set field in non-object at {}", parts[i])),
        }
    }
    if let serde_json::Value::Object(ref mut map) = current {
        map.insert(parts[parts.len() - 1].to_string(), value);
        Ok(())
    } else {
        Err("target is not an object".to_string())
    }
}

fn call_builtin(name: &str, args: &[&str]) -> Result<serde_json::Value, String> {
    match name {
        "uuid" => Ok(serde_json::Value::String(uuid::Uuid::new_v4().to_string())),
        "now" => Ok(serde_json::Value::String(now_iso())),
        "lower" => {
            let s = args.first().unwrap_or(&"");
            Ok(serde_json::Value::String(s.to_lowercase()))
        }
        "upper" => {
            let s = args.first().unwrap_or(&"");
            Ok(serde_json::Value::String(s.to_uppercase()))
        }
        "trim" => {
            let s = args.first().unwrap_or(&"");
            Ok(serde_json::Value::String(s.trim().to_string()))
        }
        "length" => {
            let s = args.first().unwrap_or(&"");
            Ok(serde_json::Value::Number(serde_json::Number::from(s.len())))
        }
        "concat" => Ok(serde_json::Value::String(args.concat())),
        "base64" => {
            let s = args.first().unwrap_or(&"");
            Ok(serde_json::Value::String(base64_encode(s)))
        }
        "json" => {
            let s = args.first().unwrap_or(&"");
            serde_json::from_str(s).map_err(|e| format!("json parse error: {}", e))
        }
        "substring" => {
            let s = args.first().unwrap_or(&"");
            let start = args
                .get(1)
                .and_then(|a| a.parse::<f64>().ok())
                .unwrap_or(0.0) as usize;
            let end = args
                .get(2)
                .and_then(|a| a.parse::<f64>().ok())
                .unwrap_or(s.len() as f64) as usize;
            let end = end.min(s.len());
            Ok(serde_json::Value::String(s[start..end].to_string()))
        }
        "replace" => {
            let s = args.first().unwrap_or(&"");
            let from = args.get(1).unwrap_or(&"");
            let to = args.get(2).unwrap_or(&"");
            Ok(serde_json::Value::String(s.replace(from, to)))
        }
        _ => Err(format!("unknown function: {}", name)),
    }
}

fn now_iso() -> String {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let total_secs = d.as_secs();
    let nanos = d.subsec_nanos();
    let days = total_secs / 86400;
    let time = total_secs % 86400;
    let hours = time / 3600;
    let mins = (time % 3600) / 60;
    let sec = time % 60;

    let year = 1970 + (days / 365) as u32;
    let month = 1 + ((days % 365) / 28) as u32;
    let day = 1 + (days % 28) as u32;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        year, month, day, hours as u32, mins as u32, sec as u32, nanos
    )
}

fn base64_encode(s: &str) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

pub fn eval_map_expression(expr_str: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    let eq_pos = expr_str
        .find('=')
        .ok_or_else(|| "not an assignment expression (missing =)".to_string())?;

    let dest = expr_str[..eq_pos].trim();
    let source_expr = expr_str[eq_pos + 1..].trim();

    if dest.is_empty() || source_expr.is_empty() {
        return Err("empty target or source in map expression".to_string());
    }

    let body_str = std::str::from_utf8(body).map_err(|e| format!("invalid utf8: {}", e))?;
    let mut body_json: serde_json::Value =
        serde_json::from_str(body_str).map_err(|e| format!("invalid json: {}", e))?;

    let source_parsed = parse_expr(source_expr)?;
    let value = eval_expr(&source_parsed, &body_json)?;

    set_field(&mut body_json, dest, value)?;

    let result = serde_json::to_string(&body_json)
        .map_err(|e| format!("serialize error: {}", e))?;
    Ok(result.into_bytes())
}

pub fn format_now() -> String {
    now_iso()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_path() {
        let body: serde_json::Value =
            serde_json::from_str(r#"{"user":{"name":"alice"}}"#).unwrap();
        let val = resolve_field(&body, "user.name").unwrap();
        assert_eq!(val, serde_json::Value::String("alice".to_string()));
    }

    #[test]
    fn test_field_copy() {
        let body = br#"{"first":"alice","last":"smith"}"#;
        let result = eval_map_expression("fullname=concat(first,last)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["fullname"], "alicesmith");
    }

    #[test]
    fn test_function_lower() {
        let body = br#"{"name":"ALICE"}"#;
        let result = eval_map_expression("name=lower(name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "alice");
    }

    #[test]
    fn test_function_upper() {
        let body = br#"{"name":"alice"}"#;
        let result = eval_map_expression("name=upper(name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "ALICE");
    }

    #[test]
    fn test_function_trim() {
        let body = br#"{"name":"  alice  "}"#;
        let result = eval_map_expression("name=trim(name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "alice");
    }

    #[test]
    fn test_function_length() {
        let body = br#"{"msg":"hello"}"#;
        let result = eval_map_expression("len=length(msg)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["len"], 5);
    }

    #[test]
    fn test_function_uuid() {
        let body = br#"{}"#;
        let result = eval_map_expression("id=uuid()", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(json["id"].as_str().unwrap().len() == 36);
    }

    #[test]
    fn test_concat_expr() {
        let body = br#"{"a":"hello","b":"world"}"#;
        let result = eval_map_expression("msg=a+b", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["msg"], "helloworld");
    }

    #[test]
    fn test_nested_field_set() {
        let body = br#"{"user":{"name":"bob"}}"#;
        let result = eval_map_expression("user.role=upper(user.name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["user"]["role"], "BOB");
    }

    #[test]
    fn test_function_base64() {
        let body = br#"{"data":"hello"}"#;
        let result = eval_map_expression("encoded=base64(data)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["encoded"], "aGVsbG8=");
    }

    #[test]
    fn test_function_replace() {
        let body = br#"{"text":"hello world"}"#;
        let result =
            eval_map_expression("text=replace(text,'world','alice')", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["text"], "hello alice");
    }

    #[test]
    fn test_function_substring() {
        let body = br#"{"text":"hello world"}"#;
        let result = eval_map_expression("sub=substring(text,0,5)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["sub"], "hello");
    }
}
