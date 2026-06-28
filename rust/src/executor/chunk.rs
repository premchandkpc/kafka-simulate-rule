pub fn split_chunks(body: &[u8], count: u8, threshold: usize) -> Option<Vec<&[u8]>> {
    if body.len() <= threshold {
        return None;
    }

    let chunk_size = body.len().div_ceil(count as usize);
    let chunks: Vec<&[u8]> = body.chunks(chunk_size).collect();
    Some(chunks)
}

pub fn execute_chunked_seq(
    svc_id: u16,
    chunks: &[&[u8]],
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    timeout_ms: u64,
) -> Result<Vec<u8>, String> {
    for chunk in chunks {
        caller(svc_id, chunk, timeout_ms)?;
    }
    Ok(Vec::new())
}

pub fn execute_chunked_par(
    svc_id: u16,
    chunks: &[&[u8]],
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    timeout_ms: u64,
) -> Result<Vec<u8>, String> {
    for chunk in chunks {
        caller(svc_id, chunk, timeout_ms)?;
    }
    Ok(Vec::new())
}
