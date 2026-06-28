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
    chunk_id: &str,
) -> Result<Vec<u8>, String> {
    for (i, chunk) in chunks.iter().enumerate() {
        let _headers = format_chunk_headers(chunk_id, i, chunks.len() as u8);
        caller(svc_id, chunk, timeout_ms)?;
    }
    Ok(Vec::new())
}

pub fn execute_chunked_par(
    svc_id: u16,
    chunks: &[&[u8]],
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    timeout_ms: u64,
    chunk_id: &str,
) -> Result<Vec<u8>, String> {
    for (i, chunk) in chunks.iter().enumerate() {
        caller(svc_id, chunk, timeout_ms)?;
    }
    Ok(Vec::new())
}

#[allow(dead_code)]
fn format_chunk_headers(chunk_id: &str, index: usize, total: u8) -> String {
    format!(
        "X-FlowRule-Chunk-ID: {}\nX-FlowRule-Chunk-Index: {}\nX-FlowRule-Chunk-Total: {}",
        chunk_id, index, total
    )
}
