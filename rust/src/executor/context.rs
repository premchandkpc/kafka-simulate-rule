pub struct ExecutionContext {
    pub message_id: String,
    pub correlation_id: String,
    pub trace_id: String,
    pub partition: u32,
    pub offset: i64,
    pub headers: Vec<(String, String)>,
    pub retry_count: u32,
    pub tenant: String,
    pub deadline_ms: u64,
}

impl ExecutionContext {
    pub fn new() -> Self {
        ExecutionContext {
            message_id: String::new(),
            correlation_id: String::new(),
            trace_id: String::new(),
            partition: 0,
            offset: 0,
            headers: Vec::new(),
            retry_count: 0,
            tenant: String::new(),
            deadline_ms: 0,
        }
    }
}
