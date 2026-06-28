use std::fmt;
use super::lexer::Token;

#[derive(Debug, Clone, PartialEq)]
pub enum ASTNode {
    Next(String),
    Async(String),
    Parallel(Vec<String>),
    Collect,
    Fallback(String),
    Gate { field: String, op: String, value: String },
    Split(String),
    Map(String),
    Emit(Vec<String>),
    Drop,
    Buffer(u64),
    Key(String),
    Retry { count: u8, strategy: Option<String>, fixed_ms: Option<u32> },
    Pipe,
    Timeout(u64),
    Chunk { count: u8, mode: String },
    Dag(String),
    Label(String),
    Jmp(String),
}

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub nodes: Vec<ASTNode>,
}

#[derive(Debug)]
pub enum ParseError {
    RetryWithoutPrecedingService(String),
    RetryAfterParallel(String),
    RetryAfterCollect(String),
    RetryAfterFallback(String),
    RetryAfterEmit(String),
    RetryAfterDrop(String),
    RetryAfterGate(String),
    RetryAfterPipe(String),
    RetryAfterLabel(String),
    RetryAfterJmp(String),
    RetryAfterRetry(String),
    CollectWithoutParallel(String),
    ChunkWithoutFollowingService(String),
    ChunkAfterDrop(String),
    DagAfterDrop(String),
    MultipleTimeoutsBetweenServices(String),
    UnexpectedToken(String),
    EmptyPipeline,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::RetryWithoutPrecedingService(s) => write!(f, "retry without preceding service: {}", s),
            ParseError::RetryAfterParallel(s) => write!(f, "retry after parallel is not allowed: {}", s),
            ParseError::RetryAfterCollect(s) => write!(f, "retry after collect is not allowed: {}", s),
            ParseError::RetryAfterFallback(s) => write!(f, "retry after fallback is not allowed: {}", s),
            ParseError::RetryAfterEmit(s) => write!(f, "retry after emit is not allowed: {}", s),
            ParseError::RetryAfterDrop(s) => write!(f, "retry after drop is not allowed: {}", s),
            ParseError::RetryAfterGate(s) => write!(f, "retry after gate is not allowed: {}", s),
            ParseError::RetryAfterPipe(s) => write!(f, "retry after pipe is not allowed: {}", s),
            ParseError::RetryAfterLabel(s) => write!(f, "retry after label is not allowed: {}", s),
            ParseError::RetryAfterJmp(s) => write!(f, "retry after jmp is not allowed: {}", s),
            ParseError::RetryAfterRetry(s) => write!(f, "retry after retry is not allowed: {}", s),
            ParseError::CollectWithoutParallel(s) => write!(f, "collect without preceding parallel: {}", s),
            ParseError::ChunkWithoutFollowingService(s) => write!(f, "chunk must be followed by next/parallel: {}", s),
            ParseError::ChunkAfterDrop(s) => write!(f, "chunk after drop is useless: {}", s),
            ParseError::DagAfterDrop(s) => write!(f, "dag after drop is useless: {}", s),
            ParseError::MultipleTimeoutsBetweenServices(s) => write!(f, "multiple timeouts without service between: {}", s),
            ParseError::UnexpectedToken(s) => write!(f, "unexpected token: {}", s),
            ParseError::EmptyPipeline => write!(f, "empty pipeline"),
        }
    }
}

impl std::error::Error for ParseError {}

fn is_service_op(t: &ASTNode) -> bool {
    matches!(t, ASTNode::Next(_) | ASTNode::Async(_) | ASTNode::Parallel(_) | ASTNode::Emit(_) | ASTNode::Drop | ASTNode::Fallback(_) | ASTNode::Jmp(_) | ASTNode::Label(_))
}

fn is_branch_point(t: &ASTNode) -> bool {
    matches!(t, ASTNode::Pipe | ASTNode::Gate { .. } | ASTNode::Label(_))
}

pub fn parse(tokens: &[Token]) -> Result<Pipeline, ParseError> {
    if tokens.is_empty() {
        return Err(ParseError::EmptyPipeline);
    }

    let mut nodes = Vec::new();
    let mut last_was_service = false;
    let mut last_was_collect = false;
    let mut last_was_retry = false;
    let mut pending_collect = false;
    let mut has_timeout = false;

    for token in tokens {
        let node = token_to_ast(token)?;

        match &node {
            ASTNode::Retry { .. } => {
                if !last_was_service {
                    return Err(ParseError::RetryWithoutPrecedingService(node.to_string()));
                }
                if last_was_retry {
                    return Err(ParseError::RetryAfterRetry(node.to_string()));
                }
                last_was_retry = true;
                has_timeout = false;
            }
            ASTNode::Timeout(_) => {
                if has_timeout && !last_was_service {
                    return Err(ParseError::MultipleTimeoutsBetweenServices(node.to_string()));
                }
                has_timeout = true;
                last_was_service = false;
                last_was_collect = false;
            }
            ASTNode::Chunk { .. } => {
                last_was_service = false;
                last_was_collect = false;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Next(_) | ASTNode::Async(_) => {
                last_was_service = true;
                last_was_collect = false;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Parallel(_) => {
                last_was_service = true;
                pending_collect = true;
                last_was_collect = false;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Collect => {
                if !pending_collect {
                    return Err(ParseError::CollectWithoutParallel(node.to_string()));
                }
                pending_collect = false;
                last_was_service = true;
                last_was_collect = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Emit(_) => {
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Drop => {
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Fallback(_) => {
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Pipe | ASTNode::Gate { .. } | ASTNode::Label(_) | ASTNode::Jmp(_) => {
                last_was_service = false;
                last_was_retry = false;
            }
            _ => {
                last_was_service = false;
                last_was_retry = false;
            }
        }

        nodes.push(node);
    }

    Ok(Pipeline { nodes })
}

fn token_to_ast(token: &Token) -> Result<ASTNode, ParseError> {
    match token {
        Token::Next(t) => Ok(ASTNode::Next(t.clone())),
        Token::Async(t) => Ok(ASTNode::Async(t.clone())),
        Token::Parallel(ts) => Ok(ASTNode::Parallel(ts.clone())),
        Token::Collect => Ok(ASTNode::Collect),
        Token::Fallback(t) => Ok(ASTNode::Fallback(t.clone())),
        Token::Gate { field, op, value } => Ok(ASTNode::Gate { field: field.clone(), op: op.clone(), value: value.clone() }),
        Token::Split(f) => Ok(ASTNode::Split(f.clone())),
        Token::Map(e) => Ok(ASTNode::Map(e.clone())),
        Token::Emit(ts) => Ok(ASTNode::Emit(ts.clone())),
        Token::Drop => Ok(ASTNode::Drop),
        Token::Buffer(n) => Ok(ASTNode::Buffer(*n)),
        Token::Key(k) => Ok(ASTNode::Key(k.clone())),
        Token::Retry { count, strategy, fixed_ms } => Ok(ASTNode::Retry { count: *count, strategy: strategy.clone(), fixed_ms: *fixed_ms }),
        Token::Pipe => Ok(ASTNode::Pipe),
        Token::Timeout(ms) => Ok(ASTNode::Timeout(*ms)),
        Token::Chunk { count, mode } => Ok(ASTNode::Chunk { count: *count, mode: mode.clone() }),
        Token::Dag(body) => Ok(ASTNode::Dag(body.clone())),
        Token::Label(l) => Ok(ASTNode::Label(l.clone())),
        Token::Jmp(l) => Ok(ASTNode::Jmp(l.clone())),
    }
}

impl fmt::Display for ASTNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ASTNode::Next(t) => write!(f, "n:{}", t),
            ASTNode::Async(t) => write!(f, "a:{}", t),
            ASTNode::Parallel(ts) => write!(f, "p:{}", ts.join(",")),
            ASTNode::Collect => write!(f, "c"),
            ASTNode::Fallback(t) => write!(f, "f:{}", t),
            ASTNode::Gate { field, op, value } => write!(f, "g:{}{}{}", field, op, value),
            ASTNode::Split(f) => write!(f, "s:{}", f),
            ASTNode::Map(e) => write!(f, "m:{}", e),
            ASTNode::Emit(ts) => write!(f, "e:{}", ts.join(",")),
            ASTNode::Drop => write!(f, "d"),
            ASTNode::Buffer(n) => write!(f, "b{}", n),
            ASTNode::Key(k) => write!(f, "k:{}", k),
            ASTNode::Retry { count, strategy, fixed_ms: _ } => {
                if let Some(s) = strategy {
                    write!(f, "r{}:{}", count, s)
                } else {
                    write!(f, "r{}", count)
                }
            }
            ASTNode::Pipe => write!(f, "|"),
            ASTNode::Timeout(ms) => write!(f, "t{}", ms),
            ASTNode::Chunk { count, mode } => write!(f, "chunk:{}:{}", count, mode),
            ASTNode::Dag(body) => write!(f, "dag:{}", body),
            ASTNode::Label(l) => write!(f, "{}:", l),
            ASTNode::Jmp(l) => write!(f, "jmp {}", l),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_str(dsl: &str) -> Result<Pipeline, ParseError> {
        let tokens = crate::dsl::lexer::lex(dsl).unwrap();
        parse(&tokens)
    }

    #[test]
    fn test_simple_next() {
        let p = parse_str("n:validate").unwrap();
        assert_eq!(p.nodes.len(), 1);
        assert_eq!(p.nodes[0], ASTNode::Next("validate".to_string()));
    }

    #[test]
    fn test_next_with_retry() {
        let p = parse_str("n:validate r3").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Next("validate".to_string()));
        assert_eq!(p.nodes[1], ASTNode::Retry { count: 3, strategy: None, fixed_ms: None });
    }

    #[test]
    fn test_retry_after_parallel_error() {
        let result = parse_str("p:a,b r3");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::RetryAfterParallel(_)));
    }

    #[test]
    fn test_collect_without_parallel_error() {
        let result = parse_str("c");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::CollectWithoutParallel(_)));
    }

    #[test]
    fn test_collect_after_parallel_ok() {
        let p = parse_str("p:a,b c").unwrap();
        assert_eq!(p.nodes.len(), 2);
    }

    #[test]
    fn test_gate_pipe_pipeline() {
        let p = parse_str("g:amount>10000 n:manual-review | t300 n:auto-approve").unwrap();
        assert_eq!(p.nodes.len(), 6);
    }

    #[test]
    fn test_full_pipeline() {
        let p = parse_str("t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics").unwrap();
        assert_eq!(p.nodes.len(), 8);
    }

    #[test]
    fn test_chunk_next() {
        let p = parse_str("chunk:10:seq n:storage").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Chunk { count: 10, mode: "seq".to_string() });
        assert_eq!(p.nodes[1], ASTNode::Next("storage".to_string()));
    }

    #[test]
    fn test_async() {
        let p = parse_str("a:job-queue e:analytics").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Async("job-queue".to_string()));
    }

    #[test]
    fn test_dag() {
        let p = parse_str("dag:{A:[B,C],D:[A]} e:audit").unwrap();
        assert_eq!(p.nodes.len(), 2);
    }

    #[test]
    fn test_empty_pipeline_error() {
        let result = parse_str("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::EmptyPipeline));
    }

    #[test]
    fn test_drop() {
        let p = parse_str("d").unwrap();
        assert_eq!(p.nodes.len(), 1);
        assert_eq!(p.nodes[0], ASTNode::Drop);
    }

    #[test]
    fn test_label_jmp() {
        let p = parse_str("start: n:svc jmp end end:").unwrap();
        assert_eq!(p.nodes.len(), 4);
        assert_eq!(p.nodes[0], ASTNode::Label("start".to_string()));
        assert_eq!(p.nodes[3], ASTNode::Label("end".to_string()));
    }

    #[test]
    fn test_key_split() {
        let p = parse_str("k:order_id s:region").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Key("order_id".to_string()));
        assert_eq!(p.nodes[1], ASTNode::Split("region".to_string()));
    }
}
