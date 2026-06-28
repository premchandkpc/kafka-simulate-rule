use super::parser::{ASTNode, Pipeline};

#[derive(Debug, Clone)]
pub struct OptimizedPipeline {
    pub nodes: Vec<ASTNode>,
}

pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Optimizer
    }

    pub fn optimize(&self, pipeline: &Pipeline) -> OptimizedPipeline {
        let nodes = self.hoist_timeouts(&pipeline.nodes);
        let nodes = self.merge_emits(&nodes);
        let nodes = self.remove_dead_code(&nodes);
        let nodes = self.merge_retries(&nodes);
        let nodes = self.remove_nops(&nodes);
        OptimizedPipeline { nodes }
    }

    fn hoist_timeouts(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut pending_timeout: Option<u64> = None;
        let mut pending_retry: Option<ASTNode> = None;

        for node in nodes {
            match node {
                ASTNode::Timeout(ms) => {
                    pending_timeout = Some(*ms);
                }
                ASTNode::Retry { count, strategy, fixed_ms } => {
                    pending_retry = Some(ASTNode::Retry { count: *count, strategy: strategy.clone(), fixed_ms: *fixed_ms });
                }
                ASTNode::Next(_) | ASTNode::Async(_) => {
                    if let Some(ms) = pending_timeout.take() {
                        result.push(ASTNode::Timeout(ms));
                    }
                    result.push(node.clone());
                    if let Some(retry) = pending_retry.take() {
                        result.push(retry);
                    }
                }
                _ => {
                    pending_timeout = None;
                    pending_retry = None;
                    result.push(node.clone());
                }
            }
        }

        if let Some(ms) = pending_timeout {
            result.push(ASTNode::Timeout(ms));
        }
        if let Some(retry) = pending_retry {
            result.push(retry);
        }

        result
    }

    fn merge_emits(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut i = 0;

        while i < nodes.len() {
            if let ASTNode::Emit(ref targets) = nodes[i] {
                let mut merged = targets.clone();
                let mut j = i + 1;
                while j < nodes.len() {
                    if let ASTNode::Emit(ref next_targets) = nodes[j] {
                        merged.extend(next_targets.clone());
                        j += 1;
                    } else {
                        break;
                    }
                }
                result.push(ASTNode::Emit(merged));
                i = j;
            } else {
                result.push(nodes[i].clone());
                i += 1;
            }
        }

        result
    }

    fn remove_dead_code(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut dead = false;
        let mut pending_labels = Vec::new();

        for node in nodes {
            if dead {
                match node {
                    ASTNode::Label(_) | ASTNode::Jmp(_) => {
                        for lbl in pending_labels.drain(..) {
                            result.push(lbl);
                        }
                        result.push(node.clone());
                        dead = false;
                    }
                    _ => {}
                }
                continue;
            }

            match node {
                ASTNode::Drop => {
                    result.push(node.clone());
                    dead = true;
                }
                ASTNode::Label(_) => {
                    pending_labels.push(node.clone());
                }
                _ => {
                    result.extend(pending_labels.drain(..));
                    result.push(node.clone());
                }
            }
        }

        result
    }

    fn merge_retries(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut i = 0;

        while i < nodes.len() {
            if let ASTNode::Retry { .. } = nodes[i] {
                if i > 0 {
                    if let Some(ASTNode::Retry { .. }) = result.last() {
                        i += 1;
                        continue;
                    }
                }
                result.push(nodes[i].clone());
            } else {
                result.push(nodes[i].clone());
            }
            i += 1;
        }

        result
    }

    fn remove_nops(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        nodes.iter()
            .filter(|n| !matches!(n, ASTNode::Pipe))
            .cloned()
            .collect()
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::lexer;
    use crate::dsl::parser;

    fn optimize_str(dsl: &str) -> OptimizedPipeline {
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = Optimizer::new();
        opt.optimize(&pipeline)
    }

    #[test]
    fn test_hoist_timeout() {
        let opt = optimize_str("t500 n:validate");
        assert_eq!(opt.nodes.len(), 2);
        assert_eq!(opt.nodes[0], ASTNode::Timeout(500));
        assert_eq!(opt.nodes[1], ASTNode::Next("validate".to_string()));
    }

    #[test]
    fn test_merge_adjacent_emits() {
        let opt = optimize_str("e:a e:b e:c");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(opt.nodes[0], ASTNode::Emit(vec!["a".to_string(), "b".to_string(), "c".to_string()]));
    }

    #[test]
    fn test_dead_code_after_drop() {
        let opt = optimize_str("d n:svc e:a");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
    }

    #[test]
    fn test_labels_preserved_after_drop() {
        let opt = optimize_str("d end: n:svc");
        assert_eq!(opt.nodes.len(), 3);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
        assert_eq!(opt.nodes[1], ASTNode::Label("end".to_string()));
        assert_eq!(opt.nodes[2], ASTNode::Next("svc".to_string()));
    }

    #[test]
    fn test_remove_pipes() {
        let opt = optimize_str("g:a>1 n:svc1 | n:svc2");
        assert_eq!(opt.nodes.len(), 3);
        assert!(!opt.nodes.iter().any(|n| matches!(n, ASTNode::Pipe)));
    }
}
