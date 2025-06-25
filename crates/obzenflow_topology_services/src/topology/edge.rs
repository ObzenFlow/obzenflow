use crate::stages::StageId;

/// Directed edge - explicit flow direction between stages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DirectedEdge {
    pub from: StageId,
    pub to: StageId,
}

impl DirectedEdge {
    pub fn new(from: StageId, to: StageId) -> Self {
        Self { from, to }
    }
}

impl std::fmt::Display for DirectedEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.from, self.to)
    }
}