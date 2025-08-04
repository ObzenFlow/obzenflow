use std::collections::{HashMap, HashSet, VecDeque};
use crate::stages::StageId;

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("Invalid edge from {from} to {to}: {reason}")]
    InvalidEdge {
        from: StageId,
        to: StageId,
        reason: String,
    },
    
    #[error("Duplicate edge from {from} to {to}")]
    DuplicateEdge {
        from: StageId,
        to: StageId,
    },
    
    #[error("Cycle detected in topology involving stages: {}", stages.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(" -> "))]
    CycleDetected {
        stages: Vec<StageId>,
    },
    
    #[error("Disconnected stages found: {}", stages.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", "))]
    DisconnectedStages {
        stages: Vec<StageId>,
    },
    
    #[error("Self-cycle detected: stage '{stage}' connects to itself")]
    SelfCycle {
        stage: String,
    },
}

/// Compute strongly connected components using Tarjan's algorithm (FLOWIP-082g)
pub fn compute_sccs<T>(
    stages: &HashMap<StageId, T>,
    downstream: &HashMap<StageId, HashSet<StageId>>,
) -> Vec<HashSet<StageId>> {
    let mut index = 0;
    let mut stack = Vec::new();
    let mut indices = HashMap::new();
    let mut lowlinks = HashMap::new();
    let mut on_stack = HashSet::new();
    let mut sccs = Vec::new();

    fn strongconnect(
        v: StageId,
        downstream: &HashMap<StageId, HashSet<StageId>>,
        index: &mut usize,
        stack: &mut Vec<StageId>,
        indices: &mut HashMap<StageId, usize>,
        lowlinks: &mut HashMap<StageId, usize>,
        on_stack: &mut HashSet<StageId>,
        sccs: &mut Vec<HashSet<StageId>>,
    ) {
        indices.insert(v, *index);
        lowlinks.insert(v, *index);
        *index += 1;
        stack.push(v);
        on_stack.insert(v);

        if let Some(neighbors) = downstream.get(&v) {
            for &w in neighbors {
                if !indices.contains_key(&w) {
                    strongconnect(w, downstream, index, stack, indices, lowlinks, on_stack, sccs);
                    let w_lowlink = *lowlinks.get(&w).unwrap();
                    let v_lowlink = *lowlinks.get(&v).unwrap();
                    lowlinks.insert(v, v_lowlink.min(w_lowlink));
                } else if on_stack.contains(&w) {
                    let w_index = *indices.get(&w).unwrap();
                    let v_lowlink = *lowlinks.get(&v).unwrap();
                    lowlinks.insert(v, v_lowlink.min(w_index));
                }
            }
        }

        if lowlinks.get(&v) == indices.get(&v) {
            let mut scc = HashSet::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack.remove(&w);
                scc.insert(w);
                if w == v {
                    break;
                }
            }
            // Only include SCCs that are actual cycles (more than 1 node or self-loop)
            if scc.len() > 1 || (scc.len() == 1 && downstream.get(&v).map(|s| s.contains(&v)).unwrap_or(false)) {
                sccs.push(scc);
            }
        }
    }

    for &v in stages.keys() {
        if !indices.contains_key(&v) {
            strongconnect(v, downstream, &mut index, &mut stack, &mut indices, &mut lowlinks, &mut on_stack, &mut sccs);
        }
    }

    sccs
}

/// Validate that the topology is acyclic using Kahn's algorithm
pub fn validate_acyclic<T>(
    stages: &HashMap<StageId, T>,
    downstream: &HashMap<StageId, HashSet<StageId>>,
) -> Result<(), TopologyError> {
    // Calculate in-degrees
    let mut in_degree: HashMap<StageId, usize> = HashMap::new();
    for &stage_id in stages.keys() {
        in_degree.entry(stage_id).or_insert(0);
    }
    
    for edges in downstream.values() {
        for &target in edges {
            *in_degree.entry(target).or_default() += 1;
        }
    }
    
    // Find all nodes with no incoming edges
    let mut queue: VecDeque<StageId> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();
    
    let mut visited = 0;
    let mut topo_order = Vec::new();
    
    while let Some(stage) = queue.pop_front() {
        visited += 1;
        topo_order.push(stage);
        
        // For each neighbor of the current stage
        if let Some(neighbors) = downstream.get(&stage) {
            for &neighbor in neighbors {
                let degree = in_degree.get_mut(&neighbor).unwrap();
                *degree -= 1;
                
                if *degree == 0 {
                    queue.push_back(neighbor);
                }
            }
        }
    }
    
    if visited != stages.len() {
        // Find a cycle for better error reporting
        let remaining: HashSet<StageId> = stages.keys()
            .filter(|id| !topo_order.contains(id))
            .copied()
            .collect();
        
        // Try to find a specific cycle
        if let Some(cycle) = find_cycle(&remaining, downstream) {
            return Err(TopologyError::CycleDetected { stages: cycle });
        }
        
        // Shouldn't happen, but provide fallback error
        return Err(TopologyError::CycleDetected { 
            stages: remaining.into_iter().collect() 
        });
    }
    
    Ok(())
}

/// Find a cycle in the graph starting from the given nodes
fn find_cycle(
    nodes: &HashSet<StageId>,
    downstream: &HashMap<StageId, HashSet<StageId>>,
) -> Option<Vec<StageId>> {
    let mut visited = HashSet::new();
    let mut rec_stack = HashSet::new();
    let mut path = Vec::new();
    
    for &start in nodes {
        if visited.contains(&start) {
            continue;
        }
        
        if let Some(cycle) = dfs_find_cycle(start, downstream, &mut visited, &mut rec_stack, &mut path) {
            return Some(cycle);
        }
    }
    
    None
}

fn dfs_find_cycle(
    node: StageId,
    downstream: &HashMap<StageId, HashSet<StageId>>,
    visited: &mut HashSet<StageId>,
    rec_stack: &mut HashSet<StageId>,
    path: &mut Vec<StageId>,
) -> Option<Vec<StageId>> {
    visited.insert(node);
    rec_stack.insert(node);
    path.push(node);
    
    if let Some(neighbors) = downstream.get(&node) {
        for &neighbor in neighbors {
            if !visited.contains(&neighbor) {
                if let Some(cycle) = dfs_find_cycle(neighbor, downstream, visited, rec_stack, path) {
                    return Some(cycle);
                }
            } else if rec_stack.contains(&neighbor) {
                // Found a cycle! Extract it from the path
                let cycle_start = path.iter().position(|&n| n == neighbor).unwrap();
                let mut cycle = path[cycle_start..].to_vec();
                cycle.push(neighbor); // Close the cycle
                return Some(cycle);
            }
        }
    }
    
    rec_stack.remove(&node);
    path.pop();
    None
}

/// Find disconnected stages in the topology
pub fn find_disconnected_stages<T>(
    stages: &HashMap<StageId, T>,
    downstream: &HashMap<StageId, HashSet<StageId>>,
    upstream: &HashMap<StageId, HashSet<StageId>>,
) -> Option<Vec<StageId>> {
    // A stage is disconnected if:
    // 1. It has no connections (isolated), OR
    // 2. It's not reachable from any source that has outgoing edges
    
    let mut disconnected = Vec::new();
    
    // First, find isolated stages (no incoming or outgoing edges)
    for &stage_id in stages.keys() {
        let has_incoming = upstream.get(&stage_id).map(|s| !s.is_empty()).unwrap_or(false);
        let has_outgoing = downstream.get(&stage_id).map(|s| !s.is_empty()).unwrap_or(false);
        
        if !has_incoming && !has_outgoing {
            disconnected.push(stage_id);
        }
    }
    
    // For non-isolated stages, check reachability from sources with outputs
    let mut reachable = HashSet::new();
    
    // Find sources that actually lead somewhere (not isolated)
    let productive_sources: Vec<StageId> = stages.keys()
        .filter(|&id| {
            let is_source = upstream.get(id).map(|s| s.is_empty()).unwrap_or(true);
            let has_outputs = downstream.get(id).map(|s| !s.is_empty()).unwrap_or(false);
            is_source && has_outputs
        })
        .copied()
        .collect();
    
    // DFS from each productive source
    for source in productive_sources {
        dfs_mark_reachable(source, downstream, &mut reachable);
    }
    
    // Find non-isolated stages that aren't reachable
    for &stage_id in stages.keys() {
        if !disconnected.contains(&stage_id) && !reachable.contains(&stage_id) {
            // Check if it's part of a cycle (will be caught by cycle detection)
            let has_connections = upstream.get(&stage_id).map(|s| !s.is_empty()).unwrap_or(false)
                || downstream.get(&stage_id).map(|s| !s.is_empty()).unwrap_or(false);
            if has_connections {
                continue; // Part of a cycle, not truly disconnected
            }
            disconnected.push(stage_id);
        }
    }
    
    if disconnected.is_empty() {
        None
    } else {
        Some(disconnected)
    }
}

fn dfs_mark_reachable(
    node: StageId,
    downstream: &HashMap<StageId, HashSet<StageId>>,
    reachable: &mut HashSet<StageId>,
) {
    if !reachable.insert(node) {
        return; // Already visited
    }
    
    if let Some(neighbors) = downstream.get(&node) {
        for &neighbor in neighbors {
            dfs_mark_reachable(neighbor, downstream, reachable);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    #[test]
    fn test_validate_acyclic_simple_dag() {
        let mut stages = HashMap::new();
        let s1 = StageId::new();
        let s2 = StageId::new();
        let s3 = StageId::new();
        
        stages.insert(s1, ());
        stages.insert(s2, ());
        stages.insert(s3, ());
        
        let mut downstream = HashMap::new();
        downstream.insert(s1, [s2].into_iter().collect());
        downstream.insert(s2, [s3].into_iter().collect());
        
        assert!(validate_acyclic(&stages, &downstream).is_ok());
    }
    
    #[test]
    fn test_validate_acyclic_with_cycle() {
        let mut stages = HashMap::new();
        let s1 = StageId::new();
        let s2 = StageId::new();
        let s3 = StageId::new();
        
        stages.insert(s1, ());
        stages.insert(s2, ());
        stages.insert(s3, ());
        
        let mut downstream = HashMap::new();
        downstream.insert(s1, [s2].into_iter().collect());
        downstream.insert(s2, [s3].into_iter().collect());
        downstream.insert(s3, [s1].into_iter().collect()); // Creates cycle: 1 -> 2 -> 3 -> 1
        
        let result = validate_acyclic(&stages, &downstream);
        assert!(result.is_err());
        
        if let Err(TopologyError::CycleDetected { stages }) = result {
            // Should contain all three stages in the cycle
            assert_eq!(stages.len(), 4); // Includes closing stage
            assert!(stages.contains(&s1));
            assert!(stages.contains(&s2));
            assert!(stages.contains(&s3));
        } else {
            panic!("Expected CycleDetected error");
        }
    }
    
    #[test]
    fn test_disconnected_stages() {
        let mut stages = HashMap::new();
        let s1 = StageId::new();
        let s2 = StageId::new();
        let s3 = StageId::new();
        let s4 = StageId::new(); // Disconnected
        
        stages.insert(s1, ());
        stages.insert(s2, ());
        stages.insert(s3, ());
        stages.insert(s4, ());
        
        let mut downstream = HashMap::new();
        let mut upstream = HashMap::new();
        
        // s1 -> s2 -> s3, but s4 is disconnected
        downstream.insert(s1, [s2].into_iter().collect());
        downstream.insert(s2, [s3].into_iter().collect());
        
        upstream.insert(s2, [s1].into_iter().collect());
        upstream.insert(s3, [s2].into_iter().collect());
        
        let disconnected = find_disconnected_stages(&stages, &downstream, &upstream);
        assert!(disconnected.is_some());
        assert_eq!(disconnected.unwrap(), vec![s4]);
    }
    
    #[test]
    fn test_no_disconnected_stages() {
        let mut stages = HashMap::new();
        let s1 = StageId::new();
        let s2 = StageId::new();
        let s3 = StageId::new();
        
        stages.insert(s1, ());
        stages.insert(s2, ());
        stages.insert(s3, ());
        
        let mut downstream = HashMap::new();
        let mut upstream = HashMap::new();
        
        // Fully connected: s1 -> s2 -> s3
        downstream.insert(s1, [s2].into_iter().collect());
        downstream.insert(s2, [s3].into_iter().collect());
        
        upstream.insert(s2, [s1].into_iter().collect());
        upstream.insert(s3, [s2].into_iter().collect());
        
        let disconnected = find_disconnected_stages(&stages, &downstream, &upstream);
        assert!(disconnected.is_none());
    }
    
    #[test]
    fn test_compute_sccs_simple_cycle() {
        let mut stages = HashMap::new();
        let s1 = StageId::new();
        let s2 = StageId::new();
        let s3 = StageId::new();
        
        stages.insert(s1, ());
        stages.insert(s2, ());
        stages.insert(s3, ());
        
        let mut downstream = HashMap::new();
        
        // Create a cycle: s1 -> s2 -> s3 -> s1
        downstream.insert(s1, [s2].into_iter().collect());
        downstream.insert(s2, [s3].into_iter().collect());
        downstream.insert(s3, [s1].into_iter().collect());
        
        let sccs = compute_sccs(&stages, &downstream);
        
        // Should have one SCC containing all three stages
        assert_eq!(sccs.len(), 1);
        assert_eq!(sccs[0].len(), 3);
        assert!(sccs[0].contains(&s1));
        assert!(sccs[0].contains(&s2));
        assert!(sccs[0].contains(&s3));
    }
    
    #[test]
    fn test_compute_sccs_multiple_components() {
        let mut stages = HashMap::new();
        let s1 = StageId::new();
        let s2 = StageId::new();
        let s3 = StageId::new();
        let s4 = StageId::new();
        let s5 = StageId::new();
        
        stages.insert(s1, ());
        stages.insert(s2, ());
        stages.insert(s3, ());
        stages.insert(s4, ());
        stages.insert(s5, ());
        
        let mut downstream = HashMap::new();
        
        // First cycle: s1 -> s2 -> s1
        downstream.insert(s1, [s2].into_iter().collect());
        downstream.insert(s2, [s1].into_iter().collect());
        
        // Second cycle: s3 -> s4 -> s5 -> s3
        downstream.insert(s3, [s4].into_iter().collect());
        downstream.insert(s4, [s5].into_iter().collect());
        downstream.insert(s5, [s3].into_iter().collect());
        
        let sccs = compute_sccs(&stages, &downstream);
        
        // Should have two SCCs
        assert_eq!(sccs.len(), 2);
        
        // Find the SCC sizes
        let mut scc_sizes: Vec<usize> = sccs.iter().map(|scc| scc.len()).collect();
        scc_sizes.sort();
        
        assert_eq!(scc_sizes, vec![2, 3]);
    }
    
    #[test]
    fn test_compute_sccs_no_cycles() {
        let mut stages = HashMap::new();
        let s1 = StageId::new();
        let s2 = StageId::new();
        let s3 = StageId::new();
        
        stages.insert(s1, ());
        stages.insert(s2, ());
        stages.insert(s3, ());
        
        let mut downstream = HashMap::new();
        
        // Linear DAG: s1 -> s2 -> s3
        downstream.insert(s1, [s2].into_iter().collect());
        downstream.insert(s2, [s3].into_iter().collect());
        
        let sccs = compute_sccs(&stages, &downstream);
        
        // Should have no SCCs (only cycles are returned)
        assert_eq!(sccs.len(), 0);
    }
}