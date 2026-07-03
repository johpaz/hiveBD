use std::collections::HashMap;

/// Reciprocal Rank Fusion.
///
/// For every document present in one or more rankings, computes
/// `score(d) = sum_i 1 / (k + rank_i(d))`, where `rank_i(d)` is the 1-based
/// position of `d` in ranking `i`. Results are returned sorted by score
/// descending.
pub fn rrf(rankings: &[Vec<(String, usize)>], k: usize) -> Vec<(String, f32)> {
    let mut scores: HashMap<String, f32> = HashMap::new();

    for ranking in rankings {
        for (id, rank) in ranking {
            let contribution = 1.0 / (k as f32 + *rank as f32);
            *scores.entry(id.clone()).or_insert(0.0) += contribution;
        }
    }

    let mut results: Vec<(String, f32)> = scores.into_iter().collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rrf_fusion_matches_reference() {
        let bm25 = vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ];
        let ann = vec![
            ("b".to_string(), 1),
            ("c".to_string(), 2),
            ("a".to_string(), 3),
        ];
        let fused = rrf(&[bm25, ann], 60);
        assert_eq!(fused[0].0, "b");
    }
}
