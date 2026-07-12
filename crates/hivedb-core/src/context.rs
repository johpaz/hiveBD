//! Adaptive causal context window for agent LLM prompts.

use crate::causal::{CausalThread, DecisionNode};
use crate::event::StreamId;
use crate::{HiveDB, HiveResult, HybridQuery, ScalarFilter};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};

/// Request to build a context window for an agent.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentContextRequest {
    pub task_id: String,
    pub current_phase: String,
    pub current_objective: String,
    pub max_tokens: usize,
    pub strategy: ContextStrategy,
}

/// Strategy knobs for context construction.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContextStrategy {
    pub causal_anchors: bool,
    pub compress_completed_phases: bool,
    pub episodic_similarity: Option<EpisodicConfig>,
    pub recent_anomalies: Option<AnomalyConfig>,
}

/// Configuration for retrieving similar past episodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodicConfig {
    pub vector: Vec<f32>,
    pub k: usize,
}

/// Configuration for including recent anomalies.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnomalyConfig {
    pub window_ms: u64,
}

/// One item inside the assembled context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ContextItem {
    Decision {
        seq: u64,
        phase: Option<String>,
        text: String,
    },
    ToolCall {
        seq: u64,
        phase: Option<String>,
        text: String,
    },
    Anomaly {
        text: String,
    },
    Episode {
        task_id: String,
        summary: String,
    },
    PhaseSummary {
        phase: String,
        text: String,
        key_decisions: Vec<u64>,
    },
}

impl ContextItem {
    /// Rough token estimate: characters / 4.
    pub fn estimated_tokens(&self) -> usize {
        let chars = match self {
            ContextItem::Decision { text, .. } => text.len() + 20,
            ContextItem::ToolCall { text, .. } => text.len() + 20,
            ContextItem::Anomaly { text } => text.len() + 10,
            ContextItem::Episode { summary, .. } => summary.len() + 20,
            ContextItem::PhaseSummary { text, .. } => text.len() + 10,
        };
        (chars / 4).max(1)
    }

    pub fn seq(&self) -> Option<u64> {
        match self {
            ContextItem::Decision { seq, .. } | ContextItem::ToolCall { seq, .. } => Some(*seq),
            _ => None,
        }
    }
}

/// Assembled context window for an LLM call.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentContext {
    pub items: Vec<ContextItem>,
    pub similar_episodes: Vec<Episode>,
    pub anomalies: Vec<ContextItem>,
}

/// A retrieved past episode.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Episode {
    pub task_id: String,
    pub summary: String,
}

/// Summary of a completed phase.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhaseSummary {
    pub is_compressed: bool,
    pub key_decisions: Vec<u64>,
}

impl AgentContext {
    /// Sum of estimated tokens of all included items.
    pub fn estimated_tokens(&self) -> usize {
        self.items.iter().map(|i| i.estimated_tokens()).sum()
    }

    /// Stable hash of the context content (for idempotency checks).
    pub fn content_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        let json = serde_json::to_string(self).unwrap_or_default();
        json.hash(&mut hasher);
        hasher.finish()
    }

    /// True if the context includes the given seq.
    pub fn contains_seq(&self, seq: u64) -> bool {
        self.items.iter().any(|i| i.seq() == Some(seq))
    }

    /// Returns a summary for a phase if present.
    pub fn phase_summary(&self, phase: &str) -> Option<PhaseSummary> {
        // Completed phases are represented by an explicit PhaseSummary marker.
        for item in &self.items {
            match item {
                ContextItem::PhaseSummary {
                    phase: p,
                    key_decisions,
                    ..
                } if p == phase => {
                    return Some(PhaseSummary {
                        is_compressed: true,
                        key_decisions: key_decisions.clone(),
                    });
                }
                _ => {}
            }
        }

        // The current phase appears as uncompressed decisions/tool calls.
        let key_decisions: Vec<u64> = self
            .items
            .iter()
            .filter_map(|i| match i {
                ContextItem::Decision {
                    seq,
                    phase: Some(p),
                    ..
                }
                | ContextItem::ToolCall {
                    seq,
                    phase: Some(p),
                    ..
                } if p == phase => Some(*seq),
                _ => None,
            })
            .collect();

        if !key_decisions.is_empty() {
            Some(PhaseSummary {
                is_compressed: false,
                key_decisions,
            })
        } else {
            None
        }
    }

    /// True if any content item belongs to the given phase.
    pub fn has_content_from_phase(&self, phase: &str) -> bool {
        self.phase_summary(phase).is_some()
            || self.items.iter().any(|i| match i {
                ContextItem::Decision { phase: p, .. } | ContextItem::ToolCall { phase: p, .. } => {
                    p.as_deref() == Some(phase)
                }
                ContextItem::PhaseSummary { phase: p, .. } => p == phase,
                _ => false,
            })
    }

    /// True if the context spans all requested session/phase names.
    pub fn spans_sessions(&self, sessions: &[&str]) -> bool {
        sessions
            .iter()
            .all(|s| self.has_content_from_phase(s) || self.contains_seq_label(s))
    }

    fn contains_seq_label(&self, label: &str) -> bool {
        self.items.iter().any(|i| match i {
            ContextItem::Decision { text, .. } | ContextItem::ToolCall { text, .. } => {
                text.contains(label)
            }
            _ => false,
        })
    }
}

impl HiveDB {
    /// Build an adaptive, token-bounded context window for the current task.
    pub fn build_agent_context(&self, req: AgentContextRequest) -> HiveResult<AgentContext> {
        let stream_id = StreamId::from(req.task_id.clone());
        let thread = self.causal_thread(stream_id.clone())?;

        let mut items: Vec<ContextItem> = Vec::new();
        let mut included_seqs: HashSet<u64> = HashSet::new();

        // 1. Always include recent anomalies.
        let anomaly_items = collect_anomalies(self, &thread, &req);
        for item in &anomaly_items {
            if let Some(seq) = item.seq() {
                included_seqs.insert(seq);
            }
        }
        items.extend(anomaly_items.clone());

        // 2. Find current objective decision.
        let current_seq = find_current_objective(&thread, &req.current_objective);

        // 3. Causal anchors (events connected to the current objective).
        match current_seq {
            Some(seq) if req.strategy.causal_anchors => {
                let anchors = collect_causal_anchors(&thread, seq, &mut included_seqs);
                items.extend(anchors);
            }
            _ => {}
        }

        // 4. Phase-based content.
        let phase_items = collect_phase_content(
            &thread,
            &req.current_phase,
            req.strategy.compress_completed_phases,
            &mut included_seqs,
        );
        items.extend(phase_items);

        // 5. Episodic similarity.
        let mut similar_episodes = Vec::new();
        if let Some(config) = &req.strategy.episodic_similarity {
            similar_episodes = collect_similar_episodes(self, config, &req.current_objective)?;
            for ep in &similar_episodes {
                items.push(ContextItem::Episode {
                    task_id: ep.task_id.clone(),
                    summary: ep.summary.clone(),
                });
            }
        }

        // 6. Apply token budget.
        let budget = req.max_tokens;
        let mut selected: Vec<ContextItem> = Vec::new();
        let mut tokens = 0usize;

        for item in items {
            let cost = item.estimated_tokens();
            if tokens + cost > budget {
                break;
            }
            tokens += cost;
            selected.push(item);
        }

        let anomalies = selected
            .iter()
            .filter(|i| matches!(i, ContextItem::Anomaly { .. }))
            .cloned()
            .collect();

        Ok(AgentContext {
            items: selected,
            similar_episodes,
            anomalies,
        })
    }
}

fn collect_anomalies(
    db: &HiveDB,
    thread: &CausalThread,
    req: &AgentContextRequest,
) -> Vec<ContextItem> {
    let mut items = Vec::new();
    let window = req.strategy.recent_anomalies.as_ref().map(|c| c.window_ms);

    for anomaly in &thread.anomalies {
        let text = match anomaly.kind {
            crate::causal::AnomalyKind::ErrorLoop => format!(
                "Error loop: tool {:?} failed {} times",
                anomaly.tool, anomaly.repetitions
            ),
            crate::causal::AnomalyKind::ObjectiveDrift => {
                let intent = anomaly
                    .original_intent_seq
                    .and_then(|seq| find_intent_description(db, thread, seq))
                    .unwrap_or_else(|| "unknown intent".into());
                format!(
                    "Objective drift: {} decisions diverged from intent '{}'",
                    anomaly.repetitions, intent
                )
            }
        };

        if let Some(window_ms) = window {
            // We don't store timestamps in CausalThread yet; include all for now.
            let _ = window_ms;
        }

        items.push(ContextItem::Anomaly { text });
    }

    items
}

fn find_intent_description(_db: &HiveDB, _thread: &CausalThread, _seq: u64) -> Option<String> {
    // The projection already carries the intent text; log lookup can be added
    // later if cross-shard intent events are not merged into the thread state.
    None
}

fn find_current_objective(thread: &CausalThread, objective: &str) -> Option<u64> {
    thread
        .decisions
        .iter()
        .rev()
        .find(|d| {
            d.description
                .to_lowercase()
                .contains(&objective.to_lowercase())
        })
        .map(|d| d.seq)
}

fn collect_causal_anchors(
    thread: &CausalThread,
    start_seq: u64,
    included: &mut HashSet<u64>,
) -> Vec<ContextItem> {
    let mut items = Vec::new();
    let mut visited: HashSet<u64> = HashSet::new();
    let mut stack = vec![start_seq];

    // Walk backwards via caused_by links.
    while let Some(seq) = stack.pop() {
        if !visited.insert(seq) {
            continue;
        }

        if let Some(decision) = thread.decisions.iter().find(|d| d.seq == seq) {
            if included.insert(seq) {
                items.push(ContextItem::Decision {
                    seq,
                    phase: decision.phase.clone(),
                    text: decision.description.clone(),
                });
            }
            if let Some(parent) = decision.caused_by {
                stack.push(parent);
            }
        }

        if let Some(tool) = thread.tool_calls.iter().find(|t| t.seq == seq) {
            if included.insert(seq) {
                items.push(ContextItem::ToolCall {
                    seq,
                    phase: None,
                    text: format!("{} -> {:?}", tool.tool, tool.outcome),
                });
            }
            if let Some(parent) = tool.caused_by {
                stack.push(parent);
            }
        }
    }

    items
}

fn collect_phase_content(
    thread: &CausalThread,
    current_phase: &str,
    compress_completed: bool,
    included: &mut HashSet<u64>,
) -> Vec<ContextItem> {
    let mut items = Vec::new();
    let mut phases: BTreeMap<String, Vec<&DecisionNode>> = BTreeMap::new();

    for decision in &thread.decisions {
        let phase = decision
            .phase
            .clone()
            .unwrap_or_else(|| current_phase.to_string());
        phases.entry(phase).or_default().push(decision);
    }

    for (phase, decisions) in phases {
        let is_current = phase == current_phase;
        if is_current {
            let current_phase_opt = if current_phase.is_empty() {
                None
            } else {
                Some(current_phase.to_string())
            };
            for d in decisions {
                if included.insert(d.seq) {
                    items.push(ContextItem::Decision {
                        seq: d.seq,
                        phase: current_phase_opt.clone(),
                        text: d.description.clone(),
                    });
                }
            }
        } else if compress_completed {
            let mut key_decisions = Vec::new();
            if let Some(d) = decisions.first() {
                key_decisions.push(d.seq);
                if included.insert(d.seq) {
                    items.push(ContextItem::Decision {
                        seq: d.seq,
                        phase: Some(phase.clone()),
                        text: d.description.clone(),
                    });
                }
            }
            items.push(ContextItem::PhaseSummary {
                phase: phase.clone(),
                text: format!(
                    "Phase {} completed with {} decisions",
                    phase,
                    decisions.len()
                ),
                key_decisions,
            });
        }
    }

    items
}

fn collect_similar_episodes(
    db: &HiveDB,
    config: &EpisodicConfig,
    current_objective: &str,
) -> HiveResult<Vec<Episode>> {
    let use_text = !current_objective.is_empty();
    let mut query = HybridQuery::default()
        .with_vector(config.vector.clone())
        .with_filters(vec![ScalarFilter::eq("kind", "episode")])
        .with_k(config.k);
    if use_text {
        query = query.with_text(current_objective.to_string());
    }

    let hits = db.query_hybrid(query)?;
    let max_text_score = hits
        .iter()
        .filter_map(|h| h.text_score)
        .fold(0.0f32, f32::max);
    let text_threshold = max_text_score * 0.4;

    let mut episodes = Vec::new();
    for hit in hits {
        // Require at least one source to show meaningful relevance. This keeps
        // unrelated episodes from leaking in when the corpus is small and RRF
        // would otherwise return every candidate.
        let vector_relevant = hit.vector_score.map(|s| s > 0.3).unwrap_or(false);
        let text_relevant = hit.text_score.map(|s| s >= text_threshold).unwrap_or(false);
        if !vector_relevant && !text_relevant {
            continue;
        }
        episodes.push(Episode {
            task_id: hit.id.clone(),
            summary: format!("Episode {}", hit.id),
        });
    }
    Ok(episodes)
}
