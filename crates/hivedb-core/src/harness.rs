//! Harness loop: pure evaluator of long-running agent task processes.

use crate::causal::{AnomalyKind, CausalThread, DecisionNode};
use crate::event::AgentId;
use crate::state::causal_thread::ToolOutcome;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Input to the harness evaluator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessInput {
    pub causal_thread: CausalThread,
    pub similar_episodes: Vec<CausalThread>,
    pub original_intent: String,
    pub current_state: Option<Value>,
    pub min_confidence: f64,
}

impl Default for HarnessInput {
    fn default() -> Self {
        Self {
            causal_thread: CausalThread::default(),
            similar_episodes: Vec::new(),
            original_intent: String::new(),
            current_state: None,
            min_confidence: 0.5,
        }
    }
}

/// A single finding produced by the harness.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    pub kind: FindingKind,
    pub seq: Option<u64>,
    pub description: String,
}

/// Classification of a finding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FindingKind {
    InefficientLoop,
    ObjectiveDrift,
    RootCause,
    InsufficientEvidence,
}

/// A learning proposal with causal evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LearningProposal {
    pub description: String,
    pub evidence_seqs: Vec<u64>,
    pub trigger_seq: Option<u64>,
    pub confidence: f64,
    pub specificity: f64,
}

/// Identifies the earliest decision that originated a failure chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootCause {
    pub seq: u64,
    pub agent: AgentId,
}

/// Result of evaluating a task with the harness loop.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessEvaluation {
    pub process_quality: f64,
    pub output_quality: f64,
    pub root_cause: Option<RootCause>,
    pub findings: Vec<Finding>,
    pub proposals: Vec<LearningProposal>,
}

impl HarnessEvaluation {
    /// Number of anomalies detected in the evaluation.
    pub fn anomaly_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| {
                matches!(
                    f.kind,
                    FindingKind::InefficientLoop | FindingKind::ObjectiveDrift
                )
            })
            .count()
    }
}

/// Pure evaluator: receives a causal thread and returns findings + proposals.
pub struct HarnessLoop;

impl HarnessLoop {
    pub fn evaluate(input: HarnessInput) -> HarnessEvaluation {
        let mut findings = Vec::new();
        let mut proposals = Vec::new();

        let process_quality = Self::compute_process_quality(&input, &mut findings);
        let output_quality = Self::compute_output_quality(&input.current_state);

        let root_cause = Self::find_root_cause(&input.causal_thread, &mut findings);

        Self::generate_proposals(&input, &mut proposals, &mut findings);

        // Apply confidence threshold.
        let (kept, dropped): (Vec<_>, Vec<_>) = proposals
            .into_iter()
            .partition(|p| p.confidence >= input.min_confidence);
        proposals = kept;

        // If no proposal survived despite having some signal, document the lack
        // of sufficient evidence.
        if proposals.is_empty() && !dropped.is_empty() {
            findings.push(Finding {
                kind: FindingKind::InsufficientEvidence,
                seq: None,
                description: "Insufficient evidence to generate a learning proposal".into(),
            });
        }

        HarnessEvaluation {
            process_quality,
            output_quality,
            root_cause,
            findings,
            proposals,
        }
    }

    fn compute_process_quality(input: &HarnessInput, findings: &mut Vec<Finding>) -> f64 {
        let mut quality = 1.0f64;
        for anomaly in &input.causal_thread.anomalies {
            match anomaly.kind {
                AnomalyKind::ErrorLoop => {
                    let penalty = 0.2f64 * (anomaly.repetitions as f64).min(5.0);
                    quality -= penalty;
                    findings.push(Finding {
                        kind: FindingKind::InefficientLoop,
                        seq: None,
                        description: format!(
                            "Tool {:?} failed {} times in a loop",
                            anomaly.tool, anomaly.repetitions
                        ),
                    });
                }
                AnomalyKind::ObjectiveDrift => {
                    let penalty = 0.05f64 * (anomaly.repetitions as f64).min(10.0);
                    quality -= penalty;
                    findings.push(Finding {
                        kind: FindingKind::ObjectiveDrift,
                        seq: anomaly.original_intent_seq,
                        description: format!(
                            "Objective drift: {} decisions diverged from intent",
                            anomaly.repetitions
                        ),
                    });
                }
            }
        }
        quality.max(0.0)
    }

    fn compute_output_quality(state: &Option<Value>) -> f64 {
        if let Some(payload) = state {
            let json = payload.to_string().to_lowercase();
            if json.contains("success") || json.contains("done") || json.contains("ok") {
                return 1.0;
            }
            if json.contains("error") || json.contains("fail") {
                return 0.0;
            }
        }
        0.5
    }

    fn find_root_cause(thread: &CausalThread, findings: &mut Vec<Finding>) -> Option<RootCause> {
        let failure_seqs: Vec<u64> = thread
            .tool_calls
            .iter()
            .filter(|t| matches!(t.outcome, ToolOutcome::Err(_)))
            .map(|t| t.seq)
            .collect();

        if failure_seqs.is_empty() {
            return None;
        }

        let decision_by_seq: HashMap<u64, &DecisionNode> =
            thread.decisions.iter().map(|d| (d.seq, d)).collect();

        // For each failure, walk backwards through caused_by until we hit a decision.
        let mut counts: HashMap<u64, usize> = HashMap::new();
        for seq in &failure_seqs {
            let mut current = Some(*seq);
            while let Some(s) = current {
                if let Some(decision) = decision_by_seq.get(&s) {
                    *counts.entry(decision.seq).or_insert(0) += 1;
                    break;
                }
                current = caused_by_of(thread, s);
            }
        }

        let best = counts.iter().max_by(|a, b| {
            a.1.cmp(b.1).then_with(|| a.0.cmp(b.0).reverse()) // earlier seq wins on ties
        })?;

        let decision = decision_by_seq.get(best.0)?;
        findings.push(Finding {
            kind: FindingKind::RootCause,
            seq: Some(decision.seq),
            description: format!(
                "Root cause: {} decision at seq {}",
                decision.agent.0, decision.seq
            ),
        });

        Some(RootCause {
            seq: decision.seq,
            agent: decision.agent.clone(),
        })
    }

    fn generate_proposals(
        input: &HarnessInput,
        proposals: &mut Vec<LearningProposal>,
        findings: &mut Vec<Finding>,
    ) {
        let similar_text = input
            .similar_episodes
            .iter()
            .map(|t| {
                t.decisions
                    .iter()
                    .map(|d| d.description.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
            .join(" ");

        for anomaly in &input.causal_thread.anomalies {
            match anomaly.kind {
                AnomalyKind::ErrorLoop => {
                    let tool = anomaly.tool.clone().unwrap_or_default();
                    let evidence: Vec<u64> = input
                        .causal_thread
                        .tool_calls
                        .iter()
                        .filter(|t| t.tool == tool && matches!(t.outcome, ToolOutcome::Err(_)))
                        .map(|t| t.seq)
                        .collect();

                    let trigger = earliest_decision_causing(&input.causal_thread, &evidence);
                    let (confidence, specificity) =
                        Self::score_proposal(input, &evidence, &similar_text, 0.4, 0.5);

                    proposals.push(LearningProposal {
                        description: format!(
                            "Add pre-checks before calling tool '{}' to avoid repeated errors",
                            tool
                        ),
                        evidence_seqs: evidence,
                        trigger_seq: trigger,
                        confidence,
                        specificity,
                    });
                }
                AnomalyKind::ObjectiveDrift => {
                    let evidence: Vec<u64> = input
                        .causal_thread
                        .decisions
                        .iter()
                        .filter(|d| {
                            !input.original_intent.is_empty()
                                && !d
                                    .description
                                    .to_lowercase()
                                    .contains(&input.original_intent.to_lowercase())
                        })
                        .map(|d| d.seq)
                        .collect();

                    let trigger = evidence.iter().copied().min();
                    let (confidence, specificity) =
                        Self::score_proposal(input, &evidence, &similar_text, 0.3, 0.4);

                    proposals.push(LearningProposal {
                        description: "Realign decisions with the original intent".into(),
                        evidence_seqs: evidence,
                        trigger_seq: trigger,
                        confidence,
                        specificity,
                    });
                }
            }
        }

        // If there were failures but no anomaly was strong enough, still document
        // the lack of evidence as a finding.
        if proposals.is_empty()
            && input
                .causal_thread
                .tool_calls
                .iter()
                .any(|t| matches!(t.outcome, ToolOutcome::Err(_)))
        {
            findings.push(Finding {
                kind: FindingKind::InsufficientEvidence,
                seq: None,
                description: "Failures detected, but not enough evidence for a proposal".into(),
            });
        }
    }

    fn score_proposal(
        input: &HarnessInput,
        evidence: &[u64],
        similar_text: &str,
        base_confidence: f64,
        base_specificity: f64,
    ) -> (f64, f64) {
        let evidence_boost = (evidence.len() as f64 * 0.1).min(0.4);
        let mut confidence = (base_confidence + evidence_boost).min(1.0);
        let mut specificity = base_specificity;

        if !input.similar_episodes.is_empty() {
            // Boost if the past episodes mention a concrete resolution.
            if similar_text.to_lowercase().contains("resuelto")
                || similar_text.to_lowercase().contains("addnullcheck")
                || similar_text.to_lowercase().contains("check")
            {
                confidence = (confidence + 0.2).min(1.0);
                specificity = (specificity + 0.3).min(1.0);
            } else {
                confidence = (confidence + 0.05).min(1.0);
                specificity = (specificity + 0.05).min(1.0);
            }
        }

        (confidence, specificity)
    }
}

fn caused_by_of(thread: &CausalThread, seq: u64) -> Option<u64> {
    thread
        .tool_calls
        .iter()
        .find(|t| t.seq == seq)
        .and_then(|t| t.caused_by)
        .or_else(|| {
            thread
                .decisions
                .iter()
                .find(|d| d.seq == seq)
                .and_then(|d| d.caused_by)
        })
}

fn earliest_decision_causing(thread: &CausalThread, seqs: &[u64]) -> Option<u64> {
    let decision_seqs: HashSet<u64> = thread.decisions.iter().map(|d| d.seq).collect();
    let mut earliest: Option<u64> = None;
    for start in seqs {
        let mut current = Some(*start);
        while let Some(s) = current {
            if decision_seqs.contains(&s) {
                earliest = Some(match earliest {
                    Some(e) => e.min(s),
                    None => s,
                });
                break;
            }
            current = caused_by_of(thread, s);
        }
    }
    earliest
}
