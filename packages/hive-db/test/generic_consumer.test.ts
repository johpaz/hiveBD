import { test, expect } from "bun:test";
import { HiveDB } from "../src";

// Regression guard for the genericity of the G9 harness contract
// (docs/AGENT_INTEGRATION.md), through the napi/JSON boundary specifically —
// crates/hivedb-core/tests/contract_generic_consumer.rs guards the same
// contract on the Rust side, but only this file can catch a camelCase/tagging
// regression introduced at the napi wire-shape layer (Phase 1 of the review).
// Deliberately uses a support-ticket-triage vocabulary that has nothing to do
// with hiveCode's "swarm"/"session-N" naming.

function tmpDir(): string {
  return `/tmp/hivedb-generic-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

const TICKET = "ticket-4821";

function intentLogged(agent: string, intent: string) {
  return {
    agentId: agent,
    streamId: TICKET,
    kind: "IntentLogged" as const,
    payload: JSON.stringify({ actor: agent, intent }),
  };
}

function decision(agent: string, description: string, phase?: string, causedBy?: number) {
  return {
    agentId: agent,
    streamId: TICKET,
    kind: "StateTransition" as const,
    payload: JSON.stringify({ description, ...(phase ? { phase } : {}) }),
    ...(causedBy !== undefined ? { causation: causedBy } : {}),
  };
}

function toolCall(
  agent: string,
  tool: string,
  outcome: "Ok" | { Err: string },
  causedBy: number,
) {
  return {
    agentId: agent,
    streamId: TICKET,
    kind: "ToolCall" as const,
    payload: JSON.stringify({ tool, outcome }),
    causation: causedBy,
  };
}

test("generic consumer > causal thread connects across agents with camelCase fields", async () => {
  const db = await HiveDB.open(tmpDir());

  const intent = await db.append(
    intentLogged("Router", "resolver timeout de checkout reportado por el cliente"),
  );
  const d1 = await db.append(decision("Analyst", "clasificar como bug de pagos", "triage", intent));
  const t1 = await db.append(toolCall("Analyst", "classify_ticket", "Ok", d1));
  const d2 = await db.append(
    decision("Auditor", "escalar a soporte de pagos", "resolution", t1),
  );
  await db.append(toolCall("Auditor", "escalate_to_human", "Ok", d2));

  const thread = (await db.causalThread(TICKET)) as any;

  // camelCase, not the old snake_case/"tool_calls" shape.
  expect(thread.toolCalls).toBeDefined();
  expect(thread.tool_calls).toBeUndefined();
  expect(thread.decisions.length).toBe(2);
  expect(thread.toolCalls.length).toBe(2);
  expect(thread.toolCalls[0].causedBy).toBe(d1);
  expect(thread.decisions[1].causedBy).toBe(t1);

  db.close();
});

test("generic consumer > evaluateHarness detects an error loop with tagged findings", async () => {
  const db = await HiveDB.open(tmpDir());

  const rootDecision = await db.append(
    decision("Analyst", "consultar historial del cliente", "triage"),
  );
  for (let i = 0; i < 3; i++) {
    await db.append(
      toolCall("Analyst", "lookup_customer", { Err: "crm_timeout" }, rootDecision),
    );
  }

  const thread = await db.causalThread(TICKET);
  const evalResult = (await db.evaluateHarness({
    causalThread: thread,
    similarEpisodes: [],
    originalIntent: "resolver timeout de checkout",
    currentState: { outcome: "unresolved" },
    minConfidence: 0.3,
  })) as any;

  expect(evalResult.processQuality).toBeLessThan(1.0);
  // camelCase finding kind, not the old capitalized "InefficientLoop".
  expect(evalResult.findings.some((f: any) => f.kind === "inefficientLoop")).toBe(true);
  expect(evalResult.rootCause).toBeDefined();
  expect(evalResult.rootCause.seq).toBe(rootDecision);
  expect(evalResult.proposals.length).toBeGreaterThan(0);

  db.close();
});

test("generic consumer > buildAgentContext tags items by type", async () => {
  const db = await HiveDB.open(tmpDir());

  for (let i = 0; i < 50; i++) {
    await db.append(decision("Analyst", `triage-step-${i}`, "triage"));
  }

  const ctx = (await db.buildAgentContext({
    taskId: TICKET,
    currentPhase: "triage",
    currentObjective: "",
    maxTokens: 4096,
    strategy: {},
  })) as any;

  const items: any[] = ctx.items ?? [];
  expect(items.length).toBeGreaterThan(0);
  // Internally-tagged discriminated union: every item carries a `type`.
  for (const item of items) {
    expect(typeof item.type).toBe("string");
    expect(item.Decision).toBeUndefined();
  }
  expect(items.some((it) => it.type === "decision")).toBe(true);

  db.close();
});

test("generic consumer > toolStats agrees with causalThread on failures", async () => {
  const db = await HiveDB.open(tmpDir());

  const d = await db.append(decision("Analyst", "reintentar lookup", "triage"));
  await db.append(toolCall("Analyst", "lookup_customer", { Err: "timeout" }, d));

  const stats = await db.toolStats("lookup_customer");
  expect(stats?.errors).toBe(1);
  expect(stats?.lastOutcome).toBe("Err: timeout");

  const thread = (await db.causalThread(TICKET)) as any;
  expect(thread.toolCalls.every((t: any) => t.outcome !== "Ok")).toBe(true);

  db.close();
});
