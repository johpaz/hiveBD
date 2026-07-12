import { test, expect } from "bun:test";
import { HiveDB } from "../src";

function tmpDir(): string {
  return `/tmp/hivedb-g9-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function decision(agent: string, streamId: string, description: string, causedBy?: number) {
  return {
    agentId: agent,
    streamId,
    kind: "StateTransition" as const,
    payload: JSON.stringify({ description, ...(causedBy !== undefined ? {} : {}) }),
    ...(causedBy !== undefined ? { causation: causedBy } : {}),
  };
}

function toolCall(agent: string, streamId: string, tool: string, error: string, causedBy: number) {
  return {
    agentId: agent,
    streamId,
    kind: "ToolCall" as const,
    payload: JSON.stringify({ tool, outcome: { Err: error } }),
    causation: causedBy,
  };
}

test("G9 harness > causal thread round-trips through napi", async () => {
  const db = await HiveDB.open(tmpDir());
  const d1 = await db.append(decision("Architect", "task-1", "usar microservicios"));
  const t1 = await db.append(toolCall("Architect", "task-1", "read_file", "ok", d1));
  const d2 = await db.append({
    ...decision("Backend", "task-1", "crear servicio de pagos"),
    causation: t1,
  });

  const thread = (await db.causalThread("task-1")) as any;
  expect(thread.decisions.length).toBe(2);
  expect(thread.toolCalls.length).toBe(1);
  expect(thread.decisions[0].caused[0]).toBe(t1);

  db.close();
});

test("G9 harness > buildAgentContext returns a token-bounded context", async () => {
  const db = await HiveDB.open(tmpDir());
  for (let i = 0; i < 500; i++) {
    await db.append(decision("Backend", "task-1", `step-${i}`));
  }

  const ctx = (await db.buildAgentContext({
    taskId: "task-1",
    currentPhase: "",
    currentObjective: "fix pagos",
    maxTokens: 4096,
    strategy: {},
  })) as any;

  // Estimated tokens are carried by the Rust side; verify the context fits.
  const items: any[] = ctx.items ?? [];
  const estimated = items.reduce((sum: number, it: any) => {
    const text = it.text ?? "";
    return sum + Math.max(1, Math.floor((text.length + 20) / 4));
  }, 0);
  expect(estimated).toBeLessThanOrEqual(4096);

  db.close();
});

test("G9 harness > evaluateHarness detects an error loop", async () => {
  const db = await HiveDB.open(tmpDir());
  for (let i = 0; i < 3; i++) {
    const d = await db.append(decision("Backend", "task-1", "compilar módulo"));
    await db.append(toolCall("Backend", "task-1", "cargo_build", "E0432", d));
  }

  const thread = await db.causalThread("task-1");
  const evalResult = (await db.evaluateHarness({
    causalThread: thread,
    similarEpisodes: [],
    originalIntent: "implementar autenticación",
    currentState: { outcome: "success" },
    minConfidence: 0.5,
  })) as any;

  expect(evalResult.processQuality).toBeLessThan(evalResult.outputQuality);
  expect(evalResult.findings.some((f: any) => f.kind === "inefficientLoop")).toBe(true);

  db.close();
});
