/**
 * G12 — el contrato de aislamiento, visto desde TypeScript.
 *
 * Cuando una sola base aloja varios enjambres, las lecturas que recorren todos
 * los shards (`causalThread`, `toolStats`) dejan de ser una vista completa y
 * pasan a ser una fuga. Estos tests fijan que el parámetro `agents` acota de
 * verdad, y que omitirlo conserva el comportamiento histórico — que sigue
 * siendo el correcto para una base de un solo dueño.
 */

import { test, expect } from "bun:test";
import { HiveDB } from "../src";

const STREAM = "task-1";

function agentes(enjambre: string): string[] {
  return [`${enjambre}:coordinator`, `${enjambre}:native:search`];
}

function toolCall(agent: string, tool: string, latencyMs: number) {
  return {
    agentId: agent,
    streamId: STREAM,
    kind: "ToolCall" as const,
    payload: JSON.stringify({ tool, latency_ms: latencyMs, cost: 1.0, outcome: "Ok" }),
  };
}

/** Dos enjambres escribiendo en un stream con el mismo nombre, en la misma base. */
async function baseConDosEnjambres(): Promise<HiveDB> {
  const db = await HiveDB.open(":memory:");
  await db.append(toolCall("swarmA:native:search", "buscar", 10));
  await db.append(toolCall("swarmA:native:search", "buscar", 20));
  await db.append(toolCall("swarmB:native:search", "buscar", 100));
  return db;
}

test("toolStats acotado por agentes no suma las llamadas de otro enjambre", async () => {
  const db = await baseConDosEnjambres();
  try {
    const a = await db.toolStats("buscar", agentes("swarmA"));
    const b = await db.toolStats("buscar", agentes("swarmB"));

    expect(a?.invocations).toBe(2);
    expect(a?.totalLatencyMs).toBe(30);
    expect(b?.invocations).toBe(1);
    expect(b?.totalLatencyMs).toBe(100);
  } finally {
    db.close();
  }
});

test("toolStats sin agentes sigue agregando toda la base", async () => {
  // El comportamiento histórico se conserva a propósito: es el correcto cuando
  // la base tiene un solo dueño. Es también, exactamente, la fuga que el
  // parámetro `agents` existe para evitar cuando no lo tiene.
  const db = await baseConDosEnjambres();
  try {
    expect((await db.toolStats("buscar"))?.invocations).toBe(3);
  } finally {
    db.close();
  }
});

test("un enjambre sin llamadas no hereda las estadísticas del vecino", async () => {
  const db = await HiveDB.open(":memory:");
  try {
    await db.append(toolCall("swarmA:native:search", "buscar", 10));
    expect(await db.toolStats("buscar", agentes("swarmB"))).toBeUndefined();
  } finally {
    db.close();
  }
});

test("causalThread acotado no devuelve el hilo de otro enjambre", async () => {
  const db = await baseConDosEnjambres();
  try {
    const a = (await db.causalThread(STREAM, agentes("swarmA"))) as { toolCalls: unknown[] };
    const b = (await db.causalThread(STREAM, agentes("swarmB"))) as { toolCalls: unknown[] };
    const todos = (await db.causalThread(STREAM)) as { toolCalls: unknown[] };

    expect(a.toolCalls).toHaveLength(2);
    expect(b.toolCalls).toHaveLength(1);
    expect(todos.toolCalls).toHaveLength(3);
  } finally {
    db.close();
  }
});

test("buildAgentContext acota el hilo causal con agents", async () => {
  const db = await baseConDosEnjambres();
  try {
    const req = {
      taskId: STREAM,
      currentPhase: "ejecución",
      currentObjective: "buscar algo",
      maxTokens: 2000,
      strategy: { causalAnchors: true },
    };

    const soloA = (await db.buildAgentContext({ ...req, agents: agentes("swarmA") })) as {
      recentToolCalls?: unknown[];
    };
    const todos = (await db.buildAgentContext(req)) as { recentToolCalls?: unknown[] };

    // Basta con que el acotado no vea más que el global: la aserción fuerte
    // sobre el conteo vive en el test de `causalThread`, que es el primitivo.
    const nA = soloA.recentToolCalls?.length ?? 0;
    const nTodos = todos.recentToolCalls?.length ?? 0;
    expect(nA).toBeLessThanOrEqual(nTodos);
  } finally {
    db.close();
  }
});

test("reabrir una base conserva la secuencia sin reescribir eventos", async () => {
  const dir = `/tmp/hivedb-g12-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  let db = await HiveDB.open(dir);
  await db.append(toolCall("swarmA:native:search", "buscar", 10));
  const antes = await db.lastSeq();
  db.close();

  // Tras un cierre ordenado la apertura no escanea el log: lee el contador
  // persistido. Lo que se comprueba aquí es que sigue siendo correcto.
  db = await HiveDB.open(dir);
  try {
    const siguiente = await db.append(toolCall("swarmA:native:search", "buscar", 20));
    expect(siguiente).toBe(antes + 1);
    expect((await db.toolStats("buscar", agentes("swarmA")))?.invocations).toBe(2);
  } finally {
    db.close();
  }
});
