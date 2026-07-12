import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HiveDB } from "../src/index.ts";

const VECTOR_DIMENSION = 384;

function vecWithOne(position: number): Float32Array {
  const v = new Float32Array(VECTOR_DIMENSION);
  v[position % VECTOR_DIMENSION] = 1;
  return v;
}

function tempDir(prefix: string): string {
  return mkdtempSync(join(tmpdir(), prefix));
}

describe("G8 napi-rs binding", () => {
  let base: string;
  let db: HiveDB;

  beforeEach(async () => {
    base = tempDir("hive-g8-");
    db = await HiveDB.open(base);
  });

  afterEach(() => {
    try {
      db.close();
    } catch {
      // ignore
    }
    try {
      rmSync(base, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  it("§4.11 appends and reads an event round-trip", async () => {
    const seq = await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "Fact",
      payload: JSON.stringify({ temperature: 21.5 }),
    });

    expect(seq).toBe(1);

    const event = await db.read(seq);
    expect(event.agentId).toBe("agent-1");
    expect(event.streamId).toBe("stream-1");
    expect(event.kindTag).toBe("Fact");
    expect(JSON.parse(event.payload)).toEqual({ temperature: 21.5 });

    const len = await db.logLen();
    expect(len).toBe(1);
  });

  it("§4.11b runs hybrid query over indexed documents", async () => {
    await db.indexDoc("doc-1", "the quick brown fox", vecWithOne(0));
    await db.indexDoc("doc-2", "the lazy dog sleeps", vecWithOne(1));
    await db.indexDoc("doc-3", "brown fox jumps high", vecWithOne(0));

    const hits = await db.queryHybrid({
      text: "brown fox",
      vector: vecWithOne(0),
      k: 5,
    });

    expect(hits.length).toBeGreaterThan(0);
    const ids = hits.map((h) => h.id);
    expect(ids).toContain("doc-1");
    expect(hits[0].score).toBeGreaterThan(0);
  });

  it("§4.11c emits matching events through async iterator subscription", async () => {
    const stream = db.events({ agentId: "agent-1", kind: "Fact" });

    const received: any[] = [];
    const consumer = (async () => {
      for await (const event of stream) {
        received.push(event);
        if (received.length === 1) break;
      }
    })();

    await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "Fact",
      payload: JSON.stringify({ note: "hello" }),
    });

    await consumer;
    stream.close();

    expect(received.length).toBe(1);
    expect(received[0].agentId).toBe("agent-1");
    expect(received[0].kindTag).toBe("Fact");
  });

  it("§4.11e subscription filters by payload equality", async () => {
    const stream = db.events({
      kind: "Fact",
      predicate: { kind: "Eq", path: "/temperature", value: 21.5 },
    });

    const received: any[] = [];
    const consumer = (async () => {
      for await (const event of stream) {
        received.push(event);
        if (received.length === 1) break;
      }
    })();

    await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "Fact",
      payload: JSON.stringify({ temperature: 22.0, room: "B" }),
    });
    await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "Fact",
      payload: JSON.stringify({ temperature: 21.5, room: "A" }),
    });

    await consumer;
    stream.close();

    expect(received.length).toBe(1);
    expect(JSON.parse(received[0].payload).temperature).toBe(21.5);
    expect(JSON.parse(received[0].payload).room).toBe("A");
  });

  it("§4.11f subscription filters by payload contains", async () => {
    const stream = db.events({
      kind: "Fact",
      predicate: { kind: "Contains", path: "/tags", value: "urgent" },
    });

    const received: any[] = [];
    const consumer = (async () => {
      for await (const event of stream) {
        received.push(event);
        if (received.length === 1) break;
      }
    })();

    await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "Fact",
      payload: JSON.stringify({ tags: ["home"], room: "B" }),
    });
    await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "Fact",
      payload: JSON.stringify({ tags: ["urgent", "home"], room: "A" }),
    });

    await consumer;
    stream.close();

    expect(received.length).toBe(1);
    expect(JSON.parse(received[0].payload).room).toBe("A");
  });

  it("§4.12 upserts text-only docs and searches with Spanish morphology", async () => {
    await db.upsertBatch([
      {
        id: "tool:send_email",
        name: "send_email",
        body: "envía un correo electrónico al destinatario",
        tags: "comunicación email",
        filters: [{ field: "type", value: "tool" }],
      },
      {
        id: "skill:reportes",
        name: "generación de reportes",
        body: "genera reportes mensuales de transacciones",
        tags: "reportes análisis",
        filters: [{ field: "type", value: "skill" }],
      },
    ]);

    // Accent-less + morphological variant matches the accented document.
    const hits = await db.queryHybrid({ text: "transacciones", k: 5 });
    expect(hits.length).toBe(1);
    expect(hits[0].id).toBe("skill:reportes");
    expect(hits[0].score).toBeGreaterThan(0);
    expect(hits[0].textScore).toBe(hits[0].score);

    // Type filter narrows results.
    const toolHits = await db.queryHybrid({
      text: "correo reportes",
      k: 5,
      filters: [{ field: "type", value: "tool" }],
    });
    expect(toolHits.length).toBe(1);
    expect(toolHits[0].id).toBe("tool:send_email");
  });

  it("§4.12b raw user input with operators never throws", async () => {
    await db.upsertDoc({ id: "d1", body: "envía el correo" });
    for (const text of ['"correo sin cerrar', "¿puedes enviar (el) correo?", "a:b* OR NOT"]) {
      const hits = await db.queryHybrid({ text, k: 5 });
      expect(Array.isArray(hits)).toBe(true);
    }
  });

  it("§4.12c upsert replaces, delete and deleteByFilter remove docs", async () => {
    await db.upsertDoc({
      id: "mcp:a/1",
      body: "herramienta uno",
      filters: [{ field: "server_id", value: "a" }],
    });
    await db.upsertDoc({
      id: "mcp:a/2",
      body: "herramienta dos",
      filters: [{ field: "server_id", value: "a" }],
    });
    await db.upsertDoc({ id: "mcp:b/1", body: "herramienta tres" });

    // Upsert replaces content without duplicating.
    await db.upsertDoc({ id: "mcp:b/1", body: "utilidad renombrada" });
    let hits = await db.queryHybrid({ text: "herramienta", k: 10 });
    expect(hits.map((h) => h.id).sort()).toEqual(["mcp:a/1", "mcp:a/2"]);

    await db.deleteByFilter({ field: "server_id", value: "a" });
    hits = await db.queryHybrid({ text: "herramienta utilidad", k: 10 });
    expect(hits.map((h) => h.id)).toEqual(["mcp:b/1"]);

    await db.deleteDoc("mcp:b/1");
    hits = await db.queryHybrid({ text: "utilidad", k: 10 });
    expect(hits.length).toBe(0);
  });

  it("§4.12d clearIndex empties the semantic index", async () => {
    await db.upsertBatch([
      { id: "x1", body: "contenido uno" },
      { id: "x2", body: "contenido dos" },
    ]);
    await db.clearIndex();
    const hits = await db.queryHybrid({ text: "contenido", k: 10 });
    expect(hits.length).toBe(0);
  });

  it('§4.12e ":memory:" mode supports the full search lifecycle', async () => {
    const mem = await HiveDB.open(":memory:");
    try {
      await mem.upsertDoc({ id: "m1", name: "buscar reuniones", body: "transcripción de reunión" });
      const hits = await mem.queryHybrid({ text: "reunion", k: 5 });
      expect(hits.length).toBe(1);
      expect(hits[0].id).toBe("m1");
    } finally {
      mem.close();
    }
  });

  it("§4.12f vector dimension is configurable at open", async () => {
    const dir = tempDir("hive-g8-dim-");
    const small = await HiveDB.open(dir, { vectorDimension: 8 });
    try {
      const v = new Float32Array(8);
      v[0] = 1;
      await small.upsertDoc({ id: "v1", body: "doc con vector corto", vector: v });
      const hits = await small.queryHybrid({ vector: v, k: 1 });
      expect(hits.length).toBe(1);
      expect(hits[0].id).toBe("v1");
      expect(hits[0].vectorScore!).toBeGreaterThan(0.99);
    } finally {
      small.close();
      rmSync(dir, { recursive: true, force: true });
    }

    // Reopening with a different dimension must fail loudly.
    const dir2 = tempDir("hive-g8-dim2-");
    try {
      await expect(async () => {
        const a = await HiveDB.open(dir2, { vectorDimension: 8 });
        a.close();
        await HiveDB.open(dir2, { vectorDimension: 16 });
      }).toThrow();
    } finally {
      rmSync(dir2, { recursive: true, force: true });
    }
  });

  it("§4.13 exposes lastSeq and toolStats", async () => {
    const seq1 = await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "ToolCall",
      payload: JSON.stringify({ tool: "search", latency_ms: 10, cost: 0.5, outcome: "Ok" }),
    });
    expect(await db.lastSeq()).toBe(seq1);

    await db.append({
      agentId: "agent-1",
      streamId: "stream-1",
      kind: "ToolCall",
      payload: JSON.stringify({
        tool: "search",
        latency_ms: 20,
        cost: 0.7,
        outcome: { Err: "rate limited" },
      }),
    });

    const stats = await db.toolStats("search");
    expect(stats).toBeDefined();
    expect(stats!.invocations).toBe(2);
    expect(stats!.errors).toBe(1);
    expect(stats!.totalLatencyMs).toBe(30);
    expect(stats!.totalCost).toBeCloseTo(1.2, 6);
    expect(stats!.lastOutcome).toBe("Err: rate limited");
    expect(stats!.lastSeq).toBe(2);
  });

  it("§4.11d 150 open/append/close cycles stay within 30 MB RSS growth", async () => {
    // Kept intentionally small: each database materializes redb shards +
    // tantivy + collections on disk, and a crashed run must not be able to
    // exhaust /tmp. Cleanup runs even on failure.
    const root = tempDir("hive-g8-leak-");
    try {
      const before = (process as any).memoryUsage().rss as number;

      for (let i = 0; i < 150; i++) {
        const path = join(root, `db-${i}`);
        const d = await HiveDB.open(path);
        await d.append({
          agentId: "agent-leak",
          streamId: "stream-leak",
          kind: "Fact",
          payload: JSON.stringify({ i }),
        });
        d.close();
      }

      const after = (process as any).memoryUsage().rss as number;
      const growthMb = (after - before) / 1024 / 1024;
      expect(growthMb).toBeLessThan(30);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, { timeout: 60000 });
});
