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

  it("§4.11d 1000 open/append/close cycles stay within 50 MB RSS growth", async () => {
    const root = tempDir("hive-g8-leak-");
    const before = (process as any).memoryUsage().rss as number;

    for (let i = 0; i < 1000; i++) {
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

    rmSync(root, { recursive: true, force: true });

    expect(growthMb).toBeLessThan(50);
  }, { timeout: 60000 });
});
