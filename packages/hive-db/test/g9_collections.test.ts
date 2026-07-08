import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { HiveDB } from "../src/index.ts";

interface Agent {
  name: string;
  role: string;
  active?: boolean;
}

describe("G10 document collections", () => {
  let db: HiveDB;

  beforeEach(async () => {
    db = await HiveDB.open(":memory:");
  });

  afterEach(() => {
    try {
      db.close();
    } catch {
      // ignore
    }
  });

  it("put/get/delete round-trip with versioning", async () => {
    const agents = db.collection<Agent>("agents");

    const v1 = await agents.put("a1", { name: "Atlas", role: "worker" });
    expect(v1).toBe(1);

    const entry = await agents.get("a1");
    expect(entry?.doc.name).toBe("Atlas");
    expect(entry?.version).toBe(1);

    const v2 = await agents.put("a1", { name: "Atlas", role: "coordinator" });
    expect(v2).toBe(2);

    expect(await agents.delete("a1")).toBe(true);
    expect(await agents.delete("a1")).toBe(false);
    expect(await agents.get("a1")).toBeUndefined();
  });

  it("optimistic concurrency via expectedVersion", async () => {
    const col = db.collection("cfg");
    await col.put("x", { a: 1 }, { expectedVersion: 0 }); // create-only

    await expect(col.put("x", { a: 2 }, { expectedVersion: 0 })).rejects.toThrow();
    await col.put("x", { a: 2 }, { expectedVersion: 1 });
    await expect(col.put("x", { a: 3 }, { expectedVersion: 1 })).rejects.toThrow();
  });

  it("scan with prefix/limit/reverse and count", async () => {
    const col = db.collection("things");
    for (const id of ["u:1", "u:2", "u:3", "g:1"]) {
      await col.put(id, { id });
    }

    expect(await col.count()).toBe(4);

    const users = await col.scan({ prefix: "u:" });
    expect(users.map((e) => e.id)).toEqual(["u:1", "u:2", "u:3"]);

    const lastTwo = await col.scan({ reverse: true, limit: 2 });
    expect(lastTwo.map((e) => e.id)).toEqual(["u:3", "u:2"]);
  });

  it("secondary index: findBy follows updates and deletes", async () => {
    const convs = db.collection<{ threadId: string; n: number }>("convs");
    await convs.put("c1", { threadId: "t-1", n: 1 });
    await convs.put("c2", { threadId: "t-1", n: 2 });
    await convs.createIndex("threadId");

    let hits = await convs.findBy("threadId", "t-1");
    expect(hits.length).toBe(2);

    await convs.put("c2", { threadId: "t-2", n: 2 });
    hits = await convs.findBy("threadId", "t-1");
    expect(hits.map((e) => e.id)).toEqual(["c1"]);

    await convs.delete("c1");
    hits = await convs.findBy("threadId", "t-1");
    expect(hits.length).toBe(0);

    // findBy without an index is an explicit error
    await expect(convs.findBy("n", 2)).rejects.toThrow();
  });

  it("unique index rejects duplicates", async () => {
    const tokens = db.collection<{ hash: string }>("tokens");
    await tokens.createIndex("hash", { unique: true });
    await tokens.put("t1", { hash: "abc" });

    await expect(tokens.put("t2", { hash: "abc" })).rejects.toThrow();
    // Same doc can be re-put
    await tokens.put("t1", { hash: "abc" });
  });

  it("batch commits atomically or not at all", async () => {
    const col = db.collection<{ balance: number }>("acc");
    await col.put("a", { balance: 10 });

    await expect(
      db.batch([
        { op: "put", collection: "acc", id: "a", doc: { balance: 0 } },
        { op: "put", collection: "acc", id: "b", doc: { balance: 10 }, expectedVersion: 7 },
      ])
    ).rejects.toThrow();

    expect((await col.get("a"))?.doc.balance).toBe(10);
    expect(await col.get("b")).toBeUndefined();

    await db.batch([
      { op: "put", collection: "acc", id: "a", doc: { balance: 0 } },
      { op: "put", collection: "acc", id: "b", doc: { balance: 10 } },
      { op: "delete", collection: "acc", id: "missing" },
    ]);
    expect((await col.get("a"))?.doc.balance).toBe(0);
    expect((await col.get("b"))?.doc.balance).toBe(10);
  });

  it("collections coexist with the semantic index and event log", async () => {
    // Same database instance serves events, search and collections.
    await db.append({
      agentId: "a",
      streamId: "s",
      kind: "Fact",
      payload: JSON.stringify({ ok: true }),
    });
    await db.upsertDoc({ id: "d1", body: "documento de prueba" });
    await db.collection("misc").put("m1", { hello: "world" });

    expect(await db.logLen()).toBe(1);
    const hits = await db.queryHybrid({ text: "prueba", k: 5 });
    expect(hits.length).toBe(1);
    expect((await db.collection<{ hello: string }>("misc").get("m1"))?.doc.hello).toBe("world");
  });
});
