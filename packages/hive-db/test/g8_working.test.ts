import { test, expect } from "bun:test";
import { HiveDB } from "../src";

function tmpDir(): string {
  return `/tmp/hivedb-working-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

test("G8 working memory > round-trips a value", async () => {
  const db = await HiveDB.open(tmpDir());
  await db.workingSet("agent-a", "draft", { text: "hello" });
  const value = await db.workingGet<{ text: string }>("agent-a", "draft");
  expect(value).toEqual({ text: "hello" });
  db.close();
});

test("G8 working memory > entries expire by TTL", async () => {
  const db = await HiveDB.open(tmpDir());
  await db.workingSet("agent-a", "temp", 42, 50);
  expect(await db.workingGet<number>("agent-a", "temp")).toBe(42);
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(await db.workingGet<number>("agent-a", "temp")).toBeUndefined();
  db.close();
});

test("G8 working memory > keys are isolated by agent", async () => {
  const db = await HiveDB.open(tmpDir());
  await db.workingSet("agent-a", "k1", 1);
  await db.workingSet("agent-b", "k1", 2);

  const keysA = await db.workingKeys("agent-a");
  const keysB = await db.workingKeys("agent-b");

  expect(keysA).toEqual(["k1"]);
  expect(keysB).toEqual(["k1"]);
  db.close();
});

test("G8 working memory > does not write to the event log", async () => {
  const db = await HiveDB.open(tmpDir());
  await db.workingSet("agent-a", "draft", { text: "hello" });
  expect(await db.logLen()).toBe(0);
  db.close();
});
