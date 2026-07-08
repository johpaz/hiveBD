import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { JsHiveDb } = require("../native.cjs") as {
  JsHiveDb: {
    open(path: string, options?: JsOpenOptions): Promise<JsHiveDbInner>;
  };
};

interface JsHiveDbInner {
  append(input: JsEventInput): Promise<number>;
  read(seq: number): Promise<JsEvent>;
  logLen(): Promise<number>;
  projectTaskState(agentId: string, streamId: string): Promise<string | null>;
  can(agent: string, action: string, resource: string): Promise<JsDecision>;
  indexDoc(
    id: string,
    text: string,
    vector: Float32Array,
    filters?: JsScalarFilter[]
  ): Promise<void>;
  upsertDoc(doc: JsIndexDoc): Promise<void>;
  upsertBatch(docs: JsIndexDoc[]): Promise<void>;
  deleteDoc(id: string): Promise<void>;
  deleteByFilter(filter: JsScalarFilter): Promise<void>;
  clearIndex(): Promise<void>;
  colPut(collection: string, id: string, json: string, options?: JsPutOptions): Promise<number>;
  colGet(collection: string, id: string): Promise<JsDocEntry | null>;
  colDelete(collection: string, id: string): Promise<boolean>;
  colScan(collection: string, options?: JsScanOptions): Promise<JsDocEntry[]>;
  colCount(collection: string): Promise<number>;
  colCreateIndex(collection: string, field: string, unique: boolean): Promise<void>;
  colFindBy(collection: string, field: string, valueJson: string, options?: JsScanOptions): Promise<JsDocEntry[]>;
  colBatch(ops: JsColOp[]): Promise<void>;
  queryHybrid(query: JsHybridQuery): Promise<JsHit[]>;
  subscribe(
    pattern: JsEventPattern,
    callback: (err: Error | null, event: JsEvent) => void
  ): JsSubscriptionInner;
  close(): void;
}

interface JsOpenOptions {
  vectorDimension?: number;
}

interface JsIndexDoc {
  id: string;
  name?: string;
  body?: string;
  tags?: string;
  vector?: Float32Array;
  filters?: JsScalarFilter[];
}

interface JsDocEntry {
  id: string;
  version: number;
  json: string;
}

interface JsPutOptions {
  expectedVersion?: number;
}

interface JsScanOptions {
  prefix?: string;
  start?: string;
  limit?: number;
  offset?: number;
  reverse?: boolean;
}

interface JsColOp {
  op: string;
  collection: string;
  id: string;
  json?: string;
  expectedVersion?: number;
}

interface JsSubscriptionInner {
  close(): void;
}

interface JsEventInput {
  agentId: string;
  streamId: string;
  kind: string;
  payload: string;
}

interface JsEvent {
  seq: number;
  agentId: string;
  streamId: string;
  kindTag: string;
  timestamp: number;
  causation?: number;
  correlation?: string;
  payload: string;
}

interface JsDecision {
  allowed: boolean;
  intentLogSeq?: number;
}

interface JsScalarFilter {
  field: string;
  value: string;
}

interface JsHybridQuery {
  text?: string;
  vector?: Float32Array;
  k: number;
  filters?: JsScalarFilter[];
  fusion?: JsFusion;
  boosts?: JsFieldBoosts;
}

interface JsFusion {
  kind: string;
  k?: number;
}

interface JsFieldBoosts {
  name?: number;
  body?: number;
  tags?: number;
}

interface JsHit {
  id: string;
  score: number;
  textScore?: number;
  vectorScore?: number;
}

interface JsEventPattern {
  agentId?: string;
  kind?: string;
  streamId?: string;
}

export interface EventInput {
  agentId: string;
  streamId: string;
  kind: "Fact" | "StateTransition" | "MemoryInvalidate" | "ToolCall" | "ConsentGranted" | "ConsentRevoked" | "IntentLogged";
  payload: string;
}

export interface Event {
  seq: number;
  agentId: string;
  streamId: string;
  kindTag: string;
  timestamp: number;
  causation?: number;
  correlation?: string;
  payload: string;
}

export interface Decision {
  allowed: boolean;
  intentLogSeq?: number;
}

export interface ScalarFilter {
  field: string;
  value: string;
}

export interface OpenOptions {
  /**
   * Dimension of vectors accepted by the semantic index (default 384).
   * Fixed at first open; reopening with a different value is an error.
   */
  vectorDimension?: number;
}

/**
 * A document for the semantic index. All text slots are optional; a document
 * without a vector never touches the vector index.
 */
export interface IndexDoc {
  id: string;
  /** Short, high-signal title (boosted highest by default). */
  name?: string;
  /** Main text content. */
  body?: string;
  /** Categories, triggers, keywords. */
  tags?: string;
  /** Optional embedding; must match the index dimension when present. */
  vector?: Float32Array;
  filters?: ScalarFilter[];
}

/** Fusion strategy, used only when both text and vector are present. */
export interface Fusion {
  kind: "rrf";
  /** RRF `k` parameter (default 60). */
  k?: number;
}

/** Per-field BM25 boosts (defaults: name 4.0, body 2.0, tags 3.0). */
export interface FieldBoosts {
  name?: number;
  body?: number;
  tags?: number;
}

export interface HybridQuery {
  /** Parsed leniently: raw user input (quotes, operators) never throws. */
  text?: string;
  vector?: Float32Array;
  k: number;
  filters?: ScalarFilter[];
  fusion?: Fusion;
  boosts?: FieldBoosts;
}

/**
 * Score semantics: text-only queries return raw BM25 (positive, higher is
 * better), vector-only queries return cosine similarity, hybrid queries
 * return RRF-fused scores with the raw components in `textScore` /
 * `vectorScore`.
 */
export interface Hit {
  id: string;
  score: number;
  textScore?: number;
  vectorScore?: number;
}

/** A document read from a collection. */
export interface DocEntry<T = unknown> {
  id: string;
  /** Monotonic per-document version, starting at 1 on first put. */
  version: number;
  doc: T;
}

export interface PutDocOptions {
  /**
   * Optimistic concurrency: the current stored version must equal this value
   * (0 = the document must not exist yet). Omit for unconditional upsert.
   */
  expectedVersion?: number;
}

export interface ScanOptions {
  /** Only ids starting with this prefix. */
  prefix?: string;
  /** Start at this id (inclusive, ascending id order). */
  start?: string;
  /** Maximum entries to return. */
  limit?: number;
  /** Entries to skip before collecting. */
  offset?: number;
  /** Return entries in descending id order. */
  reverse?: boolean;
}

/** One operation inside an atomic HiveDB.batch(). */
export type BatchOp =
  | { op: "put"; collection: string; id: string; doc: unknown; expectedVersion?: number }
  | { op: "delete"; collection: string; id: string };

/**
 * Typed handle to a named document collection (mutable CRUD storage,
 * separate from the immutable event log). Every write commits atomically
 * with its secondary-index maintenance.
 */
export class Collection<T = unknown> {
  constructor(
    private inner: JsHiveDbInner,
    private name: string
  ) {}

  /** Insert or replace a document. Returns the new version (starts at 1). */
  async put(id: string, doc: T, options?: PutDocOptions): Promise<number> {
    return this.inner.colPut(this.name, id, JSON.stringify(doc), options);
  }

  /** Read a document by id. */
  async get(id: string): Promise<DocEntry<T> | undefined> {
    const entry = await this.inner.colGet(this.name, id);
    if (!entry) return undefined;
    return { id: entry.id, version: entry.version, doc: JSON.parse(entry.json) as T };
  }

  /** Delete a document. Returns true if it existed. */
  async delete(id: string): Promise<boolean> {
    return this.inner.colDelete(this.name, id);
  }

  /** Scan the collection in id order. */
  async scan(options?: ScanOptions): Promise<DocEntry<T>[]> {
    const entries = await this.inner.colScan(this.name, options);
    return entries.map((e) => ({ id: e.id, version: e.version, doc: JSON.parse(e.json) as T }));
  }

  /** Number of documents in the collection. */
  async count(): Promise<number> {
    return this.inner.colCount(this.name);
  }

  /**
   * Create an equality index on a top-level field (optionally unique).
   * Backfills existing documents; idempotent for an identical definition.
   */
  async createIndex(field: string, options?: { unique?: boolean }): Promise<void> {
    return this.inner.colCreateIndex(this.name, field, options?.unique ?? false);
  }

  /**
   * Look up documents whose indexed field equals `value` (scalar equality).
   * Requires a previous createIndex on that field.
   */
  async findBy(
    field: string,
    value: string | number | boolean,
    options?: ScanOptions
  ): Promise<DocEntry<T>[]> {
    const entries = await this.inner.colFindBy(this.name, field, JSON.stringify(value), options);
    return entries.map((e) => ({ id: e.id, version: e.version, doc: JSON.parse(e.json) as T }));
  }
}

export interface EventPattern {
  agentId?: string;
  kind?: EventInput["kind"];
  streamId?: string;
}

export interface Subscription {
  close(): void;
}

export class HiveDB {
  private inner: JsHiveDbInner;

  private constructor(inner: JsHiveDbInner) {
    this.inner = inner;
  }

  /**
   * Open (or create) a database at `path`. Pass `":memory:"` for an
   * ephemeral database that never touches persistent storage.
   */
  static async open(path: string, options?: OpenOptions): Promise<HiveDB> {
    return new HiveDB(await JsHiveDb.open(path, options));
  }

  async append(input: EventInput): Promise<number> {
    return this.inner.append(input);
  }

  async read(seq: number): Promise<Event> {
    return this.inner.read(seq);
  }

  async logLen(): Promise<number> {
    return this.inner.logLen();
  }

  async projectTaskState(agentId: string, streamId: string): Promise<string | undefined> {
    return (await this.inner.projectTaskState(agentId, streamId)) ?? undefined;
  }

  async can(agent: string, action: string, resource: string): Promise<Decision> {
    return this.inner.can(agent, action, resource);
  }

  /** @deprecated Use {@link upsertDoc}; `text` maps to the `body` field. */
  async indexDoc(
    id: string,
    text: string,
    vector: Float32Array,
    filters?: ScalarFilter[]
  ): Promise<void> {
    return this.inner.indexDoc(id, text, vector, filters);
  }

  /** Insert or replace a document in the semantic index. */
  async upsertDoc(doc: IndexDoc): Promise<void> {
    return this.inner.upsertDoc(doc);
  }

  /**
   * Insert or replace a batch of documents under a single index commit.
   * Much faster than repeated `upsertDoc` calls.
   */
  async upsertBatch(docs: IndexDoc[]): Promise<void> {
    return this.inner.upsertBatch(docs);
  }

  /** Delete a document from the semantic index. Missing ids are a no-op. */
  async deleteDoc(id: string): Promise<void> {
    return this.inner.deleteDoc(id);
  }

  /** Delete every indexed document carrying the given scalar filter. */
  async deleteByFilter(filter: ScalarFilter): Promise<void> {
    return this.inner.deleteByFilter(filter);
  }

  /** Remove every document from the semantic index. */
  async clearIndex(): Promise<void> {
    return this.inner.clearIndex();
  }

  /** Typed handle to a named document collection. */
  collection<T = unknown>(name: string): Collection<T> {
    return new Collection<T>(this.inner, name);
  }

  /**
   * Apply several puts/deletes atomically across collections: either every
   * operation commits or none does.
   */
  async batch(ops: BatchOp[]): Promise<void> {
    return this.inner.colBatch(
      ops.map((op) =>
        op.op === "put"
          ? {
              op: "put",
              collection: op.collection,
              id: op.id,
              json: JSON.stringify(op.doc),
              expectedVersion: op.expectedVersion,
            }
          : { op: "delete", collection: op.collection, id: op.id }
      )
    );
  }

  async queryHybrid(query: HybridQuery): Promise<Hit[]> {
    return this.inner.queryHybrid(query);
  }

  subscribe(pattern: EventPattern, onEvent: (event: Event) => void): Subscription {
    return this.inner.subscribe(pattern, (_err: Error | null, event: JsEvent | null | undefined) => {
      if (event != null) onEvent(event);
    });
  }

  events(pattern: EventPattern): AsyncIterable<Event> & { close(): void } {
    const buffer: Event[] = [];
    let resolveNext: ((result: IteratorResult<Event>) => void) | null = null;
    let closed = false;

    const subscription = this.inner.subscribe(pattern, (_err: Error | null, event: JsEvent | null | undefined) => {
      if (closed || event == null) return;
      if (resolveNext) {
        resolveNext({ value: event, done: false });
        resolveNext = null;
      } else {
        buffer.push(event);
      }
    });

    const iterator: AsyncIterable<Event> & { close(): void } = {
      [Symbol.asyncIterator](): AsyncIterator<Event> {
        return {
          next: async (): Promise<IteratorResult<Event>> => {
            if (buffer.length > 0) {
              return { value: buffer.shift()!, done: false };
            }
            if (closed) {
              return { value: undefined, done: true } as IteratorResult<Event>;
            }
            return new Promise<IteratorResult<Event>>((resolve) => {
              resolveNext = resolve;
            });
          },
        };
      },
      close: () => {
        closed = true;
        if (resolveNext) {
          resolveNext({ value: undefined, done: true } as IteratorResult<Event>);
          resolveNext = null;
        }
        subscription.close();
      },
    };

    return iterator;
  }

  close(): void {
    this.inner.close();
  }
}