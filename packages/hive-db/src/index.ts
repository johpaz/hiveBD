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