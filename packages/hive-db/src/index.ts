import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { JsHiveDb } = require("../native.cjs") as {
  JsHiveDb: {
    open(path: string): Promise<JsHiveDbInner>;
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
  queryHybrid(query: JsHybridQuery): Promise<JsHit[]>;
  subscribe(
    pattern: JsEventPattern,
    callback: (err: Error | null, event: JsEvent) => void
  ): JsSubscriptionInner;
  close(): void;
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
}

interface JsHit {
  id: string;
  score: number;
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

export interface HybridQuery {
  text?: string;
  vector?: Float32Array;
  k: number;
  filters?: ScalarFilter[];
}

export interface Hit {
  id: string;
  score: number;
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

  static async open(path: string): Promise<HiveDB> {
    return new HiveDB(await JsHiveDb.open(path));
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

  async indexDoc(
    id: string,
    text: string,
    vector: Float32Array,
    filters?: ScalarFilter[]
  ): Promise<void> {
    return this.inner.indexDoc(id, text, vector, filters);
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