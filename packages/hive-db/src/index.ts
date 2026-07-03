import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

/** Triple de plataforma usado para nombrar los paquetes de binarios nativos. */
function platformTriple(): string {
  const { platform, arch } = process;
  switch (platform) {
    case "linux":
      return `linux-${arch}-gnu`;
    case "darwin":
      return `darwin-${arch}`;
    case "win32":
      return `win32-${arch}-msvc`;
    default:
      throw new Error(`@johpaz/hive-db: plataforma no soportada: ${platform}-${arch}`);
  }
}

function loadNative(): any {
  const errors: string[] = [];

  // 1. Binario local de desarrollo (generado con `bun run build:napi`).
  try {
    return require("../hivedb-napi.node");
  } catch (e: any) {
    errors.push(String(e?.message ?? e));
  }

  // 2. Paquete de binarios instalado como optionalDependency desde npm.
  try {
    return require(`@johpaz/hive-db-${platformTriple()}`);
  } catch (e: any) {
    errors.push(String(e?.message ?? e));
  }

  throw new Error(
    `@johpaz/hive-db: no se encontró el binario nativo para ${process.platform}-${process.arch}.\n` +
      `Instala el paquete desde npm (incluye binarios por plataforma) o compílalo con "bun run build:napi".\n` +
      `Detalles:\n- ${errors.join("\n- ")}`
  );
}

const addon = loadNative();

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
  private inner: any;

  private constructor(inner: any) {
    this.inner = inner;
  }

  static async open(path: string): Promise<HiveDB> {
    return new HiveDB(await addon.JsHiveDb.open(path));
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
    return this.inner.projectTaskState(agentId, streamId);
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
    // The native binding invokes the callback as (err, event); hide the err
    // channel so the public signature stays (event) => void.
    return this.inner.subscribe(pattern, (_err: unknown, event: Event | null | undefined) => {
      if (event != null) onEvent(event);
    });
  }

  events(pattern: EventPattern): AsyncIterable<Event> & { close(): void } {
    const buffer: Event[] = [];
    let resolveNext: ((result: IteratorResult<Event>) => void) | null = null;
    let closed = false;

    const subscription = this.inner.subscribe(pattern, (_err: unknown, event: Event | null | undefined) => {
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
        // Wake up any consumer blocked on next() so `for await` terminates.
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
