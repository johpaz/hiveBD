# HiveDB — Guía de uso para agentes

> Cómo usar HiveDB desde **Bun** a través del paquete TypeScript `@johpaz/hive-db`.

---

## 1. Instalación y primer arranque

Una vez publicado en npm (ver `docs/DISTRIBUTION.md`), desde cualquier proyecto y SO:

```bash
bun add @johpaz/hive-db   # instala el binario nativo de tu plataforma automáticamente
```

Hay binarios para Linux x64 (glibc y musl/Alpine), Linux arm64, macOS x64 y arm64, y Windows x64.

Para desarrollo dentro de este monorepo:

```bash
# Dentro de un workspace Bun que ya tenga el paquete local
bun install

# Compila el binding nativo y genera native.cjs + hivedb-napi.<triple>.node
cd packages/hive-db
bun run build:native
```

Desde tu aplicación:

```ts
import { HiveDB } from "@johpaz/hive-db";

const db = await HiveDB.open("./data/my-agent");
```

`open` crea el directorio si no existe. Una vez abierta, la base de datos es un único directorio autocontenido; puedes moverlo, versionarlo (excepto el `.node`) o respaldarlo copiando la carpeta.

---

## 2. Conceptos para diseñar un agente

HiveDB modela la memoria de un agente como un **event-log inmutable** sobre el que se derivan vistas (proyecciones). En lugar de hacer `UPDATE` o `DELETE`, siempre **añades un nuevo evento**.

| Concepto | Qué representa | Ejemplo |
|---|---|---|
| `agentId` | Partición lógica de escritura. Dos agentes no se bloquean entre sí. | `"travel-agent-7"` |
| `streamId` | Sub-flujo dentro de un agente (una tarea, una conversación, un objetivo). | `"trip-to-paris"` |
| `kind` | Tipo semántico del evento. | `"Fact"`, `"StateTransition"`, `"ToolCall"` |
| `payload` | Cuerpo JSON libre con los datos del evento. | `{ "temperature": 21.5 }` |
| `seq` | Número de secuencia global asignado por el motor. Es inmutable. | `1`, `2`, `3`… |

El motor nunca te deja fijar `seq` ni `timestamp`: ambos los asigna HiveDB para garantizar orden causal y auditoría.

---

## 3. Eventos básicos: append y read

```ts
const seq = await db.append({
  agentId: "travel-agent-7",
  streamId: "trip-to-paris",
  kind: "Fact",
  payload: JSON.stringify({ temperature: 21.5 }),
});

console.log("secuencia asignada:", seq); // 1, 2, 3…

const event = await db.read(seq);
console.log(event.agentId, event.kindTag, JSON.parse(event.payload));
```

`append` devuelve el `seq` global. `read(seq)` devuelve el evento completo, incluyendo `timestamp`, `causation` y `correlation`.

### Corrección sin mutación

Si un hecho cambia, no lo editas: emites un evento `MemoryInvalidate` que apunta al `seq` del hecho anterior:

```ts
const oldFact = await db.append({
  agentId: "travel-agent-7",
  streamId: "trip-to-paris",
  kind: "Fact",
  payload: JSON.stringify({ price: 100 }),
});

await db.append({
  agentId: "travel-agent-7",
  streamId: "trip-to-paris",
  kind: "MemoryInvalidate",
  payload: JSON.stringify({ target_seq: oldFact }),
});
```

El hecho original sigue en el log (auditoría), pero las proyecciones vigentes lo ignoran.

---

## 4. Suscripciones reactivas

El motor puede notificarte cuando ocurre un evento que coincide con un patrón. Es útil para triggers, workflows y actualizar UI.

### Con async iterator (recomendado)

```ts
const facts = db.events({ agentId: "travel-agent-7", kind: "Fact" });

for await (const event of facts) {
  console.log("nuevo hecho:", event.seq, JSON.parse(event.payload));
  if (shouldStop(event)) break;
}

facts.close();
```

### Con callback directo

```ts
const sub = db.subscribe(
  { agentId: "travel-agent-7", kind: "Fact" },
  (event) => {
    console.log("llegó:", event.seq);
  }
);

// …más tarde
sub.close();
```

El patrón puede filtrar por `agentId`, `kind` y/o `streamId`. Si omites todos, recibes todos los eventos.

---

## 5. Memoria semántica híbrida

HiveDB combina búsqueda full-text (BM25 sobre `tantivy`) con búsqueda vectorial (HNSW) y las fusiona con Reciprocal Rank Fusion (RRF). El análisis de texto está optimizado para español: minúsculas, plegado de acentos ("transacción" ≈ "transaccion") y stemming Snowball ("pagos" ≈ "pago"). El texto en inglés pasa casi intacto por el stemmer español, así que catálogos bilingües funcionan; lo que NO hace el motor es traducir entre idiomas ("correo" no encuentra "email" sin embeddings).

### Indexar documentos (upsert)

Cada documento tiene tres campos de texto opcionales con pesos distintos en el ranking — `name` (boost 4.0), `tags` (3.0) y `body` (2.0) — un vector opcional y filtros escalares opcionales. `upsertDoc` reemplaza el documento si el id ya existe; nunca duplica.

```ts
await db.upsertDoc({
  id: "tool:send_email",
  name: "send_email",
  body: "envía un correo electrónico al destinatario",
  tags: "comunicación email",
  filters: [{ field: "type", value: "tool" }],
});

// Lote grande: un solo commit de índice, mucho más rápido.
await db.upsertBatch(docs);
```

El vector es opcional: los documentos solo-texto nunca tocan el índice vectorial. Cuando incluyas vectores, la dimensión (384 por defecto) se fija en el primer `open` y puede configurarse: `HiveDB.open(path, { vectorDimension: 768 })`.

### Consultar

```ts
const hits = await db.queryHybrid({
  text: "¿cómo genero reportes de transacciones?", // texto crudo: nunca lanza error
  k: 5,
  filters: [{ field: "type", value: "tool" }],
  boosts: { name: 4, tags: 3, body: 2 }, // opcional
});

for (const hit of hits) {
  console.log(hit.id, hit.score);
}
```

Puedes consultar solo por texto, solo por vector, o ambos. La semántica del `score` depende del modo:

| Modo | `score` |
|---|---|
| Solo texto | BM25 crudo (positivo, mayor = mejor) |
| Solo vector | Similitud coseno (0-1) |
| Híbrido | Fusión RRF (`fusion: { kind: "rrf", k: 60 }` configurable); `textScore` y `vectorScore` traen los componentes crudos |

El parsing del texto es tolerante: comillas sin cerrar, operadores y signos de puntuación degradan a una búsqueda bolsa-de-palabras en lugar de fallar.

### Borrar y mantener

```ts
await db.deleteDoc("tool:send_email");                       // por id
await db.deleteByFilter({ field: "server_id", value: "a" }); // por filtro (p. ej. hot-reload MCP)
await db.clearIndex();                                       // vaciar todo el índice
```

`HiveDB.open(":memory:")` abre una base efímera (ideal para tests) con el índice semántico completo.

---

## 6. Consentimiento y autorización

HiveDB incluye un grafo de consentimiento. Un agente puede delegar permisos a otro, revocarlos, y luego consultar si una acción está autorizada.

### Otorgar consentimiento

```ts
await db.append({
  agentId: "owner",
  streamId: "consent",
  kind: "ConsentGranted",
  payload: JSON.stringify({
    from: "owner",
    to: "assistant",
    scope: { action: "read", resource: "trips/*" },
    expires: Date.now() + 24 * 60 * 60 * 1000, // opcional, epoch ms
  }),
});
```

### Revocar consentimiento

```ts
await db.append({
  agentId: "owner",
  streamId: "consent",
  kind: "ConsentRevoked",
  payload: JSON.stringify({ grant_seq: 1 }),
});
```

### Consultar autorización

```ts
const decision = await db.can("assistant", "read", "trips/paris");
console.log(decision.allowed); // true | false
console.log(decision.intentLogSeq); // seq del evento IntentLogged de auditoría
```

Cada llamada a `can` genera automáticamente un evento `IntentLogged` en el log para dejar traza auditable.

---

## 7. Memoria de trabajo (Working Memory)

Para datos transitorios que no necesitan durabilidad de log pero sí rápido acceso con TTL:

> Actualmente esta API está expuesta solo en el núcleo Rust. En futuras versiones del binding TS se añadirá `workingSet` / `workingGet`.

---

## 8. Cierre y ciclo de vida

```ts
db.close();
```

`close` libera el handle nativo. Las suscripciones abiertas se cancelan automáticamente al cerrar.

Si necesitas reconstruir las proyecciones desde cero (por ejemplo tras añadir una nueva proyección en el motor), usa el comando correspondiente en Rust; no está expuesto en TS por ser una operación de mantenimiento.

---

## 9. Patrones recomendados para agentes

1. **Un `agentId` por agente autónomo.** No compartas `agentId` entre instancias concurrentes si no quieres contención de escritura.
2. **Un `streamId` por objetivo o conversación.** Facilita leer la historia completa de una tarea.
3. **No uses el log como cola de trabajo de alta frecuencia.** Las suscripciones reactivas son *at-least-once*; para trabajo crítico usa confirmaciones idempotentes.
4. **Indexa documentos justo después de append.** Así textos y vectores quedan disponibles para búsqueda inmediata.
5. **Mide RSS si abres/cerras muchas bases.** Cada `HiveDB.open` carga índices en memoria; reutiliza una instancia compartida cuando sea posible.

---

## 10. Referencia rápida de tipos

```ts
interface EventInput {
  agentId: string;
  streamId: string;
  kind: "Fact" | "StateTransition" | "MemoryInvalidate" | "ToolCall" | "ConsentGranted" | "ConsentRevoked" | "IntentLogged";
  payload: string; // JSON string
}

interface Event {
  seq: number;
  agentId: string;
  streamId: string;
  kindTag: string;
  timestamp: number;
  causation?: number;
  correlation?: string;
  payload: string;
}

interface IndexDoc {
  id: string;
  name?: string;   // boost 4.0
  body?: string;   // boost 2.0
  tags?: string;   // boost 3.0
  vector?: Float32Array; // dimensión configurable (default 384)
  filters?: ScalarFilter[];
}

interface HybridQuery {
  text?: string;
  vector?: Float32Array;
  k: number;
  filters?: ScalarFilter[];
  fusion?: { kind: "rrf"; k?: number };
  boosts?: { name?: number; body?: number; tags?: number };
}

interface ScalarFilter {
  field: string;
  value: string;
}

interface Hit {
  id: string;
  score: number;       // BM25 | coseno | RRF según el modo
  textScore?: number;
  vectorScore?: number;
}

interface Decision {
  allowed: boolean;
  intentLogSeq?: number;
}

class HiveDB {
  static open(path: string, options?: { vectorDimension?: number }): Promise<HiveDB>;
  append(input: EventInput): Promise<number>;
  read(seq: number): Promise<Event>;
  logLen(): Promise<number>;
  can(agent: string, action: string, resource: string): Promise<Decision>;
  upsertDoc(doc: IndexDoc): Promise<void>;
  upsertBatch(docs: IndexDoc[]): Promise<void>;
  deleteDoc(id: string): Promise<void>;
  deleteByFilter(filter: ScalarFilter): Promise<void>;
  clearIndex(): Promise<void>;
  indexDoc(id: string, text: string, vector: Float32Array, filters?: ScalarFilter[]): Promise<void>; // deprecado
  queryHybrid(query: HybridQuery): Promise<Hit[]>;
  events(pattern: EventPattern): AsyncIterable<Event> & { close(): void };
  subscribe(pattern: EventPattern, onEvent: (event: Event) => void): Subscription;
  close(): void;
}
```
