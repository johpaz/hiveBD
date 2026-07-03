# HiveDB — Guía de distribución

> Cómo consumir `@johpaz/hive-db` hoy, y cómo publicarlo en npm con binarios para todos los sistemas operativos.

---

## 1. ¿De dónde lo importo HOY? (sin publicar)

El paquete vive en `packages/hive-db` de este monorepo y **aún no está publicado en npm**. Tienes tres formas de consumirlo:

### a) Dentro de este monorepo (workspace)

El `package.json` raíz declara `"workspaces": ["packages/*"]`. Cualquier paquete o app que añadas bajo `packages/` puede importarlo directamente:

```ts
import { HiveDB } from "@johpaz/hive-db";
```

### b) Desde otro proyecto Bun en la misma máquina (`file:`)

```bash
cd mi-otro-proyecto
bun add file:../ruta/a/hiveBD/packages/hive-db
```

### c) Con `bun link` (desarrollo activo)

```bash
cd hiveBD/packages/hive-db && bun link
cd mi-otro-proyecto && bun link @johpaz/hive-db
```

En los tres casos necesitas el binario nativo compilado para **tu** máquina:

```bash
cd packages/hive-db
bun run build:napi   # requiere Rust instalado
```

---

## 2. Distribución multiplataforma (npm)

La estrategia es la estándar de napi-rs: un paquete principal en TypeScript/JavaScript más **un paquete de binarios por plataforma** que npm/bun instala automáticamente según el SO gracias a los campos `os`/`cpu`/`libc`:

| Paquete | Plataforma |
|---|---|
| `@johpaz/hive-db` | principal (TS/JS, sin binarios) |
| `@johpaz/hive-db-linux-x64-gnu` | Linux x64 (glibc) |
| `@johpaz/hive-db-linux-arm64-gnu` | Linux arm64 (glibc) |
| `@johpaz/hive-db-darwin-x64` | macOS Intel |
| `@johpaz/hive-db-darwin-arm64` | macOS Apple Silicon |
| `@johpaz/hive-db-win32-x64-msvc` | Windows x64 |

El loader de `src/index.ts` primero busca el binario local de desarrollo (`hivedb-napi.node`) y, si no existe, carga el paquete de plataforma correspondiente.

El consumidor final solo hace:

```bash
bun add @johpaz/hive-db     # o npm install / pnpm add
```

y recibe el binario correcto para su SO sin necesitar Rust.

---

## 3. Cómo publicar una versión

### Requisitos (una sola vez)

1. **Repo git en GitHub.** Este directorio aún no es un repo: `git init`, primer commit y push a `github.com/johpaz/hive-db` (o ajusta la URL en los `package.json` y `Cargo.toml`).
2. **Cuenta npm con el scope `@johpaz`.** Los scopes de usuario ya funcionan; publica con `--access public` (el workflow ya lo hace).
3. **Token npm** de tipo *Automation* → guardarlo como secret `NPM_TOKEN` en el repo de GitHub (Settings → Secrets → Actions).

### Flujo de release

```bash
# 1. Sube la versión en packages/hive-db/package.json y en los 5 npm/*/package.json
#    (y las versiones de optionalDependencies del principal — deben coincidir).
# 2. Etiqueta y push:
git tag v0.1.0
git push origin v0.1.0
```

El workflow `.github/workflows/release.yml`:

1. Compila `hivedb-napi` en 5 runners (Ubuntu x64/arm64, macOS 13/latest, Windows) y renombra cada `cdylib` a `hivedb-napi.node`.
2. Coloca cada binario en su subpaquete `npm/<plataforma>/` y lo publica.
3. Compila el TypeScript (`dist/`) y publica el paquete principal.

También puedes lanzarlo a mano desde la pestaña Actions (`workflow_dispatch`).

### Publicación manual (sin CI)

Solo publicarías el binario de tu plataforma — útil para probar el flujo, no para distribuir a todos los SO:

```bash
cd packages/hive-db
bun run build:napi
cp hivedb-napi.node npm/linux-x64-gnu/
npm publish ./npm/linux-x64-gnu --access public
bun run build && npm publish --access public
```

---

## 4. Alternativas a npm público

- **GitHub Packages** (registro npm privado): añade `"publishConfig": { "registry": "https://npm.pkg.github.com" }` y usa `GITHUB_TOKEN` en el workflow. Los consumidores necesitan un `.npmrc` con auth. Útil si Hive es de código cerrado.
- **Registro privado propio** (Verdaccio) para uso interno del ecosistema Hive.
- **Vendoring**: copiar `packages/hive-db` + el `.node` como submódulo git. Solo razonable mientras todo corra en máquinas Linux x64 idénticas.

---

## 5. Plataformas no cubiertas (por ahora)

- **Alpine/musl** (`linux-x64-musl`): añadir un target con `rust` + `musl-tools` en la matriz si despliegas en contenedores Alpine. Con imágenes Debian/Ubuntu no hace falta.
- **Windows arm64**: añadir `aarch64-pc-windows-msvc` a la matriz cuando haya demanda.

Para añadir una plataforma: (1) nueva entrada en la matriz de `release.yml`, (2) nuevo subpaquete en `npm/`, (3) nueva `optionalDependency` en el paquete principal, (4) caso en `platformTriple()` de `src/index.ts` si el triple no sigue el patrón actual.
