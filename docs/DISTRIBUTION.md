# HiveDB — Guía de distribución

> Cómo consumir `@johpaz/hive-db` y cómo publicarlo en npm con binarios para todos los sistemas operativos usando `@napi-rs/cli`.

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
bun run build:native   # napi build --platform --release + renombra index.js -> native.cjs
```

---

## 2. Distribución multiplataforma (npm)

La estrategia usa `@napi-rs/cli` 3.x: un paquete principal en TypeScript/JavaScript más **un paquete de binarios por plataforma** que npm/bun instala automáticamente según el SO gracias a los campos `os`/`cpu`/`libc`:

| Paquete | Plataforma |
|---|---|
| `@johpaz/hive-db` | principal (TS/JS, sin binarios) |
| `@johpaz/hive-db-linux-x64-gnu` | Linux x64 (glibc) |
| `@johpaz/hive-db-linux-x64-musl` | Linux x64 (musl / Alpine) |
| `@johpaz/hive-db-linux-arm64-gnu` | Linux arm64 (glibc) |
| `@johpaz/hive-db-darwin-x64` | macOS Intel |
| `@johpaz/hive-db-darwin-arm64` | macOS Apple Silicon |
| `@johpaz/hive-db-win32-x64-msvc` | Windows x64 |

El loader `native.cjs` (generado por `napi build --platform`) detecta la plataforma en runtime —incluyendo la distinción glibc vs musl— y carga el binario correcto desde el subpaquete instalado o desde el archivo local de desarrollo.

El consumidor final solo hace:

```bash
bun add @johpaz/hive-db     # o npm install / pnpm add
```

y recibe el binario correcto para su SO sin necesitar Rust.

---

## 3. Cómo publicar una versión

### Requisitos (una sola vez)

1. **Repo git en GitHub.** `git init`, primer commit y push a `github.com/johpaz/hive-db`.
2. **Cuenta npm con el scope `@johpaz`.** Publica con `--access public` (el workflow ya lo hace).
3. **Token npm** de tipo *Automation* → guardarlo como secret `NPM_TOKEN` en el repo de GitHub (Settings → Secrets → Actions).

### Flujo de release (local)

```bash
# 1. Subir la versión desde la raíz del paquete:
cd packages/hive-db
npm version patch   # o minor / major

# Esto dispara los lifecycle scripts:
#   preversion  → napi build --platform && git add .
#   version     → napi version  (sincroniza subpaquetes npm/)
#
# 2. Push del tag:
git push --follow-tags
```

Al detectar el tag `v*`, el workflow `.github/workflows/ci.yml` job `publish`:

1. `napi create-npm-dirs` — regenera los subpaquetes `npm/<triple>/`.
2. Descarga los binarios compilados en los runners de la matriz.
3. `napi artifacts` — coloca cada `.node` en su subpaquete.
4. Publica cada subpaquete: `npm publish ./npm/<triple> --access public`.
5. Compila el TypeScript (`tsc`).
6. Publica el paquete principal: `npm publish --access public`.

También puedes lanzarlo a mano desde la pestaña Actions (`workflow_dispatch`).

### Publicación manual (sin CI)

Solo publicarías el binario de tu plataforma — útil para probar el flujo, no para distribuir a todos los SO:

```bash
cd packages/hive-db
bun run build:native
cp hivedb-napi.linux-x64-gnu.node npm/linux-x64-gnu/
npm publish ./npm/linux-x64-gnu --access public
bun run tsc -p tsconfig.json
npm publish --access public
```

---

## 4. Alternativas a npm público

- **GitHub Packages** (registro npm privado): añade `"publishConfig": { "registry": "https://npm.pkg.github.com" }` y usa `GITHUB_TOKEN` en el workflow. Los consumidores necesitan un `.npmrc` con auth. Útil si Hive es de código cerrado.
- **Registro privado propio** (Verdaccio) para uso interno del ecosistema Hive.
- **Vendoring**: copiar `packages/hive-db` + el `.node` como submódulo git. Solo razonable mientras todo corra en máquinas Linux x64 idénticas.

---

## 5. Plataformas no cubiertas (por ahora)

- **Windows arm64** (`win32-arm64-msvc`): añadir `aarch64-pc-windows-msvc` a `napi.targets` y a la matriz de CI.
- **Linux arm64 musl** (`linux-arm64-musl`): añadir `aarch64-unknown-linux-musl` a `napi.targets` y a la matriz con `-x` (zigbuild). El `target-feature=-crt-static` ya está en `.cargo/config.toml`.
- **Android**: `aarch64-linux-android` y `armv7-linux-androideabi` son soportados por `@napi-rs/cli`.

Para añadir una plataforma: (1) nueva entrada en `napi.targets` del `package.json`, (2) nueva entrada en la matriz de `ci.yml`, (3) ejecutar `napi create-npm-dirs` para regenerar los subpaquetes.