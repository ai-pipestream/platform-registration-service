# Inline module registration migration checklist

Demand-pull modules no longer expose inbound `PipeStepProcessorService` for registration.
Catalog metadata (especially `json_config_schema`) is pushed inline on `RegisterRequest.module`
at startup. Platform-registration stores it in DB + Apicurio; UIs read it via
`GET /modules/{name}/schema` (no live module callback).

**Reference implementation:** `echo-pr0` worktree (inline config only; no `pipeline-module` proto).

**Hold:** Do not convert the modules below until echo is validated end-to-end (see gate below).

---

## Gate: validate echo first

Complete these before touching chunker/embedder/etc.

| Step | What to verify |
|------|----------------|
| 1. **Stack versions** | `pipestream-protos` (`ModuleRegistration` on `RegisterRequest`) published; `pipestream-service-registration` + `platform-registration-service` rebuilt/restarted |
| 2. **Registration stream** | Echo logs: `CONSUL_HEALTHY` → `METADATA_RETRIEVED` → `SCHEMA_VALIDATED` → `DATABASE_SAVED` → `APICURIO_REGISTERED` → `COMPLETED` (no `FAILED`) |
| 3. **Schema REST** | `GET /modules/echo/schema` on platform-registration returns JSON with `schema_json` (synthesized key-value schema is fine for echo) |
| 4. **Work loop** | Echo `ModuleWorkerLoop`: `Hello accepted` → `NoWorkAvailable` or `WorkUnit` → `Ack` → `AckConfirmed` (engine `enabled-services=echo`) |
| 5. **Intake path** | Optional: one doc through intake → `pipestream.module.echo` → echo processes → engine commits (full demand-pull loop) |

---

## Common conversion pattern (every module)

Apply to each module once echo gate passes.

### 1. Registration client (startup push)

Add to `src/main/resources/application.properties`:

```properties
pipestream.registration.module.display-name=...
pipestream.registration.module.description=...
pipestream.registration.module.health-check-passed=true
pipestream.registration.module.health-check-message=...
# One of:
pipestream.registration.module.json-config-schema-path=classpath:...
# pipestream.registration.module.json-config-schema={...}   # small schemas only
# Optional:
pipestream.registration.module.expected-max-processing-seconds=...
pipestream.registration.module.dependencies=...
```

**Schema sourcing:** Move logic from `getServiceRegistration()` into startup — either:

- Export schema to a classpath JSON file at build time, or
- Add a small `@ApplicationScoped` helper invoked from a custom `ModuleRegistrationMetadataProvider` (future SPI), or
- Call existing `SchemaExtractorService` / `*StepOptions.getJsonV7Schema()` once at startup and set `json-config-schema` programmatically in a `@StartupEvent` observer that writes into config (heavier; prefer path or SPI).

### 2. Remove inbound registration/data-plane gRPC (when on demand-pull)

| Keep | Remove (when module uses `ModuleWorkerLoop`) |
|------|-----------------------------------------------|
| HTTP `/q/health`, admin REST, OpenAPI | `PipeStepProcessorService.processData` |
| Outbound `ModuleWorkService` client | `getServiceRegistration` on inbound gRPC |
| | `pipeline-module` proto dep (if nothing else needs it) |

**Push-era modules** may keep `processData` until they migrate to demand-pull; they still **must** add inline `RegisterRequest.module` or registration will fail.

### 3. Connectivity / Consul

- Advertised port = **HTTP** port (`quarkus.http.port`), not legacy gRPC+10000.
- Prefer unified gRPC-on-HTTP (drop `quarkus.grpc.server.use-separate-server=true` unless required).

### 4. Tests to update

- [ ] Remove or rewrite tests that call `getServiceRegistration()` on the module stub
- [ ] Add test that `ModuleRegistrationMetadataCollector`-equivalent output includes non-empty schema (where applicable)
- [ ] Registration integration test: stream reaches `COMPLETED`
- [ ] `GET /modules/{name}/schema` returns same schema that was registered

### 5. Verify

- [ ] Platform-registration DB row + Apicurio artifact for module version
- [ ] JSONForms / graph editor loads schema from platform-registration REST (not module gRPC)

---

## Per-module checklist

### module-chunker (`modules/module-chunker`)

**Today:** `ChunkerGrpcImpl` — `getServiceRegistration` uses `SchemaExtractorService.extractChunkerConfigSchemaResolvedForJsonForms()`; optional `test_request` → `processData` health smoke test.

- [ ] Export chunker JSON schema at startup (from `SchemaExtractorService` or `ChunkerOptions.getJsonV7Schema()`)
- [ ] Set `pipestream.registration.module.*` in `application.properties`
- [ ] Map `health_check_passed=false` when schema extraction fails (today fails registration via gRPC response)
- [ ] Decide demand-pull timeline: keep `processData` for push bench vs add `ModuleWorkerLoop`
- [ ] Update `ChunkerServiceTestBase` / `ChunkerConfigSchemaCompatibilityTest` (registration assertions)
- [ ] Keep or trim `ChunkerServiceEndpoint` admin REST (schema preview for devs)

**Schema note:** Must remain JSONForms-ready (refs resolved) — same quality bar as today.

---

### module-embedder (`modules/module-embedder`)

**Today:** `EmbedderGrpcImpl.getServiceRegistration` — `EmbedderStepOptions.getJsonV7Schema()`, DJL model readiness in health message.

- [ ] Inline schema from `EmbedderStepOptions.getJsonV7Schema()`
- [ ] Startup readiness: set `health-check-passed=false` until models loaded (replaces gRPC health probe)
- [ ] `expected-max-processing-seconds` if embedder registers watchdog hint
- [ ] `EmbedderStreamingGrpcImpl` — separate from registration; review when cutting push path
- [ ] Update embedder registration tests

**Schema note:** Health check is real (model availability), not optional.

---

### module-parser (`modules/module-parser`)

**Today:** `ParserServiceImpl.getServiceRegistration` + `SchemaExtractorService`; `ServiceRegistrationIT` calls gRPC directly.

- [ ] Inline schema via `SchemaExtractorService` (ParserConfig)
- [ ] Parser often heavy / long-running — set `expected-max-processing-seconds`
- [ ] Rewrite `ServiceRegistrationIT` to assert inline registration + REST schema fetch
- [ ] Keep parser admin REST (`ParserServiceEndpoint`) for dev; not used by platform callback anymore

---

### module-semantic-graph (`modules/module-semantic-graph`)

**Today:** `SemanticGraphGrpcImpl.getServiceRegistration`; schema tied to `SemanticGraphStepOptions`.

- [ ] Inline schema (from step options or schema extractor)
- [ ] Update registration + config tests
- [ ] Demand-pull: add `ModuleWorkerLoop` when this node joins demand-pull graphs (not registration-specific)

---

### module-opensearch-sink (`modules/module-opensearch-sink`)

**Today:** `OpenSearchIngestionServiceImpl` implements sink proto + `PipeStepProcessorService`; schema via `SchemaExtractorService`.

- [ ] Inline OpenSearch sink config schema
- [ ] Sink capability metadata in `pipestream.registration.capabilities` / module tags if needed
- [ ] Update tests using `MutinyPipeStepProcessorServiceStub`
- [ ] `OpenSearchSinkConfigEndpoint` — keep for dev; document REST schema path for UI

---

### module-testing-sidecar (`modules/module-testing-sidecar`)

**Today:** Dual role — (1) `TestProcessorServiceImpl.getServiceRegistration` for itself, (2) **calls other modules'** `getServiceRegistration` via gRPC (`ModuleTestingSidecarServiceImpl`) for discovery/probe tests.

- [ ] Inline metadata for sidecar's own `TestProcessor` registration
- [ ] **Replace cross-module `getServiceRegistration` probes** with `GET /modules/{name}/schema` on platform-registration (or gRPC `GetModuleSchema` on platform-registration service)
- [ ] Update stress tests / `GrpcHealthCheckTestBase` that reference `PipeStepProcessorService`
- [ ] Sidecar may keep `processData` longer as a **test harness** for push-path E2E until those tests are retired

**Special:** This module is the migration canary for **consumers** of module schema, not just producers.

---

## Modules explicitly out of scope (for now)

| Module | Notes |
|--------|--------|
| **echo** | Done in `echo-pr0` — validate first |
| **module-proxy** | Separate; assess when proxy role is clarified |
| **Legacy push-only benches** | Quarantine / process-compose benches using `processData` — don't block module registration migration |

---

## Platform / dependency order (each rollout)

1. Publish `pipestream-protos` (`RegisterRequest.module`)
2. Publish `pipestream-service-registration` (metadata collector)
3. Deploy `platform-registration-service` (inline handler + REST schema)
4. Convert + deploy one module at a time
5. Confirm graph editor / JSONForms pointed at platform-registration schema URL

---

## Quick reference: config keys

| Key | Purpose |
|-----|---------|
| `pipestream.registration.module.display-name` | UI catalog |
| `pipestream.registration.module.description` | UI catalog |
| `pipestream.registration.module.json-config-schema-path` | JSONForms schema (preferred) |
| `pipestream.registration.module.json-config-schema` | Inline schema (small) |
| `pipestream.registration.module.health-check-passed` | Gate registration (default `true`) |
| `pipestream.registration.module.health-check-message` | Shown on failure |
| `pipestream.registration.module.expected-max-processing-seconds` | Engine watchdog hint (doc 15) |

REST: `GET http://<platform-registration>/modules/{name}/schema?version=`

gRPC (unchanged): `PlatformRegistrationService.GetModuleSchema`
