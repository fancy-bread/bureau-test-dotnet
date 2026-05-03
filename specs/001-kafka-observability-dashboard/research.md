# Research: Kafka Observability Dashboard — Core Scaffold

**Phase 0 output for**: `specs/001-kafka-observability-dashboard/plan.md`
**Date**: 2026-05-02

---

## Decision 1: Kafka Client Library

**Decision**: `Confluent.Kafka` (NuGet, v2.x) + `Confluent.Kafka.DependencyInjection`
**Rationale**: De facto standard .NET Kafka client; binding to librdkafka with multi-platform support (linux-x64, osx-arm64, win-x64). `IConsumer<TKey, TValue>` interface enables mocking in unit tests. DependencyInjection extension integrates with `IConfiguration` and `ILogger` natively.
**Alternatives considered**: MassTransit (too heavy for a scaffold — it abstracts broker topology); Confluent REST Proxy (HTTP polling adds latency, defeats real-time goal).

---

## Decision 2: Background Consumer Pattern

**Decision**: `BackgroundService` (abstract base from `Microsoft.Extensions.Hosting`) with `AddHostedService<KafkaConsumerService>()`
**Rationale**: Standard ASP.NET Core pattern for long-running work. Receives application lifetime via `CancellationToken`; shuts down cleanly. `EnableAutoCommit = false` with manual commit after store write ensures no event loss on crash.
**Alternatives considered**: `IHostedService` directly (more boilerplate, no benefit over `BackgroundService`); Confluent's `IConsumerBuilder` as a hosted service (same pattern, different wiring).

---

## Decision 3: Real-Time Browser Push

**Decision**: Server-Sent Events (SSE) via ASP.NET Core .NET 10 first-class support (`IAsyncEnumerable` response + `text/event-stream`)
**Rationale**: .NET 10 adds native SSE support that handles protocol details, chunked flushing, and cancellation automatically. SSE is unidirectional (server → browser), which is exactly what an observability feed requires. No third-party dependency. Browser's native `EventSource` API requires no client library.
**Alternatives considered**: SignalR — adds `Microsoft.AspNetCore.SignalR` dependency and bidirectional complexity not needed here. WebSockets — lower-level, more setup, same capability for this use case.

---

## Decision 4: Test Mocking Library

**Decision**: NSubstitute (NuGet `NSubstitute`)
**Rationale**: New project; NSubstitute's cleaner, refactor-friendly syntax is preferred for greenfield work. Works seamlessly with xUnit. `IConsumer<string, string>` can be substituted to unit-test `KafkaConsumerService` without a broker.
**Alternatives considered**: Moq — most widespread but verbosity is unnecessary for a new project. FakeItEasy — smaller ecosystem.

---

## Decision 5: Kafka Integration Testing

**Decision**: Out of scope for this scaffold; deferred to a later spec
**Rationale**: Constitution Principle IV (Minimal Scope) — Testcontainers.Kafka requires Docker and adds significant test setup complexity. Unit tests with NSubstitute cover `EventStore` and `KafkaConsumerService` logic. Integration tests against a real broker are a natural follow-on spec.
**Alternatives considered**: Testcontainers.Kafka (`Testcontainers.Kafka` NuGet, v4.x) — correct choice for integration testing, tracked as a future spec.

---

## Decision 6: CloudEvents Format

**Decision**: Parse Kafka messages as CloudEvents 1.0 using `CloudNative.CloudEvents` + `CloudNative.CloudEvents.Kafka` (NuGet)
**Rationale**: Bureau CLI produces CloudEvents 1.0 structured in the Kafka message value (`datacontenttype: application/json`). The `CloudNative.CloudEvents.Kafka` extension provides `ToCloudEvent()` on `ConsumeResult<K,V>`, handling both structured and binary content modes. The `type` attribute (`com.fancybread.bureau.*`) is the authoritative event type identifier. Known event types and their `data` shapes are documented in `data-model.md`.
**Alternatives considered**: Manual JSON parsing of the CloudEvents envelope — unnecessary when the official SDK handles it, and would miss edge cases in content mode detection.

---

## Decision 7: Project Structure

**Decision**: Single solution (`BureauObservability.sln`), two projects: `src/BureauObservability.Web` + `tests/BureauObservability.Tests`
**Rationale**: No separate Core library — the spec is a scaffold; extracting a Core lib before any consuming project exists is premature abstraction (Principle IV). `WebApplicationFactory<Program>` in the test project enables endpoint testing without a separate process.
**Alternatives considered**: Three-project layout (Web + Core + Tests) — rejected, no second consumer of Core exists yet.
