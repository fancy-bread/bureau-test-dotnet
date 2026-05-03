# Implementation Plan: Kafka Observability Dashboard — Core Scaffold

**Branch**: `001-kafka-observability-dashboard` | **Date**: 2026-05-02 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/001-kafka-observability-dashboard/spec.md`

## Summary

Scaffold a browser-accessible ASP.NET Core web application that continuously consumes events from the `bureau.runs` Kafka topic and displays them in a live feed with real-time server-to-client push via Server-Sent Events (SSE). Events are held in an in-memory circular buffer (last 100). An operator can view the feed, inspect individual event payloads, and monitor connection status — all without a page refresh.

## Technical Context

**Language/Version**: C# / .NET 10
**Primary Dependencies**: Confluent.Kafka (Kafka consumer), Confluent.Kafka.DependencyInjection (DI integration), CloudNative.CloudEvents + CloudNative.CloudEvents.Kafka (CloudEvents deserialization), NSubstitute (test mocking)
**Storage**: In-memory circular buffer (`ConcurrentQueue<BureauEvent>`, capped at 100 events)
**Testing**: dotnet test (xUnit + NSubstitute)
**Target Platform**: Linux server (Docker: `mcr.microsoft.com/dotnet/sdk:10`)
**Project Type**: web-service (ASP.NET Core Minimal API + SSE)
**Performance Goals**: Events delivered to browser within 2 seconds of Kafka publish; 100-event history without UI degradation
**Constraints**: In-memory only (no persistent storage), no Kafka authentication for initial scaffold
**Scale/Scope**: Single operator, single topic (`bureau.runs`), single consumer group

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Specs as Test Cases | ✅ PASS | Spec is independently runnable via `bureau run`; acceptance criteria are command-verifiable |
| II. Test-First | ✅ PASS | Plan mandates test files written and failing before any implementation file |
| III. C#/.NET | ✅ PASS | All implementation is C#; no mixed-language scope |
| IV. Minimal Scope | ✅ PASS | SSE chosen over SignalR (simpler, no extra dependency); single web project (no separate Core lib); no auth; in-memory only |
| V. Verifiable Outputs | ✅ PASS | `dotnet test` exits 0 is the primary acceptance gate |

**Quality Gates** (must all pass before PR):
1. `dotnet test` exits 0
2. No CRITICAL Critic findings
3. All acceptance scenarios from spec verified
4. No files modified outside declared scope

**Post-design re-check**: ✅ All principles satisfied — SSE keeps dependencies minimal, single project avoids premature abstraction, no auth deferred per spec.

## Project Structure

### Documentation (this feature)

```text
specs/001-kafka-observability-dashboard/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── sse-events.md
│   ├── rest-api.md
│   └── configuration.md
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
src/
├── BureauObservability.slnx
├── BureauObservability.Web/
│   ├── BureauObservability.Web.csproj
│   ├── Program.cs
│   ├── Models/
│   │   ├── BureauEvent.cs
│   │   └── KafkaConnectionState.cs
│   ├── Services/
│   │   ├── IEventStore.cs
│   │   ├── EventStore.cs
│   │   └── KafkaConsumerService.cs
│   ├── Endpoints/
│   │   └── EventsEndpoints.cs
│   └── wwwroot/
│       └── index.html
└── BureauObservability.Tests/
    ├── BureauObservability.Tests.csproj
    ├── Services/
    │   ├── EventStoreTests.cs
    │   └── KafkaConsumerServiceTests.cs
    └── Endpoints/
        └── EventsEndpointsTests.cs
```

**Structure Decision**: Single solution under `src/`, two projects. `BureauObservability.Web` is the runnable ASP.NET Core app — all Kafka, SSE, and serving logic lives here. `BureauObservability.Tests` is the xUnit test project — it references Web and exercises `EventStore`, `KafkaConsumerService`, and the events endpoint via `WebApplicationFactory`. No separate Core library — minimal scope.
