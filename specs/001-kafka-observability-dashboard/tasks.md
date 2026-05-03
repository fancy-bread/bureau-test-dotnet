---
description: "Task list for Kafka Observability Dashboard — Core Scaffold"
---

# Tasks: Kafka Observability Dashboard — Core Scaffold

**Input**: Design documents from `specs/001-kafka-observability-dashboard/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/ ✅

**Tests**: Mandatory — constitution Principle II (Test-First) requires tests written and failing before each implementation file.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing. Each phase ends with a Reviewer review and a commit before the next phase begins.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2)
- Include exact file paths in all task descriptions

## Path Conventions

- Solution: `src/BureauObservability.sln`
- Web project: `src/BureauObservability.Web/`
- Test project: `src/BureauObservability.Tests/`

---

## Phase 1: Setup

**Purpose**: Initialize the .NET solution and project structure

- [ ] T001 Create solution: `mkdir src && dotnet new sln -n BureauObservability -o src` → `src/BureauObservability.sln`
- [ ] T002 Create web project: `dotnet new web -n BureauObservability.Web -o src/BureauObservability.Web` and add to solution: `dotnet sln src/BureauObservability.sln add src/BureauObservability.Web/BureauObservability.Web.csproj`
- [ ] T003 Create test project: `dotnet new xunit -n BureauObservability.Tests -o src/BureauObservability.Tests` and add to solution: `dotnet sln src/BureauObservability.sln add src/BureauObservability.Tests/BureauObservability.Tests.csproj`
- [ ] T004 Add project reference: test project references web project in `src/BureauObservability.Tests/BureauObservability.Tests.csproj`
- [ ] T005 [P] Add NuGet packages to web project: `Confluent.Kafka`, `Confluent.Kafka.DependencyInjection`, `CloudNative.CloudEvents`, `CloudNative.CloudEvents.Kafka`
- [ ] T006 [P] Add NuGet packages to test project: `NSubstitute`, `Microsoft.AspNetCore.Mvc.Testing`
- [ ] T007 Verify scaffold: `dotnet build src/BureauObservability.sln` exits 0
- [ ] T008 Phase 1 checkpoint: Reviewer reviews phase, verify `dotnet build src/BureauObservability.sln` exits 0, commit — `"feat: phase 1 — solution and project scaffold"`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared models and interfaces that both user stories depend on. No user story work begins until this phase is complete.

**⚠️ CRITICAL**: Complete before Phase 3 or Phase 4.

- [ ] T009 [P] Create `ConnectionStatus.cs` enum in `src/BureauObservability.Web/Models/ConnectionStatus.cs` — values: `Unknown`, `Connected`, `Disconnected`, `Error`
- [ ] T010 [P] Create `KafkaConnectionState.cs` in `src/BureauObservability.Web/Models/KafkaConnectionState.cs` — fields: `Status`, `BrokerEndpoint`, `ConsumerGroup`, `LastUpdated`, `ErrorMessage?` (per data-model.md)
- [ ] T011 [P] Create `BureauEvent.cs` in `src/BureauObservability.Web/Models/BureauEvent.cs` — CloudEvents fields: `Id`, `CloudEventId`, `Source`, `Type`, `Time`, `DataContentType`, `Data`, `IsParseError`, `Partition`, `Offset` (per data-model.md)
- [ ] T012 Create `IEventStore.cs` interface in `src/BureauObservability.Web/Services/IEventStore.cs` — methods: `Add(BureauEvent)`, `GetRecent(int)`, `ConnectionState` property, `UpdateConnectionState(KafkaConnectionState)` (per data-model.md)
- [ ] T013 Create `appsettings.json` at `src/BureauObservability.Web/appsettings.json` — `Kafka:BootstrapServers`, `Kafka:Topic`, `Kafka:ConsumerGroup` (per contracts/configuration.md)
- [ ] T014 Expose `Program` for testing: add `public partial class Program { }` at end of `src/BureauObservability.Web/Program.cs`
- [ ] T015 Phase 2 checkpoint: Reviewer reviews phase, verify `dotnet build src/BureauObservability.sln` exits 0, commit — `"feat: phase 2 — models, interfaces, and configuration"`

---

## Phase 3: User Story 1 — Live Event Feed (Priority: P1) 🎯 MVP

**Goal**: Operator sees bureau CloudEvents arriving from `bureau.runs` in real-time in the browser.

**Independent Test**: Start the app, publish a CloudEvent to `bureau.runs`, verify it appears in the browser feed within 2 seconds and connection status shows `Connected`.

### Tests for User Story 1 ⚠️ WRITE FIRST — MUST FAIL BEFORE IMPLEMENTATION

- [ ] T016 [P] [US1] Write failing tests for `EventStore`: `Add` stores event, `GetRecent` returns newest-first, capacity cap evicts oldest at 101 events — `src/BureauObservability.Tests/Services/EventStoreTests.cs`
- [ ] T017 [P] [US1] Write failing tests for `KafkaConsumerService`: successfully parsed CloudEvent calls `IEventStore.Add()`, malformed message sets `IsParseError=true` and still calls `Add()`, connection state transitions are recorded — `src/BureauObservability.Tests/Services/KafkaConsumerServiceTests.cs` (use NSubstitute for `IConsumer<string,string>` and `IEventStore`)
- [ ] T018 [P] [US1] Write failing contract test: `GET /api/events/stream` returns `Content-Type: text/event-stream` and `200 OK` — `src/BureauObservability.Tests/Endpoints/SseEndpointTests.cs` (use `WebApplicationFactory<Program>`)

### Implementation for User Story 1

- [ ] T019 [US1] Implement `EventStore.cs` in `src/BureauObservability.Web/Services/EventStore.cs` — thread-safe `ConcurrentQueue<BureauEvent>` capped at 100, implements `IEventStore`
- [ ] T020 [US1] Implement `KafkaConsumerService.cs` in `src/BureauObservability.Web/Services/KafkaConsumerService.cs` — `BackgroundService` that polls `IConsumer<string,string>`, deserializes CloudEvents via `CloudNative.CloudEvents.Kafka`, calls `IEventStore.Add()`, updates connection state; `EnableAutoCommit=false`
- [ ] T021 [US1] Implement SSE endpoint in `src/BureauObservability.Web/Endpoints/EventsEndpoints.cs` — `GET /api/events/stream` streams `bureau-event` and `connection-state` SSE events (per contracts/sse-events.md); uses `IAsyncEnumerable` + `text/event-stream`
- [ ] T022 [US1] Implement `GET /api/status` endpoint in `src/BureauObservability.Web/Endpoints/EventsEndpoints.cs` — returns `KafkaConnectionState` as JSON (per contracts/rest-api.md)
- [ ] T023 [US1] Register services in `src/BureauObservability.Web/Program.cs` — `AddSingleton<IEventStore, EventStore>()`, `AddHostedService<KafkaConsumerService>()`, `MapGet` for `/api/events/stream` and `/api/status`, bind `Kafka` config section
- [ ] T024 [US1] Create `src/BureauObservability.Web/wwwroot/index.html` — connects to `/api/events/stream` via `EventSource`, renders incoming events as rows in a feed showing `type`, `time`, and `source`; shows connection status indicator
- [ ] T025 Phase 3 checkpoint: Reviewer reviews phase, verify `dotnet test src/BureauObservability.sln` exits 0, commit — `"feat: phase 3 — live event feed (US1)"`

---

## Phase 4: User Story 2 — Event Detail Inspection (Priority: P2)

**Goal**: Operator selects an event from the feed to view its full CloudEvents payload.

**Independent Test**: With at least one event in the feed, click it and verify `cloudEventId`, `source`, `type`, `time`, and full `data` JSON are displayed. Verify a new event arriving does not dismiss the detail view.

### Tests for User Story 2 ⚠️ WRITE FIRST — MUST FAIL BEFORE IMPLEMENTATION

- [ ] T026 [P] [US2] Write failing contract test: `GET /api/events` returns JSON array of up to 100 events newest-first, empty array when no events — `src/BureauObservability.Tests/Endpoints/EventsEndpointsTests.cs` (use `WebApplicationFactory<Program>`)

### Implementation for User Story 2

- [ ] T027 [US2] Implement `GET /api/events` REST endpoint in `src/BureauObservability.Web/Endpoints/EventsEndpoints.cs` — returns `{ events: [...], count: N }` from `IEventStore.GetRecent(100)` (per contracts/rest-api.md)
- [ ] T028 [US2] Update `src/BureauObservability.Web/wwwroot/index.html` — add detail panel; clicking a feed row displays full CloudEvents fields (`cloudEventId`, `source`, `type`, `time`, `dataContentType`, `data` formatted as JSON); new SSE events do not close the panel; load initial events from `GET /api/events` on page open
- [ ] T029 Phase 4 checkpoint: Reviewer reviews phase, verify `dotnet test src/BureauObservability.sln` exits 0, commit — `"feat: phase 4 — event detail inspection (US2)"`

---

## Phase 5: Polish & Cross-Cutting Concerns

- [ ] T030 [P] Run `dotnet format src/BureauObservability.sln --verify-no-changes` — fix any formatting issues
- [ ] T031 Run `dotnet test src/BureauObservability.sln` — all tests pass, exits 0 (constitution Quality Gate 1)
- [ ] T032 Validate acceptance scenarios from `specs/001-kafka-observability-dashboard/quickstart.md` against running dashboard
- [ ] T033 Phase 5 checkpoint: Reviewer reviews phase, verify `dotnet test` exits 0 and `dotnet format --verify-no-changes` passes, commit — `"chore: phase 5 — polish and acceptance validation"`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately; ends with T008 commit
- **Foundational (Phase 2)**: Depends on Phase 1 commit — BLOCKS both user stories; ends with T015 commit
- **User Story 1 (Phase 3)**: Depends on Phase 2 commit — tests written first, then implementation; ends with T025 commit
- **User Story 2 (Phase 4)**: Depends on Phase 2 commit — can start in parallel with US1 after T015; ends with T029 commit
- **Polish (Phase 5)**: Depends on T025 and T029 commits; ends with T033 commit

### Within Each User Story

- Test tasks MUST be written and confirmed failing before any implementation task in that story
- `IEventStore` and models before services
- Services before endpoints
- Endpoints before UI

### Parallel Opportunities

- T005 and T006 (NuGet packages) can run in parallel
- T009, T010, T011, T012 (models + interface) can run in parallel
- T016, T017, T018 (US1 test tasks) can run in parallel
- T026 (US2 test) can start after T015 commit, parallel to US1 implementation
- T030 (format check) can run parallel to T031 (test run)

---

## Implementation Strategy

### MVP (User Story 1 Only)

1. Complete Phase 1 → T008 commit
2. Complete Phase 2 → T015 commit
3. Write and confirm T016, T017, T018 tests are failing
4. Complete Phase 3 implementation → T025 commit
5. **STOP and VALIDATE**: `dotnet test` passes, event feed works in browser

### Full Delivery

1. MVP (above)
2. Write and confirm T026 test is failing
3. Complete Phase 4 → T029 commit
4. Phase 5 polish → T033 commit → open PR
