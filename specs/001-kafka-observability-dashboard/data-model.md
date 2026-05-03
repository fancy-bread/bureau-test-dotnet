# Data Model: Kafka Observability Dashboard — Core Scaffold

**Phase 1 output for**: `specs/001-kafka-observability-dashboard/plan.md`
**Date**: 2026-05-02

---

## Entities

### BureauEvent

Represents a single CloudEvents 1.0 message received from the `bureau.runs` Kafka topic, produced by the bureau CLI.

| Field | Type | Description |
|-------|------|-------------|
| `Id` | `string` | Composite key: `{Partition}-{Offset}` (for store identity) |
| `CloudEventId` | `string` | CloudEvents `id` attribute (UUID from bureau) |
| `Source` | `string` | CloudEvents `source` (e.g., `urn:bureau:run:abc123`) |
| `Type` | `string` | CloudEvents `type` (e.g., `com.fancybread.bureau.run.started`) |
| `Time` | `DateTimeOffset` | CloudEvents `time` attribute (UTC); falls back to Kafka message timestamp |
| `DataContentType` | `string` | CloudEvents `datacontenttype` (always `application/json` from bureau) |
| `Data` | `string` | CloudEvents `data` field serialised as JSON string |
| `IsParseError` | `bool` | `true` when the message could not be parsed as a valid CloudEvent |
| `Partition` | `int` | Kafka partition number |
| `Offset` | `long` | Kafka offset within partition |

**Known event types** (from bureau CLI):

| Type | Data Fields |
|------|------------|
| `com.fancybread.bureau.run.started` | `id`, `spec`, `repo` |
| `com.fancybread.bureau.run.completed` | `id`, `pr`, `duration` |
| `com.fancybread.bureau.run.failed` | `id`, `phase`, `error` |
| `com.fancybread.bureau.run.escalated` | `id`, `phase`, `reason` |
| `com.fancybread.bureau.phase.started` | `phase`, `stub` (optional) |
| `com.fancybread.bureau.phase.completed` | `phase`, `duration`, `stub` (optional) |
| `com.fancybread.bureau.ralph.started` | `phase`, `round` |
| `com.fancybread.bureau.ralph.attempt` | `phase`, `round`, `attempt`, `result`, `exit_code` |
| `com.fancybread.bureau.ralph.completed` | `rounds`, `verdict` |
| `com.fancybread.bureau.builder.tool` | `tool`, `exit_code` |
| `com.fancybread.bureau.pr.created` | *(TBD from bureau source)* |

**Validation rules**:
- `Id` is never null or empty
- `Time` always has a value
- `Data` may be an empty JSON object (`{}`) but never null
- When `IsParseError` is `true`, `Type` is set to `"parse_error"` and `Data` holds the raw unparseable string

**State transitions**: None — `BureauEvent` is immutable once created.

---

### KafkaConnectionState

Represents the current connection status of the Kafka consumer.

| Field | Type | Description |
|-------|------|-------------|
| `Status` | `ConnectionStatus` | Current connection state |
| `BrokerEndpoint` | `string` | Configured broker address (e.g., `localhost:9092`) |
| `ConsumerGroup` | `string` | Consumer group ID |
| `LastUpdated` | `DateTimeOffset` | When status last changed |
| `ErrorMessage` | `string?` | Set when `Status == Error`; null otherwise |

**State transitions**:

```
Unknown → Connected  (on first successful Consume() call)
Connected → Disconnected  (on broker unreachable / clean shutdown)
Connected → Error  (on unrecoverable exception)
Disconnected → Connected  (on reconnect)
Error → Connected  (on reconnect after error)
```

---

### ConnectionStatus (enum)

| Value | Meaning |
|-------|---------|
| `Unknown` | Initial state before first connection attempt |
| `Connected` | Consumer is polling successfully |
| `Disconnected` | Consumer stopped or broker unreachable |
| `Error` | Unrecoverable error; consumer stopped |

---

## IEventStore Interface

The in-memory store exposed as an interface to enable unit testing.

```
IEventStore
  Add(BureauEvent event) → void
  GetRecent(int count) → IReadOnlyList<BureauEvent>   // newest first
  ConnectionState → KafkaConnectionState              // current status
  UpdateConnectionState(KafkaConnectionState state) → void
```

**Capacity**: Capped at 100 events. When full, the oldest event is evicted on each `Add()`.
