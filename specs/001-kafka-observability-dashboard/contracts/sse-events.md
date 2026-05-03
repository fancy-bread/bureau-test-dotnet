# Contract: Server-Sent Events Feed

**Interface type**: SSE stream (unidirectional, server → browser)
**Endpoint**: `GET /api/events/stream`
**Content-Type**: `text/event-stream`

## Overview

The browser connects once and holds the connection open. The server pushes events as they arrive from Kafka. The connection stays open until the browser disconnects or the server shuts down.

## Event Types

### `bureau-event`

Emitted for each new CloudEvent consumed from the `bureau.runs` topic.

```
event: bureau-event
data: {"id":"0-42","cloudEventId":"550e8400-e29b-41d4-a716-446655440000","source":"urn:bureau:run:abc123","type":"com.fancybread.bureau.run.started","time":"2026-05-02T10:00:00Z","dataContentType":"application/json","data":"{\"id\":\"abc123\",\"spec\":\"001-...\",\"repo\":\"./\"}","isParseError":false,"partition":0,"offset":42}
```

**Payload fields**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | `{partition}-{offset}` (store key) |
| `cloudEventId` | string | CloudEvents `id` attribute (UUID from bureau) |
| `source` | string | CloudEvents `source` (e.g., `urn:bureau:run:abc123`) |
| `type` | string | CloudEvents `type` (e.g., `com.fancybread.bureau.run.started`) |
| `time` | ISO 8601 string | CloudEvents `time` attribute (UTC) |
| `dataContentType` | string | CloudEvents `datacontenttype` (always `application/json`) |
| `data` | string | CloudEvents `data` serialised as JSON string |
| `isParseError` | boolean | `true` if message could not be parsed as a valid CloudEvent |
| `partition` | integer | Kafka partition |
| `offset` | integer | Kafka offset |

---

### `connection-state`

Emitted when the Kafka connection state changes.

```
event: connection-state
data: {"status":"Connected","brokerEndpoint":"localhost:9092","consumerGroup":"bureau-dashboard","lastUpdated":"2026-05-02T10:00:00Z","errorMessage":null}
```

**Payload fields**:

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `Unknown` / `Connected` / `Disconnected` / `Error` |
| `brokerEndpoint` | string | Configured broker address |
| `consumerGroup` | string | Consumer group ID |
| `lastUpdated` | ISO 8601 string | When status last changed |
| `errorMessage` | string or null | Present only when `status == "Error"` |
