# Contract: REST API

**Base path**: `/api`

---

## GET /api/events

Returns the most recent events from the in-memory store (up to 100). Used on page load to populate the feed before the SSE stream connects.

**Response**: `200 OK`, `application/json`

```json
{
  "events": [
    {
      "id": "0-42",
      "cloudEventId": "550e8400-e29b-41d4-a716-446655440000",
      "source": "urn:bureau:run:abc123",
      "type": "com.fancybread.bureau.run.started",
      "time": "2026-05-02T10:00:00Z",
      "dataContentType": "application/json",
      "data": "{\"id\":\"abc123\",\"spec\":\"001-...\",\"repo\":\"./\"}",
      "isParseError": false,
      "partition": 0,
      "offset": 42
    }
  ],
  "count": 1
}
```

Events are ordered newest first. Returns an empty `events` array (not an error) when no events have been received.

---

## GET /api/status

Returns the current Kafka connection state.

**Response**: `200 OK`, `application/json`

```json
{
  "status": "Connected",
  "brokerEndpoint": "localhost:9092",
  "consumerGroup": "bureau-dashboard",
  "lastUpdated": "2026-05-02T10:00:00Z",
  "errorMessage": null
}
```

`status` is one of: `Unknown`, `Connected`, `Disconnected`, `Error`.
`errorMessage` is present only when `status` is `"Error"`.
