# Quickstart: Kafka Observability Dashboard — Core Scaffold

**Acceptance validation guide** — run these commands to verify the scaffold satisfies spec `001`.

---

## Prerequisites

- .NET 10 SDK (`dotnet --version` → `10.x.x`)
- Docker (for running a local Kafka broker during manual validation)

---

## 1. Restore & Build

```bash
dotnet restore
dotnet build --no-incremental
```

Both commands must exit 0 with no errors.

---

## 2. Run Tests (Primary Acceptance Gate)

```bash
dotnet test
```

Must exit 0. All tests must pass. This is the constitution Quality Gate 1.

---

## 3. Manual Validation (Acceptance Scenarios)

Start a local Kafka broker:

```bash
docker run -d --name kafka-test \
  -p 9092:9092 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  apache/kafka:latest
```

Run the dashboard:

```bash
dotnet run --project src/BureauObservability.Web/BureauObservability.Web.csproj
```

Open `http://localhost:5000` in a browser. Verify:

- [ ] Dashboard loads and shows connection status (SC-001 baseline)
- [ ] Status updates to `Connected` within 5 seconds (SC-004)
- [ ] Feed is empty on first load with no events published

Publish a test event to `bureau.runs`:

```bash
docker exec kafka-test /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic bureau.runs <<< '{"eventType":"run.started","runId":"test-001"}'
```

Verify:

- [ ] Event appears in feed within 2 seconds (SC-001)
- [ ] Event shows timestamp and event type `run.started` (SC-003)
- [ ] Clicking the event shows full payload (US2)

Publish a malformed event:

```bash
docker exec kafka-test /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic bureau.runs <<< 'not-json'
```

Verify:

- [ ] Dashboard shows parse error indicator, does not crash (SC-005)
- [ ] New events continue to appear after the malformed event

Stop the Kafka broker:

```bash
docker stop kafka-test
```

Verify:

- [ ] Connection status updates to `Disconnected` within 5 seconds (SC-004)
