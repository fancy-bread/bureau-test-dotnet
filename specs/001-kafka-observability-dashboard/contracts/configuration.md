# Contract: Configuration Schema

Configuration is supplied via `appsettings.json` (and overridden by environment variables using standard .NET `__` separator notation).

## Schema

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "Topic": "bureau.runs",
    "ConsumerGroup": "bureau-dashboard"
  }
}
```

## Fields

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Kafka:BootstrapServers` | string | `localhost:9092` | Comma-separated list of broker addresses |
| `Kafka:Topic` | string | `bureau.runs` | Topic to consume from |
| `Kafka:ConsumerGroup` | string | `bureau-dashboard` | Consumer group ID |

## Environment Variable Override

Standard .NET configuration convention: replace `:` with `__`.

```
KAFKA__BOOTSTRAPSERVERS=broker:9092
KAFKA__TOPIC=bureau.runs
KAFKA__CONSUMERGROUP=bureau-dashboard
```
