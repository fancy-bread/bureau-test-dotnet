namespace BureauObservability.Web.Models;

public sealed class KafkaConnectionState
{
    public ConnectionStatus Status { get; init; }
    public string BrokerEndpoint { get; init; } = string.Empty;
    public string ConsumerGroup { get; init; } = string.Empty;
    public DateTimeOffset LastUpdated { get; init; }
    public string? ErrorMessage { get; init; }
}
