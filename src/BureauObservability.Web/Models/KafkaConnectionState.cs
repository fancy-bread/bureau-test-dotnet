namespace BureauObservability.Web.Models;

public sealed class KafkaConnectionState
{
    public ConnectionStatus Status { get; init; } = ConnectionStatus.Unknown;
    public string BrokerEndpoint { get; init; } = string.Empty;
    public string ConsumerGroup { get; init; } = string.Empty;
    public DateTimeOffset LastUpdated { get; init; } = DateTimeOffset.UtcNow;
    public string? ErrorMessage { get; init; }
}
