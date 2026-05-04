namespace BureauObservability.Web.Models;

public sealed class BureauEvent
{
    public string Id { get; init; } = string.Empty;
    public string CloudEventId { get; init; } = string.Empty;
    public string Source { get; init; } = string.Empty;
    public string Type { get; init; } = string.Empty;
    public DateTimeOffset Time { get; init; }
    public string DataContentType { get; init; } = string.Empty;
    public string Data { get; init; } = "{}";
    public bool IsParseError { get; init; }
    public int Partition { get; init; }
    public long Offset { get; init; }
}
