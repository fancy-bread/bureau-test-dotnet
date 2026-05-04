using System.Collections.Concurrent;
using BureauObservability.Web.Models;

namespace BureauObservability.Web.Services;

public sealed class EventStore : IEventStore
{
    private const int Capacity = 100;
    private readonly ConcurrentQueue<BureauEvent> _queue = new();
    private volatile KafkaConnectionState _connectionState = new KafkaConnectionState
    {
        Status = ConnectionStatus.Unknown,
        BrokerEndpoint = string.Empty,
        ConsumerGroup = string.Empty,
        LastUpdated = DateTimeOffset.UtcNow,
    };

    public KafkaConnectionState ConnectionState => _connectionState;

    public void Add(BureauEvent bureauEvent)
    {
        _queue.Enqueue(bureauEvent);
        while (_queue.Count > Capacity)
        {
            _queue.TryDequeue(out _);
        }
    }

    public IReadOnlyList<BureauEvent> GetRecent(int count)
    {
        var all = _queue.ToArray();
        // Return newest first, up to count
        var result = all.Reverse().Take(count).ToList();
        return result.AsReadOnly();
    }

    public void UpdateConnectionState(KafkaConnectionState state)
    {
        _connectionState = state;
    }
}
