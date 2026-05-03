using System.Collections.Concurrent;
using BureauObservability.Web.Models;

namespace BureauObservability.Web.Services;

public sealed class EventStore : IEventStore
{
    private const int Capacity = 100;

    private readonly ConcurrentQueue<BureauEvent> _events = new();
    private KafkaConnectionState _connectionState = new();
    private readonly object _stateLock = new();

    public void Add(BureauEvent bureauEvent)
    {
        _events.Enqueue(bureauEvent);
        while (_events.Count > Capacity)
        {
            _events.TryDequeue(out _);
        }
    }

    public IReadOnlyList<BureauEvent> GetRecent(int count)
    {
        return _events
            .Reverse()
            .Take(count)
            .ToList();
    }

    public KafkaConnectionState ConnectionState
    {
        get
        {
            lock (_stateLock)
            {
                return _connectionState;
            }
        }
    }

    public void UpdateConnectionState(KafkaConnectionState state)
    {
        lock (_stateLock)
        {
            _connectionState = state;
        }
    }
}
