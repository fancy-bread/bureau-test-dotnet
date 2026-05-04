using BureauObservability.Web.Models;
using BureauObservability.Web.Services;

namespace BureauObservability.Tests.Services;

public class EventStoreTests
{
    private static BureauEvent MakeEvent(int partition, long offset) =>
        new BureauEvent
        {
            Id = $"{partition}-{offset}",
            CloudEventId = Guid.NewGuid().ToString(),
            Source = "urn:bureau:run:test",
            Type = "com.fancybread.bureau.run.started",
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/json",
            Data = "{}",
            IsParseError = false,
            Partition = partition,
            Offset = offset,
        };

    [Fact]
    public void Add_StoresEvent()
    {
        var store = new EventStore();
        var evt = MakeEvent(0, 1);

        store.Add(evt);

        var recent = store.GetRecent(10);
        Assert.Single(recent);
        Assert.Equal(evt.Id, recent[0].Id);
    }

    [Fact]
    public void GetRecent_ReturnsNewestFirst()
    {
        var store = new EventStore();
        store.Add(MakeEvent(0, 1));
        store.Add(MakeEvent(0, 2));
        store.Add(MakeEvent(0, 3));

        var recent = store.GetRecent(10);

        Assert.Equal(3, recent.Count);
        Assert.Equal("0-3", recent[0].Id);
        Assert.Equal("0-2", recent[1].Id);
        Assert.Equal("0-1", recent[2].Id);
    }

    [Fact]
    public void GetRecent_RespectsCountLimit()
    {
        var store = new EventStore();
        for (int i = 0; i < 10; i++)
        {
            store.Add(MakeEvent(0, i));
        }

        var recent = store.GetRecent(3);

        Assert.Equal(3, recent.Count);
    }

    [Fact]
    public void Add_CapAt100_EvictsOldest()
    {
        var store = new EventStore();
        for (int i = 0; i < 101; i++)
        {
            store.Add(MakeEvent(0, i));
        }

        var recent = store.GetRecent(200);

        Assert.Equal(100, recent.Count);
        // newest-first: first item should be offset 100, last should be offset 1
        Assert.Equal("0-100", recent[0].Id);
        Assert.Equal("0-1", recent[99].Id);
    }

    [Fact]
    public void GetRecent_EmptyStore_ReturnsEmptyList()
    {
        var store = new EventStore();

        var recent = store.GetRecent(10);

        Assert.Empty(recent);
    }

    [Fact]
    public void UpdateConnectionState_SetsConnectionState()
    {
        var store = new EventStore();
        var state = new KafkaConnectionState
        {
            Status = ConnectionStatus.Connected,
            BrokerEndpoint = "localhost:9092",
            ConsumerGroup = "bureau-dashboard",
            LastUpdated = DateTimeOffset.UtcNow,
        };

        store.UpdateConnectionState(state);

        Assert.Equal(ConnectionStatus.Connected, store.ConnectionState.Status);
        Assert.Equal("localhost:9092", store.ConnectionState.BrokerEndpoint);
    }

    [Fact]
    public void ConnectionState_InitialStatus_IsUnknown()
    {
        var store = new EventStore();

        Assert.Equal(ConnectionStatus.Unknown, store.ConnectionState.Status);
    }
}
