using BureauObservability.Web.Models;
using BureauObservability.Web.Services;

namespace BureauObservability.Tests.Services;

public class EventStoreTests
{
    private static BureauEvent MakeEvent(int partition, long offset) => new()
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
        for (var i = 0; i < 5; i++)
        {
            store.Add(MakeEvent(0, i));
        }

        var recent = store.GetRecent(5);

        Assert.Equal(5, recent.Count);
        Assert.Equal("0-4", recent[0].Id);
        Assert.Equal("0-3", recent[1].Id);
        Assert.Equal("0-0", recent[4].Id);
    }

    [Fact]
    public void Add_CapAt100_EvictsOldestWhenOver100()
    {
        var store = new EventStore();

        // Add 101 events
        for (var i = 0; i < 101; i++)
        {
            store.Add(MakeEvent(0, i));
        }

        var recent = store.GetRecent(200);

        Assert.Equal(100, recent.Count);
        // Newest should be offset 100, oldest retained should be offset 1 (offset 0 evicted)
        Assert.Equal("0-100", recent[0].Id);
        Assert.DoesNotContain(recent, e => e.Id == "0-0");
    }

    [Fact]
    public void GetRecent_ReturnsAtMostRequestedCount()
    {
        var store = new EventStore();
        for (var i = 0; i < 10; i++)
        {
            store.Add(MakeEvent(0, i));
        }

        var recent = store.GetRecent(3);

        Assert.Equal(3, recent.Count);
    }

    [Fact]
    public void ConnectionState_DefaultsToUnknown()
    {
        var store = new EventStore();
        Assert.Equal(ConnectionStatus.Unknown, store.ConnectionState.Status);
    }

    [Fact]
    public void UpdateConnectionState_UpdatesState()
    {
        var store = new EventStore();
        var newState = new KafkaConnectionState
        {
            Status = ConnectionStatus.Connected,
            BrokerEndpoint = "localhost:9092",
            ConsumerGroup = "test-group",
            LastUpdated = DateTimeOffset.UtcNow,
        };

        store.UpdateConnectionState(newState);

        Assert.Equal(ConnectionStatus.Connected, store.ConnectionState.Status);
        Assert.Equal("localhost:9092", store.ConnectionState.BrokerEndpoint);
    }
}
