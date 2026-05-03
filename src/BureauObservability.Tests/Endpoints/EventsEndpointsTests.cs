using System.Text.Json;
using BureauObservability.Web.Models;
using BureauObservability.Web.Services;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace BureauObservability.Tests.Endpoints;

public class EventsEndpointsTests
{
    private static WebApplicationFactory<Program> CreateFactory(Action<IServiceCollection>? configure = null)
    {
        var factory = new WebApplicationFactory<Program>();
        if (configure is not null)
        {
            return factory.WithWebHostBuilder(builder =>
                builder.ConfigureServices(configure));
        }
        return factory;
    }

    [Fact]
    public async Task GetEvents_ReturnsOkWithEmptyArray_WhenNoEvents()
    {
        using var factory = CreateFactory();
        var client = factory.CreateClient();

        var response = await client.GetAsync("/api/events");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);

        var json = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("events", out var eventsEl));
        Assert.Equal(JsonValueKind.Array, eventsEl.ValueKind);
        Assert.Equal(0, eventsEl.GetArrayLength());

        Assert.True(root.TryGetProperty("count", out var countEl));
        Assert.Equal(0, countEl.GetInt32());
    }

    [Fact]
    public async Task GetEvents_ReturnsEventsNewestFirst()
    {
        using var factory = CreateFactory(services =>
        {
            // Replace IEventStore with a pre-populated one
            services.AddSingleton<IEventStore>(_ =>
            {
                var store = new EventStore();
                store.Add(MakeEvent(0, 1, "2026-05-02T10:00:00Z"));
                store.Add(MakeEvent(0, 2, "2026-05-02T10:00:01Z"));
                store.Add(MakeEvent(0, 3, "2026-05-02T10:00:02Z"));
                return store;
            });
        });

        var client = factory.CreateClient();
        var response = await client.GetAsync("/api/events");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);

        var json = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var events = doc.RootElement.GetProperty("events");

        Assert.Equal(3, events.GetArrayLength());
        Assert.Equal(3, doc.RootElement.GetProperty("count").GetInt32());

        // Newest first: offset 3 should be first
        var firstId = events[0].GetProperty("id").GetString();
        Assert.Equal("0-3", firstId);
    }

    [Fact]
    public async Task GetStatus_ReturnsOkWithConnectionState()
    {
        using var factory = CreateFactory();
        var client = factory.CreateClient();

        var response = await client.GetAsync("/api/status");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);

        var json = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("status", out _));
    }

    private static BureauEvent MakeEvent(int partition, long offset, string time) => new()
    {
        Id = $"{partition}-{offset}",
        CloudEventId = Guid.NewGuid().ToString(),
        Source = "urn:bureau:run:test",
        Type = "com.fancybread.bureau.run.started",
        Time = DateTimeOffset.Parse(time),
        DataContentType = "application/json",
        Data = "{}",
        IsParseError = false,
        Partition = partition,
        Offset = offset,
    };
}
