using System.Net.Http.Json;
using System.Text.Json;
using BureauObservability.Web.Models;
using BureauObservability.Web.Services;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace BureauObservability.Tests.Endpoints;

public class EventsEndpointsTests(WebApplicationFactory<Program> factory)
    : IClassFixture<WebApplicationFactory<Program>>
{
    [Fact]
    public async Task GetEvents_EmptyStore_ReturnsEmptyArray()
    {
        var client = factory.CreateClient();

        var response = await client.GetAsync("/api/events");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);

        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        var events = body.GetProperty("events");
        var count = body.GetProperty("count").GetInt32();

        Assert.Equal(JsonValueKind.Array, events.ValueKind);
        Assert.Equal(0, events.GetArrayLength());
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task GetEvents_WithEvents_ReturnsNewestFirst()
    {
        var app = factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureServices(services =>
            {
                // Replace EventStore with one that has pre-seeded events
                var seeded = new EventStore();
                seeded.Add(new BureauEvent
                {
                    Id = "0-1",
                    CloudEventId = "id-1",
                    Source = "urn:bureau:run:a",
                    Type = "com.fancybread.bureau.run.started",
                    Time = DateTimeOffset.UtcNow.AddSeconds(-10),
                    DataContentType = "application/json",
                    Data = "{}",
                    IsParseError = false,
                    Partition = 0,
                    Offset = 1,
                });
                seeded.Add(new BureauEvent
                {
                    Id = "0-2",
                    CloudEventId = "id-2",
                    Source = "urn:bureau:run:b",
                    Type = "com.fancybread.bureau.run.completed",
                    Time = DateTimeOffset.UtcNow,
                    DataContentType = "application/json",
                    Data = "{}",
                    IsParseError = false,
                    Partition = 0,
                    Offset = 2,
                });
                services.AddSingleton<IEventStore>(seeded);
            });
        });

        var client = app.CreateClient();
        var response = await client.GetAsync("/api/events");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);

        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        var events = body.GetProperty("events");
        var count = body.GetProperty("count").GetInt32();

        Assert.Equal(2, events.GetArrayLength());
        Assert.Equal(2, count);
        // Newest first: offset 2 before offset 1
        Assert.Equal("0-2", events[0].GetProperty("id").GetString());
        Assert.Equal("0-1", events[1].GetProperty("id").GetString());
    }

    [Fact]
    public async Task GetStatus_ReturnsConnectionState()
    {
        var client = factory.CreateClient();

        var response = await client.GetAsync("/api/status");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);

        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(body.TryGetProperty("status", out _));
    }
}
