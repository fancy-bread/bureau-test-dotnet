using Microsoft.AspNetCore.Mvc.Testing;

namespace BureauObservability.Tests.Endpoints;

public class SseEndpointTests(WebApplicationFactory<Program> factory)
    : IClassFixture<WebApplicationFactory<Program>>
{
    [Fact]
    public async Task GetStream_Returns200WithTextEventStream()
    {
        var client = factory.CreateClient();

        // Use a cancellation token so we don't hang waiting for the infinite stream
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/events/stream");

        using var response = await client.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            cts.Token
        );

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);
    }
}
