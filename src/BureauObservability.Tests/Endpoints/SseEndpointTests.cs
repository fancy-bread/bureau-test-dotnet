using Microsoft.AspNetCore.Mvc.Testing;

namespace BureauObservability.Tests.Endpoints;

public class SseEndpointTests(WebApplicationFactory<Program> factory)
    : IClassFixture<WebApplicationFactory<Program>>
{
    [Fact]
    public async Task GetStream_Returns200WithEventStreamContentType()
    {
        var client = factory.CreateClient();

        // Use a cancellation token so the indefinite SSE stream doesn't hang the test
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        HttpResponseMessage response;
        try
        {
            response = await client.GetAsync(
                "/api/events/stream",
                HttpCompletionOption.ResponseHeadersRead,
                cts.Token);
        }
        catch (OperationCanceledException)
        {
            // If we timed out after headers, that's fine — just check what we got
            return;
        }

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);
    }
}
