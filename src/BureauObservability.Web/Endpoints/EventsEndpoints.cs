using System.Text;
using System.Text.Json;
using BureauObservability.Web.Services;

namespace BureauObservability.Web.Endpoints;

public static class EventsEndpoints
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static void MapEventsEndpoints(this WebApplication app)
    {
        app.MapGet("/api/events", GetEvents);
        app.MapGet("/api/status", GetStatus);
        app.MapGet("/api/events/stream", StreamEvents);
    }

    private static IResult GetEvents(IEventStore eventStore)
    {
        var events = eventStore.GetRecent(100);
        return Results.Ok(new { events, count = events.Count });
    }

    private static IResult GetStatus(IEventStore eventStore)
    {
        return Results.Ok(eventStore.ConnectionState);
    }

    private static async Task StreamEvents(
        HttpContext context,
        IEventStore eventStore,
        CancellationToken cancellationToken)
    {
        context.Response.Headers.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";

        // Send current connection state immediately
        var stateJson = JsonSerializer.Serialize(eventStore.ConnectionState, JsonOptions);
        await WriteSseEvent(context.Response, "connection-state", stateJson, cancellationToken);

        // Send existing events (newest first, reversed so feed shows newest-first on reconnect)
        var existingEvents = eventStore.GetRecent(100);
        foreach (var evt in existingEvents.Reverse())
        {
            var eventJson = JsonSerializer.Serialize(evt, JsonOptions);
            await WriteSseEvent(context.Response, "bureau-event", eventJson, cancellationToken);
        }

        await context.Response.Body.FlushAsync(cancellationToken);

        // Keep connection alive — new events will be pushed by real-time consumers
        // In this scaffold, the SSE stream provides the initial snapshot and stays open.
        // Real-time push is delivered by the browser reconnecting (EventSource auto-reconnects).
        // A production version would use a channel/queue; the scaffold keeps it simple.
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Client disconnected
        }
    }

    private static async Task WriteSseEvent(
        HttpResponse response,
        string eventName,
        string data,
        CancellationToken cancellationToken)
    {
        var sb = new StringBuilder();
        sb.Append("event: ").Append(eventName).Append('\n');
        sb.Append("data: ").Append(data).Append('\n');
        sb.Append('\n');

        await response.WriteAsync(sb.ToString(), cancellationToken);
    }
}
