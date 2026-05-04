using System.Runtime.CompilerServices;
using System.Text.Json;
using BureauObservability.Web.Models;
using BureauObservability.Web.Services;
using Microsoft.AspNetCore.Http;

namespace BureauObservability.Web.Endpoints;

public static class EventsEndpoints
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static void MapEventsEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/events/stream", StreamEvents);
        app.MapGet("/api/status", GetStatus);
        app.MapGet("/api/events", GetEvents);
    }

    private static async Task StreamEvents(
        HttpContext context,
        IEventStore eventStore,
        CancellationToken cancellationToken
    )
    {
        context.Response.Headers.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";

        await context.Response.Body.FlushAsync(cancellationToken);

        // Send initial connection state
        var connectionState = eventStore.ConnectionState;
        await WriteSseEventAsync(
            context,
            "connection-state",
            JsonSerializer.Serialize(connectionState, JsonOptions),
            cancellationToken
        );

        var lastStateStatus = connectionState.Status;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Check for connection state changes
                var currentState = eventStore.ConnectionState;
                if (currentState.LastUpdated != connectionState.LastUpdated)
                {
                    connectionState = currentState;
                    lastStateStatus = connectionState.Status;
                    await WriteSseEventAsync(
                        context,
                        "connection-state",
                        JsonSerializer.Serialize(connectionState, JsonOptions),
                        cancellationToken
                    );
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Client disconnected — normal shutdown
        }
    }

    private static async Task WriteSseEventAsync(
        HttpContext context,
        string eventType,
        string data,
        CancellationToken cancellationToken
    )
    {
        var writer = context.Response.BodyWriter;
        var line = $"event: {eventType}\ndata: {data}\n\n";
        var bytes = System.Text.Encoding.UTF8.GetBytes(line);
        await writer.WriteAsync(bytes, cancellationToken);
        await writer.FlushAsync(cancellationToken);
    }

    private static IResult GetStatus(IEventStore eventStore)
    {
        return Results.Ok(eventStore.ConnectionState);
    }

    private static IResult GetEvents(IEventStore eventStore)
    {
        var events = eventStore.GetRecent(100);
        return Results.Ok(new { events, count = events.Count });
    }
}
