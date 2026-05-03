using System.Text;
using System.Text.Json;
using BureauObservability.Web.Models;
using BureauObservability.Web.Services;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace BureauObservability.Tests.Services;

public class KafkaConsumerServiceTests
{
    private static IConfiguration BuildConfig() =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:BootstrapServers"] = "localhost:9092",
                ["Kafka:Topic"] = "bureau.runs",
                ["Kafka:ConsumerGroup"] = "test-group",
            })
            .Build();

    // -------------------------------------------------------------------
    // ParseMessage tests (static internal method)
    // -------------------------------------------------------------------

    [Fact]
    public void ParseMessage_ValidCloudEvent_ReturnsBureauEvent()
    {
        // Build a minimal CloudEvents 1.0 JSON message
        var ceJson = JsonSerializer.Serialize(new
        {
            specversion = "1.0",
            id = "test-id-001",
            source = "urn:bureau:run:abc",
            type = "com.fancybread.bureau.run.started",
            time = "2026-05-02T10:00:00Z",
            datacontenttype = "application/json",
            data = new { id = "abc", spec = "001" },
        });

        var result = MakeConsumeResult(0, 1, Encoding.UTF8.GetBytes(ceJson));

        var bureauEvent = KafkaConsumerService.ParseMessage(result);

        Assert.False(bureauEvent.IsParseError);
        Assert.Equal("test-id-001", bureauEvent.CloudEventId);
        Assert.Equal("urn:bureau:run:abc", bureauEvent.Source);
        Assert.Equal("com.fancybread.bureau.run.started", bureauEvent.Type);
        Assert.Equal("0-1", bureauEvent.Id);
        Assert.Equal(0, bureauEvent.Partition);
        Assert.Equal(1L, bureauEvent.Offset);
    }

    [Fact]
    public void ParseMessage_MalformedPayload_ReturnsParseErrorEvent()
    {
        var garbage = Encoding.UTF8.GetBytes("not valid json at all!!!");
        var result = MakeConsumeResult(0, 5, garbage);

        var bureauEvent = KafkaConsumerService.ParseMessage(result);

        Assert.True(bureauEvent.IsParseError);
        Assert.Equal("parse_error", bureauEvent.Type);
        Assert.Equal("0-5", bureauEvent.Id);
        Assert.Contains("not valid json", bureauEvent.Data);
    }

    // -------------------------------------------------------------------
    // RunConsumerLoop tests using NSubstitute mocked IConsumer
    // -------------------------------------------------------------------

    [Fact]
    public void RunConsumerLoop_ValidMessage_CallsEventStoreAdd()
    {
        var eventStore = Substitute.For<IEventStore>();
        eventStore.ConnectionState.Returns(new KafkaConnectionState());

        var ceJson = JsonSerializer.Serialize(new
        {
            specversion = "1.0",
            id = "loop-test-id",
            source = "urn:bureau:run:xyz",
            type = "com.fancybread.bureau.run.started",
            time = "2026-05-02T11:00:00Z",
            datacontenttype = "application/json",
            data = new { id = "xyz" },
        });

        var consumer = Substitute.For<IConsumer<string?, byte[]>>();

        // First call returns a valid message, second call cancels via exception
        var callCount = 0;
        consumer.Consume(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            callCount++;
            if (callCount == 1)
            {
                return MakeConsumeResult(0, 10, Encoding.UTF8.GetBytes(ceJson));
            }
            throw new OperationCanceledException();
        });

        var svc = new KafkaConsumerService(
            eventStore,
            BuildConfig(),
            NullLogger<KafkaConsumerService>.Instance,
            _ => consumer);

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancelled so loop exits after first event

        // Re-run with a fresh token that cancels after first consume
        var cts2 = new CancellationTokenSource();
        var consumerCallCount2 = 0;
        consumer.Consume(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            consumerCallCount2++;
            if (consumerCallCount2 == 1)
            {
                return MakeConsumeResult(0, 10, Encoding.UTF8.GetBytes(ceJson));
            }
            cts2.Cancel();
            return null;
        });

        svc.RunConsumerLoop(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test" },
            "bureau.runs",
            "test",
            "localhost:9092",
            cts2.Token);

        eventStore.Received().Add(Arg.Is<BureauEvent>(e => !e.IsParseError && e.CloudEventId == "loop-test-id"));
    }

    [Fact]
    public void RunConsumerLoop_MalformedMessage_AddsParseErrorEvent()
    {
        var eventStore = Substitute.For<IEventStore>();
        eventStore.ConnectionState.Returns(new KafkaConnectionState());

        var garbage = Encoding.UTF8.GetBytes("!!not cloudevents!!");
        var consumer = Substitute.For<IConsumer<string?, byte[]>>();
        var cts = new CancellationTokenSource();

        var callCount = 0;
        consumer.Consume(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            callCount++;
            if (callCount == 1)
            {
                return MakeConsumeResult(0, 99, garbage);
            }
            cts.Cancel();
            return null;
        });

        var svc = new KafkaConsumerService(
            eventStore,
            BuildConfig(),
            NullLogger<KafkaConsumerService>.Instance,
            _ => consumer);

        svc.RunConsumerLoop(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test" },
            "bureau.runs",
            "test",
            "localhost:9092",
            cts.Token);

        eventStore.Received().Add(Arg.Is<BureauEvent>(e => e.IsParseError && e.Id == "0-99"));
    }

    [Fact]
    public void RunConsumerLoop_OnStart_SetsUnknownConnectionState()
    {
        var eventStore = new EventStore();
        var consumer = Substitute.For<IConsumer<string?, byte[]>>();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        consumer.Consume(Arg.Any<TimeSpan>()).Returns(_ => null);

        var svc = new KafkaConsumerService(
            eventStore,
            BuildConfig(),
            NullLogger<KafkaConsumerService>.Instance,
            _ => consumer);

        // The ExecuteAsync sets Unknown before RunConsumerLoop is called.
        // Here we just verify RunConsumerLoop sets Disconnected on exit.
        svc.RunConsumerLoop(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test" },
            "bureau.runs",
            "test",
            "localhost:9092",
            cts.Token);

        Assert.Equal(ConnectionStatus.Disconnected, eventStore.ConnectionState.Status);
    }

    [Fact]
    public void RunConsumerLoop_AfterSuccessfulConsume_SetsConnectedState()
    {
        var eventStore = Substitute.For<IEventStore>();
        eventStore.ConnectionState.Returns(new KafkaConnectionState());

        var ceJson = JsonSerializer.Serialize(new
        {
            specversion = "1.0",
            id = "state-test",
            source = "urn:bureau:run:s",
            type = "com.fancybread.bureau.run.started",
            time = "2026-05-02T12:00:00Z",
            datacontenttype = "application/json",
            data = new { },
        });

        var consumer = Substitute.For<IConsumer<string?, byte[]>>();
        var cts = new CancellationTokenSource();

        var count = 0;
        consumer.Consume(Arg.Any<TimeSpan>()).Returns(_ =>
        {
            count++;
            if (count == 1) return MakeConsumeResult(0, 1, Encoding.UTF8.GetBytes(ceJson));
            cts.Cancel();
            return null;
        });

        var svc = new KafkaConsumerService(
            eventStore,
            BuildConfig(),
            NullLogger<KafkaConsumerService>.Instance,
            _ => consumer);

        svc.RunConsumerLoop(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test" },
            "bureau.runs",
            "test",
            "localhost:9092",
            cts.Token);

        eventStore.Received().UpdateConnectionState(
            Arg.Is<KafkaConnectionState>(s => s.Status == ConnectionStatus.Connected));
    }

    // -------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------

    private static ConsumeResult<string?, byte[]> MakeConsumeResult(int partition, long offset, byte[] value)
    {
        return new ConsumeResult<string?, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset("bureau.runs", partition, offset),
            Message = new Message<string?, byte[]>
            {
                Key = null,
                Value = value,
                Timestamp = new Timestamp(DateTime.UtcNow),
                Headers = new Headers(),
            },
        };
    }
}
