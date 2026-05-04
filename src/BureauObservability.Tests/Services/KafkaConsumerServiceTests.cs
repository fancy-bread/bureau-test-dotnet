using BureauObservability.Web.Models;
using BureauObservability.Web.Services;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace BureauObservability.Tests.Services;

public class KafkaConsumerServiceTests
{
    private static IConfiguration BuildConfig(
        string bootstrapServers = "localhost:9092",
        string topic = "bureau.runs",
        string consumerGroup = "bureau-dashboard"
    )
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["Kafka:BootstrapServers"] = bootstrapServers,
                    ["Kafka:Topic"] = topic,
                    ["Kafka:ConsumerGroup"] = consumerGroup,
                }
            )
            .Build();
        return config;
    }

    [Fact]
    public async Task ConsumeLoop_ValidCloudEvent_CallsEventStoreAdd()
    {
        var store = Substitute.For<IEventStore>();
        store.ConnectionState.Returns(new KafkaConnectionState
        {
            Status = ConnectionStatus.Unknown,
            BrokerEndpoint = "localhost:9092",
            ConsumerGroup = "bureau-dashboard",
            LastUpdated = DateTimeOffset.UtcNow,
        });

        var consumer = Substitute.For<IConsumer<string?, byte[]>>();
        var cts = new CancellationTokenSource();

        int callCount = 0;
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1)
                {
                    return BuildValidConsumeResult(0, 1);
                }
                cts.Cancel();
                throw new OperationCanceledException();
            });

        var service = new KafkaConsumerService(
            store,
            BuildConfig(),
            NullLogger<KafkaConsumerService>.Instance,
            consumer
        );

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.RunAsync(cts.Token)
        );

        store.Received().Add(
            Arg.Is<BureauEvent>(e => !e.IsParseError && e.Partition == 0 && e.Offset == 1)
        );
    }

    [Fact]
    public async Task ConsumeLoop_MalformedMessage_SetsIsParseErrorAndCallsAdd()
    {
        var store = Substitute.For<IEventStore>();
        store.ConnectionState.Returns(new KafkaConnectionState
        {
            Status = ConnectionStatus.Unknown,
            BrokerEndpoint = "localhost:9092",
            ConsumerGroup = "bureau-dashboard",
            LastUpdated = DateTimeOffset.UtcNow,
        });

        var consumer = Substitute.For<IConsumer<string?, byte[]>>();
        var cts = new CancellationTokenSource();

        int callCount = 0;
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1)
                {
                    return BuildMalformedConsumeResult(0, 5);
                }
                cts.Cancel();
                throw new OperationCanceledException();
            });

        var service = new KafkaConsumerService(
            store,
            BuildConfig(),
            NullLogger<KafkaConsumerService>.Instance,
            consumer
        );

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.RunAsync(cts.Token)
        );

        store.Received().Add(
            Arg.Is<BureauEvent>(e => e.IsParseError && e.Partition == 0 && e.Offset == 5)
        );
    }

    [Fact]
    public async Task ConsumeLoop_OnFirstConsume_SetsConnectedState()
    {
        var store = Substitute.For<IEventStore>();
        store.ConnectionState.Returns(new KafkaConnectionState
        {
            Status = ConnectionStatus.Unknown,
            BrokerEndpoint = "localhost:9092",
            ConsumerGroup = "bureau-dashboard",
            LastUpdated = DateTimeOffset.UtcNow,
        });

        var consumer = Substitute.For<IConsumer<string?, byte[]>>();
        var cts = new CancellationTokenSource();

        int callCount = 0;
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1)
                {
                    return BuildValidConsumeResult(0, 10);
                }
                cts.Cancel();
                throw new OperationCanceledException();
            });

        var service = new KafkaConsumerService(
            store,
            BuildConfig(),
            NullLogger<KafkaConsumerService>.Instance,
            consumer
        );

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.RunAsync(cts.Token)
        );

        store.Received().UpdateConnectionState(
            Arg.Is<KafkaConnectionState>(s => s.Status == ConnectionStatus.Connected)
        );
    }

    private static ConsumeResult<string?, byte[]> BuildValidConsumeResult(
        int partition,
        long offset
    )
    {
        const string cloudEventJson =
            """{"specversion":"1.0","id":"test-id","source":"urn:bureau:run:test","type":"com.fancybread.bureau.run.started","time":"2026-05-02T10:00:00Z","datacontenttype":"application/json","data":{"id":"test"}}""";

        return new ConsumeResult<string?, byte[]>
        {
            Message = new Message<string?, byte[]>
            {
                Key = null,
                Value = System.Text.Encoding.UTF8.GetBytes(cloudEventJson),
                Headers = new Headers
                {
                    {
                        "content-type",
                        System.Text.Encoding.UTF8.GetBytes("application/cloudevents+json")
                    },
                },
                Timestamp = new Timestamp(
                    new DateTimeOffset(2026, 5, 2, 10, 0, 0, TimeSpan.Zero).UtcDateTime
                ),
            },
            TopicPartitionOffset = new TopicPartitionOffset("bureau.runs", partition, offset),
        };
    }

    private static ConsumeResult<string?, byte[]> BuildMalformedConsumeResult(
        int partition,
        long offset
    )
    {
        return new ConsumeResult<string?, byte[]>
        {
            Message = new Message<string?, byte[]>
            {
                Key = null,
                Value = System.Text.Encoding.UTF8.GetBytes("not-a-cloud-event-at-all!!"),
                Headers = new Headers(),
                Timestamp = new Timestamp(DateTime.UtcNow),
            },
            TopicPartitionOffset = new TopicPartitionOffset("bureau.runs", partition, offset),
        };
    }
}
