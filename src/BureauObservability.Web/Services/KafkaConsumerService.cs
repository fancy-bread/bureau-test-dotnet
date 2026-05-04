using System.Text.Json;
using BureauObservability.Web.Models;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BureauObservability.Web.Services;

public sealed class KafkaConsumerService : BackgroundService
{
    private readonly IEventStore _eventStore;
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly string _consumerGroup;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IConsumer<string?, byte[]>? _injectedConsumer;
    private static readonly JsonEventFormatter Formatter = new JsonEventFormatter();

    public KafkaConsumerService(
        IEventStore eventStore,
        IConfiguration configuration,
        ILogger<KafkaConsumerService> logger,
        IConsumer<string?, byte[]>? injectedConsumer = null
    )
    {
        _eventStore = eventStore;
        _logger = logger;
        _injectedConsumer = injectedConsumer;
        _bootstrapServers =
            configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        _topic = configuration["Kafka:Topic"] ?? "bureau.runs";
        _consumerGroup = configuration["Kafka:ConsumerGroup"] ?? "bureau-dashboard";
    }

    internal Task RunAsync(CancellationToken cancellationToken) =>
        ExecuteAsync(cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var consumer = _injectedConsumer ?? BuildConsumer();

        if (_injectedConsumer == null)
        {
            ((IConsumer<string?, byte[]>)consumer).Subscribe(_topic);
        }

        _eventStore.UpdateConnectionState(new KafkaConnectionState
        {
            Status = ConnectionStatus.Unknown,
            BrokerEndpoint = _bootstrapServers,
            ConsumerGroup = _consumerGroup,
            LastUpdated = DateTimeOffset.UtcNow,
        });

        bool connected = false;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string?, byte[]> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (KafkaException ex)
                {
                    _logger.LogError(ex, "Kafka error while consuming");
                    _eventStore.UpdateConnectionState(new KafkaConnectionState
                    {
                        Status = ConnectionStatus.Error,
                        BrokerEndpoint = _bootstrapServers,
                        ConsumerGroup = _consumerGroup,
                        LastUpdated = DateTimeOffset.UtcNow,
                        ErrorMessage = ex.Message,
                    });
                    connected = false;
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                    continue;
                }

                if (!connected)
                {
                    connected = true;
                    _eventStore.UpdateConnectionState(new KafkaConnectionState
                    {
                        Status = ConnectionStatus.Connected,
                        BrokerEndpoint = _bootstrapServers,
                        ConsumerGroup = _consumerGroup,
                        LastUpdated = DateTimeOffset.UtcNow,
                    });
                }

                var bureauEvent = ParseMessage(result);
                _eventStore.Add(bureauEvent);
            }
        }
        finally
        {
            if (_injectedConsumer == null)
            {
                consumer.Close();
                consumer.Dispose();
            }

            _eventStore.UpdateConnectionState(new KafkaConnectionState
            {
                Status = ConnectionStatus.Disconnected,
                BrokerEndpoint = _bootstrapServers,
                ConsumerGroup = _consumerGroup,
                LastUpdated = DateTimeOffset.UtcNow,
            });
        }
    }

    private IConsumer<string?, byte[]> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
        };
        return new ConsumerBuilder<string?, byte[]>(config).Build();
    }

    private BureauEvent ParseMessage(ConsumeResult<string?, byte[]> result)
    {
        var partition = result.Partition.Value;
        var offset = result.Offset.Value;
        var kafkaTimestamp = new DateTimeOffset(result.Message.Timestamp.UtcDateTime);

        try
        {
            if (!KafkaExtensions.IsCloudEvent(result.Message))
            {
                var rawText = result.Message.Value != null
                    ? System.Text.Encoding.UTF8.GetString(result.Message.Value)
                    : string.Empty;
                return new BureauEvent
                {
                    Id = $"{partition}-{offset}",
                    CloudEventId = string.Empty,
                    Source = string.Empty,
                    Type = "parse_error",
                    Time = kafkaTimestamp,
                    DataContentType = string.Empty,
                    Data = rawText,
                    IsParseError = true,
                    Partition = partition,
                    Offset = offset,
                };
            }

            var cloudEvent = KafkaExtensions.ToCloudEvent(result.Message, Formatter);

            var dataJson = cloudEvent.Data switch
            {
                JsonElement element => element.GetRawText(),
                null => "{}",
                _ => JsonSerializer.Serialize(cloudEvent.Data),
            };

            return new BureauEvent
            {
                Id = $"{partition}-{offset}",
                CloudEventId = cloudEvent.Id ?? string.Empty,
                Source = cloudEvent.Source?.ToString() ?? string.Empty,
                Type = cloudEvent.Type ?? string.Empty,
                Time = cloudEvent.Time ?? kafkaTimestamp,
                DataContentType = cloudEvent.DataContentType ?? "application/json",
                Data = dataJson,
                IsParseError = false,
                Partition = partition,
                Offset = offset,
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to parse CloudEvent at {Partition}-{Offset}", partition, offset);
            var rawText = result.Message.Value != null
                ? System.Text.Encoding.UTF8.GetString(result.Message.Value)
                : string.Empty;
            return new BureauEvent
            {
                Id = $"{partition}-{offset}",
                CloudEventId = string.Empty,
                Source = string.Empty,
                Type = "parse_error",
                Time = kafkaTimestamp,
                DataContentType = string.Empty,
                Data = rawText,
                IsParseError = true,
                Partition = partition,
                Offset = offset,
            };
        }
    }
}
