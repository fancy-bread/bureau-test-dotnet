using System.Text.Json;
using BureauObservability.Web.Models;
using CloudNative.CloudEvents.Kafka;
using Confluent.Kafka;

namespace BureauObservability.Web.Services;

public sealed class KafkaConsumerService : BackgroundService
{
    private readonly IEventStore _eventStore;
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly Func<ConsumerConfig, IConsumer<string?, byte[]>>? _consumerFactory;

    public KafkaConsumerService(
        IEventStore eventStore,
        IConfiguration configuration,
        ILogger<KafkaConsumerService> logger,
        Func<ConsumerConfig, IConsumer<string?, byte[]>>? consumerFactory = null)
    {
        _eventStore = eventStore;
        _configuration = configuration;
        _logger = logger;
        _consumerFactory = consumerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        var topic = _configuration["Kafka:Topic"] ?? "bureau.runs";
        var consumerGroup = _configuration["Kafka:ConsumerGroup"] ?? "bureau-dashboard";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
        };

        _eventStore.UpdateConnectionState(new KafkaConnectionState
        {
            Status = ConnectionStatus.Unknown,
            BrokerEndpoint = bootstrapServers,
            ConsumerGroup = consumerGroup,
            LastUpdated = DateTimeOffset.UtcNow,
        });

        await Task.Run(() => RunConsumerLoop(config, topic, consumerGroup, bootstrapServers, stoppingToken), stoppingToken);
    }

    internal void RunConsumerLoop(
        ConsumerConfig config,
        string topic,
        string consumerGroup,
        string bootstrapServers,
        CancellationToken stoppingToken)
    {
        var consumer = _consumerFactory is not null
            ? _consumerFactory(config)
            : new ConsumerBuilder<string?, byte[]>(config).Build();

        try
        {
            consumer.Subscribe(topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string?, byte[]>? result = null;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (result is null)
                    {
                        continue;
                    }

                    _eventStore.UpdateConnectionState(new KafkaConnectionState
                    {
                        Status = ConnectionStatus.Connected,
                        BrokerEndpoint = bootstrapServers,
                        ConsumerGroup = consumerGroup,
                        LastUpdated = DateTimeOffset.UtcNow,
                    });

                    var bureauEvent = ParseMessage(result);
                    _eventStore.Add(bureauEvent);
                    consumer.Commit(result);
                }
                catch (ConsumeException ex) when (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogWarning(ex, "Kafka consume error");

                    if (result is not null)
                    {
                        var errorEvent = new BureauEvent
                        {
                            Id = $"{result.Partition.Value}-{result.Offset.Value}",
                            CloudEventId = string.Empty,
                            Source = string.Empty,
                            Type = "parse_error",
                            Time = result.Message.Timestamp.UtcDateTime,
                            DataContentType = string.Empty,
                            Data = result.Message.Value is not null
                                ? System.Text.Encoding.UTF8.GetString(result.Message.Value)
                                : string.Empty,
                            IsParseError = true,
                            Partition = result.Partition.Value,
                            Offset = result.Offset.Value,
                        };
                        _eventStore.Add(errorEvent);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unrecoverable Kafka consumer error");

            _eventStore.UpdateConnectionState(new KafkaConnectionState
            {
                Status = ConnectionStatus.Error,
                BrokerEndpoint = bootstrapServers,
                ConsumerGroup = consumerGroup,
                LastUpdated = DateTimeOffset.UtcNow,
                ErrorMessage = ex.Message,
            });
        }
        finally
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error closing Kafka consumer");
            }

            consumer.Dispose();

            _eventStore.UpdateConnectionState(new KafkaConnectionState
            {
                Status = ConnectionStatus.Disconnected,
                BrokerEndpoint = bootstrapServers,
                ConsumerGroup = consumerGroup,
                LastUpdated = DateTimeOffset.UtcNow,
            });
        }
    }

    internal static BureauEvent ParseMessage(ConsumeResult<string?, byte[]> result)
    {
        try
        {
            var cloudEventsFormatter = new CloudNative.CloudEvents.SystemTextJson.JsonEventFormatter();
            CloudNative.CloudEvents.CloudEvent cloudEvent;

            // Try Kafka structured/binary content mode first (headers-based).
            // If no CE headers are present, fall back to parsing the body directly
            // as a CloudEvents JSON envelope (structured mode without Kafka headers).
            var headers = result.Message.Headers;
            bool hasCeHeaders = headers is not null &&
                headers.Any(h => h.Key.StartsWith("ce_", StringComparison.OrdinalIgnoreCase)
                              || h.Key.Equals("content-type", StringComparison.OrdinalIgnoreCase));

            if (hasCeHeaders)
            {
                var message = new Message<string?, byte[]>
                {
                    Key = result.Message.Key,
                    Value = result.Message.Value,
                    Headers = result.Message.Headers,
                    Timestamp = result.Message.Timestamp,
                };
                cloudEvent = message.ToCloudEvent(cloudEventsFormatter);
            }
            else
            {
                // Structured mode: the entire CloudEvent is the message body as JSON
                using var stream = new System.IO.MemoryStream(result.Message.Value ?? Array.Empty<byte>());
                cloudEvent = cloudEventsFormatter.DecodeStructuredModeMessage(
                    stream, new System.Net.Mime.ContentType("application/cloudevents+json"), null);
            }

            var dataJson = cloudEvent.Data is null
                ? "{}"
                : JsonSerializer.Serialize(cloudEvent.Data);

            return new BureauEvent
            {
                Id = $"{result.Partition.Value}-{result.Offset.Value}",
                CloudEventId = cloudEvent.Id ?? string.Empty,
                Source = cloudEvent.Source?.ToString() ?? string.Empty,
                Type = cloudEvent.Type ?? string.Empty,
                Time = cloudEvent.Time ?? (DateTimeOffset)result.Message.Timestamp.UtcDateTime,
                DataContentType = cloudEvent.DataContentType ?? string.Empty,
                Data = dataJson,
                IsParseError = false,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value,
            };
        }
        catch (Exception)
        {
            var rawValue = result.Message.Value is not null
                ? System.Text.Encoding.UTF8.GetString(result.Message.Value)
                : string.Empty;

            return new BureauEvent
            {
                Id = $"{result.Partition.Value}-{result.Offset.Value}",
                CloudEventId = string.Empty,
                Source = string.Empty,
                Type = "parse_error",
                Time = result.Message.Timestamp.UtcDateTime,
                DataContentType = string.Empty,
                Data = rawValue,
                IsParseError = true,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value,
            };
        }
    }
}
