using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using MasteringKafka.Shared;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Consumer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly KafkaSettings _kafkaSettings;
    private readonly AsyncRetryPolicy _retryPolicy;
    private CachedSchemaRegistryClient _schemaRegistry = default!;
    private IConsumer<string, UserCreatedRequest> _consumer = default!;
    private IProducer<string, string> _dlqProducer = default!;

    public Worker(ILogger<Worker> logger, IOptions<KafkaSettings> kafkaSettings)
    {
        _logger = logger;
        _kafkaSettings = kafkaSettings.Value;

        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                3,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (exception, timeSpan, retryCount, _) =>
                {
                    _logger.LogWarning(
                        exception,
                        "Processing failed. Retry {RetryCount} in {Delay}s",
                        retryCount,
                        timeSpan.TotalSeconds
                    );
                }
            );
    }

    public override Task StartAsync(CancellationToken ct)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = _kafkaSettings.SchemaRegistryUrl,
        };

        _schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        _consumer = new ConsumerBuilder<string, UserCreatedRequest>(consumerConfig)
            .SetValueDeserializer(
                new ProtobufDeserializer<UserCreatedRequest>(_schemaRegistry).AsSyncOverAsync()
            )
            .Build();

        _consumer.Subscribe(_kafkaSettings.TopicName);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
        };

        _dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

        _logger.LogInformation(
            "Kafka consumer started and subscribed to topic: {Topic}",
            _kafkaSettings.TopicName
        );

        return base.StartAsync(ct);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer?.Consume(stoppingToken);
                if (result == null)
                    continue;

                var userCreatedRequest = result.Message.Value;

                try
                {
                    await _retryPolicy.ExecuteAsync(async () =>
                    {
                        await ProcessUserAsync(userCreatedRequest);
                    });

                    _logger.LogInformation(
                        "Successfully processed user: {UserId} - {Email}",
                        userCreatedRequest.Id,
                        userCreatedRequest.Email
                    );
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Message field after retries. Sending to DLQ...");

                    var dlpPayload = JsonSerializer.Serialize(userCreatedRequest);
                    await _dlqProducer.ProduceAsync(
                        _kafkaSettings.DlqTopicName,
                        new Message<string, string> { Key = result.Message.Key, Value = dlpPayload }
                    );
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Error while consuming message.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "General Exception launched");
            }
        }
    }

    private async Task ProcessUserAsync(UserCreatedRequest request)
    {
        await Task.Delay(10);
        if (request.Email == "fail@example.com")
            throw new Exception("Simulated failure");
    }
}
