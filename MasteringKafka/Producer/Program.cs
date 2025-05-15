// See https://aka.ms/new-console-template for more information

using Bogus;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using MasteringKafka.Shared;
using Microsoft.Extensions.Configuration;

var config = new ConfigurationBuilder().AddJsonFile("appSettings.json", optional: false).Build();

var kafkaSettings = config.GetSection(KafkaSettings.Section).Get<KafkaSettings>();
if (kafkaSettings == null)
    Environment.Exit(-1);

var schemaRegistryConfig = new SchemaRegistryConfig { Url = kafkaSettings.SchemaRegistryUrl };

var producerConfig = new ProducerConfig
{
    BootstrapServers = kafkaSettings.BootstrapServers,
    ClientId = "user-producer", // You can use kafkaSettings.ClientId if exposed
    EnableIdempotence = true, // Ensure no duplicate messages
    Acks = Acks.All, // Wait for all replicas to acknowledge
    CompressionType = CompressionType.Gzip, // Compress messages to reduce size
};

var randomizer = new Random();

using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

using var producer = new ProducerBuilder<string, UserCreatedRequest>(producerConfig)
    .SetValueSerializer(new ProtobufSerializer<UserCreatedRequest>(schemaRegistry))
    .Build();

bool _running = true;

Console.CancelKeyPress += (_, args) =>
{
    _running = false;
    args.Cancel = true;
    Console.WriteLine("\nCTRL+C detected. Exiting...");
};

while (_running)
{
    var faker = new Faker();
    int numberRandom = randomizer.Next(1, 10);

    UserCreatedRequest message = new()
    {
        Id = Guid.NewGuid().ToString(),
        UserName = faker.Person.UserName,
        Email = (numberRandom == 5) ? "fail@example.com" : faker.Person.Email,
    };
    Console.WriteLine($"Sending users... {message.Email}");

    await producer.ProduceAsync(
        kafkaSettings.TopicName,
        new Message<string, UserCreatedRequest> { Key = message.Id, Value = message }
    );

    await Task.Delay(TimeSpan.FromSeconds(30));
}
