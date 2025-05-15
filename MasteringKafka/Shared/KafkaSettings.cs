namespace MasteringKafka.Shared;

public class KafkaSettings
{
    public const string Section = "KafkaSettings";

    public required string BootstrapServers { get; set; }
    public required string SchemaRegistryUrl { get; set; }
    public required string GroupId { get; set; }
    public required string TopicName { get; set; }
    public required string DlqTopicName { get; set; }
}
