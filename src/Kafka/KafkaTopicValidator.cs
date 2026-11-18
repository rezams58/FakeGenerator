using Confluent.Kafka;

namespace FakeOrderGenerator.Kafka;

public static class KafkaTopicValidator
{
    public static void Validate(string bootstrapServers, string topic, int minimumPartitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(15));
        var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topic)
            ?? throw new InvalidOperationException($"Kafka topic '{topic}' was not found.");
        if (topicMetadata.Error.IsError)
            throw new InvalidOperationException($"Kafka topic '{topic}' metadata error: {topicMetadata.Error}");
        if (topicMetadata.Partitions.Any(p => p.Error.IsError))
            throw new InvalidOperationException($"Kafka topic '{topic}' contains a partition metadata error: {topicMetadata.Partitions.First(p => p.Error.IsError).Error}");
        if (topicMetadata.Partitions.Count < minimumPartitions)
            throw new InvalidOperationException($"Kafka topic '{topic}' has {topicMetadata.Partitions.Count} partitions; at least {minimumPartitions} are required. Increase the topic partition count before starting the publisher.");
    }
}

/// <summary>Seam so the publisher worker can be unit-tested without a broker.</summary>
public interface ITopicValidator
{
    void Validate(string bootstrapServers, string topic, int minimumPartitions);
}

public sealed class KafkaTopicValidatorAdapter : ITopicValidator
{
    public void Validate(string bootstrapServers, string topic, int minimumPartitions) =>
        KafkaTopicValidator.Validate(bootstrapServers, topic, minimumPartitions);
}
