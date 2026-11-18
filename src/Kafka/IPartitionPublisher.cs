using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;

namespace FakeOrderGenerator.Kafka;

/// <summary>Test seam over the per-partition transactional Kafka producer.</summary>
public interface IPartitionPublisher : IDisposable
{
    Task PublishBatchAsync(IReadOnlyList<OutboxMessage> batch, CancellationToken ct);
}

public interface IPartitionPublisherFactory
{
    IPartitionPublisher Create(int partitionId);
}

public sealed class KafkaPartitionPublisherFactory(Microsoft.Extensions.Options.IOptions<KafkaSettings> options)
    : IPartitionPublisherFactory
{
    public IPartitionPublisher Create(int partitionId) => new PartitionedKafkaPublisher(options.Value, partitionId);
}
