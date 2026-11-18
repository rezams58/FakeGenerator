using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Kafka;

namespace FakeGenerator.Tests.Kafka;

public sealed class FencingTests
{
    [KafkaFact]
    public async Task Second_producer_with_same_transactional_id_fences_the_first()
    {
        await using var topic = await TestTopic.CreateAsync();
        var settings = new KafkaSettings { BootstrapServers = TestEnv.Kafka!, Topic = topic.Name, ClientId = $"fence-{Guid.NewGuid():N}" };
        OutboxMessage Msg(long n) => new(Guid.NewGuid(), n, "FENCE", 0, "OrderCreated", "{}");

        using var zombie = new PartitionedKafkaPublisher(settings, 0);
        await zombie.PublishBatchAsync([Msg(1)], CancellationToken.None);

        using var owner = new PartitionedKafkaPublisher(settings, 0); // InitTransactions bumps the epoch
        await owner.PublishBatchAsync([Msg(2)], CancellationToken.None);

        await Assert.ThrowsAnyAsync<Exception>(() => zombie.PublishBatchAsync([Msg(3)], CancellationToken.None));
    }
}
