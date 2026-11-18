using Confluent.Kafka;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Kafka;

namespace FakeGenerator.Tests.Kafka;

public sealed class PublisherKafkaTests
{
    [KafkaFact]
    public async Task Batch_is_committed_to_its_partition_in_sequence_order()
    {
        await using var topic = await TestTopic.CreateAsync();
        var settings = new KafkaSettings { BootstrapServers = TestEnv.Kafka!, Topic = topic.Name, ClientId = $"test-{Guid.NewGuid():N}" };
        const int partition = 0;
        string key = $"K-{Guid.NewGuid():N}";
        var batch = Enumerable.Range(1, 20).Select(i =>
            new OutboxMessage(Guid.NewGuid(), i, key, partition, "OrderCreated", $"{{\"n\":{i}}}")).ToList();

        using (var pub = new PartitionedKafkaPublisher(settings, partition))
            await pub.PublishBatchAsync(batch, CancellationToken.None);

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers, GroupId = Guid.NewGuid().ToString(),
            IsolationLevel = IsolationLevel.ReadCommitted, EnableAutoCommit = false
        }).Build();
        consumer.Assign(new TopicPartitionOffset(settings.Topic, partition, Offset.Beginning));
        var seen = new List<string>();
        var until = DateTime.UtcNow.AddSeconds(30);
        while (seen.Count < 20 && DateTime.UtcNow < until)
        {
            var r = consumer.Consume(TimeSpan.FromSeconds(1));
            if (r?.Message.Key == key) seen.Add(r.Message.Value);
        }
        Assert.Equal(batch.Select(b => b.Payload), seen);
    }
}
