using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace FakeGenerator.Tests.Kafka;

/// <summary>Isolates integration runs from historical records and other tests.</summary>
internal sealed class TestTopic(string name) : IAsyncDisposable
{
    public string Name { get; } = name;

    public static async Task<TestTopic> CreateAsync(int partitions = 6)
    {
        var topic = new TestTopic($"fakegen-test-{Guid.NewGuid():N}");
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = TestEnv.Kafka! }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic.Name, NumPartitions = partitions, ReplicationFactor = 1 }]);
        return topic;
    }

    public async ValueTask DisposeAsync()
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = TestEnv.Kafka! }).Build();
        await admin.DeleteTopicsAsync([Name]);
    }
}
