using FakeOrderGenerator.Kafka;

namespace FakeGenerator.Tests.Kafka;

public sealed class TopicValidationTests
{
    [Fact]
    public void Unreachable_broker_fails_with_an_exception()
    {
        Assert.ThrowsAny<Exception>(() => KafkaTopicValidator.Validate("localhost:1", "orders", 1));
    }

    [KafkaFact]
    public void Missing_topic_fails()
    {
        var ex = Assert.ThrowsAny<Exception>(() => KafkaTopicValidator.Validate(TestEnv.Kafka!, $"no-such-{Guid.NewGuid():N}", 1));
        Assert.Contains("topic", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [KafkaFact]
    public void Too_few_partitions_fails()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => KafkaTopicValidator.Validate(TestEnv.Kafka!, "orders", 100000));
        Assert.Contains("partitions", ex.Message);
    }
}
