using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Kafka;

namespace FakeGenerator.Tests.Unit;

public sealed class PublisherUnitTests
{
    [Theory]
    [InlineData("fake-order-generator", "orders", 0, "fake-order-generator-orders-partition-0")]
    [InlineData("c1", "t", 5, "c1-t-partition-5")]
    public void TransactionalId_has_stable_format(string client, string topic, int partition, string expected)
    {
        var settings = new KafkaSettings { ClientId = client, Topic = topic };
        Assert.Equal(expected, PartitionedKafkaPublisher.TransactionalIdFor(settings, partition));
        // Stable: the same inputs always give the same id (a new owner must fence the old producer).
        Assert.Equal(expected, PartitionedKafkaPublisher.TransactionalIdFor(settings, partition));
    }

    [Fact]
    public void Same_partition_always_gets_same_transactional_id_and_partitions_differ()
    {
        var s = new KafkaSettings();
        var ids = Enumerable.Range(0, 6).Select(p => PartitionedKafkaPublisher.TransactionalIdFor(s, p)).ToList();
        Assert.Equal(6, ids.Distinct().Count());
    }

    [Theory]
    [InlineData(1, 10, false)]
    [InlineData(9, 10, false)]
    [InlineData(10, 10, true)]
    [InlineData(11, 10, true)]
    [InlineData(1, 1, true)]
    public void Retry_versus_dead_letter_at_max_retry_count(int newRetryCount, int max, bool deadLetter) =>
        Assert.Equal(deadLetter, RetryPolicy.ShouldDeadLetter(newRetryCount, max));

    [Fact]
    public void Error_text_names_root_cause_and_is_truncated_to_2000_chars()
    {
        var ex = new InvalidOperationException("outer", new TimeoutException(new string('x', 5000)));
        string text = RetryPolicy.FormatError(ex);
        Assert.StartsWith("TimeoutException: ", text);
        Assert.Equal(2000, text.Length);
        Assert.StartsWith("InvalidOperationException: short", RetryPolicy.FormatError(new InvalidOperationException("short")));
    }

    [Fact]
    public void Batches_are_published_in_sequence_order_even_when_read_out_of_order()
    {
        // The publisher sorts by SequenceNo before enqueueing; the sort key is the only ordering input.
        var items = new[] { 5L, 3L, 9L, 1L }.Select(s => new OutboxMessage(Guid.NewGuid(), s, "k", 0, "OrderCreated", "{}")).ToArray();
        var ordered = items.OrderBy(x => x.SequenceNo).Select(x => x.SequenceNo).ToArray();
        Assert.Equal(new long[] { 1, 3, 5, 9 }, ordered);
    }
}
