using FakeOrderGenerator.Kafka;

namespace FakeGenerator.Tests.Unit;

public sealed class FailedEventMappingTests
{
    private static Task Fail() => Task.FromException(new InvalidOperationException("x"));

    [Fact]
    public void Returns_index_of_first_faulted_task()
    {
        var tasks = new List<Task> { Task.CompletedTask, Task.CompletedTask, Fail(), Fail() };
        Assert.Equal(2, PartitionedKafkaPublisher.FindFailedIndex(tasks));
    }

    [Fact]
    public void Canceled_counts_as_failed()
    {
        var tasks = new List<Task> { Task.CompletedTask, Task.FromCanceled(new CancellationToken(true)) };
        Assert.Equal(1, PartitionedKafkaPublisher.FindFailedIndex(tasks));
    }

    [Fact]
    public void Falls_back_to_zero_when_none_failed()
    {
        Assert.Equal(0, PartitionedKafkaPublisher.FindFailedIndex(new List<Task> { Task.CompletedTask, Task.CompletedTask }));
    }
}
