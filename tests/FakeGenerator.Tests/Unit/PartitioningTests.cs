using FakeOrderGenerator.Data;

namespace FakeGenerator.Tests.Unit;

public sealed class PartitioningTests
{
    [Theory]
    [InlineData("CUS-000001", 6)]
    [InlineData("CUS-000002", 6)]
    [InlineData("customer-α", 6)]
    public void Same_key_always_maps_to_same_partition(string key, int count)
    {
        int first = Partitioning.GetPartition(key, count);
        Assert.Equal(first, Partitioning.GetPartition(key, count));
        Assert.InRange(first, 0, count - 1);
    }

    [Fact]
    public void Non_positive_partition_count_is_rejected()
        => Assert.Throws<ArgumentOutOfRangeException>(() => Partitioning.GetPartition("key", 0));
}
