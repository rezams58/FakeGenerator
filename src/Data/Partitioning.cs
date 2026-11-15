
namespace FakeOrderGenerator.Data;

public static class Partitioning
{
    public static int GetPartition(string key, int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount));

        // Stable FNV-1a hash
        uint hash = 2166136261;

        foreach (byte b in System.Text.Encoding.UTF8.GetBytes(key))
        {
            hash ^= b;
            hash *= 16777619;
        }

        return (int)(hash % (uint)partitionCount);
    }
}

public sealed record Order(
    Guid OrderId,
    long SequenceNo,
    string OrderNumber,
    string CustomerId,
    decimal Amount,
    DateTime CreatedAtUtc);

public sealed record OutboxMessage(
    Guid EventId,
    long SequenceNo,
    string AggregateKey,
    int KafkaPartition,
    string EventType,
    string Payload);
