namespace FakeOrderGenerator.Data;

/// <summary>Lease and outbox operations used by the publisher worker (test seam over SqlOrderRepository).</summary>
public interface IOutboxRepository
{
    Task EnsureLeasePartitionsAsync(int partitionCount, CancellationToken ct);
    Task ValidateLeasePartitionsAsync(int partitionCount, CancellationToken ct);
    Task<IReadOnlyList<PartitionLease>> ClaimPartitionsAsync(Guid ownerId, int maxPartitions, int leaseSeconds, CancellationToken ct);
    Task<bool> RenewLeaseAsync(PartitionLease lease, int leaseSeconds, CancellationToken ct);
    Task ReleaseLeaseAsync(PartitionLease lease, CancellationToken ct);
    Task<List<OutboxMessage>> ReadPendingAsync(PartitionLease lease, int batchSize, int maxRetryCount, CancellationToken ct);
    Task<int> MarkPublishedAsync(IEnumerable<Guid> eventIds, PartitionLease lease, CancellationToken ct);
    Task<bool> MarkFailedAsync(OutboxMessage item, PartitionLease lease, string error, int maxRetryCount, CancellationToken ct);
}
