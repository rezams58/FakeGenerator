using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Kafka;
using FakeOrderGenerator.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Unit;

internal sealed class FakeTopicValidator : ITopicValidator
{
    public Exception? Throw { get; set; }
    public int Calls { get; private set; }
    public void Validate(string bootstrapServers, string topic, int minimumPartitions)
    {
        Calls++;
        if (Throw is not null) throw Throw;
    }
}

internal sealed class FakePublisher : IPartitionPublisher
{
    public List<IReadOnlyList<OutboxMessage>> Calls { get; } = [];
    public Func<IReadOnlyList<OutboxMessage>, int, CancellationToken, Task> Behavior { get; set; } = (_, _, _) => Task.CompletedTask;
    public Action<string>? Log { get; set; }
    public DateTime LastPublishStartUtc { get; private set; }

    public async Task PublishBatchAsync(IReadOnlyList<OutboxMessage> batch, CancellationToken ct)
    {
        lock (Calls) Calls.Add(batch.ToList());
        LastPublishStartUtc = DateTime.UtcNow;
        Log?.Invoke("publish");
        await Behavior(batch, Calls.Count, ct);
        Log?.Invoke("committed");
    }

    public void Dispose() { }
}

internal sealed class FakePublisherFactory(FakePublisher publisher) : IPartitionPublisherFactory
{
    public List<int> CreatedPartitions { get; } = [];
    public IPartitionPublisher Create(int partitionId)
    {
        lock (CreatedPartitions) CreatedPartitions.Add(partitionId);
        return publisher;
    }
}

internal sealed class FakeOutboxRepository : IOutboxRepository
{
    private readonly object _gate = new();
    public List<OutboxMessage> Pending { get; } = [];
    public List<PartitionLease> LeasesToHand { get; } = [];
    public Func<bool> RenewResult { get; set; } = () => true;
    public Action<string>? Log { get; set; }

    public List<List<Guid>> MarkPublishedCalls { get; } = [];
    public List<(OutboxMessage Item, string Error)> MarkFailedCalls { get; } = [];
    public List<PartitionLease> Released { get; } = [];
    public int RenewCalls { get; private set; }
    public DateTime? RenewFailedAtUtc { get; private set; }
    public List<int> ClaimLimits { get; } = [];

    public void EnqueueLease(PartitionLease lease)
    {
        lock (_gate) LeasesToHand.Add(lease);
    }

    public Task EnsureLeasePartitionsAsync(int partitionCount, CancellationToken ct) => Task.CompletedTask;
    public Task ValidateLeasePartitionsAsync(int partitionCount, CancellationToken ct) => Task.CompletedTask;

    public Task<IReadOnlyList<PartitionLease>> ClaimPartitionsAsync(Guid ownerId, int maxPartitions, int leaseSeconds, CancellationToken ct)
    {
        lock (_gate)
        {
            ClaimLimits.Add(maxPartitions);
            var result = LeasesToHand.Take(maxPartitions).Select(l => l with { OwnerId = ownerId }).ToList();
            LeasesToHand.RemoveRange(0, result.Count);
            return Task.FromResult<IReadOnlyList<PartitionLease>>(result);
        }
    }

    public Task<bool> RenewLeaseAsync(PartitionLease lease, int leaseSeconds, CancellationToken ct)
    {
        RenewCalls++;
        bool ok = RenewResult();
        if (!ok) RenewFailedAtUtc ??= DateTime.UtcNow;
        return Task.FromResult(ok);
    }

    public Task ReleaseLeaseAsync(PartitionLease lease, CancellationToken ct)
    {
        lock (_gate) Released.Add(lease);
        return Task.CompletedTask;
    }

    public Task<List<OutboxMessage>> ReadPendingAsync(PartitionLease lease, int batchSize, int maxRetryCount, CancellationToken ct)
    {
        lock (_gate) return Task.FromResult(Pending.OrderBy(x => x.SequenceNo).Take(batchSize).ToList());
    }

    public Task<int> MarkPublishedAsync(IEnumerable<Guid> eventIds, PartitionLease lease, CancellationToken ct)
    {
        var ids = eventIds.ToList();
        lock (_gate)
        {
            Log?.Invoke("mark-published");
            MarkPublishedCalls.Add(ids);
            return Task.FromResult(Pending.RemoveAll(p => ids.Contains(p.EventId)));
        }
    }

    public Task<bool> MarkFailedAsync(OutboxMessage item, PartitionLease lease, string error, int maxRetryCount, CancellationToken ct)
    {
        lock (_gate)
        {
            Log?.Invoke("mark-failed");
            MarkFailedCalls.Add((item, error));
            return Task.FromResult(true);
        }
    }
}

internal static class WorkerHarness
{
    public static OutboxMessage Msg(long seq, int partition = 0) =>
        new(Guid.NewGuid(), seq, "CUS-000001", partition, "OrderCreated", "{}");

    public static PartitionLease Lease(int partition = 0) =>
        new(partition, Guid.Empty, 1, DateTime.UtcNow.AddMinutes(1));

    public static KafkaPublisherWorker Create(FakeOutboxRepository repo, FakePublisher publisher, FakeTopicValidator validator,
        int renewSeconds = 20)
    {
        var publisherConfig = new PublisherConfig
        {
            BatchSize = 10, IdleDelayMilliseconds = 10, RetryDelayMilliseconds = 10, MaxRetryCount = 3,
            LeaseDurationSeconds = 90, LeaseRenewIntervalSeconds = renewSeconds, MaxPartitionsPerInstance = 1
        };
        return new KafkaPublisherWorker(repo, new FakePublisherFactory(publisher), validator,
            Options.Create(new KafkaSettings()), Options.Create(publisherConfig), NullLogger<KafkaPublisherWorker>.Instance);
    }

    public static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        var until = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > until) throw new TimeoutException("Condition not met in time.");
            await Task.Delay(10);
        }
    }
}
