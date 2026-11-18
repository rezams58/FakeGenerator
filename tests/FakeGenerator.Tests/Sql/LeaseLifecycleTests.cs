using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Sql;

[Collection("SqlLease")]
public sealed class LeaseLifecycleTests
{
    private static async Task<(SqlOrderRepository Repo, string Cs)> SetupAsync(CancellationToken ct)
    {
        string cs = TestEnv.Sql!;
        await SqlTestSupport.ApplySchemaAsync(cs, ct);
        var repo = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));
        await repo.EnsureLeasePartitionsAsync(6, ct);
        await SqlTestSupport.ScalarAsync(cs, "UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc = SYSUTCDATETIME(); SELECT 1", ct);
        return (repo, cs);
    }

    [SqlFact]
    public async Task Concurrent_claim_gives_one_owner_and_extra_instance_claims_nothing_without_error()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var (repo, _) = await SetupAsync(cts.Token);
        var a = Guid.NewGuid(); var b = Guid.NewGuid();
        var results = await Task.WhenAll(repo.ClaimPartitionsAsync(a, 6, 60, cts.Token), repo.ClaimPartitionsAsync(b, 6, 60, cts.Token));
        var all = results.SelectMany(r => r).ToList();
        Assert.Equal(6, all.Count);
        Assert.Equal(6, all.Select(l => l.PartitionId).Distinct().Count());

        var extra = await repo.ClaimPartitionsAsync(Guid.NewGuid(), 6, 60, cts.Token);
        Assert.Empty(extra);
        foreach (var l in all) await repo.ReleaseLeaseAsync(l, cts.Token);
    }

    [SqlFact]
    public async Task Expired_takeover_increments_fencing_token_and_stale_owner_is_rejected_everywhere()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var (repo, cs) = await SetupAsync(cts.Token);
        var oldLease = (await repo.ClaimPartitionsAsync(Guid.NewGuid(), 1, 60, cts.Token)).Single();
        await SqlTestSupport.ScalarAsync(cs,
            $"UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc = SYSUTCDATETIME() WHERE PartitionId = {oldLease.PartitionId}; SELECT 1", cts.Token);

        var newLease = (await repo.ClaimPartitionsAsync(Guid.NewGuid(), 6, 60, cts.Token)).Single(l => l.PartitionId == oldLease.PartitionId);
        Assert.Equal(oldLease.FencingToken + 1, newLease.FencingToken);

        Guid ev = Guid.NewGuid();
        long seq = await repo.GetMaxSequenceAsync(cts.Token) + 5000;
        await SqlTestSupport.ScalarAsync(cs,
            $"INSERT dbo.Outbox(EventId,SequenceNo,AggregateKey,KafkaPartition,EventType,Payload,CreatedAtUtc) " +
            $"VALUES('{ev}',{seq},'K',{newLease.PartitionId},'OrderCreated','{{}}',SYSUTCDATETIME()); SELECT 1", cts.Token);
        var item = new OutboxMessage(ev, seq, "K", newLease.PartitionId, "OrderCreated", "{}");

        Assert.False(await repo.RenewLeaseAsync(oldLease, 60, cts.Token));
        Assert.Empty(await repo.ReadPendingAsync(oldLease, 10, 10, cts.Token));
        Assert.Equal(0, await repo.MarkPublishedAsync([ev], oldLease, cts.Token));
        Assert.False(await repo.MarkFailedAsync(item, oldLease, "x", 10, cts.Token));

        // A release by the old owner must not free the new ownership.
        await repo.ReleaseLeaseAsync(oldLease, cts.Token);
        Assert.True(await repo.RenewLeaseAsync(newLease, 60, cts.Token));
        Assert.Empty(await repo.ClaimPartitionsAsync(Guid.NewGuid(), 6, 60, cts.Token));

        Assert.Equal(1, await repo.MarkPublishedAsync([ev], newLease, cts.Token));
        await repo.ReleaseLeaseAsync(newLease, cts.Token);
    }
}
