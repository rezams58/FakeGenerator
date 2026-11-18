using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Sql;

public sealed class OutboxSelectionTests
{
    [SqlFact]
    public async Task Pending_read_excludes_published_deadletter_and_exhausted_and_orders_by_sequence()
    {
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await SqlTestSupport.ApplySchemaAsync(cs, cts.Token);
        var repo = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));

        const int partition = 0;
        await repo.EnsureLeasePartitionsAsync(6, cts.Token);
        long baseSeq = await repo.GetMaxSequenceAsync(cts.Token) + 1000;
        var ids = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToArray();
        // seq order intentionally differs from insert order; index 1 published, 2 dead-letter, 3 retries exhausted.
        long[] seq = [baseSeq + 4, baseSeq + 1, baseSeq + 2, baseSeq + 3, baseSeq];
        string[] extra = ["NULL,NULL,0", "SYSUTCDATETIME(),NULL,0", "NULL,SYSUTCDATETIME(),0", "NULL,NULL,10", "NULL,NULL,1"];
        for (int i = 0; i < ids.Length; i++)
            await SqlTestSupport.ScalarAsync(cs,
                $"INSERT dbo.Outbox(EventId,SequenceNo,AggregateKey,KafkaPartition,EventType,Payload,CreatedAtUtc,PublishedAtUtc,DeadLetterAtUtc,RetryCount) " +
                $"VALUES('{ids[i]}',{seq[i]},'K',{partition},'OrderCreated','{{}}',SYSUTCDATETIME(),{extra[i]}); SELECT 1", cts.Token);

        var leases = await repo.ClaimPartitionsAsync(Guid.NewGuid(), 6, 60, cts.Token);
        var lease = leases.First(l => l.PartitionId == partition);
        var read = await repo.ReadPendingAsync(lease, 100, 10, cts.Token);
        var mine = read.Where(m => ids.Contains(m.EventId)).ToList();

        Assert.Equal([ids[4], ids[0]], mine.Select(m => m.EventId));
        Assert.True(read.Zip(read.Skip(1)).All(p => p.First.SequenceNo < p.Second.SequenceNo));
        await repo.ReleaseLeaseAsync(lease, cts.Token);
    }
}
