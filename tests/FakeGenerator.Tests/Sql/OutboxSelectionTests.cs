using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Sql;

[Collection("SqlLease")]
public sealed class OutboxSelectionTests
{
    [SqlFact]
    public async Task Lowered_retry_limit_finalizes_exhausted_rows_before_selecting_successors_and_checks_fencing()
    {
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await SqlTestSupport.ApplySchemaAsync(cs, cts.Token);
        await SqlTestSupport.ScalarAsync(cs,
            "UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc=DATEADD(SECOND,-1,SYSUTCDATETIME()); SELECT 1", cts.Token);
        var repo = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));
        var leases = await repo.ClaimPartitionsAsync(Guid.NewGuid(), 6, 60, cts.Token);
        var lease = leases.Single(l => l.PartitionId == 0);
        long seq = await repo.GetMaxSequenceAsync(cts.Token) + 100;
        var head = Guid.NewGuid(); var tail = Guid.NewGuid();
        await SqlTestSupport.ScalarAsync(cs, $"""
            INSERT dbo.Outbox(EventId,SequenceNo,AggregateKey,KafkaPartition,EventType,Payload,CreatedAtUtc,RetryCount,LastError)
            VALUES ('{head}',{seq},'lower-limit',0,'OrderCreated','null',SYSUTCDATETIME(),5,'original failure'),
                   ('{tail}',{seq + 1},'lower-limit',0,'OrderCreated','null',SYSUTCDATETIME(),0,NULL); SELECT 1;
            """, cts.Token);
        try
        {
            Assert.Empty(await repo.ReadPendingAsync(lease with { FencingToken = lease.FencingToken - 1 }, int.MaxValue, 3, cts.Token));
            Assert.Equal(0, await SqlTestSupport.ScalarAsync(cs,
                $"SELECT COUNT(*) FROM dbo.Outbox WHERE EventId='{head}' AND DeadLetterAtUtc IS NOT NULL", cts.Token));
            var selected = await repo.ReadPendingAsync(lease, int.MaxValue, 3, cts.Token);
            Assert.DoesNotContain(selected, x => x.EventId == head);
            Assert.Contains(selected, x => x.EventId == tail);
            Assert.Equal(1, await SqlTestSupport.ScalarAsync(cs, $"""
                SELECT COUNT(*) FROM dbo.Outbox WHERE EventId='{head}' AND RetryCount=5
                AND DeadLetterAtUtc IS NOT NULL AND LastError='original failure'
                """, cts.Token));
        }
        finally { foreach (var claimed in leases) await repo.ReleaseLeaseAsync(claimed, CancellationToken.None); }
    }

    [SqlFact]
    public async Task Pending_read_excludes_published_deadletter_and_exhausted_and_orders_by_sequence()
    {
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await SqlTestSupport.ApplySchemaAsync(cs, cts.Token);
        var repo = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));

        const int partition = 0;
        await repo.EnsureLeasePartitionsAsync(6, cts.Token);
        await SqlTestSupport.ScalarAsync(cs,
            "UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc = DATEADD(SECOND, -1, SYSUTCDATETIME()); SELECT 1", cts.Token);
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
        // Earlier runs may leave pending rows before this test's sequence numbers.
        // Include those rows instead of assuming our events occur in the first 100.
        var read = await repo.ReadPendingAsync(lease, int.MaxValue, 10, cts.Token);
        var mine = read.Where(m => ids.Contains(m.EventId)).ToList();

        Assert.Equal([ids[4], ids[0]], mine.Select(m => m.EventId));
        Assert.True(read.Zip(read.Skip(1)).All(p => p.First.SequenceNo < p.Second.SequenceNo));
        foreach (var claimed in leases) await repo.ReleaseLeaseAsync(claimed, cts.Token);
    }
}
