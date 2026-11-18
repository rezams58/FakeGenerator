using System.Text.Json;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Sql;

[Collection("SqlLease")]
public sealed class OutboxConcurrentReadTests
{
    [SqlFact]
    public async Task Concurrent_batch_inserts_and_pending_reads_do_not_skip_lower_sequences()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var ct = cts.Token;
        string cs = TestEnv.Sql!;
        await SqlTestSupport.ApplySchemaAsync(cs, ct);
        await SqlTestSupport.ScalarAsync(cs,
            "UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc=DATEADD(SECOND,-1,SYSUTCDATETIME()); SELECT 1", ct);
        var repo = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));
        var leases = await repo.ClaimPartitionsAsync(Guid.NewGuid(), 6, 120, ct);
        var lease = leases.Single(l => l.PartitionId == 0);
        long baseline = await repo.GetMaxSequenceAsync(ct);
        string key = Guid.NewGuid().ToString("N");
        const int count = 10000, chunk = 250;
        var producer = Task.Run(async () =>
        {
            await using var cn = new SqlConnection(cs);
            await cn.OpenAsync(ct);
            for (int start = 1; start <= count; start += chunk)
            {
                await using var cmd = new SqlCommand("""
                    INSERT dbo.Outbox(EventId,SequenceNo,AggregateKey,KafkaPartition,EventType,Payload,CreatedAtUtc)
                    SELECT NEWID(), @base+CONVERT(BIGINT,[value]), @key, 0, 'OrderCreated', '{}', SYSUTCDATETIME()
                    FROM OPENJSON(@numbers);
                    """, cn);
                cmd.Parameters.AddWithValue("@base", baseline);
                cmd.Parameters.AddWithValue("@key", key);
                cmd.Parameters.AddWithValue("@numbers", JsonSerializer.Serialize(Enumerable.Range(start, chunk)));
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }, ct);
        int seen = 0;
        long last = baseline;
        try
        {
            while (!producer.IsCompleted || seen < count)
            {
                if (producer.IsFaulted) await producer;
                var batch = await repo.ReadPendingAsync(lease, 128, 10, ct);
                foreach (var row in batch.Where(x => x.AggregateKey == key))
                {
                    Assert.Equal(last + 1, row.SequenceNo);
                    last = row.SequenceNo;
                    seen++;
                }
                if (batch.Count > 0)
                    Assert.Equal(batch.Count, await repo.MarkPublishedAsync(batch.Select(x => x.EventId), lease, ct));
                else await Task.Delay(1, ct);
            }
            await producer;
            Assert.Equal(count, seen);
        }
        finally
        {
            cts.Cancel();
            try { await producer; } catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            foreach (var claimed in leases) await repo.ReleaseLeaseAsync(claimed, CancellationToken.None);
        }
    }
}
