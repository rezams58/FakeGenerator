using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Kafka;
using FakeOrderGenerator.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Kafka;

/// <summary>Delegates to SQL but throws once on MarkPublishedAsync, i.e. a crash between Kafka commit and SQL mark.</summary>
internal sealed class CrashOnceRepository(SqlOrderRepository inner) : IOutboxRepository
{
    private int _crashed;
    public bool Crashed => _crashed > 0;
    public Task EnsureLeasePartitionsAsync(int n, CancellationToken ct) => inner.EnsureLeasePartitionsAsync(n, ct);
    public Task ValidateLeasePartitionsAsync(int n, CancellationToken ct) => inner.ValidateLeasePartitionsAsync(n, ct);
    public Task<IReadOnlyList<PartitionLease>> ClaimPartitionsAsync(Guid o, int m, int s, CancellationToken ct) => inner.ClaimPartitionsAsync(o, m, s, ct);
    public Task<bool> RenewLeaseAsync(PartitionLease l, int s, CancellationToken ct) => inner.RenewLeaseAsync(l, s, ct);
    public Task ReleaseLeaseAsync(PartitionLease l, CancellationToken ct) => inner.ReleaseLeaseAsync(l, ct);
    public Task<List<OutboxMessage>> ReadPendingAsync(PartitionLease l, int b, int r, CancellationToken ct) => inner.ReadPendingAsync(l, b, r, ct);
    public Task<bool> MarkFailedAsync(OutboxMessage i, PartitionLease l, string e, int r, CancellationToken ct) => inner.MarkFailedAsync(i, l, e, r, ct);

    public Task<int> MarkPublishedAsync(IEnumerable<Guid> ids, PartitionLease l, CancellationToken ct)
    {
        if (Interlocked.Increment(ref _crashed) == 1) throw new InvalidOperationException("simulated crash after Kafka commit");
        return inner.MarkPublishedAsync(ids, l, ct);
    }
}

[Collection("SqlLease")]
public sealed class ConsumerDeduplicationE2ETests
{
    [SqlKafkaFact]
    public async Task Replay_after_crash_republishes_same_event_ids_and_inbox_applies_each_effect_once()
    {
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(180));
        await Sql.SqlTestSupport.ApplySchemaAsync(cs, cts.Token);
        await InboxConsumer.EnsureTablesAsync(cs, cts.Token);

        var real = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));
        var repo = new CrashOnceRepository(real);
        var kafka = new KafkaSettings { BootstrapServers = TestEnv.Kafka!, Topic = "orders", ClientId = $"dedup-{Guid.NewGuid():N}" };
        await real.EnsureLeasePartitionsAsync(kafka.PartitionCount, cts.Token);
        await Sql.SqlTestSupport.ScalarAsync(cs, "UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc = SYSUTCDATETIME(); SELECT 1", cts.Token);
        // Make sure nothing else is pending, so only this test's events are published.
        await Sql.SqlTestSupport.ScalarAsync(cs,
            "UPDATE dbo.Outbox SET PublishedAtUtc = SYSUTCDATETIME() WHERE PublishedAtUtc IS NULL AND DeadLetterAtUtc IS NULL; SELECT 1", cts.Token);

        const int count = 40;
        long seq = await real.GetMaxSequenceAsync(cts.Token) + 20_000;
        var ids = Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToList();
        for (int i = 0; i < count; i++)
            await Sql.SqlTestSupport.ScalarAsync(cs,
                $"INSERT dbo.Outbox(EventId,SequenceNo,AggregateKey,KafkaPartition,EventType,Payload,CreatedAtUtc) " +
                $"VALUES('{ids[i]}',{seq + i},'DEDUP',0,'OrderCreated','{{}}',SYSUTCDATETIME()); SELECT 1", cts.Token);
        string idList = string.Join(",", ids.Select(i => $"'{i}'"));

        var pub = new PublisherConfig
        {
            BatchSize = 10, IdleDelayMilliseconds = 100, RetryDelayMilliseconds = 200, MaxRetryCount = 5,
            LeaseDurationSeconds = 6, LeaseRenewIntervalSeconds = 2, MaxPartitionsPerInstance = kafka.PartitionCount
        };
        var worker = new KafkaPublisherWorker(repo, new KafkaPartitionPublisherFactory(Options.Create(kafka)),
            new KafkaTopicValidatorAdapter(), Options.Create(kafka), Options.Create(pub), NullLogger<KafkaPublisherWorker>.Instance);

        await worker.StartAsync(cts.Token);
        try
        {
            while (await Sql.SqlTestSupport.ScalarAsync(cs,
                       $"SELECT COUNT(*) FROM dbo.Outbox WHERE EventId IN ({idList}) AND PublishedAtUtc IS NOT NULL", cts.Token) < count)
                await Task.Delay(200, cts.Token);
        }
        finally { await worker.StopAsync(CancellationToken.None); }
        Assert.True(repo.Crashed, "the simulated crash did not happen");

        var consumer = new InboxConsumer(kafka.BootstrapServers, kafka.Topic, cs);
        await consumer.RunAsync($"dedup-{Guid.NewGuid():N}",
            () => Sql.SqlTestSupport.ScalarAsync(cs, $"SELECT COUNT(*) FROM dbo.Inbox WHERE EventId IN ({idList})", cts.Token).GetAwaiter().GetResult() >= count,
            TimeSpan.FromSeconds(60), cts.Token);

        Assert.Equal(count, await Sql.SqlTestSupport.ScalarAsync(cs, $"SELECT COUNT(*) FROM dbo.InboxEffect WHERE EventId IN ({idList})", cts.Token));
        Assert.Equal(count, await Sql.SqlTestSupport.ScalarAsync(cs, $"SELECT COUNT(DISTINCT EventId) FROM dbo.InboxEffect WHERE EventId IN ({idList})", cts.Token));
    }
}
