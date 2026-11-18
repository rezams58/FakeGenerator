using System.Diagnostics;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Kafka;
using FakeOrderGenerator.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Kafka;

[Collection("SqlLease")]
public sealed class TwoInstanceE2ETests
{
    [SqlKafkaFact]
    public async Task Dead_instance_is_taken_over_within_lease_plus_renew_and_no_event_is_lost()
    {
        await using var topic = await TestTopic.CreateAsync();
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await Sql.SqlTestSupport.ApplySchemaAsync(cs, cts.Token);
        var repo = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));
        var kafka = new KafkaSettings { BootstrapServers = TestEnv.Kafka!, Topic = topic.Name, ClientId = $"e2e-{Guid.NewGuid():N}" };
        await repo.EnsureLeasePartitionsAsync(kafka.PartitionCount, cts.Token);
        await Sql.SqlTestSupport.ScalarAsync(cs, "UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc = DATEADD(SECOND, -1, SYSUTCDATETIME()); SELECT 1", cts.Token);

        const int leaseSeconds = 6, renewSeconds = 2;
        // "Instance A" claims every partition and then dies: it never renews or releases.
        var dead = await repo.ClaimPartitionsAsync(Guid.NewGuid(), kafka.PartitionCount, leaseSeconds, cts.Token);
        Assert.Equal(kafka.PartitionCount, dead.Count);

        long seq = await repo.GetMaxSequenceAsync(cts.Token) + 10_000;
        var ids = Enumerable.Range(0, 30).Select(_ => Guid.NewGuid()).ToList();
        for (int i = 0; i < ids.Count; i++)
            await Sql.SqlTestSupport.ScalarAsync(cs,
                $"INSERT dbo.Outbox(EventId,SequenceNo,AggregateKey,KafkaPartition,EventType,Payload,CreatedAtUtc) " +
                $"VALUES('{ids[i]}',{seq + i},'E2E',0,'OrderCreated','{{}}',SYSUTCDATETIME()); SELECT 1", cts.Token);
        string idList = string.Join(",", ids.Select(i => $"'{i}'"));

        var pub = new PublisherConfig
        {
            BatchSize = 10, IdleDelayMilliseconds = 100, RetryDelayMilliseconds = 200, MaxRetryCount = 5,
            LeaseDurationSeconds = leaseSeconds, LeaseRenewIntervalSeconds = renewSeconds, MaxPartitionsPerInstance = kafka.PartitionCount
        };
        var worker = new KafkaPublisherWorker(repo, new KafkaPartitionPublisherFactory(Options.Create(kafka)),
            new KafkaTopicValidatorAdapter(), Options.Create(kafka), Options.Create(pub), NullLogger<KafkaPublisherWorker>.Instance);

        var sw = Stopwatch.StartNew();
        await worker.StartAsync(cts.Token);
        try
        {
            while (await Sql.SqlTestSupport.ScalarAsync(cs,
                       $"SELECT COUNT(*) FROM dbo.Outbox WHERE EventId IN ({idList}) AND PublishedAtUtc IS NOT NULL", cts.Token) < ids.Count)
            {
                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(leaseSeconds + renewSeconds + 20), "takeover/publish too slow");
                await Task.Delay(200, cts.Token);
            }
        }
        finally { await worker.StopAsync(CancellationToken.None); }
        Assert.True(sw.Elapsed >= TimeSpan.FromSeconds(1)); // it had to wait for the dead lease to expire
    }
}
