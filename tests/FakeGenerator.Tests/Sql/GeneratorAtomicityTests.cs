using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Generation;
using FakeOrderGenerator.Generation.Order;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Sql;

[Collection("SqlLease")]
public sealed class GeneratorAtomicityTests
{
    [SqlFact]
    public async Task Entity_change_and_outbox_row_commit_together_and_failure_rolls_both_back()
    {
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await SqlTestSupport.ApplySchemaAsync(cs, cts.Token);

        var conn = Options.Create(new ConnectionSettings { SqlServer = cs });
        var repo = new SqlOrderRepository(conn);
        var cfg = new GeneratorConfig { TargetEventCount = int.MaxValue, BatchSize = 50, CustomerCount = 20, AdvanceProbability = 0.5 };
        var def = new OrderDefinition(cfg);
        var writer = new OrderWriter(conn, def);

        long maxSeq = await repo.GetMaxSequenceAsync(cts.Token);
        long events0 = await repo.CountOutboxAsync(cts.Token);
        int orders0 = await SqlTestSupport.ScalarAsync(cs, "SELECT COUNT(*) FROM dbo.Orders", cts.Token);
        var builder = new EventBatchBuilder(def, cfg, 6, new Random(1), maxSeq, events0);

        // 1. Happy path: both tables change together.
        var ok = builder.Build(DateTime.UtcNow);
        await repo.WriteBatchAsync(writer, ok, cts.Token);
        Assert.Equal(events0 + ok.Events.Count, await repo.CountOutboxAsync(cts.Token));
        Assert.Equal(orders0 + ok.NewEntities.Count, await SqlTestSupport.ScalarAsync(cs, "SELECT COUNT(*) FROM dbo.Orders", cts.Token));

        // 2. Second batch has transitions; force the Outbox insert to fail (duplicate SequenceNo) after the entity writes.
        var bad = builder.Build(DateTime.UtcNow);
        Assert.NotEmpty(bad.Transitioned);
        bad.Events[0] = bad.Events[0] with { SequenceNo = ok.Events[0].SequenceNo };
        long events1 = await repo.CountOutboxAsync(cts.Token);
        int orders1 = await SqlTestSupport.ScalarAsync(cs, "SELECT COUNT(*) FROM dbo.Orders", cts.Token);

        await Assert.ThrowsAsync<SqlException>(() => repo.WriteBatchAsync(writer, bad, cts.Token));

        Assert.Equal(events1, await repo.CountOutboxAsync(cts.Token));
        Assert.Equal(orders1, await SqlTestSupport.ScalarAsync(cs, "SELECT COUNT(*) FROM dbo.Orders", cts.Token));
        foreach (var e in bad.NewEntities)
            Assert.Equal(0, await SqlTestSupport.ScalarAsync(cs, $"SELECT COUNT(*) FROM dbo.Orders WHERE OrderId = '{e.EntityId}'", cts.Token));
    }
}
