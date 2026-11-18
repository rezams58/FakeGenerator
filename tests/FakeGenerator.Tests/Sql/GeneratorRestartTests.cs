using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Generation.Order;
using FakeOrderGenerator.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace FakeGenerator.Tests.Sql;

[Collection("SqlLease")]
public sealed class GeneratorRestartTests
{
    [SqlFact]
    public async Task Restart_reuses_no_sequence_keeps_lifecycles_going_and_ends_exactly_at_target()
    {
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await SqlTestSupport.ApplySchemaAsync(cs, cts.Token);

        var conn = Options.Create(new ConnectionSettings { SqlServer = cs });
        var repo = new SqlOrderRepository(conn);
        long start = await repo.CountOutboxAsync(cts.Token);
        long target = start + 600;

        GeneratorConfig NewConfig(int seed) => new()
        {
            TargetEventCount = (int)target, BatchSize = 40, IntervalMilliseconds = 30, CustomerCount = 30,
            AdvanceProbability = 0.7, RandomSeed = seed
        };
        GeneratorWorker NewWorker(int seed)
        {
            var c = NewConfig(seed);
            var def = new OrderDefinition(c);
            return new GeneratorWorker(repo, def, new OrderWriter(conn, def), Options.Create(c),
                Options.Create(new KafkaSettings()), NullLogger<GeneratorWorker>.Instance);
        }

        // Run 1: stop mid-run.
        var first = NewWorker(42);
        await first.StartAsync(cts.Token);
        while (await repo.CountOutboxAsync(cts.Token) < start + 120)
        {
            // Surface a failed background worker instead of polling until the test hangs.
            if (first.ExecuteTask!.IsCompleted) await first.ExecuteTask;
            Assert.False(first.ExecuteTask.IsCompleted, "Generator stopped before producing the first batches.");
            await Task.Delay(20, cts.Token);
        }
        await first.StopAsync(CancellationToken.None);
        long afterFirst = await repo.CountOutboxAsync(cts.Token);
        long maxAfterFirst = await repo.GetMaxSequenceAsync(cts.Token);
        Assert.InRange(afterFirst, start + 120, target - 1);

        var activeBefore = (await new OrderWriter(conn, new OrderDefinition(NewConfig(0))).LoadActiveAsync(cts.Token))
            .Select(e => e.EntityId).ToList();
        Assert.NotEmpty(activeBefore);

        // Run 2: restart and run to completion.
        var second = NewWorker(43);
        await second.StartAsync(cts.Token);
        await second.ExecuteTask!.WaitAsync(cts.Token);
        await second.StopAsync(CancellationToken.None);

        Assert.Equal(target, await repo.CountOutboxAsync(cts.Token));
        Assert.Equal(await SqlTestSupport.ScalarAsync(cs, "SELECT COUNT(*) FROM dbo.Outbox", cts.Token),
                     await SqlTestSupport.ScalarAsync(cs, "SELECT COUNT(DISTINCT SequenceNo) FROM dbo.Outbox", cts.Token));
        Assert.Equal(0, await SqlTestSupport.ScalarAsync(cs,
            "SELECT COUNT(*) FROM (SELECT EventId FROM dbo.Outbox GROUP BY EventId HAVING COUNT(*) > 1) d", cts.Token));
        Assert.Equal(0, await SqlTestSupport.ScalarAsync(cs,
            "SELECT COUNT(*) FROM (SELECT OrderId FROM dbo.Orders GROUP BY OrderId HAVING COUNT(*) > 1) d", cts.Token));

        // No sequence number allocated before the restart was reused: the restart only added numbers above the old max.
        Assert.Equal((int)(target - afterFirst), await SqlTestSupport.ScalarAsync(cs,
            $"SELECT COUNT(*) FROM dbo.Outbox WHERE SequenceNo > {maxAfterFirst}", cts.Token));

        // At least one order that was active before the restart continued its lifecycle after it.
        string ids = JsonSerializer.Serialize(activeBefore).Replace("'", "''");
        int continued = await SqlTestSupport.ScalarAsync(cs,
            $"SELECT COUNT(*) FROM dbo.Orders WHERE OrderId IN (SELECT [value] FROM OPENJSON('{ids}')) AND LastEventSequenceNo > {maxAfterFirst}",
            cts.Token);
        Assert.True(continued > 0, "no pre-restart active order advanced after the restart");
    }
}
