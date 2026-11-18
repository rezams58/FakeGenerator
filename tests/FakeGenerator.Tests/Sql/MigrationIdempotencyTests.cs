using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Sql;

public sealed class MigrationIdempotencyTests
{
    [SqlFact]
    public async Task Lifecycle_migration_and_lease_ensure_are_idempotent()
    {
        string cs = TestEnv.Sql!;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var repo = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = cs }));

        foreach (var f in new[] { "01-schema.sql", "03-multinstance-publisher.sql" })
            await SqlTestSupport.RunScriptAsync(cs, Path.Combine(SqlTestSupport.SqlDir(), f), cts.Token);

        for (int i = 0; i < 2; i++)
        {
            await SqlTestSupport.RunScriptAsync(cs, Path.Combine(SqlTestSupport.SqlDir(), "04-lifecycle.sql"), cts.Token);
            await repo.EnsureLeasePartitionsAsync(6, cts.Token);
        }

        Assert.Equal(3, await SqlTestSupport.ScalarAsync(cs,
            "SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Orders') AND name IN ('Status','LastEventSequenceNo','UpdatedAtUtc')", cts.Token));
        Assert.Equal(1, await SqlTestSupport.ScalarAsync(cs,
            "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Orders') AND name = 'IX_Orders_ActiveStatus'", cts.Token));
        Assert.Equal(6, await SqlTestSupport.ScalarAsync(cs,
            "SELECT COUNT(*) FROM dbo.PublisherPartitionLease WHERE PartitionId BETWEEN 0 AND 5", cts.Token));
        await repo.ValidateLeasePartitionsAsync(6, cts.Token);
    }
}
