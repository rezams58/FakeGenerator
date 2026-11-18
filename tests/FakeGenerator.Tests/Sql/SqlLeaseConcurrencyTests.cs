using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace FakeGenerator.Tests.Sql;

[Collection("SqlLease")]
public sealed class SqlLeaseConcurrencyTests
{
    [SqlFact]
    public async Task Concurrent_claims_do_not_assign_the_same_partition_to_two_owners_and_stale_token_cannot_renew()
    {
        // Set FAKEGEN_TEST_SQL to a disposable database after applying sql/01-schema.sql and
        // sql/03-multinstance-publisher.sql. The test deliberately does not create/drop schema.
        string connectionString = TestEnv.Sql!;

        var repository = new SqlOrderRepository(Options.Create(new ConnectionSettings { SqlServer = connectionString }));
        Guid ownerA = Guid.NewGuid(), ownerB = Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using (var reset = new SqlConnection(connectionString))
        {
            await reset.OpenAsync(cts.Token);
            await using var cmd = new SqlCommand("UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc = SYSUTCDATETIME();", reset);
            await cmd.ExecuteNonQueryAsync(cts.Token);
        }

        var claims = await Task.WhenAll(
            repository.ClaimPartitionsAsync(ownerA, 6, 30, cts.Token),
            repository.ClaimPartitionsAsync(ownerB, 6, 30, cts.Token));
        var claimed = claims.SelectMany(x => x).ToArray();
        Assert.Equal(claimed.Length, claimed.Select(x => x.PartitionId).Distinct().Count());
        Assert.Equal(6, claimed.Length);

        var stale = claimed[0];
        await using (var cn = new SqlConnection(connectionString))
        {
            await cn.OpenAsync(cts.Token);
            await using var cmd = new SqlCommand("UPDATE dbo.PublisherPartitionLease SET LeaseUntilUtc = DATEADD(SECOND, -1, SYSUTCDATETIME()) WHERE PartitionId = @p;", cn);
            cmd.Parameters.AddWithValue("@p", stale.PartitionId);
            await cmd.ExecuteNonQueryAsync(cts.Token);
        }

        var takeover = await repository.ClaimPartitionsAsync(Guid.NewGuid(), 1, 30, cts.Token);
        Assert.Contains(takeover, x => x.PartitionId == stale.PartitionId && x.FencingToken > stale.FencingToken);
        Assert.False(await repository.RenewLeaseAsync(stale, 30, cts.Token));

        foreach (var lease in claimed.Concat(takeover))
            await repository.ReleaseLeaseAsync(lease, cts.Token);
    }
}
