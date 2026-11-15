using FakeOrderGenerator.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Data;
using System.Text.Json;

namespace FakeOrderGenerator.Data;

public sealed record PartitionLease(int PartitionId, Guid OwnerId, long FencingToken, DateTime LeaseUntilUtc);

public sealed class SqlOrderRepository
{
    private readonly string _connectionString;

    public SqlOrderRepository(IOptions<ConnectionSettings> connectionOption)
        => _connectionString = connectionOption.Value.SqlServer;

    public async Task<long> GetMaxSequenceAsync(CancellationToken ct)
    {
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand("SELECT ISNULL(MAX(SequenceNo), 0) FROM dbo.Outbox;", cn);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    public async Task InsertOrdersAsync(IReadOnlyList<Order> orders, int partitionCount, CancellationToken ct)
    {
        if (orders.Count == 0) return;

        var rows = orders.Select(o => new
        {
            o.OrderId, o.SequenceNo, o.OrderNumber, o.CustomerId, o.Amount, o.CreatedAtUtc,
            EventId = Guid.NewGuid(),
            KafkaPartition = Partitioning.GetPartition(o.CustomerId, partitionCount),
            EventType = "OrderCreated",
            Payload = JsonSerializer.Serialize(o)
        }).ToList();

        string json = JsonSerializer.Serialize(rows);
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        try
        {
            const string insertOrders = """
                INSERT INTO dbo.Orders (OrderId, SequenceNo, OrderNumber, CustomerId, Amount, CreatedAtUtc)
                SELECT OrderId, SequenceNo, OrderNumber, CustomerId, Amount, CreatedAtUtc
                FROM OPENJSON(@json) WITH
                (
                    OrderId UNIQUEIDENTIFIER '$.OrderId', SequenceNo BIGINT '$.SequenceNo',
                    OrderNumber NVARCHAR(40) '$.OrderNumber', CustomerId NVARCHAR(40) '$.CustomerId',
                    Amount DECIMAL(18,2) '$.Amount', CreatedAtUtc DATETIME2 '$.CreatedAtUtc'
                );
                """;
            await using (var cmd = new SqlCommand(insertOrders, cn, tx))
            {
                cmd.Parameters.Add("@json", SqlDbType.NVarChar, -1).Value = json;
                await cmd.ExecuteNonQueryAsync(ct);
            }

            const string insertOutbox = """
                INSERT INTO dbo.Outbox
                    (EventId, SequenceNo, AggregateKey, KafkaPartition, EventType, Payload, CreatedAtUtc)
                SELECT EventId, SequenceNo, CustomerId, KafkaPartition, EventType, Payload, CreatedAtUtc
                FROM OPENJSON(@json) WITH
                (
                    EventId UNIQUEIDENTIFIER '$.EventId', SequenceNo BIGINT '$.SequenceNo',
                    CustomerId NVARCHAR(40) '$.CustomerId', KafkaPartition INT '$.KafkaPartition',
                    EventType NVARCHAR(100) '$.EventType', Payload NVARCHAR(MAX) '$.Payload',
                    CreatedAtUtc DATETIME2 '$.CreatedAtUtc'
                );
                """;
            await using (var cmd = new SqlCommand(insertOutbox, cn, tx))
            {
                cmd.Parameters.Add("@json", SqlDbType.NVarChar, -1).Value = json;
                await cmd.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    public async Task ValidateLeasePartitionsAsync(int partitionCount, CancellationToken ct)
    {
        const string sql = "SELECT COUNT(*) FROM dbo.PublisherPartitionLease WHERE PartitionId >= 0 AND PartitionId < @PartitionCount;";
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@PartitionCount", SqlDbType.Int).Value = partitionCount;
        int found = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        if (found != partitionCount)
            throw new InvalidOperationException($"PublisherPartitionLease has {found} rows for partitions 0..{partitionCount - 1}; expected {partitionCount}. Apply sql/03-multinstance-publisher.sql and align it with Kafka:PartitionCount.");
    }

    public async Task<IReadOnlyList<PartitionLease>> ClaimPartitionsAsync(
        Guid ownerId, int maxPartitions, int leaseSeconds, CancellationToken ct)
    {
        const string sql = """
            ;WITH candidates AS
            (
                SELECT TOP (@MaxPartitions) PartitionId
                FROM dbo.PublisherPartitionLease WITH (UPDLOCK, READPAST, ROWLOCK, READCOMMITTEDLOCK)
                WHERE LeaseUntilUtc <= SYSUTCDATETIME()
                ORDER BY PartitionId
            )
            UPDATE lease
            SET OwnerId = @OwnerId,
                LeaseUntilUtc = DATEADD(SECOND, @LeaseSeconds, SYSUTCDATETIME()),
                FencingToken = FencingToken + 1,
                UpdatedAtUtc = SYSUTCDATETIME()
            OUTPUT inserted.PartitionId, inserted.OwnerId, inserted.FencingToken, inserted.LeaseUntilUtc
            FROM dbo.PublisherPartitionLease AS lease
            INNER JOIN candidates AS c ON c.PartitionId = lease.PartitionId;
            """;

        var result = new List<PartitionLease>();
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@MaxPartitions", SqlDbType.Int).Value = maxPartitions;
        cmd.Parameters.Add("@OwnerId", SqlDbType.UniqueIdentifier).Value = ownerId;
        cmd.Parameters.Add("@LeaseSeconds", SqlDbType.Int).Value = leaseSeconds;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(new PartitionLease(reader.GetInt32(0), reader.GetGuid(1), reader.GetInt64(2), reader.GetDateTime(3)));
        return result;
    }

    public async Task<bool> RenewLeaseAsync(PartitionLease lease, int leaseSeconds, CancellationToken ct)
    {
        const string sql = """
            UPDATE dbo.PublisherPartitionLease
            SET LeaseUntilUtc = DATEADD(SECOND, @LeaseSeconds, SYSUTCDATETIME()), UpdatedAtUtc = SYSUTCDATETIME()
            WHERE PartitionId = @PartitionId AND OwnerId = @OwnerId AND FencingToken = @FencingToken
              AND LeaseUntilUtc > SYSUTCDATETIME();
            SELECT @@ROWCOUNT;
            """;
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@LeaseSeconds", SqlDbType.Int).Value = leaseSeconds;
        cmd.Parameters.Add("@PartitionId", SqlDbType.Int).Value = lease.PartitionId;
        cmd.Parameters.Add("@OwnerId", SqlDbType.UniqueIdentifier).Value = lease.OwnerId;
        cmd.Parameters.Add("@FencingToken", SqlDbType.BigInt).Value = lease.FencingToken;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)) == 1;
    }

    public async Task ReleaseLeaseAsync(PartitionLease lease, CancellationToken ct)
    {
        const string sql = """
            UPDATE dbo.PublisherPartitionLease
            SET LeaseUntilUtc = SYSUTCDATETIME(), UpdatedAtUtc = SYSUTCDATETIME()
            WHERE PartitionId = @PartitionId AND OwnerId = @OwnerId AND FencingToken = @FencingToken;
            """;
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@PartitionId", SqlDbType.Int).Value = lease.PartitionId;
        cmd.Parameters.Add("@OwnerId", SqlDbType.UniqueIdentifier).Value = lease.OwnerId;
        cmd.Parameters.Add("@FencingToken", SqlDbType.BigInt).Value = lease.FencingToken;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<List<OutboxMessage>> ReadPendingAsync(PartitionLease lease, int batchSize, int maxRetryCount, CancellationToken ct)
    {
        const string sql = """
            SELECT TOP (@BatchSize) EventId, SequenceNo, AggregateKey, KafkaPartition, EventType, Payload
            FROM dbo.Outbox o
            WHERE o.PublishedAtUtc IS NULL AND o.DeadLetterAtUtc IS NULL
              AND o.RetryCount < @MaxRetryCount AND o.KafkaPartition = @PartitionId
              AND EXISTS
              (
                  SELECT 1 FROM dbo.PublisherPartitionLease l
                  WHERE l.PartitionId = @PartitionId AND l.OwnerId = @OwnerId
                    AND l.FencingToken = @FencingToken AND l.LeaseUntilUtc > SYSUTCDATETIME()
              )
            ORDER BY o.SequenceNo;
            """;
        var result = new List<OutboxMessage>();
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
        cmd.Parameters.Add("@MaxRetryCount", SqlDbType.Int).Value = maxRetryCount;
        cmd.Parameters.Add("@PartitionId", SqlDbType.Int).Value = lease.PartitionId;
        cmd.Parameters.Add("@OwnerId", SqlDbType.UniqueIdentifier).Value = lease.OwnerId;
        cmd.Parameters.Add("@FencingToken", SqlDbType.BigInt).Value = lease.FencingToken;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(new OutboxMessage(reader.GetGuid(0), reader.GetInt64(1), reader.GetString(2),
                reader.GetInt32(3), reader.GetString(4), reader.GetString(5)));
        return result;
    }

    public async Task<int> MarkPublishedAsync(IEnumerable<Guid> eventIds, PartitionLease lease, CancellationToken ct)
    {
        var ids = eventIds.Distinct().ToArray();
        if (ids.Length == 0) return 0;
        const string sql = """
            UPDATE o SET PublishedAtUtc = SYSUTCDATETIME(), LastError = NULL
            FROM dbo.Outbox AS o
            INNER JOIN OPENJSON(@ids) AS j ON o.EventId = TRY_CONVERT(UNIQUEIDENTIFIER, j.[value])
            WHERE o.PublishedAtUtc IS NULL AND o.KafkaPartition = @PartitionId
              AND EXISTS
              (
                  SELECT 1 FROM dbo.PublisherPartitionLease l
                  WHERE l.PartitionId = @PartitionId AND l.OwnerId = @OwnerId
                    AND l.FencingToken = @FencingToken AND l.LeaseUntilUtc > SYSUTCDATETIME()
              );
            SELECT @@ROWCOUNT;
            """;
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@ids", SqlDbType.NVarChar, -1).Value = JsonSerializer.Serialize(ids);
        cmd.Parameters.Add("@PartitionId", SqlDbType.Int).Value = lease.PartitionId;
        cmd.Parameters.Add("@OwnerId", SqlDbType.UniqueIdentifier).Value = lease.OwnerId;
        cmd.Parameters.Add("@FencingToken", SqlDbType.BigInt).Value = lease.FencingToken;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    public async Task<bool> MarkFailedAsync(OutboxMessage item, PartitionLease lease, string error,
        int maxRetryCount, CancellationToken ct)
    {
        const string sql = """
            UPDATE o
            SET RetryCount = RetryCount + 1,
                LastError = LEFT(@Error, 2000),
                DeadLetterAtUtc = CASE WHEN RetryCount + 1 >= @MaxRetryCount THEN SYSUTCDATETIME() ELSE NULL END
            FROM dbo.Outbox o
            WHERE o.EventId = @EventId AND o.KafkaPartition = @PartitionId
              AND o.PublishedAtUtc IS NULL AND o.DeadLetterAtUtc IS NULL
              AND EXISTS
              (
                  SELECT 1 FROM dbo.PublisherPartitionLease l
                  WHERE l.PartitionId = @PartitionId AND l.OwnerId = @OwnerId
                    AND l.FencingToken = @FencingToken AND l.LeaseUntilUtc > SYSUTCDATETIME()
              );
            SELECT @@ROWCOUNT;
            """;
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@Error", SqlDbType.NVarChar, 2000).Value = error;
        cmd.Parameters.Add("@MaxRetryCount", SqlDbType.Int).Value = maxRetryCount;
        cmd.Parameters.Add("@EventId", SqlDbType.UniqueIdentifier).Value = item.EventId;
        cmd.Parameters.Add("@PartitionId", SqlDbType.Int).Value = lease.PartitionId;
        cmd.Parameters.Add("@OwnerId", SqlDbType.UniqueIdentifier).Value = lease.OwnerId;
        cmd.Parameters.Add("@FencingToken", SqlDbType.BigInt).Value = lease.FencingToken;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)) == 1;
    }
}
