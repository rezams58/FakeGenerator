using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using FakeOrderGenerator.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace FakeOrderGenerator.Generation.Order;

/// <summary>Writes <c>dbo.Orders</c> inside the shared generator transaction.</summary>
public sealed class OrderWriter : IEntityWriter
{
    private readonly string _connectionString;
    private readonly LifecycleDefinition _lifecycle;

    public OrderWriter(IOptions<ConnectionSettings> connection, OrderDefinition definition)
    {
        _connectionString = connection.Value.SqlServer;
        _lifecycle = definition.Lifecycle;
    }

    public string EntityName => OrderDefinition.EntityName;

    private static string Ts(DateTime d) => d.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture);

    public async Task InsertNewAsync(DbTransaction? transaction, IReadOnlyList<EntityState> entities, CancellationToken ct)
    {
        if (entities.Count == 0) return;
        var tx = Require(transaction);
        var rows = entities.Select(e =>
        {
            var o = (Data.Order)e.Data;
            return new
            {
                o.OrderId, SequenceNo = o.SequenceNo, o.OrderNumber, o.CustomerId, o.Amount,
                CreatedAtUtc = Ts(o.CreatedAtUtc), e.Status, e.LastEventSequenceNo, UpdatedAtUtc = Ts(e.UpdatedAtUtc)
            };
        });
        const string sql = """
            INSERT INTO dbo.Orders
                (OrderId, SequenceNo, OrderNumber, CustomerId, Amount, CreatedAtUtc, Status, LastEventSequenceNo, UpdatedAtUtc)
            SELECT OrderId, SequenceNo, OrderNumber, CustomerId, Amount, CreatedAtUtc, Status, LastEventSequenceNo, UpdatedAtUtc
            FROM OPENJSON(@json) WITH
            (
                OrderId UNIQUEIDENTIFIER '$.OrderId', SequenceNo BIGINT '$.SequenceNo',
                OrderNumber NVARCHAR(40) '$.OrderNumber', CustomerId NVARCHAR(40) '$.CustomerId',
                Amount DECIMAL(18,2) '$.Amount', CreatedAtUtc DATETIME2(3) '$.CreatedAtUtc',
                Status NVARCHAR(30) '$.Status', LastEventSequenceNo BIGINT '$.LastEventSequenceNo',
                UpdatedAtUtc DATETIME2(3) '$.UpdatedAtUtc'
            );
            """;
        await ExecuteJsonAsync(tx, sql, JsonSerializer.Serialize(rows), ct);
    }

    public async Task ApplyTransitionAsync(DbTransaction? transaction, IReadOnlyList<EntityState> entities, CancellationToken ct)
    {
        if (entities.Count == 0) return;
        var tx = Require(transaction);
        var rows = entities.Select(e => new
        {
            OrderId = e.EntityId, e.Status, e.LastEventSequenceNo, UpdatedAtUtc = Ts(e.UpdatedAtUtc)
        });
        const string sql = """
            UPDATE o
            SET Status = j.Status, LastEventSequenceNo = j.LastEventSequenceNo, UpdatedAtUtc = j.UpdatedAtUtc
            FROM dbo.Orders AS o
            INNER JOIN OPENJSON(@json) WITH
            (
                OrderId UNIQUEIDENTIFIER '$.OrderId', Status NVARCHAR(30) '$.Status',
                LastEventSequenceNo BIGINT '$.LastEventSequenceNo', UpdatedAtUtc DATETIME2(3) '$.UpdatedAtUtc'
            ) AS j ON j.OrderId = o.OrderId;
            """;
        int affected = await ExecuteJsonAsync(tx, sql, JsonSerializer.Serialize(rows), ct);
        if (affected != entities.Count)
            throw new InvalidOperationException($"Order transition updated {affected} rows; expected {entities.Count}.");
    }

    public async Task<IReadOnlyList<EntityState>> LoadActiveAsync(CancellationToken ct)
    {
        const string sql = """
            SELECT OrderId, SequenceNo, OrderNumber, CustomerId, Amount, CreatedAtUtc, Status,
                   ISNULL(LastEventSequenceNo, SequenceNo), ISNULL(UpdatedAtUtc, CreatedAtUtc)
            FROM dbo.Orders
            WHERE Status NOT IN (SELECT [value] FROM OPENJSON(@terminal));
            """;
        var result = new List<EntityState>();
        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.Add("@terminal", SqlDbType.NVarChar, -1).Value = JsonSerializer.Serialize(_lifecycle.TerminalStates);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var order = new Data.Order(r.GetGuid(0), r.GetInt64(1), r.GetString(2), r.GetString(3), r.GetDecimal(4),
                DateTime.SpecifyKind(r.GetDateTime(5), DateTimeKind.Utc));
            result.Add(new EntityState
            {
                EntityId = order.OrderId,
                PartitionKey = order.CustomerId,
                Status = r.GetString(6),
                CreationSequenceNo = order.SequenceNo,
                LastEventSequenceNo = r.GetInt64(7),
                UpdatedAtUtc = DateTime.SpecifyKind(r.GetDateTime(8), DateTimeKind.Utc),
                Data = order
            });
        }
        return result;
    }

    private static SqlTransaction Require(DbTransaction? tx) =>
        tx as SqlTransaction ?? throw new InvalidOperationException("OrderWriter requires the shared SqlTransaction.");

    private static async Task<int> ExecuteJsonAsync(SqlTransaction tx, string sql, string json, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(sql, tx.Connection, tx);
        cmd.Parameters.Add("@json", SqlDbType.NVarChar, -1).Value = json;
        return await cmd.ExecuteNonQueryAsync(ct);
    }
}
