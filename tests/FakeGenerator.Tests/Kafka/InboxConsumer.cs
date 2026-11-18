using System.Text;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace FakeGenerator.Tests.Kafka;

/// <summary>
/// Reference consumer for contracts/consumer-inbox.md: read_committed, manual offset commit, Inbox PK on EventId,
/// Inbox insert + business effect in ONE SQL transaction, PK violation (2627/2601) = duplicate = skip,
/// Kafka offset committed only after the SQL commit.
/// </summary>
internal sealed class InboxConsumer
{
    private readonly string _bootstrap, _topic, _cs;
    public long RecordsSeen { get; private set; }
    public long DuplicatesSkipped { get; private set; }

    public InboxConsumer(string bootstrap, string topic, string connectionString)
    {
        _bootstrap = bootstrap; _topic = topic; _cs = connectionString;
    }

    public static async Task EnsureTablesAsync(string cs, CancellationToken ct)
    {
        const string sql = """
            IF OBJECT_ID('dbo.Inbox') IS NULL CREATE TABLE dbo.Inbox (EventId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, ReceivedAtUtc DATETIME2(3) NOT NULL);
            IF OBJECT_ID('dbo.InboxEffect') IS NULL CREATE TABLE dbo.InboxEffect (Id BIGINT IDENTITY PRIMARY KEY, EventId UNIQUEIDENTIFIER NOT NULL);
            DELETE dbo.InboxEffect; DELETE dbo.Inbox;
            """;
        await using var cn = new SqlConnection(cs);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Consumes until <paramref name="stopWhen"/> is true (checked after each poll) or the timeout passes.</summary>
    public async Task RunAsync(string groupId, Func<bool> stopWhen, TimeSpan timeout, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _bootstrap, GroupId = groupId, EnableAutoCommit = false,
            IsolationLevel = IsolationLevel.ReadCommitted, AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(_topic);
        var until = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < until && !stopWhen())
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(1));
            if (cr is null) continue;
            if (!cr.Message.Headers.TryGetLastBytes("event-id", out var bytes) || !Guid.TryParse(Encoding.UTF8.GetString(bytes), out var eventId))
                continue;
            RecordsSeen++;
            if (!await ApplyOnceAsync(eventId, ct)) DuplicatesSkipped++;
            consumer.Commit(cr); // only after the SQL transaction committed
        }
    }

    private async Task<bool> ApplyOnceAsync(Guid eventId, CancellationToken ct)
    {
        await using var cn = new SqlConnection(_cs);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        try
        {
            await using (var ins = new SqlCommand("INSERT dbo.Inbox(EventId, ReceivedAtUtc) VALUES(@id, SYSUTCDATETIME())", cn, tx))
            {
                ins.Parameters.AddWithValue("@id", eventId);
                await ins.ExecuteNonQueryAsync(ct);
            }
            await using (var effect = new SqlCommand("INSERT dbo.InboxEffect(EventId) VALUES(@id)", cn, tx))
            {
                effect.Parameters.AddWithValue("@id", eventId);
                await effect.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
            return true;
        }
        catch (SqlException ex) when (ex.Number is 2627 or 2601)
        {
            await tx.RollbackAsync(ct);
            return false;
        }
    }
}
