using System.Text;
using System.Text.Json;
using System.Diagnostics;
using Confluent.Kafka;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Generation;
using Microsoft.Data.SqlClient;

namespace FakeOrderGenerator.Reporting;

/// <summary>
/// Reads the whole topic with a read_committed consumer and checks it against SQL (contracts/consumer-inbox.md).
/// The checks themselves live in <see cref="StreamVerifierCore"/>.
/// </summary>
public sealed class StreamVerifier
{
    private readonly KafkaSettings _kafka;
    private readonly string _connectionString;

    public StreamVerifier(KafkaSettings kafka, string connectionString)
    {
        _kafka = kafka;
        _connectionString = connectionString;
    }

    public async Task<VerificationResult> VerifyAsync(LifecycleDefinition lifecycle, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var deadEntities = new HashSet<Guid>();
        var published = new HashSet<Guid>();
        await using (var cn = new SqlConnection(_connectionString))
        {
            await cn.OpenAsync(ct);
            await using (var cmd = new SqlCommand(
                "SELECT DISTINCT TRY_CAST(JSON_VALUE(Payload, '$.OrderId') AS UNIQUEIDENTIFIER) FROM dbo.Outbox WHERE DeadLetterAtUtc IS NOT NULL", cn))
            await using (var r = await cmd.ExecuteReaderAsync(ct))
                while (await r.ReadAsync(ct)) if (!r.IsDBNull(0)) deadEntities.Add(r.GetGuid(0));

            await using (var cmd = new SqlCommand("SELECT EventId FROM dbo.Outbox WHERE PublishedAtUtc IS NOT NULL", cn) { CommandTimeout = 300 })
            await using (var r = await cmd.ExecuteReaderAsync(ct))
                while (await r.ReadAsync(ct)) published.Add(r.GetGuid(0));
        }

        var stream = ReadStream(ct);
        ct.ThrowIfCancellationRequested();
        return StreamVerifierCore.Verify(stream.Records, lifecycle, deadEntities, published) with { Complete = stream.Complete };
    }

    private (List<StreamRecord> Records, bool Complete) ReadStream(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = $"fakegen-verify-{Guid.NewGuid():N}",
            IsolationLevel = IsolationLevel.ReadCommitted,
            EnableAutoCommit = false,
            EnablePartitionEof = true,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        var partitions = Enumerable.Range(0, _kafka.PartitionCount).Select(p => new TopicPartition(_kafka.Topic, p)).ToList();
        var ends = partitions.ToDictionary(tp => tp, tp => consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(30)).High.Value);
        consumer.Assign(partitions.Select(tp => new TopicPartitionOffset(tp, Offset.Beginning)));

        return ReadCapturedStream(consumer, ends, ct);
    }

    public static (List<StreamRecord> Records, bool Complete) ReadCapturedStream(IConsumer<string, string> consumer,
        IReadOnlyDictionary<TopicPartition, long> ends, CancellationToken ct, TimeSpan? idleTimeout = null)
    {
        ct.ThrowIfCancellationRequested();
        var result = new List<StreamRecord>();
        var pending = new HashSet<TopicPartition>(ends.Keys.Where(tp => ends[tp] > 0));
        var idle = Stopwatch.StartNew();
        var positions = new Dictionary<TopicPartition, long>();
        // Completed partitions must not keep supplying newly appended records while others catch up.
        var empty = ends.Keys.Where(tp => !pending.Contains(tp)).ToList();
        if (empty.Count > 0) consumer.Pause(empty);
        while (pending.Count > 0)
        {
            ct.ThrowIfCancellationRequested();
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            ct.ThrowIfCancellationRequested();
            if (cr is not null && pending.Contains(cr.TopicPartition))
            {
                if (!cr.IsPartitionEOF && cr.Offset.Value < ends[cr.TopicPartition])
                {
                    idle.Restart();
                    var rec = Parse(cr);
                    if (rec is not null) result.Add(rec);
                }
                // EOF includes skipped transaction-control/aborted offsets, unlike the last data record.
                if (cr.IsPartitionEOF && cr.Offset.Value >= ends[cr.TopicPartition])
                {
                    pending.Remove(cr.TopicPartition);
                    consumer.Pause([cr.TopicPartition]);
                    idle.Restart();
                }
            }
            foreach (var tp in pending.ToList())
            {
                long position = consumer.Position(tp).Value;
                if (position >= 0 && (!positions.TryGetValue(tp, out long previous) || position > previous))
                {
                    positions[tp] = position;
                    idle.Restart();
                }
                if (position >= ends[tp])
                {
                    pending.Remove(tp);
                    consumer.Pause([tp]);
                }
            }
            if (idle.Elapsed >= (idleTimeout ?? TimeSpan.FromSeconds(30))) break;
        }
        ct.ThrowIfCancellationRequested();
        return (result, pending.Count == 0);
    }

    private static StreamRecord? Parse(ConsumeResult<string, string> cr)
    {
        string? Header(string name) => cr.Message.Headers.TryGetLastBytes(name, out var b) ? Encoding.UTF8.GetString(b) : null;
        if (!Guid.TryParse(Header("event-id"), out var eventId) || !long.TryParse(Header("sequence-no"), out long seq)) return null;
        using var doc = JsonDocument.Parse(cr.Message.Value);
        var root = doc.RootElement;
        if (!root.TryGetProperty("OrderId", out var id) || !Guid.TryParse(id.GetString(), out var entityId)) return null;
        string? prev = root.TryGetProperty("PreviousStatus", out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;
        string status = root.GetProperty("Status").GetString() ?? "";
        return new StreamRecord(cr.Partition.Value, cr.Offset.Value, eventId, seq, entityId, prev, status);
    }
}
