using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Generation;
using FakeOrderGenerator.Generation.Order;
using FakeOrderGenerator.Models;
using System.Text.Json;

namespace FakeGenerator.Tests.Unit;

public sealed class EventBatchBuilderTests
{
    private const int Partitions = 6;

    private static (EventBatchBuilder builder, GeneratorConfig cfg, OrderDefinition def) Make(
        int target, int batchSize, int maxActive = 10_000, int seed = 1, long maxSeq = 0, long existing = 0,
        IEnumerable<EntityState>? active = null)
    {
        var cfg = new GeneratorConfig
        {
            TargetEventCount = target, BatchSize = batchSize, MaxActiveEntities = maxActive,
            CustomerCount = 50, AdvanceProbability = 0.6
        };
        var def = new OrderDefinition(cfg);
        return (new EventBatchBuilder(def, cfg, Partitions, new Random(seed), maxSeq, existing, active), cfg, def);
    }

    private static List<EventBatch> RunToTarget(EventBatchBuilder b)
    {
        var batches = new List<EventBatch>();
        while (!b.TargetReached)
        {
            var batch = b.Build(DateTime.UtcNow);
            Assert.NotEmpty(batch.Events);
            batches.Add(batch);
        }
        return batches;
    }

    [Theory]
    [InlineData(1000, 100)]
    [InlineData(1001, 100)] // stops mid-batch
    [InlineData(37, 100)]   // target smaller than one batch
    [InlineData(500, 7)]
    public void Stops_exactly_at_target(int target, int batchSize)
    {
        var (b, _, _) = Make(target, batchSize);
        var batches = RunToTarget(b);
        Assert.Equal(target, batches.Sum(x => x.Events.Count));
        Assert.Equal(target, b.TotalEvents);
        Assert.True(b.Build(DateTime.UtcNow).Events.Count == 0, "no events after target");
    }

    [Fact]
    public void Stops_at_target_in_the_middle_of_a_lifecycle()
    {
        var (b, _, _) = Make(target: 3, batchSize: 1, seed: 3);
        RunToTarget(b);
        Assert.Equal(3, b.TotalEvents);
        // 3 events cannot finish a lifecycle that needs 4 events to reach Delivered, so an incomplete entity remains.
        Assert.True(b.ActiveCount > 0);
    }

    [Fact]
    public void Existing_events_count_toward_target()
    {
        var (b, _, _) = Make(target: 100, batchSize: 30, maxSeq: 60, existing: 60);
        var batches = RunToTarget(b);
        Assert.Equal(40, batches.Sum(x => x.Events.Count));
        Assert.Equal(61, batches[0].Events[0].SequenceNo);
    }

    [Fact]
    public void At_most_one_event_per_entity_per_batch_and_sequence_strictly_increases()
    {
        var (b, _, _) = Make(5000, 250, seed: 9);
        long last = 0;
        foreach (var batch in RunToTarget(b))
        {
            var keys = batch.Events.Select(e => JsonSerializer.Deserialize<OrderEventPayload>(e.Payload)!.OrderId).ToList();
            Assert.Equal(keys.Count, keys.Distinct().Count());
            foreach (var e in batch.Events)
            {
                Assert.True(e.SequenceNo > last, "sequence must strictly increase");
                last = e.SequenceNo;
            }
        }
    }

    [Fact]
    public void Same_aggregate_key_maps_to_same_partition_and_matches_partitioning()
    {
        var (b, _, _) = Make(5000, 500, seed: 4);
        var byKey = new Dictionary<string, int>();
        foreach (var e in RunToTarget(b).SelectMany(x => x.Events))
        {
            Assert.Equal(Partitioning.GetPartition(e.AggregateKey, Partitions), e.KafkaPartition);
            if (byKey.TryGetValue(e.AggregateKey, out int p)) Assert.Equal(p, e.KafkaPartition);
            else byKey[e.AggregateKey] = e.KafkaPartition;
        }
    }

    [Fact]
    public void Per_order_events_follow_declared_lifecycle_order()
    {
        var (b, _, def) = Make(20_000, 500, seed: 11);
        var perOrder = new Dictionary<Guid, List<OrderEventPayload>>();
        foreach (var e in RunToTarget(b).SelectMany(x => x.Events))
        {
            var p = JsonSerializer.Deserialize<OrderEventPayload>(e.Payload)!;
            Assert.Equal(e.SequenceNo, p.SequenceNo);
            Assert.Equal("Order" + p.Status, e.EventType);
            (perOrder.TryGetValue(p.OrderId, out var l) ? l : perOrder[p.OrderId] = []).Add(p);
        }
        foreach (var events in perOrder.Values)
        {
            Assert.Null(events[0].PreviousStatus);
            Assert.Equal(def.Lifecycle.InitialState, events[0].Status);
            for (int i = 1; i < events.Count; i++)
            {
                Assert.True(def.Lifecycle.IsValidTransition(events[i - 1].Status, events[i].Status));
                Assert.Equal(events[i - 1].Status, events[i].PreviousStatus);
                Assert.True(events[i].SequenceNo > events[i - 1].SequenceNo);
            }
        }
    }

    [Fact]
    public void Active_pool_never_exceeds_cap()
    {
        var (b, cfg, _) = Make(5000, 50, maxActive: 20, seed: 5);
        while (!b.TargetReached)
        {
            Assert.NotEmpty(b.Build(DateTime.UtcNow).Events);
            Assert.True(b.ActiveCount <= cfg.MaxActiveEntities);
        }
    }

    [Fact]
    public void Resumes_active_entities_and_continues_their_lifecycle()
    {
        var (b1, cfg, def) = Make(10, 10, seed: 2);
        var first = b1.Build(DateTime.UtcNow);
        var pending = first.NewEntities.Concat(first.Transitioned).DistinctBy(e => e.EntityId)
            .Where(e => !def.Lifecycle.IsTerminal(e.Status)).ToList();
        Assert.NotEmpty(pending);

        cfg.TargetEventCount = 200;
        var b2 = new EventBatchBuilder(def, cfg, Partitions, new Random(8), 10, 10, pending);
        var all = RunToTarget(b2).SelectMany(x => x.Events).ToList();
        Assert.Equal(190, all.Count);
        Assert.All(all, e => Assert.True(e.SequenceNo > 10));
        var resumedIds = pending.Select(p => p.EntityId).ToHashSet();
        Assert.Contains(all, e => resumedIds.Contains(JsonSerializer.Deserialize<OrderEventPayload>(e.Payload)!.OrderId));
    }
}
