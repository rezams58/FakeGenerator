using FakeGenerator.Tests.Unit.SampleEntity;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Generation;

namespace FakeGenerator.Tests.Unit;

public sealed class NewEntityPipelineTests
{
    private static List<NewOutboxEvent> RunAll(int target, out EventBatchBuilder builder, out SampleEntityDefinition def)
    {
        def = new SampleEntityDefinition();
        var cfg = new GeneratorConfig { TargetEventCount = target, BatchSize = 25, AdvanceProbability = 0.6, MaxActiveEntities = 50 };
        builder = new EventBatchBuilder(def, cfg, 6, new Random(7), 0, 0);
        var events = new List<NewOutboxEvent>();
        int guard = 0;
        while (!builder.TargetReached && guard++ < 10_000) events.AddRange(builder.Build(DateTime.UtcNow).Events);
        return events;
    }

    [Fact]
    public void Sample_entity_runs_through_unchanged_pipeline_and_stops_at_target()
    {
        var events = RunAll(300, out var builder, out _);
        Assert.Equal(300, events.Count);
        Assert.True(builder.TargetReached);
        Assert.Equal(events.Count, events.Select(e => e.SequenceNo).Distinct().Count());
    }

    [Fact]
    public void Events_use_the_sample_partition_key_and_its_own_event_types()
    {
        var events = RunAll(200, out _, out _);
        Assert.All(events, e =>
        {
            Assert.StartsWith("USR-", e.AggregateKey);
            Assert.Equal(Partitioning.GetPartition(e.AggregateKey, 6), e.KafkaPartition);
            Assert.Contains(e.EventType, new[] { "TicketOpen", "TicketInProgress", "TicketClosed" });
        });
    }

    [Fact]
    public void Per_entity_event_types_follow_valid_paths()
    {
        var events = RunAll(400, out _, out _);
        var order = new[] { "TicketOpen", "TicketInProgress", "TicketClosed" };
        foreach (var g in events.GroupBy(e => System.Text.Json.JsonDocument.Parse(e.Payload).RootElement.GetProperty("EntityId").GetGuid()))
        {
            var idx = g.OrderBy(e => e.SequenceNo).Select(e => Array.IndexOf(order, e.EventType)).ToList();
            Assert.Equal(0, idx[0]);
            Assert.True(idx.Zip(idx.Skip(1)).All(p => p.Second == p.First + 1), "path must advance one state at a time");
        }
    }
}
