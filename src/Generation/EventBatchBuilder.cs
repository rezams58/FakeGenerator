using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;

namespace FakeOrderGenerator.Generation;

public sealed record NewOutboxEvent(
    Guid EventId, long SequenceNo, string AggregateKey, int KafkaPartition,
    string EventType, string Payload, DateTime CreatedAtUtc);

/// <summary>One SQL transaction worth of work: entity changes plus their Outbox events.</summary>
public sealed class EventBatch
{
    public List<EntityState> NewEntities { get; } = [];
    public List<EntityState> Transitioned { get; } = [];
    public List<NewOutboxEvent> Events { get; } = [];
}

/// <summary>
/// Mixes new entities and lifecycle transitions into ordered events (research R4). Keeps an active pool of
/// non-terminal entities, allocates strictly increasing global sequence numbers, emits at most one event
/// per entity per batch, and stops exactly at the target event count.
/// </summary>
public sealed class EventBatchBuilder
{
    private readonly IFakeEntityDefinition _definition;
    private readonly GeneratorConfig _config;
    private readonly int _partitionCount;
    private readonly Random _random;
    private readonly List<EntityState> _active;
    private long _nextSequence;
    private long _totalEvents;

    public EventBatchBuilder(IFakeEntityDefinition definition, GeneratorConfig config, int partitionCount,
        Random random, long maxSequenceNo, long existingEventCount, IEnumerable<EntityState>? activeEntities = null)
    {
        _definition = definition;
        _config = config;
        _partitionCount = partitionCount;
        _random = random;
        _nextSequence = maxSequenceNo + 1;
        _totalEvents = existingEventCount;
        _active = activeEntities?.Where(e => !definition.Lifecycle.IsTerminal(e.Status)).ToList() ?? [];
    }

    public long TotalEvents => _totalEvents;
    public int ActiveCount => _active.Count;
    public bool TargetReached => _totalEvents >= _config.TargetEventCount;

    /// <summary>Builds the next batch (empty only when the target is reached). Advances builder state.</summary>
    public EventBatch Build(DateTime utcNow)
    {
        var batch = new EventBatch();
        long remaining = _config.TargetEventCount - _totalEvents;
        int slots = (int)Math.Min(_config.BatchSize, Math.Max(0, remaining));
        var touched = new HashSet<Guid>();
        var lifecycle = _definition.Lifecycle;

        for (int i = 0; i < slots; i++)
        {
            bool poolFull = _active.Count >= _config.MaxActiveEntities;
            bool wantAdvance = _active.Count > 0 && (poolFull || _random.NextDouble() < _config.AdvanceProbability);

            EntityState? candidate = wantAdvance ? PickUntouched(touched) : null;
            if (candidate is null && poolFull) break; // pool full and every active entity already advanced in this batch

            long seq = _nextSequence++;
            DateTime at = utcNow;

            if (candidate is null)
            {
                var created = _definition.CreateNew(_random, seq, at);
                batch.NewEntities.Add(created);
                _active.Add(created);
                touched.Add(created.EntityId);
                batch.Events.Add(MakeEvent(created, null, created.Status, seq, at));
            }
            else
            {
                var step = lifecycle.NextTransition(candidate.Status, _random);
                string from = candidate.Status;
                candidate.Status = step.To;
                candidate.LastEventSequenceNo = seq;
                candidate.UpdatedAtUtc = at;
                touched.Add(candidate.EntityId);
                batch.Transitioned.Add(candidate);
                batch.Events.Add(MakeEvent(candidate, from, step.To, seq, at));
            }
        }

        // Terminal entities leave the pool (swap-remove keeps this O(n) per batch).
        for (int i = _active.Count - 1; i >= 0; i--)
            if (lifecycle.IsTerminal(_active[i].Status))
            {
                _active[i] = _active[^1];
                _active.RemoveAt(_active.Count - 1);
            }

        _totalEvents += batch.Events.Count;
        return batch;
    }

    private EntityState? PickUntouched(HashSet<Guid> touched)
    {
        for (int attempt = 0; attempt < 8; attempt++)
        {
            var e = _active[_random.Next(_active.Count)];
            if (!touched.Contains(e.EntityId)) return e;
        }
        int start = _random.Next(_active.Count);
        for (int k = 0; k < _active.Count; k++)
        {
            var e = _active[(start + k) % _active.Count];
            if (!touched.Contains(e.EntityId)) return e;
        }
        return null;
    }

    private NewOutboxEvent MakeEvent(EntityState entity, string? from, string to, long seq, DateTime at)
    {
        string key = _definition.PartitionKey(entity);
        return new NewOutboxEvent(
            Guid.NewGuid(), seq, key, Partitioning.GetPartition(key, _partitionCount),
            _definition.Name + to, _definition.BuildPayload(entity, from, to, at), at);
    }
}
