using System.Data.Common;
using System.Text.Json;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Generation;

namespace FakeGenerator.Tests.Unit.SampleEntity;

/// <summary>Test-only "Ticket" entity: different partition key (Assignee) and a 3-state lifecycle. Used to prove SC-008.</summary>
internal sealed class SampleEntityDefinition : IFakeEntityDefinition
{
    public sealed record TicketData(string Assignee);

    public SampleEntityDefinition()
    {
        Lifecycle = new LifecycleDefinition(new LifecycleOptions
        {
            InitialState = "Open",
            States = ["Open", "InProgress", "Closed"],
            TerminalStates = ["Closed"],
            Transitions =
            [
                new TransitionOption { From = "Open", To = "InProgress", Weight = 1 },
                new TransitionOption { From = "InProgress", To = "Closed", Weight = 1 }
            ]
        });
    }

    public string Name => "Ticket";
    public LifecycleDefinition Lifecycle { get; }
    public IReadOnlyList<string> PartitionKeyFields => ["Assignee"];

    public EntityState CreateNew(Random random, long sequenceNo, DateTime utcNow) => new()
    {
        EntityId = Guid.NewGuid(),
        PartitionKey = $"USR-{random.Next(1, 8):D3}",
        Status = Lifecycle.InitialState,
        CreationSequenceNo = sequenceNo,
        LastEventSequenceNo = sequenceNo,
        UpdatedAtUtc = utcNow,
        Data = new TicketData("x")
    };

    public string PartitionKey(EntityState entity) => entity.PartitionKey;

    public string BuildPayload(EntityState entity, string? fromState, string toState, DateTime occurredAtUtc) =>
        JsonSerializer.Serialize(new { entity.EntityId, From = fromState, To = toState, Assignee = entity.PartitionKey });
}

internal sealed class SampleEntityWriter : IEntityWriter
{
    public Dictionary<Guid, EntityState> Store { get; } = [];
    public string EntityName => "Ticket";

    public Task InsertNewAsync(DbTransaction? transaction, IReadOnlyList<EntityState> entities, CancellationToken ct)
    {
        foreach (var e in entities) Store[e.EntityId] = e;
        return Task.CompletedTask;
    }

    public Task ApplyTransitionAsync(DbTransaction? transaction, IReadOnlyList<EntityState> entities, CancellationToken ct)
    {
        foreach (var e in entities) Store[e.EntityId] = e;
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<EntityState>> LoadActiveAsync(CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<EntityState>>(Store.Values.Where(e => e.Status != "Closed").ToList());
}
