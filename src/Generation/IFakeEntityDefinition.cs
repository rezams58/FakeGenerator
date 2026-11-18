namespace FakeOrderGenerator.Generation;

/// <summary>
/// Entity-agnostic state tracked by the generation pipeline. <see cref="Data"/> is owned by the
/// entity definition and writer (Order: <c>Data.Order</c>).
/// </summary>
public sealed class EntityState
{
    public required Guid EntityId { get; init; }
    public required string PartitionKey { get; init; }
    public required string Status { get; set; }
    /// <summary>SequenceNo of the event that created the entity.</summary>
    public required long CreationSequenceNo { get; init; }
    /// <summary>SequenceNo of the latest event; strictly increasing per entity.</summary>
    public long LastEventSequenceNo { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
    public required object Data { get; init; }
}

/// <summary>Defines one fake entity type. The pipeline depends only on this interface and <see cref="IEntityWriter"/>.</summary>
public interface IFakeEntityDefinition
{
    /// <summary>Entity name; event types are <c>{Name}{State}</c>.</summary>
    string Name { get; }

    LifecycleDefinition Lifecycle { get; }

    /// <summary>Field names that may be configured as the partition key.</summary>
    IReadOnlyList<string> PartitionKeyFields { get; }

    /// <summary>Creates a new entity in the lifecycle's initial state; <paramref name="sequenceNo"/> is its creation event.</summary>
    EntityState CreateNew(Random random, long sequenceNo, DateTime utcNow);

    /// <summary>The key used as Outbox AggregateKey and Kafka key.</summary>
    string PartitionKey(EntityState entity);

    /// <summary>
    /// JSON payload for the event that moves <paramref name="entity"/> from <paramref name="fromState"/>
    /// (null for creation) to <paramref name="toState"/>. The entity already carries the event's sequence number.
    /// </summary>
    string BuildPayload(EntityState entity, string? fromState, string toState, DateTime occurredAtUtc);
}
