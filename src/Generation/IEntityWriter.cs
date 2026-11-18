using System.Data.Common;

namespace FakeOrderGenerator.Generation;

/// <summary>Persists an entity's own state inside the transaction that also writes its Outbox rows.</summary>
public interface IEntityWriter
{
    string EntityName { get; }

    Task InsertNewAsync(DbTransaction? transaction, IReadOnlyList<EntityState> entities, CancellationToken ct);

    /// <summary>Persists new <c>Status</c>, <c>LastEventSequenceNo</c> and <c>UpdatedAtUtc</c> of the given entities.</summary>
    Task ApplyTransitionAsync(DbTransaction? transaction, IReadOnlyList<EntityState> entities, CancellationToken ct);

    /// <summary>Loads entities in non-terminal states (restart path).</summary>
    Task<IReadOnlyList<EntityState>> LoadActiveAsync(CancellationToken ct);
}
