using System.Text.Json;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Models;

namespace FakeOrderGenerator.Generation.Order;

public sealed class OrderDefinition : IFakeEntityDefinition
{
    public const string EntityName = "Order";
    public static readonly IReadOnlyList<string> Fields = ["CustomerId"];

    private readonly GeneratorConfig _config;
    private readonly string _partitionKeyField;

    public OrderDefinition(GeneratorConfig config, EntityConfig? entityConfig = null)
    {
        _config = config;
        _partitionKeyField = entityConfig?.PartitionKey ?? "CustomerId";
        Lifecycle = new LifecycleDefinition(entityConfig?.Lifecycle ?? LifecycleOptions.OrderDefault());
    }

    public string Name => EntityName;
    public LifecycleDefinition Lifecycle { get; }
    public IReadOnlyList<string> PartitionKeyFields => Fields;

    public EntityState CreateNew(Random random, long sequenceNo, DateTime utcNow)
    {
        string customerId = $"CUS-{random.Next(1, _config.CustomerCount + 1):D6}";
        decimal range = _config.MaxOrderAmount - _config.MinOrderAmount;
        decimal amount = Math.Round(_config.MinOrderAmount + (decimal)random.NextDouble() * range, 2);
        var createdAt = utcNow - TimeSpan.FromDays(random.NextDouble() * _config.OrderHistoryDays);
        createdAt = new DateTime(createdAt.Ticks - createdAt.Ticks % TimeSpan.TicksPerMillisecond, DateTimeKind.Utc);

        var order = new Data.Order(Guid.NewGuid(), sequenceNo, $"ORD-{sequenceNo:D10}", customerId, amount, createdAt);
        return new EntityState
        {
            EntityId = order.OrderId,
            PartitionKey = customerId,
            Status = Lifecycle.InitialState,
            CreationSequenceNo = sequenceNo,
            LastEventSequenceNo = sequenceNo,
            UpdatedAtUtc = utcNow,
            Data = order
        };
    }

    public string PartitionKey(EntityState entity) => entity.PartitionKey;

    public string BuildPayload(EntityState entity, string? fromState, string toState, DateTime occurredAtUtc)
    {
        var order = (Data.Order)entity.Data;
        var payload = new OrderEventPayload(
            order.OrderId, entity.LastEventSequenceNo, order.OrderNumber, order.CustomerId, order.Amount,
            order.CreatedAtUtc, Name + toState, fromState, toState, occurredAtUtc);
        return JsonSerializer.Serialize(payload);
    }

    /// <summary>Validates <c>Generator:Entities:Order:PartitionKey</c> names a field this definition exposes.</summary>
    public static IReadOnlyList<string> ValidateConfig(EntityConfig? entityConfig)
    {
        var errors = new List<string>();
        string key = entityConfig?.PartitionKey ?? "CustomerId";
        if (!Fields.Contains(key))
            errors.Add($"Generator:Entities:Order:PartitionKey '{key}' is not a field of Order (allowed: {string.Join(", ", Fields)}).");
        return errors;
    }
}
