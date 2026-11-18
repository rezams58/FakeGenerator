namespace FakeOrderGenerator.Models;

/// <summary>Kafka value for every order event (contracts/kafka-message.md). Consumers must ignore unknown fields.</summary>
public sealed record OrderEventPayload(
    Guid OrderId,
    long SequenceNo,
    string OrderNumber,
    string CustomerId,
    decimal Amount,
    DateTime CreatedAtUtc,
    string EventType,
    string? PreviousStatus,
    string Status,
    DateTime OccurredAtUtc,
    int EventVersion = 1);
