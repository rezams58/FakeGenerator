using Confluent.Kafka;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using System.Text;

namespace FakeOrderGenerator.Kafka;

public sealed class KafkaPublishBatchException : Exception
{
    public OutboxMessage FailedMessage { get; }
    public KafkaPublishBatchException(OutboxMessage failedMessage, Exception inner)
        : base($"Kafka delivery failed for event {failedMessage.EventId} at sequence {failedMessage.SequenceNo}.", inner)
        => FailedMessage = failedMessage;
}

/// <summary>One transactional producer per owned partition. The stable transactional.id fences a stale owner in Kafka.</summary>
public sealed class PartitionedKafkaPublisher : IPartitionPublisher
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public PartitionedKafkaPublisher(KafkaSettings settings, int partitionId)
    {
        _topic = settings.Topic;
        var config = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = $"{settings.ClientId}-p{partitionId}",
            TransactionalId = TransactionalIdFor(settings, partitionId),
            Acks = Acks.All,
            EnableIdempotence = true,
            CompressionType = Enum.Parse<CompressionType>(settings.CompressionType, true),
            LingerMs = settings.LingerMs,
            BatchSize = settings.BatchSize,
            MessageTimeoutMs = 30000
        };
        _producer = new ProducerBuilder<string, string>(config).Build();
        _producer.InitTransactions(TimeSpan.FromSeconds(30));
    }

    /// <summary>Stable per topic/partition so a new owner fences the old producer in Kafka (Constitution II).</summary>
    public static string TransactionalIdFor(KafkaSettings settings, int partitionId) =>
        $"{settings.ClientId}-{settings.Topic}-partition-{partitionId}";

    /// <summary>
    /// Index of the delivery that actually failed: the first faulted/canceled task in send order.
    /// Earlier successful deliveries are never charged. Returns 0 when none is marked faulted.
    /// </summary>
    public static int FindFailedIndex(IReadOnlyList<Task> deliveries)
    {
        for (int i = 0; i < deliveries.Count; i++)
            if (deliveries[i].IsFaulted || deliveries[i].IsCanceled) return i;
        return 0;
    }

    public async Task PublishBatchAsync(IReadOnlyList<OutboxMessage> items, CancellationToken ct)
    {
        if (items.Count == 0) return;
        _producer.BeginTransaction();
        try
        {
            var ordered = items.OrderBy(x => x.SequenceNo).ToArray();
            var deliveries = new List<Task<DeliveryResult<string, string>>>(ordered.Length);
            foreach (var item in ordered)
            {
                ct.ThrowIfCancellationRequested();
                var message = new Message<string, string>
                {
                    Key = item.AggregateKey,
                    Value = item.Payload,
                    Headers = new Headers
                    {
                        { "event-id", Encoding.UTF8.GetBytes(item.EventId.ToString()) },
                        { "sequence-no", Encoding.UTF8.GetBytes(item.SequenceNo.ToString()) },
                        { "event-type", Encoding.UTF8.GetBytes(item.EventType) }
                    }
                };
                try
                {
                    // Enqueue in SequenceNo order without awaiting each delivery. Kafka can batch the
                    // in-flight records while the producer preserves order for this partition.
                    deliveries.Add(_producer.ProduceAsync(
                        new TopicPartition(_topic, new Partition(item.KafkaPartition)), message, ct));
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    throw new KafkaPublishBatchException(item, ex);
                }
            }

            try
            {
                await Task.WhenAll(deliveries);
            }
            catch (Exception ex)
            {
                if (ct.IsCancellationRequested) throw new OperationCanceledException(ct);
                int failedIndex = FindFailedIndex(deliveries);
                var failed = ordered[failedIndex];
                throw new KafkaPublishBatchException(failed, ex);
            }
            _producer.CommitTransaction(TimeSpan.FromSeconds(30));
        }
        catch
        {
            try { _producer.AbortTransaction(TimeSpan.FromSeconds(10)); }
            catch { /* Producer may already be fenced or in an unrecoverable state. */ }
            throw;
        }
    }

    public void Dispose() => _producer.Dispose();
}
