using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Generation;
using FakeOrderGenerator.Reporting;
using Confluent.Kafka;
using System.Reflection;
using System.Text;

namespace FakeGenerator.Tests.Unit;

public sealed class StreamVerifierLogicTests
{
    public class ConsumerProxy : DispatchProxy
    {
        public Queue<ConsumeResult<string, string>> Results { get; } = new();
        public Dictionary<TopicPartition, Offset> Positions { get; } = new();
        public Action? OnConsume { get; set; }
        public int ConsumeCalls { get; private set; }
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "Consume")
            {
                ConsumeCalls++;
                OnConsume?.Invoke();
                if (!Results.TryDequeue(out var next)) return null;
                if (!next.IsPartitionEOF) Positions[next.TopicPartition] = next.Offset + 1;
                return next;
            }
            if (method.Name == "Position") return Positions.GetValueOrDefault((TopicPartition)args![0]!, Offset.Unset);
            if (method.Name == "Pause") return null;
            throw new NotSupportedException(method.Name);
        }
    }

    private static (IConsumer<string, string> Consumer, ConsumerProxy Fake) Consumer()
    {
        var consumer = DispatchProxy.Create<IConsumer<string, string>, ConsumerProxy>();
        return (consumer, (ConsumerProxy)(object)consumer);
    }

    private static ConsumeResult<string, string> KafkaRecord(TopicPartition tp, long offset) => new()
    {
        Topic = tp.Topic, Partition = tp.Partition, Offset = offset,
        Message = new Message<string, string>
        {
            Headers = new Headers
            {
                { "event-id", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) },
                { "sequence-no", Encoding.UTF8.GetBytes("1") }
            },
            Value = $"{{\"OrderId\":\"{Guid.NewGuid()}\",\"Status\":\"Created\"}}"
        }
    };

    [Fact]
    public void Idle_before_captured_end_is_incomplete()
    {
        var (consumer, _) = Consumer();
        var result = StreamVerifier.ReadCapturedStream(consumer, new Dictionary<TopicPartition, long> { [new("orders", 0)] = 3 },
            CancellationToken.None, TimeSpan.Zero);
        Assert.False(result.Complete);
    }

    [Fact]
    public void Records_at_or_after_captured_end_are_excluded()
    {
        var (consumer, fake) = Consumer(); var tp = new TopicPartition("orders", 0);
        fake.Results.Enqueue(KafkaRecord(tp, 3));
        var result = StreamVerifier.ReadCapturedStream(consumer, new Dictionary<TopicPartition, long> { [tp] = 3 }, CancellationToken.None);
        Assert.Empty(result.Records);
        Assert.True(result.Complete);
    }

    [Fact]
    public void Eof_after_transaction_control_offsets_completes_without_a_data_record_at_end()
    {
        var (consumer, fake) = Consumer(); var tp = new TopicPartition("orders", 0);
        fake.Results.Enqueue(new ConsumeResult<string, string> { Topic = tp.Topic, Partition = tp.Partition, Offset = 3, IsPartitionEOF = true });
        var result = StreamVerifier.ReadCapturedStream(consumer, new Dictionary<TopicPartition, long> { [tp] = 3 }, CancellationToken.None, TimeSpan.Zero);
        Assert.True(result.Complete);
        Assert.Empty(result.Records);
    }

    [Fact]
    public void Cancellation_during_consume_throws_instead_of_returning_partial_success()
    {
        var (consumer, fake) = Consumer(); using var cts = new CancellationTokenSource();
        fake.OnConsume = cts.Cancel;
        Assert.Throws<OperationCanceledException>(() => StreamVerifier.ReadCapturedStream(consumer,
            new Dictionary<TopicPartition, long> { [new("orders", 0)] = 3 }, cts.Token, TimeSpan.Zero));
    }

    [Fact]
    public void Already_cancelled_empty_stream_throws()
    {
        var (consumer, _) = Consumer();
        Assert.Throws<OperationCanceledException>(() => StreamVerifier.ReadCapturedStream(consumer,
            new Dictionary<TopicPartition, long>(), new CancellationToken(true), TimeSpan.Zero));
    }

    [Fact]
    public void Eof_before_captured_end_remains_incomplete()
    {
        var (consumer, fake) = Consumer(); var tp = new TopicPartition("orders", 0);
        fake.Results.Enqueue(new ConsumeResult<string, string> { Topic = tp.Topic, Partition = tp.Partition, Offset = 1, IsPartitionEOF = true });
        var result = StreamVerifier.ReadCapturedStream(consumer, new Dictionary<TopicPartition, long> { [tp] = 3 }, CancellationToken.None, TimeSpan.Zero);
        Assert.False(result.Complete);
    }

    [Fact]
    public void Every_partition_must_finish_even_when_another_partition_reaches_its_end()
    {
        var (consumer, fake) = Consumer(); var tp = new TopicPartition("orders", 0);
        fake.Results.Enqueue(KafkaRecord(tp, 0));
        var result = StreamVerifier.ReadCapturedStream(consumer,
            new Dictionary<TopicPartition, long> { [tp] = 1, [new("orders", 1)] = 1 }, CancellationToken.None, TimeSpan.Zero);
        Assert.False(result.Complete);
        Assert.Single(result.Records);
    }

    [Fact]
    public void Incomplete_verification_is_a_failure_even_without_missing_published_ids()
    {
        Assert.True((new VerificationResult(0, 0, 0, 0, 0, 0) { Complete = false }).HasViolations);
    }

    private static readonly LifecycleDefinition Life = new(LifecycleOptions.OrderDefault());
    private static readonly IReadOnlySet<Guid> None = new HashSet<Guid>();

    private static StreamRecord R(int p, long off, long seq, Guid entity, string? prev, string status, Guid? id = null) =>
        new(p, off, id ?? Guid.NewGuid(), seq, entity, prev, status);

    private static VerificationResult Run(StreamRecord[] rs, IReadOnlySet<Guid>? dead = null, IReadOnlySet<Guid>? published = null) =>
        StreamVerifierCore.Verify(rs, Life, dead ?? None, published ?? new HashSet<Guid>(rs.Select(x => x.EventId)));

    [Fact]
    public void Valid_path_has_no_findings()
    {
        var o = Guid.NewGuid();
        var v = Run([R(0, 0, 1, o, null, "Created"), R(0, 1, 5, o, "Created", "Paid"), R(0, 2, 9, o, "Paid", "Shipped"), R(0, 3, 12, o, "Shipped", "Delivered")]);
        Assert.False(v.HasViolations);
        Assert.Equal(0, v.DeadLetterGaps);
    }

    [Fact]
    public void Dead_lettered_terminal_step_is_reported_for_a_valid_consumed_prefix()
    {
        var o = Guid.NewGuid();
        var v = Run([R(0, 0, 1, o, null, "Created"), R(0, 1, 2, o, "Created", "Paid"),
            R(0, 2, 3, o, "Paid", "Shipped")], dead: new HashSet<Guid> { o });
        Assert.Equal(1, v.DeadLetterGaps);
        Assert.Equal(0, v.LifecycleViolations);
        Assert.False(v.HasViolations);
    }

    [Fact]
    public void Entirely_dead_lettered_order_is_reported_even_without_consumed_records()
    {
        var v = Run([], dead: new HashSet<Guid> { Guid.NewGuid() });
        Assert.Equal(1, v.DeadLetterGaps);
        Assert.False(v.HasViolations);
    }

    [Fact]
    public void Bad_path_is_a_lifecycle_violation()
    {
        var o = Guid.NewGuid();
        Assert.Equal(1, Run([R(0, 0, 1, o, null, "Created"), R(0, 1, 2, o, "Created", "Delivered")]).LifecycleViolations);
    }

    [Fact]
    public void Event_after_terminal_state_is_a_violation()
    {
        var o = Guid.NewGuid();
        var v = Run([R(0, 0, 1, o, null, "Created"), R(0, 1, 2, o, "Created", "Cancelled"), R(0, 2, 3, o, "Cancelled", "Paid")]);
        Assert.Equal(1, v.LifecycleViolations);
    }

    [Fact]
    public void Missing_step_caused_by_dead_letter_is_a_gap_not_a_violation()
    {
        var o = Guid.NewGuid();
        var v = Run([R(0, 0, 1, o, null, "Created"), R(0, 1, 9, o, "Paid", "Shipped")], dead: new HashSet<Guid> { o });
        Assert.Equal(0, v.LifecycleViolations);
        Assert.Equal(1, v.DeadLetterGaps);
    }

    [Fact]
    public void Redelivery_is_a_duplicate_and_not_an_ordering_violation()
    {
        var o = Guid.NewGuid(); var id = Guid.NewGuid();
        var v = Run([R(0, 0, 1, o, null, "Created", id), R(0, 1, 2, o, "Created", "Paid"), R(0, 2, 1, o, null, "Created", id)]);
        Assert.Equal(1, v.Duplicates);
        Assert.Equal(0, v.OrderingViolations);
        Assert.Equal(0, v.LifecycleViolations);
        Assert.False(v.HasViolations); // Expected at-least-once replays are counted, not rejected.
    }

    [Fact]
    public void Lower_sequence_that_is_not_a_known_duplicate_is_an_ordering_violation()
    {
        var v = Run([R(0, 0, 10, Guid.NewGuid(), null, "Created"), R(0, 1, 4, Guid.NewGuid(), null, "Created")]);
        Assert.Equal(1, v.OrderingViolations);
    }

    [Fact]
    public void Sequence_order_is_checked_per_partition_only()
    {
        var v = Run([R(0, 0, 10, Guid.NewGuid(), null, "Created"), R(1, 0, 4, Guid.NewGuid(), null, "Created")]);
        Assert.Equal(0, v.OrderingViolations);
    }

    [Fact]
    public void Published_in_sql_but_absent_from_stream_is_missing()
    {
        var r = R(0, 0, 1, Guid.NewGuid(), null, "Created");
        var v = Run([r], published: new HashSet<Guid> { r.EventId, Guid.NewGuid() });
        Assert.Equal(1, v.MissingOnStream);
        Assert.True(v.HasViolations);
    }
}
