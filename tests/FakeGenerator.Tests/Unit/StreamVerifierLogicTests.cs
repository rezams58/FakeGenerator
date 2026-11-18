using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Generation;
using FakeOrderGenerator.Reporting;

namespace FakeGenerator.Tests.Unit;

public sealed class StreamVerifierLogicTests
{
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
