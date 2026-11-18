using FakeOrderGenerator.Generation;

namespace FakeOrderGenerator.Reporting;

/// <summary>One record read from the stream with a read_committed consumer, in per-partition offset order.</summary>
public sealed record StreamRecord(int Partition, long Offset, Guid EventId, long SequenceNo, Guid EntityId,
    string? PreviousStatus, string Status);

public sealed record VerificationResult(long Duplicates, long OrderingViolations, long LifecycleViolations,
    long DeadLetterGaps, long MissingOnStream, long RecordsRead)
{
    public bool Complete { get; init; } = true;
    public bool HasViolations => !Complete || OrderingViolations > 0 || LifecycleViolations > 0 || MissingOnStream > 0;
}

/// <summary>Pure verification logic (contracts/consumer-inbox.md): duplicates, ordering, lifecycle, dead-letter gaps, missing events.</summary>
public static class StreamVerifierCore
{
    public static VerificationResult Verify(IEnumerable<StreamRecord> records, LifecycleDefinition lifecycle,
        IReadOnlySet<Guid> deadLetteredEntityIds, IReadOnlySet<Guid> sqlPublishedEventIds)
    {
        var seen = new HashSet<Guid>();
        var maxSeqByPartition = new Dictionary<int, long>();
        var unique = new List<StreamRecord>();
        long duplicates = 0, ordering = 0, read = 0;

        foreach (var r in records)
        {
            read++;
            if (!seen.Add(r.EventId)) { duplicates++; continue; } // a re-delivery is absorbed by the Inbox; counted, not an ordering error
            if (maxSeqByPartition.TryGetValue(r.Partition, out long max) && r.SequenceNo <= max) ordering++;
            else maxSeqByPartition[r.Partition] = r.SequenceNo;
            unique.Add(r);
        }

        long lifecycleViolations = 0, gaps = deadLetteredEntityIds.Count;
        foreach (var g in unique.GroupBy(x => x.EntityId))
        {
            string? previous = null;
            bool terminalSeen = false, broken = false;
            foreach (var e in g.OrderBy(x => x.SequenceNo))
            {
                bool valid = previous is null
                    ? e.PreviousStatus is null && e.Status == lifecycle.InitialState
                    : !terminalSeen && e.PreviousStatus == previous && lifecycle.IsValidTransition(previous, e.Status);
                if (!valid) { broken = true; break; }
                previous = e.Status;
                terminalSeen = lifecycle.IsTerminal(e.Status);
            }
            if (broken && !deadLetteredEntityIds.Contains(g.Key)) lifecycleViolations++;
        }

        long missing = sqlPublishedEventIds.Count(id => !seen.Contains(id));
        return new VerificationResult(duplicates, ordering, lifecycleViolations, gaps, missing, read);
    }
}
