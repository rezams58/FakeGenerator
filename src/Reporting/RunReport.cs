namespace FakeOrderGenerator.Reporting;

/// <summary>Run report per data-model.md section 6. Verifier fields are null (reported as "not run") when the verifier did not run.</summary>
public sealed class RunReport
{
    public string RunId { get; set; } = Guid.NewGuid().ToString("N");
    public DateTime StartedAtUtc { get; set; }
    public DateTime FinishedAtUtc { get; set; }
    public string Environment { get; set; } = "";
    public long TargetEventCount { get; set; }
    public long GeneratedTotal { get; set; }
    public long GeneratedThisRun { get; set; }
    public long OrdersGenerated { get; set; }
    public long Published { get; set; }
    public long DeadLettered { get; set; }
    public long Pending { get; set; }
    public long IncompleteLifecycles { get; set; }
    public double? PublishWindowSeconds { get; set; }
    public double? EventsPerSecond { get; set; }

    public long? Duplicates { get; set; }
    public long? OrderingViolations { get; set; }
    public long? LifecycleViolations { get; set; }
    public long? DeadLetterGaps { get; set; }
    public long? MissingOnStream { get; set; }
    public string VerifierStatus { get; set; } = "not run";

    public double? BaselineEventsPerSecond { get; set; }
    public double? RegressionPercent { get; set; }
    public bool BaselineCreated { get; set; }
    public bool Passed { get; set; }
    public List<string> Failures { get; set; } = [];
}
