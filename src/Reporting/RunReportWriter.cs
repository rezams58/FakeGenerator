using System.Text.Json;
using FakeOrderGenerator.Configuration;
using Microsoft.Data.SqlClient;

namespace FakeOrderGenerator.Reporting;

/// <summary>Builds the run report from real SQL counts and the verifier result, compares with the baseline, writes the files.</summary>
public sealed class RunReportWriter
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };
    private readonly ReportingConfig _reporting;
    private readonly string _connectionString;

    public RunReportWriter(ReportingConfig reporting, string connectionString)
    {
        _reporting = reporting;
        _connectionString = connectionString;
    }

    public async Task<RunReport> BuildAsync(DateTime startedAtUtc, long targetEventCount, IReadOnlyCollection<string> terminalStates,
        VerificationResult? verification, CancellationToken ct, long? generationStartCount = null)
    {
        var report = new RunReport
        {
            StartedAtUtc = startedAtUtc, FinishedAtUtc = DateTime.UtcNow,
            Environment = _reporting.Environment, TargetEventCount = targetEventCount
        };

        await using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync(ct);
        async Task<object?> Scalar(string sql, Action<SqlCommand>? bind = null)
        {
            await using var cmd = new SqlCommand(sql, cn) { CommandTimeout = 300 };
            bind?.Invoke(cmd);
            var v = await cmd.ExecuteScalarAsync(ct);
            return v is DBNull ? null : v;
        }

        report.GeneratedTotal = Convert.ToInt64(await Scalar("SELECT COUNT_BIG(*) FROM dbo.Outbox"));
        report.OrdersGenerated = Convert.ToInt64(await Scalar("SELECT COUNT_BIG(*) FROM dbo.Orders"));
        report.Published = Convert.ToInt64(await Scalar("SELECT COUNT_BIG(*) FROM dbo.Outbox WHERE PublishedAtUtc IS NOT NULL"));
        report.DeadLettered = Convert.ToInt64(await Scalar("SELECT COUNT_BIG(*) FROM dbo.Outbox WHERE DeadLetterAtUtc IS NOT NULL"));
        report.Pending = Convert.ToInt64(await Scalar(
            "SELECT COUNT_BIG(*) FROM dbo.Outbox WHERE PublishedAtUtc IS NULL AND DeadLetterAtUtc IS NULL"));
        string terminalJson = JsonSerializer.Serialize(terminalStates);
        report.IncompleteLifecycles = Convert.ToInt64(await Scalar(
            "SELECT COUNT_BIG(*) FROM dbo.Orders WHERE Status NOT IN (SELECT [value] FROM OPENJSON(@t))",
            c => c.Parameters.AddWithValue("@t", terminalJson)));

        var min = await Scalar("SELECT MIN(PublishedAtUtc) FROM dbo.Outbox WHERE PublishedAtUtc IS NOT NULL");
        var max = await Scalar("SELECT MAX(PublishedAtUtc) FROM dbo.Outbox WHERE PublishedAtUtc IS NOT NULL");
        if (min is DateTime lo && max is DateTime hi && hi > lo)
        {
            report.PublishWindowSeconds = Math.Round((hi - lo).TotalSeconds, 3);
            report.EventsPerSecond = Math.Round(report.Published / (hi - lo).TotalSeconds, 2);
        }

        ct.ThrowIfCancellationRequested();
        return Complete(report, verification, generationStartCount);
    }

    public RunReport Complete(RunReport report, VerificationResult? verification, long? generationStartCount = null)
    {
        // A previous verifier report may describe another database or an earlier reset.
        // Only an explicitly captured start count can measure generation in this run.
        report.GeneratedThisRun = generationStartCount is long start && start >= 0 && start <= report.GeneratedTotal
            ? report.GeneratedTotal - start : null;
        if (report.GeneratedTotal != report.TargetEventCount)
            report.Failures.Add($"generated {report.GeneratedTotal} events; target is {report.TargetEventCount}");
        if (report.Pending > 0) report.Failures.Add($"{report.Pending} events still pending");
        if (report.Published + report.DeadLettered != report.GeneratedTotal)
            report.Failures.Add("published and dead-lettered counts do not account for all generated events");

        if (verification is not null)
        {
            report.Duplicates = verification.Duplicates;
            report.OrderingViolations = verification.OrderingViolations;
            report.LifecycleViolations = verification.LifecycleViolations;
            report.DeadLetterGaps = verification.DeadLetterGaps;
            report.MissingOnStream = verification.MissingOnStream;
            report.VerifierComplete = verification.Complete;
            report.VerifierStatus = !verification.Complete ? "incomplete" : verification.HasViolations ? "failed" : "passed";
            if (!verification.Complete) report.Failures.Add("stream verification did not reach every captured partition end");
            if (verification.OrderingViolations > 0) report.Failures.Add($"{verification.OrderingViolations} ordering violations");
            if (verification.LifecycleViolations > 0) report.Failures.Add($"{verification.LifecycleViolations} lifecycle violations");
            if (verification.MissingOnStream > 0) report.Failures.Add($"{verification.MissingOnStream} published events missing on the stream");
        }
        else report.Failures.Add("stream verification was not run");

        ApplyBaseline(report);
        report.Passed = report.Failures.Count == 0;
        return report;
    }

    private void ApplyBaseline(RunReport report)
    {
        // Only the specified 500K sample is comparable with the environment baseline.
        if (report.TargetEventCount != 500000 || report.EventsPerSecond is null || report.Pending > 0 || report.GeneratedTotal != report.TargetEventCount ||
            report.VerifierStatus != "passed" || report.Failures.Count > 0) return;
        string path = Path.Combine(_reporting.Directory, $"baseline.{_reporting.Environment}.json");
        double? baseline = null;
        if (File.Exists(path))
            baseline = JsonDocument.Parse(File.ReadAllText(path)).RootElement.GetProperty("eventsPerSecond").GetDouble();

        var result = BaselineComparer.Compare(report.EventsPerSecond.Value, baseline, _reporting.RegressionThresholdPercent);
        report.BaselineEventsPerSecond = result.BaselineEventsPerSecond;
        report.RegressionPercent = result.RegressionPercent;
        report.BaselineCreated = result.CreateBaseline;
        if (!result.Passed)
            report.Failures.Add($"throughput regression {result.RegressionPercent}% exceeds {_reporting.RegressionThresholdPercent}%");
        if (result.CreateBaseline)
        {
            Directory.CreateDirectory(_reporting.Directory);
            File.WriteAllText(path, JsonSerializer.Serialize(new { eventsPerSecond = report.EventsPerSecond, createdAtUtc = DateTime.UtcNow }, Json));
        }
    }

    public string Write(RunReport report)
    {
        Directory.CreateDirectory(_reporting.Directory);
        string path = Path.Combine(_reporting.Directory, $"run-{report.FinishedAtUtc:yyyyMMddTHHmmssfff}Z.json");
        File.WriteAllText(path, JsonSerializer.Serialize(report, Json));
        return path;
    }
}
