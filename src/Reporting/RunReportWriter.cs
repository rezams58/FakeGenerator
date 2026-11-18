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
        VerificationResult? verification, CancellationToken ct)
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

        var previous = LoadPreviousTotal();
        report.GeneratedThisRun = previous is null ? report.GeneratedTotal : Math.Max(0, report.GeneratedTotal - previous.Value);

        if (verification is not null)
        {
            report.Duplicates = verification.Duplicates;
            report.OrderingViolations = verification.OrderingViolations;
            report.LifecycleViolations = verification.LifecycleViolations;
            report.DeadLetterGaps = verification.DeadLetterGaps;
            report.MissingOnStream = verification.MissingOnStream;
            report.VerifierStatus = verification.HasViolations ? "failed" : "passed";
            if (verification.OrderingViolations > 0) report.Failures.Add($"{verification.OrderingViolations} ordering violations");
            if (verification.LifecycleViolations > 0) report.Failures.Add($"{verification.LifecycleViolations} lifecycle violations");
            if (verification.MissingOnStream > 0) report.Failures.Add($"{verification.MissingOnStream} published events missing on the stream");
            if (verification.Duplicates > 0) report.Failures.Add($"{verification.Duplicates} duplicate deliveries (consumers must deduplicate by event-id)");
        }

        ApplyBaseline(report);
        report.Passed = report.Failures.Count == 0;
        return report;
    }

    private void ApplyBaseline(RunReport report)
    {
        // Only a full run (target reached, nothing pending) is a valid performance sample.
        if (report.EventsPerSecond is null || report.Pending > 0 || report.GeneratedTotal < report.TargetEventCount) return;
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

    private long? LoadPreviousTotal()
    {
        if (!Directory.Exists(_reporting.Directory)) return null;
        var last = Directory.GetFiles(_reporting.Directory, "run-*.json").OrderBy(f => f, StringComparer.Ordinal).LastOrDefault();
        if (last is null) return null;
        try { return JsonDocument.Parse(File.ReadAllText(last)).RootElement.GetProperty("GeneratedTotal").GetInt64(); }
        catch { return null; }
    }

    public string Write(RunReport report)
    {
        Directory.CreateDirectory(_reporting.Directory);
        string path = Path.Combine(_reporting.Directory, $"run-{report.FinishedAtUtc:yyyyMMddTHHmmssfff}Z.json");
        File.WriteAllText(path, JsonSerializer.Serialize(report, Json));
        return path;
    }
}
