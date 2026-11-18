using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Reporting;

namespace FakeGenerator.Tests.Unit;

public sealed class RunReportWriterTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "fakegen-report-" + Guid.NewGuid().ToString("N"));
    private RunReportWriter Writer() => new(new ReportingConfig { Directory = _directory, Environment = "test" }, "unused");
    private static VerificationResult Verified() => new(0, 0, 0, 0, 0, 500000);
    private static RunReport Report(long target = 500000) => new()
    {
        TargetEventCount = target, GeneratedTotal = target, Published = target, EventsPerSecond = 1000
    };

    [Fact]
    public void Pending_events_fail_even_when_all_published_events_were_consumed()
    {
        var report = Report(); report.Published--; report.Pending = 1;
        Assert.False(Writer().Complete(report, Verified()).Passed);
    }

    [Fact]
    public void Short_generation_fails_even_when_every_generated_event_was_published()
    {
        var report = Report(); report.GeneratedTotal--; report.Published--;
        Assert.False(Writer().Complete(report, Verified()).Passed);
    }

    [Fact]
    public void Unreconciled_final_counts_fail()
    {
        var report = Report(); report.Published--;
        Assert.False(Writer().Complete(report, Verified()).Passed);
    }

    [Fact]
    public void Unmeasured_generation_count_is_unknown_even_after_database_reset()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(Path.Combine(_directory, "run-previous.json"), "{\"GeneratedTotal\":500000}");
        Assert.Null(Writer().Complete(Report(), Verified()).GeneratedThisRun);
    }

    [Fact]
    public void Explicit_generation_start_count_measures_only_this_run()
    {
        Assert.Equal(375000L, Writer().Complete(Report(), Verified(), generationStartCount: 125000).GeneratedThisRun);
    }

    [Fact]
    public void Partial_stream_consumption_fails_report_and_suppresses_baseline()
    {
        var report = Writer().Complete(Report(), Verified() with { Complete = false });
        Assert.False(report.Passed);
        Assert.Equal(false, report.VerifierComplete);
        Assert.Equal("incomplete", report.VerifierStatus);
        Assert.False(report.BaselineCreated);
    }

    [Fact]
    public void Missing_verification_cannot_pass_or_create_baseline()
    {
        var report = Writer().Complete(Report(), null);
        Assert.False(report.Passed);
        Assert.False(report.BaselineCreated);
    }

    [Fact]
    public void Small_trial_does_not_create_environment_baseline()
    {
        var report = Writer().Complete(Report(10000), Verified());
        Assert.True(report.Passed);
        Assert.False(report.BaselineCreated);
        Assert.False(File.Exists(Path.Combine(_directory, "baseline.test.json")));
    }

    [Fact]
    public void Full_run_creates_baseline_then_twenty_one_percent_regression_fails()
    {
        Assert.True(Writer().Complete(Report(), Verified()).BaselineCreated);
        var slower = Report(); slower.EventsPerSecond = 790;
        var report = Writer().Complete(slower, Verified());
        Assert.False(report.Passed);
        Assert.Equal(21d, report.RegressionPercent);
    }

    [Fact]
    public void Small_trial_does_not_compare_against_full_run_baseline()
    {
        Writer().Complete(Report(), Verified());
        var trial = Report(10000); trial.EventsPerSecond = 1;
        var report = Writer().Complete(trial, Verified());
        Assert.True(report.Passed);
        Assert.Null(report.RegressionPercent);
    }

    public void Dispose() { if (Directory.Exists(_directory)) Directory.Delete(_directory, true); }
}
