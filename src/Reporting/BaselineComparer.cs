namespace FakeOrderGenerator.Reporting;

public sealed record BaselineResult(double? BaselineEventsPerSecond, double? RegressionPercent, bool CreateBaseline, bool Passed);

public static class BaselineComparer
{
    /// <summary>
    /// First run (no baseline) creates one and passes. Otherwise the run fails when it is more than
    /// <paramref name="thresholdPercent"/> slower than the baseline (exactly at the threshold passes).
    /// </summary>
    public static BaselineResult Compare(double currentEventsPerSecond, double? baselineEventsPerSecond, int thresholdPercent)
    {
        if (baselineEventsPerSecond is null or <= 0)
            return new BaselineResult(null, null, CreateBaseline: true, Passed: true);
        double regression = (baselineEventsPerSecond.Value - currentEventsPerSecond) / baselineEventsPerSecond.Value * 100.0;
        return new BaselineResult(baselineEventsPerSecond, Math.Round(regression, 2), false, regression <= thresholdPercent);
    }
}
