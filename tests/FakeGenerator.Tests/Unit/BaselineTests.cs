using FakeOrderGenerator.Reporting;

namespace FakeGenerator.Tests.Unit;

public sealed class BaselineTests
{
    [Fact]
    public void First_run_creates_baseline_and_passes()
    {
        var r = BaselineComparer.Compare(1000, null, 20);
        Assert.True(r.CreateBaseline);
        Assert.True(r.Passed);
        Assert.Null(r.RegressionPercent);
    }

    [Fact] public void Nineteen_percent_slower_passes() => Assert.True(BaselineComparer.Compare(810, 1000, 20).Passed);
    [Fact] public void Twenty_one_percent_slower_fails() => Assert.False(BaselineComparer.Compare(790, 1000, 20).Passed);

    [Fact]
    public void Faster_than_baseline_passes_with_negative_regression()
    {
        var r = BaselineComparer.Compare(1200, 1000, 20);
        Assert.True(r.Passed);
        Assert.True(r.RegressionPercent < 0);
    }
}
