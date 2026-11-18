using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Generation;

namespace FakeGenerator.Tests.Unit;

public sealed class LifecyclePathTests
{
    [Fact]
    public void Ten_thousand_seeded_paths_are_valid_and_cancelled_only_from_created_or_paid()
    {
        var lifecycle = new LifecycleDefinition(LifecycleOptions.OrderDefault());
        var rnd = new Random(12345);
        int cancelled = 0, delivered = 0;

        for (int i = 0; i < 10_000; i++)
        {
            string state = lifecycle.InitialState;
            while (!lifecycle.IsTerminal(state))
            {
                var step = lifecycle.NextTransition(state, rnd);
                Assert.Equal(state, step.From);
                Assert.True(lifecycle.IsValidTransition(state, step.To), $"{state}->{step.To} is not a declared transition");
                if (step.To == "Cancelled") Assert.True(state is "Created" or "Paid", $"Cancelled reached from {state}");
                state = step.To;
            }
            if (state == "Cancelled") cancelled++;
            if (state == "Delivered") delivered++;
        }

        Assert.Equal(10_000, cancelled + delivered);
        Assert.InRange(cancelled, 1, 9_999);
        Assert.InRange(delivered, 1, 9_999);
    }

    [Fact]
    public void Same_seed_gives_same_paths()
    {
        var lifecycle = new LifecycleDefinition(LifecycleOptions.OrderDefault());
        string Walk(int seed)
        {
            var rnd = new Random(seed);
            var parts = new List<string>();
            for (int i = 0; i < 50; i++)
            {
                string s = lifecycle.InitialState;
                while (!lifecycle.IsTerminal(s)) { s = lifecycle.NextTransition(s, rnd).To; parts.Add(s); }
            }
            return string.Join(",", parts);
        }
        Assert.Equal(Walk(7), Walk(7));
    }
}
