using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Generation;

namespace FakeGenerator.Tests.Unit;

public sealed class LifecycleDefinitionTests
{
    private static LifecycleOptions Valid() => LifecycleOptions.OrderDefault();

    private static IReadOnlyList<string> Errors(Action<LifecycleOptions> mutate)
    {
        var o = Valid();
        mutate(o);
        return new LifecycleDefinition(o).Validate();
    }

    private static TransitionOption T(string from, string to, double w = 1) => new() { From = from, To = to, Weight = w };

    [Fact] public void Default_order_lifecycle_is_valid() => Assert.Empty(new LifecycleDefinition(Valid()).Validate());

    [Fact]
    public void Initial_state_must_be_declared() =>
        Assert.Contains(Errors(o => o.InitialState = "Nope"), e => e.Contains("Initial state"));

    [Fact]
    public void Initial_state_must_not_be_terminal() =>
        Assert.Contains(Errors(o => o.InitialState = "Delivered"), e => e.Contains("must not be terminal"));

    [Fact]
    public void Transition_endpoints_must_be_declared()
    {
        Assert.Contains(Errors(o => o.Transitions = [.. o.Transitions, T("Created", "Ghost")]), e => e.Contains("'Ghost' is not a declared state"));
        Assert.Contains(Errors(o => o.Transitions = [.. o.Transitions, T("Ghost", "Paid")]), e => e.Contains("'Ghost' is not a declared state"));
    }

    [Fact]
    public void Terminal_states_must_be_declared() =>
        Assert.Contains(Errors(o => o.TerminalStates = [.. o.TerminalStates, "Ghost"]), e => e.Contains("Terminal state 'Ghost'"));

    [Fact]
    public void Non_terminal_state_needs_an_outgoing_transition() =>
        Assert.Contains(Errors(o => o.Transitions = o.Transitions.Where(t => t.From != "Shipped").ToArray()),
            e => e.Contains("Non-terminal state 'Shipped'"));

    [Fact]
    public void Terminal_state_must_have_no_outgoing_transition() =>
        Assert.Contains(Errors(o => o.Transitions = [.. o.Transitions, T("Delivered", "Cancelled")]),
            e => e.Contains("Terminal state 'Delivered' must have no outgoing"));

    [Fact]
    public void All_states_must_be_reachable_from_initial()
    {
        var errors = Errors(o =>
        {
            o.States = [.. o.States, "Orphan"];
            o.Transitions = [.. o.Transitions, T("Orphan", "Cancelled")];
        });
        Assert.Contains(errors, e => e.Contains("'Orphan' is not reachable"));
    }

    [Fact]
    public void Every_state_must_reach_a_terminal_state()
    {
        var o = new LifecycleOptions
        {
            InitialState = "A",
            States = ["A", "B", "C", "D"],
            TerminalStates = ["D"],
            // C is a non-terminal sink only via a cycle B<->C; B->C, C->B never reaches D.
            Transitions = [T("A", "B"), T("A", "D"), T("B", "C"), T("C", "B")]
        };
        var errors = new LifecycleDefinition(o).Validate();
        Assert.Contains(errors, e => e.Contains("cannot reach a terminal state"));
    }

    [Fact]
    public void Cycles_are_rejected()
    {
        var o = new LifecycleOptions
        {
            InitialState = "A",
            States = ["A", "B", "Done"],
            TerminalStates = ["Done"],
            Transitions = [T("A", "B"), T("B", "A"), T("B", "Done")]
        };
        Assert.Contains(new LifecycleDefinition(o).Validate(), e => e.Contains("cycle"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Weights_must_be_positive(double weight) =>
        Assert.Contains(Errors(o => o.Transitions[0].Weight = weight), e => e.Contains("Weight must be > 0"));

    [Fact]
    public void State_names_must_be_unique() =>
        Assert.Contains(Errors(o => o.States = [.. o.States, "Paid"]), e => e.Contains("unique"));
}
