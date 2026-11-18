using FakeOrderGenerator.Configuration;

namespace FakeOrderGenerator.Generation;

public sealed record LifecycleTransition(string From, string To, double Weight);

/// <summary>
/// Configurable entity lifecycle: a weighted, acyclic state graph (research R3, data-model.md §4).
/// Construction never throws; call <see cref="Validate"/> before use and refuse to start on errors.
/// </summary>
public sealed class LifecycleDefinition
{
    public string InitialState { get; }
    public IReadOnlyList<string> States { get; }
    public IReadOnlySet<string> TerminalStates { get; }
    public IReadOnlyList<LifecycleTransition> Transitions { get; }

    private readonly Dictionary<string, LifecycleTransition[]> _outgoing;

    public LifecycleDefinition(LifecycleOptions options)
    {
        InitialState = options.InitialState ?? "";
        States = (options.States ?? []).ToArray();
        TerminalStates = new HashSet<string>(options.TerminalStates ?? []);
        Transitions = (options.Transitions ?? []).Select(t => new LifecycleTransition(t.From, t.To, t.Weight)).ToArray();
        _outgoing = Transitions.GroupBy(t => t.From).ToDictionary(g => g.Key, g => g.ToArray());
    }

    public bool IsTerminal(string state) => TerminalStates.Contains(state);

    public IReadOnlyList<LifecycleTransition> Outgoing(string state) =>
        _outgoing.TryGetValue(state, out var list) ? list : [];

    public bool IsValidTransition(string from, string to) =>
        Outgoing(from).Any(t => t.To == to);

    /// <summary>Weighted random choice among the allowed outgoing transitions of <paramref name="state"/>.</summary>
    public LifecycleTransition NextTransition(string state, Random random)
    {
        var options = Outgoing(state);
        if (options.Count == 0)
            throw new InvalidOperationException($"State '{state}' has no outgoing transition.");
        double total = options.Sum(t => t.Weight);
        double pick = random.NextDouble() * total;
        double acc = 0;
        foreach (var t in options)
        {
            acc += t.Weight;
            if (pick < acc) return t;
        }
        return options[^1];
    }

    /// <summary>Returns every violated rule; an empty list means the lifecycle is valid.</summary>
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>();
        var declared = new HashSet<string>(States);

        if (States.Count == 0) errors.Add("Lifecycle declares no states.");
        if (States.Any(string.IsNullOrWhiteSpace)) errors.Add("Lifecycle has an empty state name.");
        if (declared.Count != States.Count) errors.Add("Lifecycle state names must be unique.");
        if (!declared.Contains(InitialState)) errors.Add($"Initial state '{InitialState}' is not a declared state.");
        else if (TerminalStates.Contains(InitialState)) errors.Add($"Initial state '{InitialState}' must not be terminal.");

        foreach (var t in TerminalStates.Where(t => !declared.Contains(t)))
            errors.Add($"Terminal state '{t}' is not a declared state.");

        foreach (var t in Transitions)
        {
            if (!declared.Contains(t.From)) errors.Add($"Transition {t.From}->{t.To}: '{t.From}' is not a declared state.");
            if (!declared.Contains(t.To)) errors.Add($"Transition {t.From}->{t.To}: '{t.To}' is not a declared state.");
            if (!(t.Weight > 0)) errors.Add($"Transition {t.From}->{t.To}: Weight must be > 0.");
        }

        foreach (var state in States.Distinct())
        {
            bool hasOut = Transitions.Any(t => t.From == state);
            if (TerminalStates.Contains(state) && hasOut) errors.Add($"Terminal state '{state}' must have no outgoing transition.");
            if (!TerminalStates.Contains(state) && !hasOut) errors.Add($"Non-terminal state '{state}' needs at least one outgoing transition.");
        }

        // Graph rules only make sense when the endpoints are sound.
        if (errors.Count > 0) return errors;

        var reachable = Reach(InitialState, t => t.From, t => t.To);
        foreach (var s in States.Where(s => !reachable.Contains(s)))
            errors.Add($"State '{s}' is not reachable from the initial state.");

        var reachesTerminal = new HashSet<string>(TerminalStates);
        bool grew = true;
        while (grew)
        {
            grew = false;
            foreach (var t in Transitions)
                if (reachesTerminal.Contains(t.To) && reachesTerminal.Add(t.From)) grew = true;
        }
        foreach (var s in States.Where(s => !reachesTerminal.Contains(s)))
            errors.Add($"State '{s}' cannot reach a terminal state.");

        if (HasCycle()) errors.Add("Lifecycle graph contains a cycle; every lifecycle must be finite.");
        return errors;
    }

    private HashSet<string> Reach(string start, Func<LifecycleTransition, string> from, Func<LifecycleTransition, string> to)
    {
        var seen = new HashSet<string> { start };
        var queue = new Queue<string>([start]);
        while (queue.Count > 0)
        {
            var cur = queue.Dequeue();
            foreach (var t in Transitions.Where(t => from(t) == cur))
                if (seen.Add(to(t))) queue.Enqueue(to(t));
        }
        return seen;
    }

    private bool HasCycle()
    {
        var indegree = States.Distinct().ToDictionary(s => s, _ => 0);
        foreach (var t in Transitions) indegree[t.To]++;
        var queue = new Queue<string>(indegree.Where(kv => kv.Value == 0).Select(kv => kv.Key));
        int visited = 0;
        while (queue.Count > 0)
        {
            var cur = queue.Dequeue();
            visited++;
            foreach (var t in Outgoing(cur))
                if (--indegree[t.To] == 0) queue.Enqueue(t.To);
        }
        return visited != indegree.Count;
    }
}
