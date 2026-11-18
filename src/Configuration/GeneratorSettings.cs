namespace FakeOrderGenerator.Configuration;

public sealed class GeneratorSettings
{
    public ConnectionSettings ConnectionStrings { get; set; } = new();
    public KafkaSettings Kafka { get; set; } = new();
    public GeneratorConfig Generator { get; set; } = new();
    public PublisherConfig Publisher { get; set; } = new();
    public ReportingConfig Reporting { get; set; } = new();
}

public sealed class ConnectionSettings { public string SqlServer { get; set; } = ""; }

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "orders";
    public string ClientId { get; set; } = "fake-order-generator";
    public string Acks { get; set; } = "All";
    public string CompressionType { get; set; } = "Lz4";
    public int LingerMs { get; set; } = 5;
    public int BatchSize { get; set; } = 262144;
    public int PartitionCount { get; set; } = 6;
}

public sealed class GeneratorConfig
{
    public bool Enabled { get; set; } = true;

    private int? _targetEventCount;

    /// <summary>Total number of events (not orders) in the Outbox at which generation stops.</summary>
    public int TargetEventCount
    {
        #pragma warning disable CS0618
        get => _targetEventCount ?? RecordCount ?? 500000;
#pragma warning restore CS0618
        set => _targetEventCount = value;
    }

    /// <summary>Obsolete alias of <see cref="TargetEventCount"/>; a startup warning is logged when it is set.</summary>
    [Obsolete("Use Generator:TargetEventCount.")]
    public int? RecordCount { get; set; }

    public int IntervalMilliseconds { get; set; } = 100;
    public int MaxActiveEntities { get; set; } = 10000;
    public double AdvanceProbability { get; set; } = 0.6;
    public int? RandomSeed { get; set; }

    /// <summary>Per-entity settings keyed by entity name (for example "Order").</summary>
    public Dictionary<string, EntityConfig> Entities { get; set; } = new();
    public int BatchSize { get; set; } = 5000;
    public decimal MinOrderAmount { get; set; } = 100;
    public decimal MaxOrderAmount { get; set; } = 5000000;
    public int CustomerCount { get; set; } = 100000;
    public int OrderHistoryDays { get; set; } = 730;
    public string NationalCodeTable { get; set; } = "dbo.NationalCodePrefixes";
}

public sealed class PublisherConfig
{
    public bool Enabled { get; set; } = true;
    public int BatchSize { get; set; } = 500;
    public int IdleDelayMilliseconds { get; set; } = 100;
    public int RetryDelayMilliseconds { get; set; } = 1000;
    public int MaxRetryCount { get; set; } = 10;
    public int LeaseDurationSeconds { get; set; } = 90;
    public int LeaseRenewIntervalSeconds { get; set; } = 20;
    public int MaxPartitionsPerInstance { get; set; } = 1;
}

public sealed class ReportingConfig
{
    public string Directory { get; set; } = "reports";
    public string Environment { get; set; } = System.Environment.MachineName;
    public int RegressionThresholdPercent { get; set; } = 20;
}

public sealed class EntityConfig
{
    public string PartitionKey { get; set; } = "CustomerId";

    /// <summary>Null means the entity definition's built-in default lifecycle.</summary>
    public LifecycleOptions? Lifecycle { get; set; }
}

public sealed class LifecycleOptions
{
    public string InitialState { get; set; } = "";
    public string[] States { get; set; } = [];
    public string[] TerminalStates { get; set; } = [];
    public TransitionOption[] Transitions { get; set; } = [];

    /// <summary>Default order lifecycle (contracts/configuration.md).</summary>
    public static LifecycleOptions OrderDefault() => new()
    {
        InitialState = "Created",
        States = ["Created", "Paid", "Shipped", "Delivered", "Cancelled"],
        TerminalStates = ["Delivered", "Cancelled"],
        Transitions =
        [
            new() { From = "Created", To = "Paid", Weight = 85 },
            new() { From = "Created", To = "Cancelled", Weight = 15 },
            new() { From = "Paid", To = "Shipped", Weight = 90 },
            new() { From = "Paid", To = "Cancelled", Weight = 10 },
            new() { From = "Shipped", To = "Delivered", Weight = 100 },
        ]
    };
}

public sealed class TransitionOption
{
    public string From { get; set; } = "";
    public string To { get; set; } = "";
    public double Weight { get; set; }
}
