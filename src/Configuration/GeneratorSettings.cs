namespace FakeOrderGenerator.Configuration;

public sealed class GeneratorSettings
{
    public ConnectionSettings ConnectionStrings { get; set; } = new();
    public KafkaSettings Kafka { get; set; } = new();
    public GeneratorConfig Generator { get; set; } = new();
    public PublisherConfig Publisher { get; set; } = new();
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
    public int RecordCount { get; set; } = 50000;
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
