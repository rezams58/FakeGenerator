using FakeOrderGenerator.Configuration;

namespace FakeGenerator.Tests.Unit;

public sealed class OptionsValidatorTests
{
    private static void Invalid<T>(Func<T, IReadOnlyList<string>> check, Action<T> breakIt, string expected) where T : new()
    {
        var o = new T();
        breakIt(o);
        Assert.Contains(check(o), m => m.Contains(expected, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Defaults_are_valid()
    {
        Assert.Empty(OptionsValidators.Check(new KafkaSettings()));
        Assert.Empty(OptionsValidators.Check(new GeneratorConfig()));
        Assert.Empty(OptionsValidators.Check(new PublisherConfig()));
        Assert.Empty(OptionsValidators.Check(new ConnectionSettings { SqlServer = "x" }));
        Assert.Empty(OptionsValidators.Check(new ReportingConfig { Directory = Path.Combine(Path.GetTempPath(), "fakegen-rep-test") }));
    }

    [Fact] public void Connection_sql_required() => Invalid<ConnectionSettings>(OptionsValidators.Check, o => o.SqlServer = " ", "SqlServer");

    [Fact] public void Kafka_bootstrap_required() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.BootstrapServers = "", "BootstrapServers");
    [Fact] public void Kafka_topic_required() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.Topic = "", "Topic");
    [Fact] public void Kafka_clientid_required() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.ClientId = "", "ClientId");
    [Fact] public void Kafka_partitions_at_least_six() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.PartitionCount = 5, "PartitionCount");
    [Fact] public void Kafka_acks_must_be_all() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.Acks = "Leader", "Acks");
    [Fact] public void Kafka_compression_must_parse() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.CompressionType = "nope", "CompressionType");
    [Fact] public void Kafka_linger_non_negative() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.LingerMs = -1, "LingerMs");
    [Fact] public void Kafka_batch_positive() => Invalid<KafkaSettings>(OptionsValidators.Check, o => o.BatchSize = 0, "BatchSize");

    [Fact] public void Generator_target_positive() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.TargetEventCount = 0, "TargetEventCount");
    [Fact] public void Generator_batch_positive() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.BatchSize = 0, "BatchSize");
    [Fact] public void Generator_interval_non_negative() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.IntervalMilliseconds = -1, "IntervalMilliseconds");
    [Fact] public void Generator_max_active_positive() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.MaxActiveEntities = 0, "MaxActiveEntities");
    [Fact] public void Generator_amount_range() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => { o.MinOrderAmount = 10; o.MaxOrderAmount = 5; }, "MinOrderAmount");
    [Fact] public void Generator_min_amount_positive() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.MinOrderAmount = 0, "MinOrderAmount");
    [Fact] public void Generator_customers_positive() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.CustomerCount = 0, "CustomerCount");
    [Fact] public void Generator_history_non_negative() => Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.OrderHistoryDays = -1, "OrderHistoryDays");

    [Theory]
    [InlineData(0.0)]
    [InlineData(1.0)]
    public void Generator_advance_probability_open_interval(double p) =>
        Invalid<GeneratorConfig>(OptionsValidators.Check, o => o.AdvanceProbability = p, "AdvanceProbability");

    [Fact]
    public void Generator_obsolete_record_count_acts_as_target()
    {
#pragma warning disable CS0618
        var g = new GeneratorConfig { RecordCount = 1234 };
#pragma warning restore CS0618
        Assert.Equal(1234, g.TargetEventCount);
        g.TargetEventCount = 99;
        Assert.Equal(99, g.TargetEventCount);
    }

    [Fact] public void Publisher_batch_positive() => Invalid<PublisherConfig>(OptionsValidators.Check, o => o.BatchSize = 0, "BatchSize");
    [Fact] public void Publisher_idle_non_negative() => Invalid<PublisherConfig>(OptionsValidators.Check, o => o.IdleDelayMilliseconds = -1, "IdleDelayMilliseconds");
    [Fact] public void Publisher_retry_delay_non_negative() => Invalid<PublisherConfig>(OptionsValidators.Check, o => o.RetryDelayMilliseconds = -1, "RetryDelayMilliseconds");
    [Fact] public void Publisher_max_retry_at_least_one() => Invalid<PublisherConfig>(OptionsValidators.Check, o => o.MaxRetryCount = 0, "MaxRetryCount");
    [Fact] public void Publisher_max_partitions_at_least_one() => Invalid<PublisherConfig>(OptionsValidators.Check, o => o.MaxPartitionsPerInstance = 0, "MaxPartitionsPerInstance");
    [Fact] public void Publisher_renew_must_be_positive() => Invalid<PublisherConfig>(OptionsValidators.Check, o => o.LeaseRenewIntervalSeconds = 0, "LeaseRenewIntervalSeconds");
    [Fact] public void Publisher_renew_below_duration() => Invalid<PublisherConfig>(OptionsValidators.Check, o => { o.LeaseRenewIntervalSeconds = 90; o.LeaseDurationSeconds = 90; }, "LeaseRenewIntervalSeconds");

    [Fact]
    public void Reporting_threshold_positive() =>
        Invalid<ReportingConfig>(OptionsValidators.Check, o => { o.Directory = Path.GetTempPath(); o.RegressionThresholdPercent = 0; }, "RegressionThresholdPercent");

    [Fact]
    public void Reporting_directory_must_be_writable() =>
        Invalid<ReportingConfig>(OptionsValidators.Check, o => o.Directory = "bad\0dir", "Directory");
}
