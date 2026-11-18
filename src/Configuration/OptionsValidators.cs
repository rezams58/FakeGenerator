using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace FakeOrderGenerator.Configuration;

/// <summary>
/// Every rule of specs/001-fake-data-kafka-publishing/contracts/configuration.md.
/// Each method returns the list of violations (empty when valid) so rules are unit-testable.
/// </summary>
public static class OptionsValidators
{
    public static IReadOnlyList<string> Check(ConnectionSettings s)
    {
        var e = new List<string>();
        if (string.IsNullOrWhiteSpace(s.SqlServer)) e.Add("ConnectionStrings:SqlServer is required.");
        return e;
    }

    public static IReadOnlyList<string> Check(KafkaSettings s)
    {
        var e = new List<string>();
        if (string.IsNullOrWhiteSpace(s.BootstrapServers)) e.Add("Kafka:BootstrapServers is required.");
        if (string.IsNullOrWhiteSpace(s.Topic)) e.Add("Kafka:Topic is required.");
        if (string.IsNullOrWhiteSpace(s.ClientId)) e.Add("Kafka:ClientId is required.");
        if (s.PartitionCount < 6) e.Add("Kafka:PartitionCount must be >= 6.");
        if (!string.Equals(s.Acks, "All", StringComparison.OrdinalIgnoreCase)) e.Add("Kafka:Acks must be 'All'.");
        if (!Enum.TryParse<CompressionType>(s.CompressionType, true, out _)) e.Add("Kafka:CompressionType is not a valid compression type.");
        if (s.LingerMs < 0) e.Add("Kafka:LingerMs must be >= 0.");
        if (s.BatchSize <= 0) e.Add("Kafka:BatchSize must be > 0.");
        return e;
    }

    public static IReadOnlyList<string> Check(GeneratorConfig s)
    {
        var e = new List<string>();
        if (s.TargetEventCount <= 0) e.Add("Generator:TargetEventCount must be > 0.");
        if (s.BatchSize <= 0) e.Add("Generator:BatchSize must be > 0.");
        if (s.IntervalMilliseconds < 0) e.Add("Generator:IntervalMilliseconds must be >= 0.");
        if (s.MaxActiveEntities <= 0) e.Add("Generator:MaxActiveEntities must be > 0.");
        if (!(s.MinOrderAmount > 0 && s.MinOrderAmount <= s.MaxOrderAmount)) e.Add("Generator: 0 < MinOrderAmount <= MaxOrderAmount is required.");
        if (s.CustomerCount <= 0) e.Add("Generator:CustomerCount must be > 0.");
        if (s.OrderHistoryDays < 0) e.Add("Generator:OrderHistoryDays must be >= 0.");
        if (!(s.AdvanceProbability > 0 && s.AdvanceProbability < 1)) e.Add("Generator:AdvanceProbability must satisfy 0 < p < 1.");
        return e;
    }

    /// <summary>Entity-level rules: the Order partition key and the Order lifecycle graph (data-model.md section 4).</summary>
    public static IReadOnlyList<string> CheckEntities(GeneratorConfig s)
    {
        s.Entities.TryGetValue(Generation.Order.OrderDefinition.EntityName, out var order);
        var errors = new List<string>(Generation.Order.OrderDefinition.ValidateConfig(order));
        errors.AddRange(new Generation.Order.OrderDefinition(s, order).Lifecycle.Validate().Select(m => "Generator:Entities:Order:Lifecycle: " + m));
        return errors;
    }

    public static IReadOnlyList<string> Check(PublisherConfig s)
    {
        var e = new List<string>();
        if (s.BatchSize <= 0) e.Add("Publisher:BatchSize must be > 0.");
        if (s.IdleDelayMilliseconds < 0) e.Add("Publisher:IdleDelayMilliseconds must be >= 0.");
        if (s.RetryDelayMilliseconds < 0) e.Add("Publisher:RetryDelayMilliseconds must be >= 0.");
        if (s.MaxRetryCount < 1) e.Add("Publisher:MaxRetryCount must be >= 1.");
        if (s.MaxPartitionsPerInstance < 1) e.Add("Publisher:MaxPartitionsPerInstance must be >= 1.");
        if (!(s.LeaseRenewIntervalSeconds > 0 && s.LeaseRenewIntervalSeconds < s.LeaseDurationSeconds))
            e.Add("Publisher: 0 < LeaseRenewIntervalSeconds < LeaseDurationSeconds is required.");
        return e;
    }

    public static IReadOnlyList<string> Check(ReportingConfig s)
    {
        var e = new List<string>();
        if (s.RegressionThresholdPercent <= 0) e.Add("Reporting:RegressionThresholdPercent must be > 0.");
        if (string.IsNullOrWhiteSpace(s.Environment)) e.Add("Reporting:Environment must not be empty.");
        if (!IsWritable(s.Directory)) e.Add($"Reporting:Directory '{s.Directory}' is not writable.");
        return e;
    }

    private static bool IsWritable(string? directory)
    {
        if (string.IsNullOrWhiteSpace(directory)) return false;
        try
        {
            Directory.CreateDirectory(directory);
            var probe = Path.Combine(directory, $".write-test-{Guid.NewGuid():N}");
            File.WriteAllText(probe, "");
            File.Delete(probe);
            return true;
        }
        catch { return false; }
    }

    /// <summary>Adapts a rule set to <see cref="ValidateOptionsResult"/>.</summary>
    public static ValidateOptionsResult ToResult(IReadOnlyList<string> errors) =>
        errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);

    public sealed class Connection : IValidateOptions<ConnectionSettings>
    { public ValidateOptionsResult Validate(string? n, ConnectionSettings o) => ToResult(Check(o)); }
    public sealed class Kafka : IValidateOptions<KafkaSettings>
    { public ValidateOptionsResult Validate(string? n, KafkaSettings o) => ToResult(Check(o)); }
    public sealed class Generator : IValidateOptions<GeneratorConfig>
    { public ValidateOptionsResult Validate(string? n, GeneratorConfig o) => ToResult(Check(o).Concat(CheckEntities(o)).ToList()); }
    public sealed class Publisher : IValidateOptions<PublisherConfig>
    { public ValidateOptionsResult Validate(string? n, PublisherConfig o) => ToResult(Check(o)); }
    public sealed class Reporting : IValidateOptions<ReportingConfig>
    { public ValidateOptionsResult Validate(string? n, ReportingConfig o) => ToResult(Check(o)); }
}
