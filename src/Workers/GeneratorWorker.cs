using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Generation;
using Microsoft.Extensions.Options;

namespace FakeOrderGenerator.Workers;

/// <summary>
/// Generic generator over an <see cref="IFakeEntityDefinition"/>: validates the lifecycle first, resumes from SQL
/// (max sequence, existing event count, active entities) and writes batches until exactly TargetEventCount events exist.
/// </summary>
public sealed class GeneratorWorker : BackgroundService
{
    private readonly SqlOrderRepository _repository;
    private readonly IFakeEntityDefinition _definition;
    private readonly IEntityWriter _writer;
    private readonly GeneratorConfig _config;
    private readonly KafkaSettings _kafka;
    private readonly ILogger<GeneratorWorker> _logger;

    public GeneratorWorker(SqlOrderRepository repository, IFakeEntityDefinition definition, IEntityWriter writer,
        IOptions<GeneratorConfig> config, IOptions<KafkaSettings> kafka, ILogger<GeneratorWorker> logger)
    {
        _repository = repository;
        _definition = definition;
        _writer = writer;
        _config = config.Value;
        _kafka = kafka.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled) return;

        // Refuse to start before any write when the lifecycle is invalid (FR-023).
        var errors = _definition.Lifecycle.Validate();
        if (errors.Count > 0)
            throw new InvalidOperationException("Invalid lifecycle for " + _definition.Name + ": " + string.Join(" ", errors));

        long maxSequence = await _repository.GetMaxSequenceAsync(stoppingToken);
        long existing = await _repository.CountOutboxAsync(stoppingToken);
        var active = await _writer.LoadActiveAsync(stoppingToken);
        var random = _config.RandomSeed is int seed ? new Random(seed) : new Random();
        var builder = new EventBatchBuilder(_definition, _config, _kafka.PartitionCount, random, maxSequence, existing, active);

        _logger.LogInformation(
            "Generator starting: target={Target} events, existing={Existing}, max sequence={Max}, active {Entity} entities={Active}",
            _config.TargetEventCount, existing, maxSequence, _definition.Name, builder.ActiveCount);

        long generatedThisRun = 0;
        int created = 0;
        try
        {
            while (!builder.TargetReached && !stoppingToken.IsCancellationRequested)
            {
                var batch = builder.Build(DateTime.UtcNow);
                if (batch.Events.Count == 0) break;
                await _repository.WriteBatchAsync(_writer, batch, stoppingToken);
                generatedThisRun += batch.Events.Count;
                created += batch.NewEntities.Count;
                _logger.LogInformation("Generated {Count} events ({New} new); total events {Total}/{Target}",
                    batch.Events.Count, batch.NewEntities.Count, builder.TotalEvents, _config.TargetEventCount);

                if (_config.IntervalMilliseconds > 0 && !builder.TargetReached)
                    await Task.Delay(_config.IntervalMilliseconds, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            _logger.LogInformation(
                "Generator stopped: {Entity} entities created this run={Created}, events this run={Run}, events total={Total}, incomplete lifecycles (active)={Incomplete}",
                _definition.Name, created, generatedThisRun, builder.TotalEvents, builder.ActiveCount);
        }
    }
}
