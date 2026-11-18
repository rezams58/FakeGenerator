using Confluent.Kafka;
using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Kafka;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace FakeOrderGenerator.Workers;

public sealed class KafkaPublisherWorker : BackgroundService
{
    private readonly KafkaSettings _kafkaSetting;
    private readonly PublisherConfig _publisherSetting;
    private readonly IOutboxRepository _repository;
    private readonly IPartitionPublisherFactory _publisherFactory;
    private readonly ITopicValidator _topicValidator;
    private readonly ILogger<KafkaPublisherWorker> _logger;
    private readonly Guid _ownerId = Guid.NewGuid();
    private readonly int _batchSize, _idleDelay, _retryDelay, _maxRetryCount;
    private readonly int _leaseSeconds, _renewSeconds, _maxPartitions;
    private readonly bool _enabled;

    public KafkaPublisherWorker(IOutboxRepository repository,
                                IPartitionPublisherFactory publisherFactory,
                                ITopicValidator topicValidator,
                                IOptions<KafkaSettings> kafkaOption,
                                IOptions<PublisherConfig> PublisherOption,
                                ILogger<KafkaPublisherWorker> logger)
    {
        _repository = repository;
        _publisherFactory = publisherFactory;
        _topicValidator = topicValidator;
        _publisherSetting = PublisherOption.Value;
        _logger = logger;
        _kafkaSetting = kafkaOption.Value;
        _enabled = _publisherSetting.Enabled;
        _batchSize = _publisherSetting.BatchSize;
        _idleDelay = _publisherSetting.IdleDelayMilliseconds;
        _retryDelay = _publisherSetting.RetryDelayMilliseconds;
        _maxRetryCount = _publisherSetting.MaxRetryCount;
        _leaseSeconds = _publisherSetting.LeaseDurationSeconds;
        _renewSeconds = _publisherSetting.LeaseRenewIntervalSeconds;
        _maxPartitions = _publisherSetting.MaxPartitionsPerInstance;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled) return;
        // Fail fast before any message is read or sent. Extra partitions are allowed; only 0..N-1 are used.
        // A validation failure propagates: the host stops and no message is read or sent.
        _topicValidator.Validate(_kafkaSetting.BootstrapServers, _kafkaSetting.Topic, _kafkaSetting.PartitionCount);
        await _repository.EnsureLeasePartitionsAsync(_kafkaSetting.PartitionCount, stoppingToken);
        await _repository.ValidateLeasePartitionsAsync(_kafkaSetting.PartitionCount, stoppingToken);
        _logger.LogInformation("Publisher {OwnerId} started for topic {Topic}; expected partitions={Count}",
                                _ownerId, _kafkaSetting.Topic, _kafkaSetting.PartitionCount);

        var lanes = new Dictionary<int, Task>();
        using var lanesCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var completed in lanes.Where(l => l.Value.IsCompleted).ToArray())
                {
                    await completed.Value;
                    lanes.Remove(completed.Key);
                }

                int capacity = _maxPartitions - lanes.Count;
                if (capacity > 0)
                {
                    var leases = await _repository.ClaimPartitionsAsync(_ownerId, capacity, _leaseSeconds, stoppingToken);
                    foreach (var lease in leases)
                        lanes.Add(lease.PartitionId, RunPartitionAsync(lease, lanesCts.Token));
                }

                // Keep checking spare capacity while existing lanes remain active.
                // A healthy lane must not prevent taking over another owner's expired partition.
                await Task.Delay(Math.Max(_idleDelay, 500), stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal host shutdown; every active lane observes the same cancellation.
        }
        finally
        {
            lanesCts.Cancel();
            await Task.WhenAll(lanes.Values);
        }
    }

    private async Task RunPartitionAsync(PartitionLease lease, CancellationToken stoppingToken)
    {
        using var laneCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeat = RenewLeaseLoopAsync(lease, laneCts);
        try
        {
            // InitTransactions uses a stable transactional.id per topic/partition. A takeover fences the old producer.
            using var publisher = _publisherFactory.Create(lease.PartitionId);
            _logger.LogInformation("Acquired partition {Partition} with fencing token {Token}",
                lease.PartitionId, lease.FencingToken);

            while (!laneCts.IsCancellationRequested)
            {
                var batch = await _repository.ReadPendingAsync(lease, _batchSize,
                    _maxRetryCount, laneCts.Token);
                if (batch.Count == 0)
                {
                    await Task.Delay(_idleDelay, laneCts.Token);
                    continue;
                }

                try
                {
                    await publisher.PublishBatchAsync(batch, laneCts.Token);
                }
                catch (OperationCanceledException) when (laneCts.IsCancellationRequested) { break; }
                catch (KafkaPublishBatchException ex)
                {
                    // The failed batch transaction was aborted. Re-publish only its successful prefix in a
                    // fresh transaction so a later poison event never causes an earlier event to be dead-lettered.
                    var prefix = batch.TakeWhile(x => x.SequenceNo < ex.FailedMessage.SequenceNo).ToList();
                    if (prefix.Count > 0)
                    {
                        await publisher.PublishBatchAsync(prefix, laneCts.Token);
                        int prefixUpdated = await _repository.MarkPublishedAsync(prefix.Select(x => x.EventId), lease, laneCts.Token);
                        if (prefixUpdated != prefix.Count)
                            throw new InvalidOperationException("Lease lost while marking the successfully replayed prefix.");
                    }

                    bool recorded = await _repository.MarkFailedAsync(ex.FailedMessage, lease, RetryPolicy.FormatError(ex),
                        _maxRetryCount, stoppingToken);
                    _logger.LogError(ex,
                        "Kafka delivery failed: partition {Partition} blocked at Sequence {Sequence}; failure recorded={Recorded}",
                        lease.PartitionId, ex.FailedMessage.SequenceNo, recorded);
                    if (!recorded) break;
                    await Task.Delay(_retryDelay, laneCts.Token);
                    continue;
                }

                // Keep this outside the Kafka-send catch: Kafka may already have committed. A SQL failure here
                // must cause replay/deduplication, not incorrectly increment RetryCount for an acknowledged event.
                int updated = await _repository.MarkPublishedAsync(batch.Select(x => x.EventId), lease, laneCts.Token);
                if (updated != batch.Count)
                    throw new InvalidOperationException($"Lease lost or outbox state changed while marking partition {lease.PartitionId}; expected {batch.Count}, updated {updated}. Kafka transaction was committed; replay is possible and consumers must deduplicate EventId.");
                _logger.LogInformation("Partition {Partition}: transaction committed and {Count} rows marked published; last sequence {Sequence}",
                    lease.PartitionId, updated, batch[^1].SequenceNo);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested || laneCts.IsCancellationRequested)
        {
            // Normal shutdown or lease loss.
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "Kafka error: producer for partition {Partition} failed/fenced; lane will release its lease", lease.PartitionId);
        }
        catch (SqlException ex)
        {
            _logger.LogError(ex, "SQL error: partition {Partition} lane stopped. If a Kafka commit preceded it, the batch is replayed and consumers deduplicate EventId.", lease.PartitionId);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogError(ex, "Partition {Partition} lane stopped unexpectedly. Unconfirmed Kafka outcomes will be replayed and deduplicated by EventId.", lease.PartitionId);
        }
        finally
        {
            laneCts.Cancel();
            try { await heartbeat; } catch (OperationCanceledException) { }
            try { await _repository.ReleaseLeaseAsync(lease, CancellationToken.None); }
            catch (Exception ex) { _logger.LogWarning(ex, "Could not release lease for partition {Partition}; it will expire", lease.PartitionId); }
        }
    }

    private async Task RenewLeaseLoopAsync(PartitionLease lease, CancellationTokenSource laneCts)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_renewSeconds));
        try
        {
            while (await timer.WaitForNextTickAsync(laneCts.Token))
            {
                if (!await _repository.RenewLeaseAsync(lease, _leaseSeconds, laneCts.Token))
                {
                    _logger.LogCritical("Lost lease for partition {Partition} (fencing token {Token}); stopping this producer immediately",
                        lease.PartitionId, lease.FencingToken);
                    laneCts.Cancel();
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (laneCts.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Lease heartbeat failed for partition {Partition}; stopping its producer lane", lease.PartitionId);
            laneCts.Cancel();
        }
    }
}
