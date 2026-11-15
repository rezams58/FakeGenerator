using FakeOrderGenerator.Data;

namespace FakeOrderGenerator.Workers;

public sealed class OrderGeneratorWorker : BackgroundService
{
    private readonly SqlOrderRepository _repository;
    private readonly IConfiguration _configuration;
    private readonly ILogger<OrderGeneratorWorker> _logger;

    public OrderGeneratorWorker(
        SqlOrderRepository repository,
        IConfiguration configuration,
        ILogger<OrderGeneratorWorker> logger)
    {
        _repository = repository;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_configuration.GetValue("Generator:Enabled", true)) return;
        int totalOrders = _configuration.GetValue<int>("Generator:RecordCount", _configuration.GetValue<int>("Generator:TotalOrders", 500000));
        int batchSize = _configuration.GetValue<int>("Generator:BatchSize", 5000);
        int interval = _configuration.GetValue<int>("Generator:IntervalMilliseconds", 100);

        // Resume sequence from the existing Outbox.
        long sequence = await _repository.GetMaxSequenceAsync(stoppingToken);

        _logger.LogInformation("Starting generation from sequence {Sequence}", sequence);

        while (sequence < totalOrders &&
               !stoppingToken.IsCancellationRequested)
        {
            int count = (int)Math.Min(batchSize, totalOrders - sequence);

            var now = DateTime.UtcNow;

            var orders = new List<Order>(count);

            for (int i = 0; i < count; i++)
            {
                long next = sequence + i + 1;

                string customerId = $"CUS-{Random.Shared.Next(1, 100001):D6}";

                orders.Add(new Order(Guid.NewGuid(),
                                     next,
                                     $"ORD-{next:D10}",
                                      customerId,
                                      Math.Round((decimal)Random.Shared.NextDouble() * 5000m, 2),
                                      now));
            }

            int partitionCount = _configuration.GetValue<int>("Kafka:PartitionCount", 6);
            await _repository.InsertOrdersAsync(orders, partitionCount, stoppingToken);

            sequence += count;

            _logger.LogInformation("Generated {Count}; total sequence {Sequence}", count, sequence);

            if (interval > 0)
            {
                await Task.Delay(interval, stoppingToken);
            }
        }

        _logger.LogInformation("Order generation completed.");
    }
}
