using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOptions<ConnectionSettings>()
    .Bind(builder.Configuration.GetSection("ConnectionStrings"))
    .Validate(s => !string.IsNullOrWhiteSpace(s.SqlServer), "ConnectionStrings:SqlServer is required.")
    .ValidateOnStart();

builder.Services.AddOptions<KafkaSettings>()
    .Bind(builder.Configuration.GetSection("Kafka"))
    .Validate(s => !string.IsNullOrWhiteSpace(s.BootstrapServers) && !string.IsNullOrWhiteSpace(s.Topic), "Kafka bootstrap servers and topic are required.")
    .Validate(s => s.PartitionCount > 0 && s.BatchSize > 0 && s.LingerMs >= 0, "Kafka partition/batch/linger settings are invalid.")
    .ValidateOnStart();

builder.Services.AddOptions<PublisherConfig>()
    .Bind(builder.Configuration.GetSection("Publisher"))
    .ValidateOnStart();
builder.Services.AddOptions<GeneratorConfig>()
    .Bind(builder.Configuration.GetSection("Generator"))
    .Validate(s => s.RecordCount > 0 && s.BatchSize > 0 &&
                   s.CustomerCount > 0 && s.OrderHistoryDays > 0 &&
                   s.MinOrderAmount >= 0 && s.MaxOrderAmount > s.MinOrderAmount, "Generator settings are invalid.")
    .ValidateOnStart();

builder.Services.AddSingleton<SqlOrderRepository>();
builder.Services.AddHostedService<OrderGeneratorWorker>();
builder.Services.AddHostedService<KafkaPublisherWorker>();

await builder.Build().RunAsync();
